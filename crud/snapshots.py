# crud/snapshots.py
from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models


# ---------- Writers ----------

def create_snapshot_for_store(db: Session, store_id: int) -> None:
    """
    Upsert one inventory snapshot row per variant for the given store at 'now()'.
    Uses inventory_levels (authoritative location from stores.sync_location_id if present)
    or sums any levels for that variant if sync_location_id is NULL.
    Persists: date, product_variant_id, store_id, on_hand, price, cost_per_item.
    """
    now = datetime.now(timezone.utc)

    # Load variants for the store with product and current price/cost
    variants: List[models.ProductVariant] = (
        db.query(models.ProductVariant)
        .options(
            joinedload(models.ProductVariant.product),
        )
        .filter(models.ProductVariant.store_id == store_id)
        .all()
    )

    # Map of variant_id -> on_hand
    # Prefer on_hand at the configured sync location if set; else sum across locations.
    sync_loc_id = (
        db.query(models.Store.sync_location_id)
        .filter(models.Store.id == store_id)
        .scalar()
    )

    # Build on-hand snapshot source using a single SQL for performance
    if sync_loc_id:
        onhand_rows = db.execute(
            text(
                """
                SELECT il.variant_id, COALESCE(il.on_hand, 0) AS on_hand
                FROM inventory_levels il
                WHERE il.location_id = :loc_id
                """
            ),
            {"loc_id": int(sync_loc_id)},
        ).fetchall()
    else:
        onhand_rows = db.execute(
            text(
                """
                SELECT il.variant_id, COALESCE(SUM(il.on_hand), 0) AS on_hand
                FROM inventory_levels il
                GROUP BY il.variant_id
                """
            )
        ).fetchall()

    onhand_by_variant = {int(r[0]): int(r[1]) for r in onhand_rows}

    # Prepare rows
    rows: List[Dict[str, Any]] = []
    for v in variants:
        rows.append(
            dict(
                date=now,
                product_variant_id=int(v.id),
                store_id=int(store_id),
                on_hand=int(onhand_by_variant.get(int(v.id), 0)),
                price=v.price,
                cost_per_item=v.cost_per_item,
            )
        )

    if not rows:
        return

    # Upsert into inventory_snapshots on unique (date, product_variant_id, store_id)
    stmt = pg_insert(models.InventorySnapshot.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "product_variant_id", "store_id"],
        set_={
            "on_hand": stmt.excluded.on_hand,
            "price": stmt.excluded.price,
            "cost_per_item": stmt.excluded.cost_per_item,
        },
    )
    db.execute(stmt)
    db.commit()


# ---------- Readers ----------

def get_snapshots_with_metrics(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Returns latest snapshot per (variant, store) within the window, plus metrics computed
    over all snapshots in the same window for that (variant, store).

    Output shape matches serialize_snapshot() and includes a 'metrics' dict.
    """

    # Compute metrics over the requested window. Latest row carries 'on_hand' and 'date'.
    # Metrics are per (variant, store) across the window.
    sql = text(
        """
WITH filtered AS (
    SELECT s.*
    FROM inventory_snapshots s
    WHERE (:store_id::int IS NULL OR s.store_id = :store_id::int)
      AND (:start_ts IS NULL OR s.date >= :start_ts)
      AND (:end_ts   IS NULL OR s.date <= :end_ts)
),
-- Latest snapshot per variant/store in the range
latest AS (
    SELECT DISTINCT ON (product_variant_id, store_id)
           id, date, product_variant_id, store_id, on_hand, price, cost_per_item
    FROM filtered
    ORDER BY product_variant_id, store_id, date DESC
),
series AS (
    SELECT
        f.product_variant_id,
        f.store_id,
        f.date,
        f.on_hand,
        f.price,
        f.cost_per_item,
        LAG(f.on_hand) OVER (PARTITION BY f.product_variant_id, f.store_id ORDER BY f.date) AS prev_on_hand
    FROM filtered f
),
agg AS (
    SELECT
        product_variant_id,
        store_id,
        AVG(on_hand)::numeric        AS average_stock_level,
        MIN(on_hand)::numeric        AS min_stock_level,
        MAX(on_hand)::numeric        AS max_stock_level,
        (MAX(on_hand) - MIN(on_hand))::numeric AS stock_range,
        STDDEV_POP(on_hand)::numeric AS stock_stddev,
        SUM(CASE WHEN on_hand = 0 THEN 1 ELSE 0 END)::numeric AS days_out_of_stock,
        100.0 * SUM(CASE WHEN on_hand = 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) AS stockout_rate,
        SUM(GREATEST(COALESCE(prev_on_hand, on_hand) - on_hand, 0))::numeric AS total_outflow,
        COUNT(*)::numeric AS obs_count
    FROM series
    GROUP BY product_variant_id, store_id
),
derived AS (
    SELECT
        a.product_variant_id,
        a.store_id,
        a.average_stock_level,
        a.min_stock_level,
        a.max_stock_level,
        a.stock_range,
        a.stock_stddev,
        a.days_out_of_stock,
        a.stockout_rate,
        a.total_outflow,
        a.obs_count,
        -- Average daily outflow
        CASE WHEN a.obs_count > 0 THEN a.total_outflow / a.obs_count ELSE NULL END AS avg_daily_outflow,
        -- Stock turnover = total_outflow / average_stock_level
        CASE WHEN a.average_stock_level > 0 THEN a.total_outflow / a.average_stock_level ELSE NULL END AS stock_turnover,
        -- Avg days in inventory = 365 / turnover
        CASE WHEN a.total_outflow > 0 AND a.average_stock_level > 0
             THEN 365.0 / (a.total_outflow / a.average_stock_level)
             ELSE NULL
        END AS avg_days_in_inventory
    FROM agg a
),
dead_stock AS (
    -- Count days where on_hand <= 10% of average across the window
    SELECT
        s.product_variant_id,
        s.store_id,
        SUM(CASE WHEN s.on_hand <= COALESCE(d.average_stock_level,0) * 0.10 THEN 1 ELSE 0 END)::numeric AS dead_stock_days
    FROM series s
    JOIN derived d USING (product_variant_id, store_id)
    GROUP BY s.product_variant_id, s.store_id
),
metrics AS (
    SELECT
        d.product_variant_id,
        d.store_id,
        d.average_stock_level,
        d.min_stock_level,
        d.max_stock_level,
        d.stock_range,
        d.stock_stddev,
        d.days_out_of_stock,
        d.stockout_rate,
        d.total_outflow,
        d.stock_turnover,
        d.avg_days_in_inventory,
        ds.dead_stock_days,
        CASE WHEN d.obs_count > 0 THEN 100.0 * ds.dead_stock_days / d.obs_count ELSE NULL END AS dead_stock_ratio,
        -- Average inventory value using average_stock_level * avg price observed (fallback to latest price if null)
        (d.average_stock_level * (
            SELECT COALESCE(AVG(NULLIF(s.price,0)), MAX(s.price))::numeric
            FROM filtered s
            WHERE s.product_variant_id = d.product_variant_id AND s.store_id = d.store_id
        ))::numeric AS avg_inventory_value,
        -- Simple stock health index in [0,1]: high when low stockout and low dead-stock. Clamp to [0,1].
        GREATEST(0, LEAST(1,
            1
            - COALESCE(d.stockout_rate, 0) / 100.0
            - COALESCE( (SELECT 1.0 * ds2.dead_stock_days / NULLIF(d.obs_count,0) FROM dead_stock ds2
                         WHERE ds2.product_variant_id=d.product_variant_id AND ds2.store_id=d.store_id), 0)
        ))::numeric AS stock_health_index
    FROM derived d
    JOIN dead_stock ds USING (product_variant_id, store_id)
)
SELECT
    l.id,
    l.date,
    l.product_variant_id,
    l.store_id,
    l.on_hand,
    l.price,
    l.cost_per_item,

    m.average_stock_level,
    m.min_stock_level,
    m.max_stock_level,
    m.stock_range,
    m.stock_stddev,
    m.days_out_of_stock,
    m.stockout_rate,
    m.total_outflow,
    m.stock_turnover,
    m.avg_days_in_inventory,
    m.dead_stock_days,
    m.dead_stock_ratio,
    m.avg_inventory_value,
    m.stock_health_index
FROM latest l
LEFT JOIN metrics m
  ON m.product_variant_id = l.product_variant_id AND m.store_id = l.store_id
ORDER BY l.date DESC, l.product_variant_id
LIMIT :cap
        """
    )

    params: Dict[str, Any] = {
        "store_id": store_id,
        "start_ts": None if start_date is None else datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc),
        "end_ts": None if end_date is None else datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc),
        # pull a large window and let the route paginate after filters
        "cap": max(10000, limit),
    }

    rows = db.execute(sql, params).fetchall()
    if not rows:
        return {"snapshots": [], "total_count": 0}

    # Fetch related variants and products in one go
    variant_ids: List[int] = [int(r["product_variant_id"]) for r in rows]
    variants: List[models.ProductVariant] = (
        db.query(models.ProductVariant)
        .options(joinedload(models.ProductVariant.product))
        .filter(models.ProductVariant.id.in_(variant_ids))
        .all()
    )
    vmap: Dict[int, Tuple[Optional[models.ProductVariant], Optional[models.Product]]] = {}
    for v in variants:
        vmap[int(v.id)] = (v, getattr(v, "product", None))

    # Build output
    out: List[Dict[str, Any]] = []
    for r in rows:
        vid = int(r["product_variant_id"])
        v, p = vmap.get(vid, (None, None))
        out.append(
            {
                "id": int(r["id"]) if r["id"] is not None else None,
                "date": r["date"],
                "store_id": int(r["store_id"]),
                "product_variant_id": vid,
                "on_hand": int(r["on_hand"]) if r["on_hand"] is not None else None,
                "product_variant": {
                    "id": int(v.id) if v else None,
                    "shopify_gid": getattr(v, "shopify_gid", None) if v else None,
                    "sku": getattr(v, "sku", None) if v else None,
                    "product": {
                        "id": int(p.id) if p else None,
                        "title": getattr(p, "title", None) if p else None,
                        "image_url": getattr(p, "image_url", None) if p else None,
                    } if p else None,
                } if v else None,
                "metrics": {
                    "average_stock_level": r["average_stock_level"],
                    "min_stock_level": r["min_stock_level"],
                    "max_stock_level": r["max_stock_level"],
                    "stock_range": r["stock_range"],
                    "stock_stddev": r["stock_stddev"],
                    "days_out_of_stock": r["days_out_of_stock"],
                    "stockout_rate": r["stockout_rate"],
                    "replenishment_days": None,  # not computed in this model
                    "depletion_days": None,      # left for UI or later improvement
                    "total_outflow": r["total_outflow"],
                    "stock_turnover": r["stock_turnover"],
                    "avg_days_in_inventory": r["avg_days_in_inventory"],
                    "dead_stock_days": r["dead_stock_days"],
                    "dead_stock_ratio": r["dead_stock_ratio"],
                    "avg_inventory_value": r["avg_inventory_value"],
                    "stock_health_index": r["stock_health_index"],
                },
            }
        )

    return {"snapshots": out, "total_count": len(out)}
