from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models

# ---------- Writers ----------

def create_snapshot_for_store(db: Session, store_id: int) -> None:
    """
    Upsert one inventory snapshot row per variant for the given store at normalized day.
    Allows negative inventory. Uses stores.sync_location_id if set, else sums across locations.
    """
    now = datetime.now(timezone.utc)
    # normalize to midnight UTC to keep one row/day per variant+store
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # on_hand by variant
    sync_loc_id = db.query(models.Store.sync_location_id).filter(models.Store.id == store_id).scalar()
    if sync_loc_id:
        onhand_rows = db.execute(
            text("""
                -- CHANGE 1: Use il.available instead of il.on_hand
                SELECT pv.id AS variant_id, COALESCE(il.available, 0) AS on_hand
                FROM product_variants pv
                LEFT JOIN inventory_levels il
                  ON il.variant_id = pv.id AND il.location_id = :loc_id
                WHERE pv.store_id = :store_id
            """),
            {"loc_id": int(sync_loc_id), "store_id": int(store_id)},
        ).fetchall()
    else:
        onhand_rows = db.execute(
            text("""
                -- CHANGE 2: Sum il.available instead of il.on_hand
                SELECT pv.id AS variant_id, COALESCE(SUM(il.available), 0) AS on_hand
                FROM product_variants pv
                LEFT JOIN inventory_levels il
                  ON il.variant_id = pv.id
                WHERE pv.store_id = :store_id
                GROUP BY pv.id
            """),
            {"store_id": int(store_id)},
        ).fetchall()

    onhand_by_variant = {int(r.variant_id): int(r.on_hand) for r in onhand_rows}

    # price/cost per variant
    vrows = db.execute(
        text("""
            SELECT id AS variant_id, price, cost_per_item
            FROM product_variants
            WHERE store_id = :store_id
        """),
        {"store_id": int(store_id)},
    ).fetchall()
    pc_map = {int(r.variant_id): (r.price, r.cost_per_item) for r in vrows}

    rows: List[Dict[str, Any]] = []
    for vid, onh in onhand_by_variant.items():
        price, cost = pc_map.get(vid, (None, None))
        rows.append({
            "date": day,
            "product_variant_id": vid,
            "store_id": int(store_id),
            "on_hand": int(onh),
            "price": price,
            "cost_per_item": cost,
        })

    if not rows:
        return

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

ALLOWED_SORT_COLS = {
    # column alias in final SELECT -> safe
    "on_hand": "on_hand",
    "average_stock_level": "average_stock_level",
    "avg_inventory_value": "avg_inventory_value",
    "stockout_rate": "stockout_rate",
    "dead_stock_ratio": "dead_stock_ratio",
    "stock_turnover": "stock_turnover",
    "avg_days_in_inventory": "avg_days_in_inventory",
    "stock_health_index": "stock_health_index",
    "product_title": "product_title",
    "sku": "sku",
}

def get_snapshots_with_metrics(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    q: Optional[str] = None,
    sort_col: str = "on_hand",
    sort_order: str = "desc",
    metric_filters: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Any]:
    # base where
    where = []
    params: Dict[str, Any] = {
        "start_ts": datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc) if start_date else None,
        "end_ts": datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc) if end_date else None,
        "q": f"%{q.strip()}%" if q else None,
        "skip": int(skip),
        "limit": int(limit),
    }

    if store_id is not None:
        where.append("s.store_id = :store_id")
        params["store_id"] = int(store_id)

    if params["start_ts"]:
        where.append("s.date >= :start_ts")
    if params["end_ts"]:
        where.append("s.date <= :end_ts")

    # optional search
    join_search = ""
    if q:
        join_search = "JOIN product_variants pvq ON pvq.id = s.product_variant_id JOIN products pq ON pq.id = pvq.product_id"
        where.append("(pvq.sku ILIKE :q OR pq.title ILIKE :q)")

    where_sql = " AND ".join(where) if where else "1=1"

    # recent window for depletion metric
    params["recent_floor"] = (params.get("end_ts") or datetime.now(timezone.utc)) - timedelta(days=14)

    # metric filters
    mf_sql = []
    if metric_filters:
        for field, bounds in metric_filters.items():
            if not bounds:
                continue
            lo = bounds.get("min")
            hi = bounds.get("max")
            if lo is not None and lo != "":
                mf_sql.append(f"({field} >= :{field}_min)")
                params[f"{field}_min"] = float(lo)
            if hi is not None and hi != "":
                mf_sql.append(f"({field} <= :{field}_max)")
                params[f"{field}_max"] = float(hi)
    mf_where = (" AND " + " AND ".join(mf_sql)) if mf_sql else ""

    # sorting
    safe_sort = ALLOWED_SORT_COLS.get(sort_col, "on_hand")
    so = "ASC" if (sort_order or "").lower() == "asc" else "DESC"
    order_sql = f"ORDER BY {safe_sort} {so}, product_title ASC, sku ASC"

    # store filter at the very end join
    final_store_filter = "WHERE pv.store_id = :store_id" if store_id is not None else ""

    # final SQL with window count over filtered set
    sql = text(f"""
    WITH filtered AS (
        SELECT s.* FROM inventory_snapshots s {join_search} WHERE {where_sql}
    ),
    series AS (
        SELECT
            f.*,
            LAG(f.on_hand) OVER (PARTITION BY f.product_variant_id, f.store_id ORDER BY f.date) AS prev_on_hand,
            (f.date AT TIME ZONE 'UTC')::date AS d
        FROM filtered f
    ),
    latest AS (
        SELECT DISTINCT ON (product_variant_id, store_id)
               *,
               (date AT TIME ZONE 'UTC')::date AS latest_date
        FROM filtered
        ORDER BY product_variant_id, store_id, date DESC
    ),
    deltas AS (
        SELECT *,
               (on_hand - COALESCE(prev_on_hand, on_hand)) AS delta
        FROM series
    ),
    agg AS (
        SELECT
            product_variant_id,
            store_id,
            COUNT(*)::numeric AS obs_count,
            AVG(on_hand)::numeric AS average_stock_level,
            MIN(on_hand)::numeric AS min_stock_level,
            MAX(on_hand)::numeric AS max_stock_level,
            (MAX(on_hand) - MIN(on_hand))::numeric AS stock_range,
            STDDEV_POP(on_hand)::numeric AS stock_stddev,
            SUM(CASE WHEN on_hand <= 0 THEN 1 ELSE 0 END)::numeric AS days_out_of_stock,
            100.0 * SUM(CASE WHEN on_hand <= 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) AS stockout_rate,
            SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END)::numeric AS total_outflow
        FROM deltas
        GROUP BY product_variant_id, store_id
    ),
    last_inflow AS (
        SELECT DISTINCT ON (product_variant_id, store_id)
               product_variant_id, store_id, d AS inflow_date
        FROM deltas
        WHERE delta > 0
        ORDER BY product_variant_id, store_id, d DESC
    ),
    recent_outflow AS (
        SELECT
            product_variant_id,
            store_id,
            AVG(CASE WHEN delta < 0 THEN -delta ELSE 0 END) FILTER (WHERE delta < 0) AS recent_outflow_rate
        FROM deltas
        WHERE date >= :recent_floor
        GROUP BY product_variant_id, store_id
    ),
    metrics_base AS (
        SELECT a.*, li.inflow_date, ro.recent_outflow_rate
        FROM agg a
        LEFT JOIN last_inflow li USING (product_variant_id, store_id)
        LEFT JOIN recent_outflow ro USING (product_variant_id, store_id)
    ),
    dead_stock AS (
        SELECT
            s.product_variant_id,
            s.store_id,
            SUM(CASE WHEN s.on_hand <= COALESCE(m.average_stock_level,0) * 0.10 THEN 1 ELSE 0 END)::numeric AS dead_stock_days
        FROM series s
        JOIN metrics_base m USING (product_variant_id, store_id)
        GROUP BY s.product_variant_id, s.store_id
    ),
    metrics AS (
        SELECT
            m.*,
            ds.dead_stock_days,
            CASE WHEN m.obs_count > 0 THEN 100.0 * ds.dead_stock_days / m.obs_count ELSE NULL END AS dead_stock_ratio,
            CASE WHEN m.average_stock_level > 0 THEN m.total_outflow / m.average_stock_level ELSE NULL END AS stock_turnover,
            (
              m.average_stock_level *
              COALESCE(
                (SELECT AVG(NULLIF(s.price,0)) FROM filtered s WHERE s.product_variant_id=m.product_variant_id AND s.store_id=m.store_id),
                (SELECT l.price FROM latest l WHERE l.product_variant_id=m.product_variant_id AND l.store_id=m.store_id)
              )
            )::numeric AS avg_inventory_value
        FROM metrics_base m
        JOIN dead_stock ds USING (product_variant_id, store_id)
    ),
    base AS (
        SELECT
            l.product_variant_id AS variant_id,
            l.store_id,
            l.on_hand,
            l.price,
            l.cost_per_item,
            l.latest_date,
            m.*,
            CASE WHEN m.stock_turnover > 0 THEN 365.0 / m.stock_turnover ELSE NULL END AS avg_days_in_inventory,
            CASE WHEN m.inflow_date IS NULL THEN NULL ELSE (l.latest_date - m.inflow_date) END AS replenishment_days,
            CASE WHEN COALESCE(m.recent_outflow_rate,0) > 0 THEN (l.on_hand::numeric / m.recent_outflow_rate) ELSE NULL END AS depletion_days,
            GREATEST(
              0,
              LEAST(
                1,
                (1 - COALESCE(m.stockout_rate,0)/100.0) * (1 - COALESCE(m.dead_stock_days / NULLIF(m.obs_count,0), 0))
              )
            )::numeric AS stock_health_index
        FROM latest l
        LEFT JOIN metrics m ON m.product_variant_id = l.product_variant_id AND m.store_id = l.store_id
    ),
    joined AS (
        SELECT
            b.*,
            pv.sku,
            pv.shopify_gid,
            p.id AS product_id,
            p.title AS product_title,
            p.image_url
        FROM base b
        JOIN product_variants pv ON pv.id = b.variant_id
        JOIN products p ON p.id = pv.product_id
        {final_store_filter}
    )
    SELECT
        j.*,
        COUNT(*) OVER() AS _total_count
    FROM joined j
    WHERE 1=1 {mf_where}
    {order_sql}
    LIMIT :limit OFFSET :skip
    """)

    rows = db.execute(sql, params).mappings().all()
    total = int(rows[0]["_total_count"]) if rows else 0

    snapshots: List[Dict[str, Any]] = []
    for r in rows:
        snapshots.append({
            "id": r["variant_id"],
            "date": r["latest_date"],
            "store_id": int(r["store_id"]),
            "product_variant_id": int(r["variant_id"]),
            "on_hand": int(r["on_hand"]) if r["on_hand"] is not None else None,
            "product_variant": {
                "id": int(r["variant_id"]),
                "shopify_gid": r["shopify_gid"],
                "sku": r["sku"],
                "product": {
                    "id": int(r["product_id"]),
                    "title": r["product_title"],
                    "image_url": r["image_url"],
                },
            },
            "metrics": {
                "average_stock_level": r["average_stock_level"],
                "min_stock_level": r["min_stock_level"],
                "max_stock_level": r["max_stock_level"],
                "stock_range": r["stock_range"],
                "stock_stddev": r["stock_stddev"],
                "days_out_of_stock": r["days_out_of_stock"],
                "stockout_rate": r["stockout_rate"],
                "replenishment_days": r["replenishment_days"],
                "depletion_days": r["depletion_days"],
                "total_outflow": r["total_outflow"],
                "stock_turnover": r["stock_turnover"],
                "avg_days_in_inventory": r["avg_days_in_inventory"],
                "dead_stock_days": r["dead_stock_days"],
                "dead_stock_ratio": r["dead_stock_ratio"],
                "avg_inventory_value": r["avg_inventory_value"],
                "stock_health_index": r["stock_health_index"],
            },
        })

    return {"snapshots": snapshots, "total_count": total}
