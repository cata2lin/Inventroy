# crud/snapshots.py
from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models

# ---------- Writers ----------

def create_snapshot_for_store(db: Session, store_id: int) -> None:
    """
    Upsert one inventory snapshot row per variant for the given store at normalized day.
    Allows negative inventory. Uses stores.sync_location_id if set, else sums across locations.
    """
    now = datetime.now(timezone.utc)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Map on_hand
    sync_loc_id = db.query(models.Store.sync_location_id).filter(models.Store.id == store_id).scalar()
    if sync_loc_id:
        onhand_rows = db.execute(
            text("""
                SELECT pv.id AS variant_id, COALESCE(il.on_hand, 0) AS on_hand
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
                SELECT pv.id AS variant_id, COALESCE(SUM(il.on_hand), 0) AS on_hand
                FROM product_variants pv
                LEFT JOIN inventory_levels il
                  ON il.variant_id = pv.id
                WHERE pv.store_id = :store_id
                GROUP BY pv.id
            """),
            {"store_id": int(store_id)},
        ).fetchall()

    onhand_by_variant = {int(r.variant_id): int(r.on_hand) for r in onhand_rows}

    # Latest price/cost from variants
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
            "date": day,  # normalized for daily uniqueness
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

def get_snapshots_with_metrics(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: int = 0,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    q: Optional[str] = None,
    sort_col: str = "on_hand",
    sort_order: str = "desc",
    metric_filters: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Any]:
    """
    Returns latest snapshot per variant within the window plus metrics.
    All filters, search, sorting, and pagination are pushed into SQL.
    - stockout counts on_hand <= 0 (includes negatives).
    - replenishment_days: days since last positive inflow.
    - depletion_days: on_hand / avg recent outflow over last 14 days in window.
    - dead_stock_days: days with on_hand <= 10% of average in window.
    - ratios returned as percentages (0-100) to match prior behavior.
    """
    if not store_id:
        return {"snapshots": [], "total_count": 0}

    # Date bounds
    start_ts = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc) if start_date else None
    end_ts = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc) if end_date else None

    # recent window for depletion estimate
    recent_floor = (end_ts or datetime.now(timezone.utc)) - timedelta(days=14)

    # Dynamic WHERE parts
    where = ["s.store_id = :store_id"]
    if start_ts is not None:
        where.append("s.date >= :start_ts")
    if end_ts is not None:
        where.append("s.date <= :end_ts")

    # Search
    join_search = ""
    if q:
        join_search = "JOIN product_variants pvq ON pvq.id = s.product_variant_id JOIN products pq ON pq.id = pvq.product_id"
        where.append("(pvq.sku ILIKE :q OR pq.title ILIKE :q)")

    where_sql = " AND ".join(where)

    # Metric filters build
    mf_sql = []
    params: Dict[str, Any] = {
        "store_id": int(store_id),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "q": f"%{q.strip()}%" if q else None,
        "recent_floor": recent_floor,
        "skip": int(skip),
        "limit": int(limit),
    }

    def add_filter(field: str, bounds: Dict[str, float]):
        if bounds is None:
            return
        lo = bounds.get("min")
        hi = bounds.get("max")
        if lo is not None:
            mf_sql.append(f"({field} >= :{field}_min)")
            params[f"{field}_min"] = lo
        if hi is not None:
            mf_sql.append(f"({field} <= :{field}_max)")
            params[f"{field}_max"] = hi

    metric_filters = metric_filters or {}
    for k, b in metric_filters.items():
        add_filter(k, b)

    mf_where = (" AND " + " AND ".join(mf_sql)) if mf_sql else ""

    # Order by safe column
    so = "ASC" if (sort_order or "").lower() == "asc" else "DESC"
    order_sql = f"ORDER BY {sort_col} {so}, title ASC, sku ASC"

    sql = text(f"""
WITH filtered AS (
    SELECT s.*
    FROM inventory_snapshots s
    {join_search}
    WHERE {where_sql}
),
series AS (
    SELECT
        f.product_variant_id,
        f.store_id,
        (f.date AT TIME ZONE 'UTC')::date AS d,
        f.date AS ts,
        f.on_hand,
        f.price,
        f.cost_per_item,
        LAG(f.on_hand) OVER (PARTITION BY f.product_variant_id, f.store_id ORDER BY f.date) AS prev_on_hand
    FROM filtered f
),
latest AS (
    SELECT DISTINCT ON (product_variant_id, store_id)
        product_variant_id, store_id, on_hand, price, cost_per_item, ts AS latest_ts, (ts AT TIME ZONE 'UTC')::date AS latest_date
    FROM series
    ORDER BY product_variant_id, store_id, ts DESC
),
deltas AS (
    SELECT
        product_variant_id, store_id, d, ts, on_hand, price, cost_per_item,
        (on_hand - COALESCE(prev_on_hand, on_hand)) AS delta,
        CASE WHEN (on_hand - COALESCE(prev_on_hand, on_hand)) < 0 THEN COALESCE(prev_on_hand,0) - on_hand ELSE 0 END AS outflow,
        CASE WHEN (on_hand - COALESCE(prev_on_hand, on_hand)) > 0 THEN (on_hand - COALESCE(prev_on_hand, on_hand)) ELSE 0 END AS inflow
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
        SUM(outflow)::numeric AS total_outflow
    FROM deltas
    GROUP BY product_variant_id, store_id
),
last_inflow AS (
    SELECT DISTINCT ON (product_variant_id, store_id)
        product_variant_id, store_id, d AS inflow_date
    FROM deltas
    WHERE inflow > 0
    ORDER BY product_variant_id, store_id, d DESC
),
recent_outflow AS (
    SELECT product_variant_id, store_id,
           AVG(outflow) FILTER (WHERE outflow > 0) AS recent_outflow_rate
    FROM deltas
    WHERE ts >= :recent_floor
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
        CASE WHEN a.average_stock_level > 0 THEN a.total_outflow / a.average_stock_level ELSE NULL END AS stock_turnover,
        CASE WHEN a.total_outflow > 0 AND a.average_stock_level > 0
             THEN 365.0 / (a.total_outflow / a.average_stock_level)
             ELSE NULL
        END AS avg_days_in_inventory,
        li.inflow_date,
        ro.recent_outflow_rate
    FROM agg a
    LEFT JOIN last_inflow li USING (product_variant_id, store_id)
    LEFT JOIN recent_outflow ro USING (product_variant_id, store_id)
),
dead_stock AS (
    SELECT
        s.product_variant_id, s.store_id,
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
        CASE WHEN (SELECT obs_count FROM agg a WHERE a.product_variant_id=d.product_variant_id AND a.store_id=d.store_id) > 0
             THEN 100.0 * ds.dead_stock_days / NULLIF((SELECT obs_count FROM agg a WHERE a.product_variant_id=d.product_variant_id AND a.store_id=d.store_id),0)
             ELSE NULL END AS dead_stock_ratio,
        -- average inventory value using average stock * average observed price in window or fallback to latest
        (d.average_stock_level * COALESCE(
            (SELECT AVG(NULLIF(s.price,0)) FROM filtered s WHERE s.product_variant_id=d.product_variant_id AND s.store_id=d.store_id),
            (SELECT l.price FROM latest l WHERE l.product_variant_id=d.product_variant_id AND l.store_id=d.store_id)
        ))::numeric AS avg_inventory_value,
        -- health index in [0,1]
        GREATEST(0, LEAST(1,
            (1 - COALESCE(d.stockout_rate,0)/100.0) *
            (1 - COALESCE(((SELECT ds2.dead_stock_days FROM dead_stock ds2 WHERE ds2.product_variant_id=d.product_variant_id AND ds2.store_id=d.store_id) / NULLIF((SELECT obs_count FROM agg a WHERE a.product_variant_id=d.product_variant_id AND a.store_id=d.store_id),0)), 0))
        ))::numeric AS stock_health_index,
        d.inflow_date,
        d.recent_outflow_rate
    FROM derived d
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
        m.stock_health_index,
        -- replenishment days = days since last inflow
        CASE WHEN m.inflow_date IS NULL THEN NULL ELSE (l.latest_date - m.inflow_date) END AS replenishment_days,
        -- depletion = on_hand / recent_outflow_rate
        CASE WHEN COALESCE(m.recent_outflow_rate,0) > 0 THEN (l.on_hand::numeric / m.recent_outflow_rate) ELSE NULL END AS depletion_days
    FROM latest l
    LEFT JOIN metrics m ON m.product_variant_id = l.product_variant_id AND m.store_id = l.store_id
),
joined AS (
    SELECT
        b.*,
        pv.sku,
        -- --- THIS IS THE FIX ---
        -- Select the shopify_gid from the product_variants table
        pv.shopify_gid,
        p.title,
        p.image_url
    FROM base b
    JOIN product_variants pv ON pv.id = b.variant_id
    JOIN products p ON p.id = pv.product_id
    WHERE pv.store_id = :store_id
)
SELECT * FROM joined
WHERE 1=1 {mf_where}
{order_sql}
LIMIT :limit OFFSET :skip
    """)

    count_sql = text(f"""
WITH filtered AS (
    SELECT s.*
    FROM inventory_snapshots s
    {join_search}
    WHERE {where_sql}
),
series AS (
    SELECT
        f.product_variant_id,
        f.store_id,
        f.date AS ts,
        f.on_hand,
        LAG(f.on_hand) OVER (PARTITION BY f.product_variant_id, f.store_id ORDER BY f.date) AS prev_on_hand
    FROM filtered f
),
latest AS (
    SELECT DISTINCT ON (product_variant_id, store_id)
        product_variant_id, store_id, on_hand, ts AS latest_ts
    FROM series
    ORDER BY product_variant_id, store_id, ts DESC
),
agg AS (
    SELECT product_variant_id, store_id, COUNT(*) AS obs_count,
           AVG(on_hand)::numeric AS average_stock_level,
           MIN(on_hand)::numeric AS min_stock_level,
           MAX(on_hand)::numeric AS max_stock_level,
           (MAX(on_hand) - MIN(on_hand))::numeric AS stock_range,
           STDDEV_POP(on_hand)::numeric AS stock_stddev,
           SUM(CASE WHEN on_hand <= 0 THEN 1 ELSE 0 END)::numeric AS days_out_of_stock,
           100.0 * SUM(CASE WHEN on_hand <= 0 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0) AS stockout_rate,
           SUM(GREATEST(COALESCE(prev_on_hand, on_hand) - on_hand, 0))::numeric AS total_outflow
    FROM series
    GROUP BY product_variant_id, store_id
),
base AS (
    SELECT l.product_variant_id, l.store_id,
           a.average_stock_level, a.min_stock_level, a.max_stock_level, a.stock_range, a.stock_stddev,
           a.days_out_of_stock, a.stockout_rate, a.total_outflow
    FROM latest l
    LEFT JOIN agg a ON a.product_variant_id = l.product_variant_id AND a.store_id = l.store_id
)
SELECT COUNT(*)
FROM base b
JOIN product_variants pv ON pv.id = b.product_variant_id
JOIN products p ON p.id = pv.product_id
WHERE pv.store_id = :store_id {(" AND (pv.sku ILIKE :q OR p.title ILIKE :q)" if q else "")}
{(" AND " + " AND ".join(mf_sql)) if mf_sql else ""}
    """)

    rows = db.execute(sql, params).fetchall()
    total = db.execute(count_sql, params).scalar() or 0

    if not rows:
        return {"snapshots": [], "total_count": int(total)}

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r.variant_id, # Use a unique ID from the data, like variant_id
            "date": r.latest_date,
            "store_id": int(r.store_id),
            "product_variant_id": int(r.variant_id),
            "on_hand": int(r.on_hand) if r.on_hand is not None else None,
            "product_variant": {
                "id": int(r.variant_id),
                # --- THIS IS THE FIX ---
                # Add the shopify_gid to the response dictionary
                "shopify_gid": r.shopify_gid,
                "sku": r.sku,
                "product": {
                    "id": None,
                    "title": r.title,
                    "image_url": r.image_url,
                },
            },
            "metrics": {
                "average_stock_level": r.average_stock_level,
                "min_stock_level": r.min_stock_level,
                "max_stock_level": r.max_stock_level,
                "stock_range": r.stock_range,
                "stock_stddev": r.stock_stddev,
                "days_out_of_stock": r.days_out_of_stock,
                "stockout_rate": r.stockout_rate,
                "replenishment_days": r.replenishment_days,
                "depletion_days": r.depletion_days,
                "total_outflow": r.total_outflow,
                "stock_turnover": r.stock_turnover,
                "avg_days_in_inventory": r.avg_days_in_inventory,
                "dead_stock_days": r.dead_stock_days,
                "dead_stock_ratio": r.dead_stock_ratio,
                "avg_inventory_value": r.avg_inventory_value,
                "stock_health_index": r.stock_health_index,
            },
        })

    return {"snapshots": out, "total_count": int(total)}