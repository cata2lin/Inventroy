# routes/snapshots.py
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import text

from database import get_db
from schemas import SnapshotWithMetricsResponse  # response model

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

# Client sends these keys in sort_field and metric filters
ALLOWED_SORT_COLUMNS = {
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

# Recognized numeric filterable fields -> SQL column expression from the metrics/joined CTE
FILTERABLE_NUMERIC = {
    "on_hand": "j.on_hand",
    "average_stock_level": "j.average_stock_level",
    "stockout_rate": "j.stockout_rate",
    "dead_stock_ratio": "j.dead_stock_ratio",
    "stock_turnover": "j.stock_turnover",
    "avg_days_in_inventory": "j.avg_days_in_inventory",
    "avg_inventory_value": "j.avg_inventory_value",
    "stock_health_index": "j.stock_health_index",
}

def _parse_metric_filters(qp: Dict[str, str]) -> List[str]:
    where_parts: List[str] = []
    for key, col in FILTERABLE_NUMERIC.items():
        min_key = f"{key}_min"
        max_key = f"{key}_max"
        if min_key in qp and qp[min_key] not in ("", None):
            where_parts.append(f"{col} >= :{min_key}")
        if max_key in qp and qp[max_key] not in ("", None):
            where_parts.append(f"{col} <= :{max_key}")
    return where_parts


@router.get("/", response_model=SnapshotWithMetricsResponse)
def list_snapshots(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=200),
    store_id: Optional[int] = Query(None, description="Filter by store id. Empty means all stores."),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    sort_field: str = Query("on_hand"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    q: Optional[str] = Query(None, description="Search in product title or SKU"),
):
    # Validate sort field
    sort_col = ALLOWED_SORT_COLUMNS.get(sort_field, "on_hand")
    order_sql = "ASC" if sort_order.lower() == "asc" else "DESC"

    qp: Dict[str, Any] = dict(request.query_params)
    metric_where = _parse_metric_filters(qp)

    # Date bounds inclusive
    if start_date is None and end_date is None:
        # default to last 30 days to keep metrics meaningful
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=30)

    params: Dict[str, Any] = {
        "skip": skip,
        "limit": limit,
        "q_ilike": f"%{q.strip().lower()}%" if q else None,
        "store_id": store_id,
        "start_d": start_date,
        "end_d_plus": (end_date + timedelta(days=1)) if end_date else None,  # exclusive upper bound
    }

    # Build base and metrics
    sql = f"""
WITH base AS (
    SELECT s.date::date AS d,
           s.date AS ts,
           s.product_variant_id,
           s.store_id,
           s.on_hand,
           COALESCE(s.price, pv.price) AS unit_price,
           pv.sku,
           pv.shopify_gid,
           pv.title AS variant_title,
           p.id AS product_id,
           p.title AS product_title,
           p.image_url
    FROM inventory_snapshots s
    JOIN product_variants pv ON pv.id = s.product_variant_id
    JOIN products p ON p.id = pv.product_id
    WHERE 1=1
      {"AND s.store_id = :store_id" if store_id else ""}
      {"AND s.date >= :start_d" if start_date else ""}
      {"AND s.date < :end_d_plus" if end_date else ""}
      {"AND (LOWER(p.title) LIKE :q_ilike OR LOWER(pv.sku) LIKE :q_ilike)" if q else ""}
),
ord AS (
    SELECT b.*,
           LAG(on_hand) OVER (PARTITION BY product_variant_id, store_id ORDER BY d) AS prev_on_hand
    FROM base b
),
d AS (
    SELECT *,
           GREATEST(COALESCE(prev_on_hand, on_hand) - on_hand, 0)::float AS outflow,
           CASE WHEN prev_on_hand IS NOT NULL AND on_hand = prev_on_hand AND on_hand > 0 THEN 1 ELSE 0 END AS is_dead_stock_day,
           CASE WHEN on_hand = 0 THEN 1 ELSE 0 END AS is_stockout_day
    FROM ord
),
metrics AS (
    SELECT product_variant_id,
           store_id,
           AVG(on_hand)::float AS average_stock_level,
           MIN(on_hand)::float AS min_stock_level,
           MAX(on_hand)::float AS max_stock_level,
           (MAX(on_hand) - MIN(on_hand))::float AS stock_range,
           STDDEV_POP(on_hand)::float AS stock_stddev,
           SUM(is_stockout_day)::int AS days_out_of_stock,
           CASE WHEN COUNT(*) > 0 THEN 100.0 * SUM(is_stockout_day)::float / COUNT(*) ELSE NULL END AS stockout_rate,
           SUM(outflow)::float AS total_outflow,
           CASE WHEN AVG(on_hand) > 0 THEN SUM(outflow)::float / NULLIF(AVG(on_hand),0) ELSE NULL END AS stock_turnover,
           CASE WHEN SUM(outflow) > 0 THEN SUM(on_hand)::float / NULLIF(SUM(outflow),0) ELSE NULL END AS avg_days_in_inventory,
           SUM(is_dead_stock_day)::int AS dead_stock_days,
           CASE WHEN COUNT(*) > 0 THEN 100.0 * SUM(is_dead_stock_day)::float / COUNT(*) ELSE NULL END AS dead_stock_ratio,
           AVG(on_hand * unit_price)::float AS avg_inventory_value
    FROM d
    GROUP BY product_variant_id, store_id
),
latest AS (
    SELECT DISTINCT ON (product_variant_id, store_id)
           product_variant_id, store_id, ts AS latest_ts, d AS latest_d, on_hand AS last_on_hand
    FROM base
    ORDER BY product_variant_id, store_id, ts DESC
),
joined AS (
    SELECT
        l.latest_d AS date,
        l.product_variant_id,
        l.store_id,
        l.last_on_hand AS on_hand,
        b.sku,
        b.shopify_gid,
        b.variant_title,
        b.product_id,
        b.product_title,
        b.image_url,
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
        -- Simple composite index 0..1
        (0.5 * GREATEST(0, LEAST(1, 1 - COALESCE(m.stockout_rate,0)/100.0))
         + 0.5 * GREATEST(0, LEAST(1, COALESCE(m.stock_turnover,0)/10.0))) AS stock_health_index
    FROM latest l
    JOIN base b
      ON b.product_variant_id = l.product_variant_id AND b.store_id = l.store_id AND b.ts = l.latest_ts
    LEFT JOIN metrics m
      ON m.product_variant_id = l.product_variant_id AND m.store_id = l.store_id
)
SELECT j.*,
       COUNT(*) OVER() AS _total_count
FROM joined j
WHERE 1=1
{ " AND " + " AND ".join(metric_where) if metric_where else "" }
ORDER BY {ALLOWED_SORT_COLUMNS.get(sort_field, 'on_hand')} {order_sql}, j.product_title ASC
LIMIT :limit OFFSET :skip
"""
    rows = db.execute(text(sql), params).mappings().all()

    total = int(rows[0]["_total_count"]) if rows else 0

    snapshots: List[Dict[str, Any]] = []
    for r in rows:
        snapshots.append({
            "date": r["date"],
            "product_variant_id": int(r["product_variant_id"]),
            "store_id": int(r["store_id"]),
            "on_hand": int(r["on_hand"]) if r["on_hand"] is not None else None,
            "product_variant": {
                "id": int(r["product_variant_id"]),
                "shopify_gid": r["shopify_gid"],
                "sku": r["sku"],
                "title": r["variant_title"],
                "product": {
                    "id": int(r["product_id"]),
                    "title": r["product_title"],
                    "image_url": r["image_url"],
                }
            },
            "metrics": {
                "average_stock_level": r["average_stock_level"],
                "min_stock_level": r["min_stock_level"],
                "max_stock_level": r["max_stock_level"],
                "stock_range": r["stock_range"],
                "stock_stddev": r["stock_stddev"],
                "days_out_of_stock": r["days_out_of_stock"],
                "stockout_rate": r["stockout_rate"],
                "total_outflow": r["total_outflow"],
                "stock_turnover": r["stock_turnover"],
                "avg_days_in_inventory": r["avg_days_in_inventory"],
                "dead_stock_days": r["dead_stock_days"],
                "dead_stock_ratio": r["dead_stock_ratio"],
                "avg_inventory_value": r["avg_inventory_value"],
                "stock_health_index": r["stock_health_index"],
            }
        })

    payload = {
        "total_count": total,
        "snapshots": snapshots,
    }
    return payload
