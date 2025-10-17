from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from crud.snapshots import (
    get_snapshots_with_metrics,
    create_snapshot_for_store,
    ALLOWED_SORT_COLS,
)

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

@router.get("/stores")
def list_stores(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT id, name FROM stores WHERE enabled = TRUE ORDER BY name")).mappings().all()
    return [{"id": int(r["id"]), "name": r["name"]} for r in rows]

@router.post("/trigger")
def trigger_snapshot(store_id: int = Query(..., ge=1), db: Session = Depends(get_db)):
    create_snapshot_for_store(db, store_id)
    return {"ok": True, "store_id": store_id}

def _collect_metric_filters(q: Dict[str, str]) -> Dict[str, Dict[str, float]]:
    numeric_keys = {
        "on_hand",
        "average_stock_level",
        "avg_inventory_value",
        "stockout_rate",
        "dead_stock_ratio",
        "stock_turnover",
        "avg_days_in_inventory",
        "stock_health_index",
    }
    out: Dict[str, Dict[str, float]] = {}
    for key in numeric_keys:
        min_key = f"{key}_min"
        max_key = f"{key}_max"
        lo = q.get(min_key)
        hi = q.get(max_key)
        if (lo is not None and lo != "") or (hi is not None and hi != ""):
            bounds: Dict[str, float] = {}
            if lo not in (None, ""):
                bounds["min"] = float(lo)
            if hi not in (None, ""):
                bounds["max"] = float(hi)
            out[key] = bounds
    return out

@router.get("/")
def list_snapshots(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=200),
    store_id: Optional[int] = Query(None, description="Leave empty for all stores"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    sort_field: str = Query("on_hand"),
    sort_order: str = Query("desc"),
    q: Optional[str] = Query(None),
):
    # default date window: last 30 days
    if start_date is None and end_date is None:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=30)

    # metric filters from query
    metric_filters = _collect_metric_filters(dict(request.query_params))

    # sanitize sort
    safe_sort = ALLOWED_SORT_COLS.get(sort_field, "on_hand")
    safe_order = "asc" if (sort_order or "").lower() == "asc" else "desc"

    payload = get_snapshots_with_metrics(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,             # None => all stores
        start_date=start_date,
        end_date=end_date,
        q=q,
        sort_col=safe_sort,
        sort_order=safe_order,
        metric_filters=metric_filters,
    )
    return payload
