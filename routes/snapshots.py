# routes/snapshots.py
from __future__ import annotations

from datetime import date
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session

from database import get_db
from schemas import SnapshotWithMetricsResponse  # keep existing response model
from crud import snapshots as crud_snapshots

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

# Column allowlist for sorting to avoid SQL injection
ALLOWED_SORT_FIELDS = {
    "date": "latest_date",
    "on_hand": "on_hand",
    "sku": "sku",
    "title": "title",
    "average_stock_level": "average_stock_level",
    "min_stock_level": "min_stock_level",
    "max_stock_level": "max_stock_level",
    "stock_range": "stock_range",
    "stock_stddev": "stock_stddev",
    "days_out_of_stock": "days_out_of_stock",
    "stockout_rate": "stockout_rate",
    "replenishment_days": "replenishment_days",
    "depletion_days": "depletion_days",
    "total_outflow": "total_outflow",
    "stock_turnover": "stock_turnover",
    "avg_days_in_inventory": "avg_days_in_inventory",
    "dead_stock_days": "dead_stock_days",
    "dead_stock_ratio": "dead_stock_ratio",
    "avg_inventory_value": "avg_inventory_value",
    "stock_health_index": "stock_health_index",
}

METRIC_KEYS: List[str] = [
    "average_stock_level","min_stock_level","max_stock_level","stock_range",
    "stock_stddev","days_out_of_stock","stockout_rate","replenishment_days",
    "depletion_days","total_outflow","stock_turnover","avg_days_in_inventory",
    "dead_stock_days","dead_stock_ratio","avg_inventory_value","stock_health_index"
]


@router.get("/", response_model=SnapshotWithMetricsResponse, summary="Retrieve snapshots with metrics")
def read_snapshots_with_metrics(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=200),
    # --- THIS IS THE FIX ---
    # The store_id is now optional to allow for the "All Stores" view.
    store_id: Optional[int] = Query(None, ge=1),
    start_date: Optional[date] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="YYYY-MM-DD"),
    q: Optional[str] = Query(None, description="Search by SKU or title"),
    sort_field: str = Query("on_hand"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
):
    sort_col = ALLOWED_SORT_FIELDS.get(sort_field, "on_hand")
    # Collect metric min/max filters from query params
    qp = dict(request.query_params)
    metric_filters: Dict[str, Dict[str, float]] = {}
    for k in METRIC_KEYS + ["on_hand"]:
        lo = qp.get(f"{k}_min")
        hi = qp.get(f"{k}_max")
        lo = float(lo) if lo not in (None, "") else None
        hi = float(hi) if hi not in (None, "") else None
        if lo is not None or hi is not None:
            metric_filters[k] = {"min": lo, "max": hi}

    payload = crud_snapshots.get_snapshots_with_metrics(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,
        start_date=start_date,
        end_date=end_date,
        q=q,
        sort_col=sort_col,
        sort_order=sort_order,
        metric_filters=metric_filters,
    )
    return payload


@router.post("/trigger", summary="Trigger inventory snapshot collection for a store")
def trigger_snapshot(store_id: int = Query(..., ge=1), db: Session = Depends(get_db)):
    if not store_id:
        raise HTTPException(status_code=400, detail="store_id is required")
    crud_snapshots.create_snapshot_for_store(db, store_id)
    return {"status": "ok"}