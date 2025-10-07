# routes/snapshots.py
from datetime import date
from typing import Optional, Any
from fastapi import APIRouter, Depends, BackgroundTasks, Query
from sqlalchemy.orm import Session

from database import get_db
from crud import snapshots as crud_snapshots
from schemas import SnapshotWithMetricsResponse
from services import snapshot_runner

router = APIRouter(
    prefix="/api/snapshots",
    tags=["Snapshots"],
)

# -------------------------------
# Trigger Snapshot Endpoint
# -------------------------------
@router.post("/trigger", summary="Trigger manual snapshot")
def trigger_snapshot(background_tasks: BackgroundTasks):
    background_tasks.add_task(snapshot_runner.run_daily_snapshot)
    return {"status": "ok", "message": "Inventory snapshot process triggered."}


# -------------------------------
# List Snapshots Endpoint
# -------------------------------
@router.get("/", response_model=SnapshotWithMetricsResponse, summary="Retrieve snapshots with metrics")
def read_snapshots_with_metrics(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=100),
    store_id: Optional[int] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    sort_field: Optional[str] = Query("date"),
    sort_order: Optional[str] = Query("desc", regex="^(asc|desc)$"),
):
    # Fetch snapshots from CRUD
    snapshots, total_count = crud_snapshots.get_snapshots_with_metrics(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,
        start_date=start_date,
        end_date=end_date
    )

    # -------------------------------
    # Safe Sorting
    # -------------------------------
    reverse = sort_order.lower() == "desc"

    def get_sort_value(snap: Any):
        # Top-level snapshot field
        value = getattr(snap, sort_field, None)
        if value is not None:
            return value
        # Metric field
        value = getattr(snap, "metrics", {}).get(sort_field)
        return value if value is not None else 0  # Coerce None to 0

    snapshots_sorted = sorted(snapshots, key=get_sort_value, reverse=reverse)

    return {"total_count": total_count, "snapshots": snapshots_sorted}


# -------------------------------
# List available metrics
# -------------------------------
@router.get("/metrics", summary="List all available metrics")
def list_metrics():
    metrics = [
        "on_hand",
        "average_stock_level",
        "min_stock_level",
        "max_stock_level",
        "stock_range",
        "stockout_rate",
        "replenishment_days",
        "depletion_days",
        "total_outflow",
        "stock_turnover",
        "avg_days_in_inventory",
        "dead_stock_days",
        "dead_stock_ratio",
        "avg_inventory_value",
        "stock_health_index",
    ]
    return {"metrics": metrics}
