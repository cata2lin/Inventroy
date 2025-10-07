# routes/snapshots.py
from datetime import date
from typing import Optional, List
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
    """
    Manually trigger the daily inventory snapshot process.
    Runs in the background to avoid blocking the request.
    """
    background_tasks.add_task(snapshot_runner.run_daily_snapshot)
    return {
        "status": "ok",
        "message": "Inventory snapshot process triggered. It will run in the background."
    }

# -------------------------------
# List Snapshots Endpoint
# -------------------------------
@router.get("/", response_model=SnapshotWithMetricsResponse, summary="Retrieve snapshots with metrics")
def read_snapshots_with_metrics(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip (pagination)"),
    limit: int = Query(25, ge=1, le=100, description="Number of records to return"),
    store_id: Optional[int] = Query(None, description="Filter snapshots by store ID"),
    start_date: Optional[date] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date filter (YYYY-MM-DD)"),
    sort_field: Optional[str] = Query("date", description="Field or metric to sort by"),
    sort_order: Optional[str] = Query("desc", regex="^(asc|desc)$", description="Sort order: asc or desc"),
):
    """
    Retrieve inventory snapshots along with performance metrics.

    Supports:
    - Filtering by store
    - Filtering by date range
    - Sorting by any metric or snapshot field (ascending/descending)
    - Pagination
    """
    # Fetch snapshots from CRUD
    result = crud_snapshots.get_snapshots_with_metrics(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,
        start_date=start_date,
        end_date=end_date
    )

    snapshots = result["snapshots"]
    total_count = int(result["total_count"])  # ensure integer for Pydantic

    # -------------------------------
    # Sorting
    # -------------------------------
    reverse = sort_order == "desc"

    def get_sort_value(snap):
        """
        Return the value for sorting. Supports:
        - top-level snapshot fields
        - metrics dictionary fields
        - None is treated as smallest value
        """
        if isinstance(snap, dict):
            return snap.get(sort_field) or snap.get("metrics", {}).get(sort_field) or 0
        else:  # SQLAlchemy object
            value = getattr(snap, sort_field, None)
            if value is None and hasattr(snap, "metrics"):
                value = snap.metrics.get(sort_field)
            return value if value is not None else 0

    snapshots_sorted = sorted(snapshots, key=get_sort_value, reverse=reverse)

    # -------------------------------
    # Convert SQLAlchemy objects to dicts
    # -------------------------------
    snapshots_serializable = []
    for s in snapshots_sorted:
        if isinstance(s, dict):
            snapshots_serializable.append(s)
        else:
            snapshots_serializable.append(
                {**s.__dict__, "metrics": getattr(s, "metrics", {})}
            )

    return {"total_count": total_count, "snapshots": snapshots_serializable}

# -------------------------------
# Optional: List Available Metrics
# -------------------------------
@router.get("/metrics", summary="List all available metrics")
def list_metrics():
    """
    Returns a list of available metrics for filtering or sorting in the frontend.
    """
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
