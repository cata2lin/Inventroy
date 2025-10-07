# routes/snapshots.py
from datetime import date
from decimal import Decimal
from typing import Optional
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

# Trigger snapshot
@router.post("/trigger", summary="Trigger manual snapshot")
def trigger_snapshot(background_tasks: BackgroundTasks):
    background_tasks.add_task(snapshot_runner.run_daily_snapshot)
    return {
        "status": "ok",
        "message": "Inventory snapshot process triggered. It will run in the background."
    }

# Helper: convert Decimal -> float
def convert_metrics(metrics: dict):
    converted = {}
    for k, v in metrics.items():
        if isinstance(v, Decimal):
            converted[k] = float(v)
        else:
            converted[k] = v
    return converted

# List snapshots
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
    result = crud_snapshots.get_snapshots_with_metrics(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,
        start_date=start_date,
        end_date=end_date
    )

    snapshots = result["snapshots"]
    total_count = int(result["total_count"])

    # Sorting
    reverse = sort_order == "desc"
    def get_sort_value(s):
        val = s.get(sort_field) or s.get("metrics", {}).get(sort_field)
        if isinstance(val, Decimal):
            return float(val)
        return val if val is not None else 0
    snapshots_sorted = sorted(snapshots, key=get_sort_value, reverse=reverse)

    # Map to exact Pydantic model structure
    snapshots_serializable = []
    for s in snapshots_sorted:
        snapshot_dict = {
            "id": s.get("id"),
            "date": s.get("date"),  # Must exist
            "store_id": s.get("store_id"),
            "product_variant_id": s.get("product_variant_id"),
            "on_hand": s.get("on_hand"),
            "metrics": convert_metrics(s.get("metrics", {})),
            "product_variant": {
                "shopify_gid": s.get("product_variant", {}).get("shopify_gid"),
                "sku": s.get("product_variant", {}).get("sku"),
                "product": s.get("product_variant", {}).get("product"),
            }
        }
        snapshots_serializable.append(snapshot_dict)

    return {"total_count": total_count, "snapshots": snapshots_serializable}

# List available metrics
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
