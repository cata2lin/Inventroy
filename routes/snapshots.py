# routes/snapshots.py
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

import schemas
from database import get_db
from crud import snapshots as crud_snapshots
from services import snapshot_runner

router = APIRouter(
    prefix="/api/snapshots",
    tags=["Snapshots"],
)

@router.post("/trigger")
def trigger_snapshot(background_tasks: BackgroundTasks):
    """Manually trigger the daily snapshot process."""
    background_tasks.add_task(snapshot_runner.run_daily_snapshot)
    return {"status": "ok", "message": "Inventory snapshot process triggered."}


@router.get("/", response_model=schemas.InventorySnapshotResponse)
def read_snapshots(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    snapshot_date: Optional[date] = None,
):
    """
    Retrieve inventory snapshots with optional filters.
    """
    snapshots, total_count = crud_snapshots.get_snapshots(
        db, skip=skip, limit=limit, store_id=store_id, date=snapshot_date
    )
    return {"total_count": total_count, "snapshots": snapshots}

@router.get("/metrics/{variant_id}", response_model=schemas.SnapshotMetrics)
def get_metrics_for_variant(
    variant_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
):
    """
    Calculate advanced inventory metrics for a single product variant over a date range.
    Defaults to the last 30 days if no date range is provided.
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    metrics = crud_snapshots.get_snapshot_metrics(db, variant_id, start_date, end_date)
    if not metrics:
        raise HTTPException(status_code=404, detail="No snapshot data found for this variant in the selected date range.")
    return metrics