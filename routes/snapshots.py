# routes/snapshots.py
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, BackgroundTasks
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


@router.get("/", response_model=schemas.SnapshotWithMetricsResponse)
def read_snapshots_with_metrics(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    """
    Retrieve inventory snapshots along with their calculated performance metrics.
    """
    snapshots, total_count = crud_snapshots.get_snapshots_with_metrics(
        db, skip=skip, limit=limit, store_id=store_id, start_date=start_date, end_date=end_date
    )
    return {"total_count": total_count, "snapshots": snapshots}