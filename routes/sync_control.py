# routes/sync_control.py

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from database import get_db, SessionLocal
from services import sync_service, sync_tracker
from crud import store as crud_store

router = APIRouter(
    prefix="/api/sync-control",
    tags=["Sync Control"],
    responses={404: {"description": "Not found"}},
)

@router.post("/orders", status_code=202)
def trigger_full_order_sync(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Triggers a background sync of all orders for ALL stores and returns the task IDs.
    """
    stores = crud_store.get_stores(db)
    task_ids = []
    for store in stores:
        task_id = sync_tracker.create_task(store.name)
        task_ids.append(task_id)
        background_tasks.add_task(
            sync_service.run_sync_in_background,
            target_function=sync_service.run_full_order_sync,
            db=SessionLocal(),
            store_id=store.id,
            task_id=task_id
        )
    return {"message": "Full order synchronization started for all stores.", "task_ids": task_ids}

@router.get("/status")
def get_all_tasks_status():
    """
    Returns the status of all current and completed sync tasks.
    """
    return sync_tracker.get_all_tasks()