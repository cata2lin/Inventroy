# routes/sync_control.py

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
import uuid

from database import get_db
from services import sync_service

router = APIRouter(
    prefix="/api/sync-control",
    tags=["Sync Control"],
    responses={404: {"description": "Not found"}},
)

class SyncRequest(BaseModel):
    store_ids: Optional[List[int]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

@router.post("/products", status_code=202)
def trigger_product_sync(request: SyncRequest, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    background_tasks.add_task(sync_service.run_sync_in_background, 
                              target_function=sync_service.run_full_product_sync, 
                              task_id=task_id,
                              store_ids=request.store_ids)
    return {"message": "Product synchronization has been started in the background.", "task_id": task_id}

@router.post("/orders", status_code=202)
def trigger_order_sync(request: SyncRequest, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    background_tasks.add_task(sync_service.run_sync_in_background,
                              target_function=sync_service.run_full_order_sync,
                              task_id=task_id,
                              store_ids=request.store_ids,
                              start_date=request.start_date,
                              end_date=request.end_date)
    return {"message": "Order synchronization has been started in the background.", "task_id": task_id}