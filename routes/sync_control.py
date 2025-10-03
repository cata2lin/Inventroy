# routes/sync_control.py
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Body, Query
from sqlalchemy.orm import Session

from database import get_db
from crud import store as crud_store
from services import product_sync_runner, sync_tracker

router = APIRouter(prefix="/api/sync-control", tags=["Sync Control"])

@router.get("/status")
def get_all_task_status() -> Dict[str, Any]:
    sync_tracker.clear_finished()
    return {"tasks": sync_tracker.list_tasks()}

@router.post("/products")
def trigger_products_sync(
    background_tasks: BackgroundTasks, db: Session = Depends(get_db),
    payload: Dict[str, Any] = Body(default={}), store_id: Optional[int] = Query(None),
) -> Dict[str, Any]:
    effective_store_id = store_id or payload.get("store_id")
    tasks: List[Dict[str, Any]] = []

    if effective_store_id:
        store = crud_store.get_store(db, int(effective_store_id))
        if not store or not store.enabled: raise HTTPException(status_code=404, detail="Store not found or disabled")
        
        task_id = sync_tracker.add_task(f"Products sync for {store.name}")
        # CORRECTED: The call now matches the function's signature
        background_tasks.add_task(product_sync_runner.run_product_sync_for_store, store.id, task_id)
        tasks.append({"store_id": store.id, "store": store.name, "task_id": task_id})
    else:
        stores = crud_store.get_enabled_stores(db)
        if not stores: raise HTTPException(status_code=404, detail="No enabled stores configured.")
        for s in stores:
            task_id = sync_tracker.add_task(f"Products sync for {s.name}")
            # CORRECTED: The call now matches the function's signature
            background_tasks.add_task(product_sync_runner.run_product_sync_for_store, s.id, task_id)
            tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})
    
    return {"status": "ok", "message": "Product sync started.", "tasks": tasks}