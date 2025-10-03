from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Body, Query
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
from crud import store as crud_store
from services import product_sync_runner, sync_tracker

router = APIRouter(prefix="/api/sync-control", tags=["Sync Control"])

@router.get("/status")
def get_all_task_status() -> Dict[str, Any]:
    try:
        sync_tracker.clear_finished(older_than_seconds=3600)
    except Exception:
        pass
    return {"tasks": sync_tracker.list_tasks()}

@router.post("/products")
def trigger_products_sync(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    payload: Dict[str, Any] = Body(default={}),
    store_id: Optional[int] = Query(None, description="If provided, sync only this store"),
) -> Dict[str, Any]:
    sid = payload.get("store_id") if isinstance(payload, dict) else None
    if store_id is None and isinstance(sid, int):
        store_id = sid

    tasks: List[Dict[str, Any]] = []

    if store_id is not None:
        store = crud_store.get_store(db, int(store_id))
        if not store or not store.enabled:
            raise HTTPException(status_code=404, detail="Store not found or disabled")
        task_id = sync_tracker.add_task(f"Products sync for {store.name}")
        background_tasks.add_task(
            product_sync_runner.run_product_sync_for_store,
            SessionLocal,
            store.id,
            task_id,
        )
        tasks.append({"store_id": store.id, "store": store.name, "task_id": task_id})
        return {"status": "ok", "message": f"Product sync started for {store.name}.", "tasks": tasks}

    stores = crud_store.get_enabled_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No enabled stores configured.")

    for s in stores:
        task_id = sync_tracker.add_task(f"Products sync for {s.name}")
        background_tasks.add_task(
            product_sync_runner.run_product_sync_for_store,
            SessionLocal,
            s.id,
            task_id,
        )
        tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})

    return {"status": "ok", "message": "Product sync kicked off for all stores.", "tasks": tasks}