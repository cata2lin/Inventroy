# routes/sync_control.py
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Body, Query
from sqlalchemy.orm import Session
import threading
import time

from database import get_db, SessionLocal
from crud import store as crud_store
from services import product_sync_runner, sync_tracker, stock_reconciliation

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
    """Sync products only (no stock reconciliation)."""
    effective_store_id = store_id or payload.get("store_id")
    tasks: List[Dict[str, Any]] = []

    if effective_store_id:
        store = crud_store.get_store(db, int(effective_store_id))
        if not store or not store.enabled: raise HTTPException(status_code=404, detail="Store not found or disabled")
        
        task_id = sync_tracker.add_task(f"Products sync for {store.name}")
        background_tasks.add_task(product_sync_runner.run_product_sync_for_store, store.id, task_id)
        tasks.append({"store_id": store.id, "store": store.name, "task_id": task_id})
    else:
        stores = crud_store.get_enabled_stores(db)
        if not stores: raise HTTPException(status_code=404, detail="No enabled stores configured.")
        for s in stores:
            task_id = sync_tracker.add_task(f"Products sync for {s.name}")
            background_tasks.add_task(product_sync_runner.run_product_sync_for_store, s.id, task_id)
            tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})
    
    return {"status": "ok", "message": "Product sync started.", "tasks": tasks}


@router.post("/products-and-reconcile")
def trigger_products_sync_with_reconciliation(
    background_tasks: BackgroundTasks, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Sync all stores then run stock reconciliation.
    This is the "Sync All Stores" action that applies minimum stock across matching barcodes.
    """
    stores = crud_store.get_enabled_stores(db)
    if not stores: 
        raise HTTPException(status_code=404, detail="No enabled stores configured.")
    
    # Collect store IDs and task IDs for the chain
    store_ids = [s.id for s in stores]
    store_task_ids = []
    tasks: List[Dict[str, Any]] = []
    
    for s in stores:
        task_id = sync_tracker.add_task(f"Products sync for {s.name}")
        store_task_ids.append(task_id)
        tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})
    
    # Add reconciliation task (will run after all syncs complete)
    reconcile_task_id = sync_tracker.add_task("Stock Reconciliation (min stock)")
    tasks.append({"store_id": None, "store": "All Stores", "task_id": reconcile_task_id, "type": "reconciliation"})
    
    # Run the chained sync in background
    background_tasks.add_task(
        _run_sync_then_reconcile, 
        store_ids, 
        store_task_ids, 
        reconcile_task_id
    )
    
    return {"status": "ok", "message": "Product sync started with stock reconciliation.", "tasks": tasks}


def _run_sync_then_reconcile(store_ids: List[int], store_task_ids: List[str], reconcile_task_id: str):
    """
    Runs product sync for all stores sequentially, then runs stock reconciliation.
    This ensures reconciliation only runs after ALL stores have finished syncing.
    """
    # Run each store sync sequentially
    for store_id, task_id in zip(store_ids, store_task_ids):
        try:
            product_sync_runner.run_product_sync_for_store(store_id, task_id)
        except Exception as e:
            print(f"[SYNC-CHAIN] Error syncing store {store_id}: {e}")
            sync_tracker.finish_task(task_id, ok=False, note=str(e))
    
    # After all stores are done, run reconciliation
    print("[SYNC-CHAIN] All stores synced. Starting stock reconciliation...")
    stock_reconciliation.reconcile_stock_by_barcode(reconcile_task_id)


@router.post("/reconcile-stock")
def trigger_stock_reconciliation(
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """
    Manually trigger stock reconciliation.
    Finds all barcodes across multiple stores and applies the minimum stock level.
    """
    task_id = sync_tracker.add_task("Stock Reconciliation (min stock)")
    background_tasks.add_task(stock_reconciliation.reconcile_stock_by_barcode, task_id)
    
    return {"status": "ok", "message": "Stock reconciliation started.", "task_id": task_id}