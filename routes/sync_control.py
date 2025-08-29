# routes/sync_control.py
from __future__ import annotations

from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Body, Query
from sqlalchemy.orm import Session

# DB session helpers
try:
    from session import get_db, SessionLocal  # preferred, if present
except Exception:
    from database import get_db, SessionLocal  # fallback

from crud import store as crud_store
from services import product_sync_runner, order_sync_runner, sync_tracker, inventory_sync_service
from jobs import reconciliation

router = APIRouter(prefix="/api/sync-control", tags=["Sync Control"])


# ---------- Status (polled by UI) ----------
@router.get("/status")
def get_all_task_status() -> Dict[str, Any]:
    """
    Returns a list of all ongoing/recent tasks for the UI to render progress bars.
    """
    # Optionally prune very old finished tasks
    try:
        sync_tracker.clear_finished(older_than_seconds=3600)
    except Exception:
        pass
    return {"tasks": sync_tracker.list_tasks()}


@router.get("/tasks/{task_id}")
def get_task_status(task_id: str) -> Dict[str, Any]:
    task = sync_tracker.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


# ---------- Products sync ----------
@router.post("/products")
def trigger_products_sync(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    # Accept both body or query for compatibility
    payload: Dict[str, Any] = Body(default={}),
    store_id: Optional[int] = Query(None, description="If provided, sync only this store"),
) -> Dict[str, Any]:
    """
    Trigger products sync:
      - If 'store_id' provided (query or JSON), sync that single store.
      - Else, sync ALL enabled stores.
    """
    sid = payload.get("store_id") if isinstance(payload, dict) else None
    if store_id is None and isinstance(sid, int):
        store_id = sid

    tasks: List[Dict[str, Any]] = []

    if store_id is not None:
        store = crud_store.get_store(db, int(store_id))
        if not store or not store.enabled:
            raise HTTPException(status_code=404, detail="Store not found or disabled")
        task_id = sync_tracker.add_task(f"Products sync for {store.name}")
        # Runner supports both legacy/new signatures:
        background_tasks.add_task(
            product_sync_runner.run_product_sync_for_store,
            SessionLocal,
            store.id,
            store.shopify_url,
            store.api_token,
            task_id,
        )
        tasks.append({"store_id": store.id, "store": store.name, "task_id": task_id})
        return {"status": "ok", "message": f"Product sync started for {store.name}.", "tasks": tasks}

    # All enabled stores
    stores = crud_store.get_enabled_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No enabled stores configured.")

    for s in stores:
        task_id = sync_tracker.add_task(f"Products sync for {s.name}")
        background_tasks.add_task(
            product_sync_runner.run_product_sync_for_store,
            SessionLocal,
            s.id,
            s.shopify_url,
            s.api_token,
            task_id,
        )
        tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})

    return {"status": "ok", "message": "Product sync kicked off for all stores.", "tasks": tasks}


# ---------- Orders sync ----------
@router.post("/orders")
def trigger_orders_sync(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    payload: Dict[str, Any] = Body(default={}),
    # Accept query too (compat)
    store_id: Optional[int] = Query(None, description="If provided, sync only this store"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD"),
) -> Dict[str, Any]:
    """
    Trigger orders sync:
      - If 'store_id' provided (query or JSON), sync that single store.
      - Optional date window via ('start'/'end') or ('start_date'/'end_date') in JSON.
      - Else, sync ALL enabled stores.
    """
    # Resolve inputs (support multiple naming variants used by the UI)
    sid_body = payload.get("store_id") if isinstance(payload, dict) else None
    if store_id is None and isinstance(sid_body, int):
        store_id = sid_body

    start_body = None
    end_body = None
    if isinstance(payload, dict):
        start_body = payload.get("start") or payload.get("start_date")
        end_body = payload.get("end") or payload.get("end_date")

    start = start or start_body
    end = end or end_body

    tasks: List[Dict[str, Any]] = []

    if store_id is not None:
        store = crud_store.get_store(db, int(store_id))
        if not store or not store.enabled:
            raise HTTPException(status_code=404, detail="Store not found or disabled")

        task_id = sync_tracker.add_task(f"Orders sync for {store.name}")
        background_tasks.add_task(
            order_sync_runner.run_orders_sync_for_store,
            SessionLocal,
            store.id,
            start,
            end,
            task_id,
        )
        tasks.append({"store_id": store.id, "store": store.name, "task_id": task_id})
        return {
            "status": "ok",
            "message": f"Order sync started for {store.name}.",
            "start": start,
            "end": end,
            "tasks": tasks,
        }

    # All enabled stores
    stores = crud_store.get_enabled_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No enabled stores configured.")

    for s in stores:
        task_id = sync_tracker.add_task(f"Orders sync for {s.name}")
        background_tasks.add_task(
            order_sync_runner.run_orders_sync_for_store,
            SessionLocal,
            s.id,
            start,
            end,
            task_id,
        )
        tasks.append({"store_id": s.id, "store": s.name, "task_id": task_id})

    return {
        "status": "ok",
        "message": "Order sync kicked off for all stores.",
        "start": start,
        "end": end,
        "tasks": tasks,
    }

@router.post("/reconcile-stock")
def trigger_stock_reconciliation(
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """
    Triggers a full stock level reconciliation across all stores for all barcode groups.
    This is a heavy operation and should be used sparingly.
    """
    task_id = sync_tracker.add_task("Full Stock Reconciliation")
    background_tasks.add_task(
        reconciliation.run_reconciliation,
        db_factory=SessionLocal,
        task_id=task_id
    )
    return {"status": "ok", "message": "Full stock reconciliation started.", "tasks": [{"task_id": task_id}]}

# FIX: Add a new endpoint for the optimistic sync logic
@router.post("/sync-all-inventory-max")
def trigger_sync_all_inventory_max(
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    """
    Triggers a sync of all grouped products to their maximum available stock level.
    This is an optimistic sync, useful for ensuring all stores can sell as much as possible.
    """
    task_id = sync_tracker.add_task("Optimistic Inventory Sync (Max)")
    background_tasks.add_task(
        inventory_sync_service.run_sync_all_stores_with_max,
        db_factory=SessionLocal,
        task_id=task_id
    )
    return {"status": "ok", "message": "Optimistic sync started.", "tasks": [{"task_id": task_id}]}