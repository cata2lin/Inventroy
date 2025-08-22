# routes/sync_control.py

from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Body
from sqlalchemy.orm import Session

from database import get_db, SessionLocal

from crud import store as crud_store
from services import product_sync_runner
from services import order_sync_runner

router = APIRouter(prefix="/api/sync-control", tags=["Sync Control"])


@router.post("/products")
def trigger_product_sync(background_tasks: BackgroundTasks, db: Session = Depends(get_db)) -> Dict[str, Any]:
    stores = crud_store.get_all_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No stores configured.")
    for s in stores:
        background_tasks.add_task(
            product_sync_runner.run_product_sync_for_store,
            db_factory=SessionLocal,
            store_id=s.id,
        )
    return {"status": "ok", "message": "Product sync kicked off for all stores."}


@router.post("/products/{store_id}")
def trigger_product_sync_for_store(store_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")
    background_tasks.add_task(
        product_sync_runner.run_product_sync_for_store,
        db_factory=SessionLocal,
        store_id=store.id,
    )
    return {"status": "ok", "message": f"Product sync kicked off for store {store.name}."}


# ---------------- Orders ----------------

@router.post("/orders")
def trigger_orders_sync_all(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    stores = crud_store.get_all_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No stores configured.")
    for s in stores:
        background_tasks.add_task(
            order_sync_runner.run_orders_sync_for_store,
            db_factory=SessionLocal,
            store_id=s.id,
            created_at_min=None,
            created_at_max=None,
        )
    return {"status": "ok", "message": "Order sync kicked off for all stores."}


@router.post("/orders/{store_id}")
def trigger_orders_sync_for_store(
    store_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")
    background_tasks.add_task(
        order_sync_runner.run_orders_sync_for_store,
        db_factory=SessionLocal,
        store_id=store.id,
        created_at_min=None,
        created_at_max=None,
    )
    return {"status": "ok", "message": f"Order sync kicked off for store {store.name}."}


@router.post("/orders/range")
def trigger_orders_sync_range(
    payload: Dict[str, Optional[str]] = Body(..., example={"start": "2025-08-01T00:00:00Z", "end": "2025-08-22T23:59:59Z"}),
    background_tasks: BackgroundTasks = Depends(),
    db: Session = Depends(get_db),
):
    start = payload.get("start")
    end = payload.get("end")
    if not start and not end:
        raise HTTPException(status_code=400, detail="Provide at least one of 'start' or 'end'.")
    stores = crud_store.get_all_stores(db)
    if not stores:
        raise HTTPException(status_code=404, detail="No stores configured.")

    for s in stores:
        background_tasks.add_task(
            order_sync_runner.run_orders_sync_for_store,
            db_factory=SessionLocal,
            store_id=s.id,
            created_at_min=start,
            created_at_max=end,
        )
    return {"status": "ok", "message": f"Order sync (range) kicked off for all stores.", "start": start, "end": end}
