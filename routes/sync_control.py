# routes/sync_control.py

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
from crud import store as crud_store
from services.product_sync_runner import run_product_sync_for_store
from services import sync_tracker

router = APIRouter(prefix="/api", tags=["Sync Control"])

@router.post("/sync-control/products")
def trigger_product_sync(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    stores = crud_store.get_all_stores(db)
    for store in stores:
        if not store.enabled:
            continue
        task_id = sync_tracker.create_task(f"Products for {store.name}")
        background_tasks.add_task(
            run_product_sync_for_store,
            db_factory=SessionLocal,
            store_id=store.id,
            shop_url=store.shopify_url,
            api_token=store.api_token,
            task_id=task_id,
        )
    return {"status": "ok"}
