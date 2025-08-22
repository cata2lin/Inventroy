# routes/sync_control.py

from __future__ import annotations

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# services
from services import sync_tracker
try:
    from services.shopify_service import ShopifyService
except Exception:  # pragma: no cover
    from shopify_service import ShopifyService  # type: ignore

# crud
try:
    from crud import product as crud_product
except Exception:  # pragma: no cover
    import product as crud_product  # type: ignore

router = APIRouter(prefix="/api/sync-control", tags=["Sync Control"])


@router.get("/tasks")
def list_tasks():
    return {"tasks": sync_tracker.get_all_tasks()}


def _sync_products_for_store(store_id: int, task_id: str):
    """
    Background job: pull all products & variants for a store and upsert into DB.
    Uses ShopifyService.get_all_products_and_variants() generator and the
    new CRUD entrypoint crud_product.create_or_update_products(db, store_id, page).
    """
    db = SessionLocal()
    try:
        sync_tracker.start_task(task_id)

        store: models.Store = db.get(models.Store, store_id)
        if not store or not store.enabled:
            raise RuntimeError("Store not found or disabled")

        svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)

        for page in svc.get_all_products_and_variants():
            # page: List[{"product": schemas.Product, "variants": [schemas.ProductVariant, ...]}, ...]
            crud_product.create_or_update_products(db, store_id=store.id, page=page)

        sync_tracker.complete_task(task_id)
    except Exception as e:
        db.rollback()
        sync_tracker.fail_task(task_id, str(e))
        print(f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}")
    finally:
        db.close()


@router.post("/products")
def trigger_product_sync(background: BackgroundTasks, db: Session = Depends(get_db)):
    stores = db.query(models.Store).filter(models.Store.enabled == True).all()
    if not stores:
        raise HTTPException(status_code=400, detail="No enabled stores.")

    task_ids = []
    for store in stores:
        tid = sync_tracker.create_task(f"Products for {store.name}")
        background.add_task(_sync_products_for_store, store.id, tid)
        task_ids.append({"store_id": store.id, "task_id": tid})

    return {"accepted": True, "tasks": task_ids}
