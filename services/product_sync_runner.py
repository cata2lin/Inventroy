from typing import Optional
import traceback
from sqlalchemy.orm import Session
from database import SessionLocal
from crud import store as crud_store
from crud import product as crud_product
from services import sync_tracker
from shopify_service import ShopifyService

def run_product_sync_for_store(
    db_factory=SessionLocal,
    store_id: int = 0,
    task_id: Optional[str] = None,
):
    db: Session = db_factory()
    processed = 0
    try:
        store = crud_store.get_store(db, store_id)
        if not store:
            raise RuntimeError(f"Store id {store_id} not found")

        if task_id:
            sync_tracker.step(task_id, 0, note=f"Fetching products from {store.shopify_url}...")

        svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)

        for page in svc.get_all_products_and_variants():
            if not page:
                continue
            crud_product.create_or_update_products(db, store_id=store_id, items=page)
            processed += len(page)
            if task_id:
                sync_tracker.step(task_id, processed, note=f"Upserted {processed} products so far")

        if task_id:
            sync_tracker.finish_task(task_id, ok=True, note=f"Completed. Total: {processed}")

    except Exception as e:
        if task_id:
            sync_tracker.finish_task(task_id, ok=False, note=f"Failed after {processed}. {e}")
        print(f"[product-sync][store={store_id}] ERROR: {e}\n{traceback.format_exc()}")
    finally:
        db.close()