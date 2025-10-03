# services/product_sync_runner.py
from __future__ import annotations

from typing import Optional
import traceback

from sqlalchemy.orm import Session

try:
    from session import SessionLocal
except Exception:
    from database import SessionLocal  # type: ignore

from crud import store as crud_store
from crud import product as crud_product
from . import sync_tracker  # <-- THE CRITICAL FIX IS HERE
from shopify_service import ShopifyService


def run_product_sync_for_store(
    db_factory=SessionLocal,
    store_id: int = 0,
    shop_url: Optional[str] = None,
    api_token: Optional[str] = None,
    task_id: Optional[str] = None,
):
    """
    Background task that syncs all products & variants for ONE store.
    """
    db: Session = db_factory()
    processed = 0
    try:
        store = crud_store.get_store(db, store_id)
        if not store:
            raise RuntimeError(f"Store id {store_id} not found")

        shop = shop_url or store.shopify_url
        token = api_token or store.api_token
        if not shop or not token:
            raise RuntimeError(f"Missing credentials for store id {store_id}")

        if task_id:
            sync_tracker.step(task_id, 0, note=f"Fetching products from {shop}...")

        svc = ShopifyService(store_url=shop, token=token)

        for page in svc.get_all_products_and_variants():
            if not page:
                continue
            crud_product.create_or_update_products(db, store_id=store_id, items=page)
            db.commit()
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