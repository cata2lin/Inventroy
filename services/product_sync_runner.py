# services/product_sync_runner.py

from typing import Callable, Optional

# Robust imports for Shopify service + crud
try:
    from services.shopify_service import ShopifyService
except Exception:
    from shopify_service import ShopifyService  # type: ignore

from sqlalchemy.orm import Session

from crud import product as crud_product
from services import sync_tracker

def run_product_sync_for_store(
    db_factory: Callable[[], Session],
    store_id: int,
    shop_url: str,
    api_token: str,
    task_id: Optional[str] = None,
):
    """
    Pulls all products+variants from Shopify and upserts them page by page.
    IMPORTANT: Do NOT pass unsupported kwargs (e.g. page=) to CRUD.
    """
    db = db_factory()
    try:
        svc = ShopifyService(store_url=shop_url, token=api_token)
        gen = svc.get_all_products_and_variants()

        page_idx = 0
        for page in gen:
            # 'page' is a list like: [{"product": ProductSchema, "variants": [VariantSchema, ...]}, ...]
            try:
                crud_product.create_or_update_products(db, store_id=store_id, products=page)  # <-- no 'page=' kw
                db.commit()
            except Exception:
                db.rollback()
                raise
            page_idx += 1
            sync_tracker.step(task_id, f"Upserted page {page_idx} for store {store_id}")

        sync_tracker.complete(task_id, f"Finished product sync for store {store_id}")
    except Exception as e:
        sync_tracker.fail(task_id, f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}")
        print(f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}")
    finally:
        db.close()
