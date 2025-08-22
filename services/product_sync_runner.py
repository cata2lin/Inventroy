# services/product_sync_runner.py

from typing import Any, Dict, List, Optional
import uuid
import inspect
import traceback

from sqlalchemy.orm import Session

# Your Shopify API client lives at project root (not under services/)
from shopify_service import ShopifyService

# Tiny in-memory tracker
from services import sync_tracker

# Prefer your CRUD module
from crud import product as crud_product


def _call_crud_upsert(db: Session, store_id: int, page: List[Any]) -> None:
    """
    Be tolerant to different CRUD signatures:
      - create_or_update_products(db, store_id, page)
      - create_or_update_products(db=db, store_id=..., page=page)
      - create_or_update_products(db=db, store_id=..., products=page)
      - alt names: upsert_products / create_or_update_products_batch
    """
    fn = None
    for name in (
        "create_or_update_products",
        "create_or_update_products_batch",
        "upsert_products",
        "bulk_upsert_products",
    ):
        fn = getattr(crud_product, name, None)
        if callable(fn):
            break
    if fn is None:
        raise RuntimeError("crud.product has no suitable upsert function (tried several names).")

    # Prefer positional first (avoids keyword mismatches)
    try:
        fn(db, store_id, page)  # type: ignore[misc]
        return
    except TypeError:
        pass

    # Try common keyword sets
    try:
        fn(db=db, store_id=store_id, page=page)  # type: ignore[misc]
        return
    except TypeError:
        pass

    try:
        fn(db=db, store_id=store_id, products=page)  # type: ignore[misc]
        return
    except TypeError:
        pass

    # Last resort: introspect signature and map common parameter names
    sig = inspect.signature(fn)
    kwargs: Dict[str, Any] = {}
    for p in sig.parameters.values():
        if p.name in ("db", "session"):
            kwargs[p.name] = db
        elif p.name in ("store_id", "storeid", "sid"):
            kwargs[p.name] = store_id
        elif p.name in ("page", "products", "items", "records", "batch"):
            kwargs[p.name] = page
    fn(**kwargs)  # type: ignore[misc]


def run_product_sync_for_store(
    db_factory,
    store_id: int,
    shop_url: str,
    api_token: str,
    task_id: Optional[str] = None,
):
    """
    Background job: pull all products+variants via GraphQL and upsert into DB.

    IMPORTANT: We now pass pages through untouched so that CRUD receives the
    original Pydantic models/objects (with attribute access like .legacyResourceId).
    """
    if not task_id:
        task_id = str(uuid.uuid4())

    print(f"Starting product data fetch from https://{shop_url}/admin/api/2025-04/graphql.json...")

    db: Session = db_factory()
    processed = 0
    try:
        svc = ShopifyService(store_url=shop_url, token=api_token)

        for page in svc.get_all_products_and_variants():
            # DO NOT normalize to dicts — CRUD expects attribute access
            try:
                _call_crud_upsert(db, store_id, page)  # pass through unchanged
                db.commit()
            except Exception:
                db.rollback()
                raise

            processed += len(page or [])
            sync_tracker.step(task_id, processed, note=f"Upserted {processed} items so far")

        sync_tracker.finish_task(task_id, ok=True, note=f"Completed. Total items: {processed}")

    except Exception as e:
        sync_tracker.finish_task(task_id, ok=False, note=f"Failed after {processed}. {e}")
        print(
            f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}\n"
            f"{traceback.format_exc()}"
        )
    finally:
        db.close()
