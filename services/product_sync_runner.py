# services/product_sync_runner.py

from typing import Any, Dict, List, Optional, Iterable
import uuid
import inspect
import traceback

from sqlalchemy.orm import Session

# Your Shopify API client lives at project root
from shopify_service import ShopifyService

# Progress tracker (simple in-memory)
from services import sync_tracker

# Prefer your CRUD module
from crud import product as crud_product


def _as_dict(obj: Any) -> Any:
    """
    Convert pydantic models or objects with .dict() to plain dicts recursively.
    Leaves primitives/lists/dicts untouched.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _as_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_as_dict(v) for v in obj]
    # pydantic or similar
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        try:
            return obj.dict()
        except Exception:
            pass
    # best effort
    try:
        return dict(obj)  # type: ignore[arg-type]
    except Exception:
        return obj


def _normalize_page(page: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    ShopifyService.get_all_products_and_variants() yields a page as:
        [{ "product": <schemas.Product>, "variants": [<schemas.ProductVariant>, ...] }, ...]
    Ensure everything is plain dicts so CRUD can work with it consistently.
    """
    norm: List[Dict[str, Any]] = []
    for item in page or []:
        if isinstance(item, dict):
            product = _as_dict(item.get("product"))
            variants = _as_dict(item.get("variants"))
            norm.append({"product": product, "variants": variants})
        else:
            norm.append(_as_dict(item))
    return norm


def _call_crud_upsert(db: Session, store_id: int, page: List[Dict[str, Any]]) -> None:
    """
    Be tolerant to different CRUD signatures:
      - create_or_update_products(db, store_id, page)
      - create_or_update_products(db=db, store_id=..., page=page)
      - create_or_update_products(db=db, store_id=..., products=page)
      - alt names: upsert_products / create_or_update_products_batch
    """
    # 1) choose the function name
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

    # 2) Prefer positional first to avoid keyword mismatches
    try:
        fn(db, store_id, page)  # type: ignore[misc]
        return
    except TypeError:
        pass

    # 3) Try common keyword sets
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

    # 4) Introspect and build kwargs dynamically as a last resort
    sig = inspect.signature(fn)
    kwargs: Dict[str, Any] = {}
    for p in sig.parameters.values():
        if p.name in ("db", "session"):
            kwargs[p.name] = db
        elif p.name in ("store_id", "storeid", "sid"):
            kwargs[p.name] = store_id
        elif p.name in ("page", "products", "items", "records", "batch"):
            kwargs[p.name] = page
    try:
        fn(**kwargs)  # type: ignore[misc]
        return
    except Exception as e:
        raise TypeError(f"{fn.__name__}() signature not compatible; tried kwargs {list(kwargs.keys())}") from e


def run_product_sync_for_store(
    db_factory,
    store_id: int,
    shop_url: str,
    api_token: str,
    task_id: Optional[str] = None,
):
    """
    Background job: pull all products+variants via GraphQL and upsert into DB.
    Resilient to different CRUD signatures and pydantic vs dict payloads.
    """
    if not task_id:
        task_id = str(uuid.uuid4())

    print(f"Starting product data fetch from https://{shop_url}/admin/api/2025-04/graphql.json...")

    # local session owned by the task
    db: Session = db_factory()
    processed = 0
    try:
        svc = ShopifyService(store_url=shop_url, token=api_token)

        for page in svc.get_all_products_and_variants():
            norm = _normalize_page(page)
            if not norm:
                continue

            try:
                _call_crud_upsert(db, store_id, norm)
                db.commit()
            except Exception:
                db.rollback()
                raise

            processed += len(norm)
            sync_tracker.step(task_id, processed, note=f"Upserted {processed} items so far")

        sync_tracker.finish_task(task_id, ok=True, note=f"Completed. Total items: {processed}")

    except Exception as e:
        sync_tracker.finish_task(task_id, ok=False, note=f"Failed after {processed}. {e}")
        # mirror your existing log format
        print(
            f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}\n"
            f"{traceback.format_exc()}"
        )
    finally:
        db.close()
