# services/order_sync_runner.py
from __future__ import annotations

from typing import Optional, List, Any
import traceback

from sqlalchemy.orm import Session

try:
    from session import SessionLocal
except Exception:
    from database import SessionLocal  # type: ignore

from crud import store as crud_store
from crud import order as crud_order
from services import sync_tracker
from shopify_service import ShopifyService
import schemas


def _ensure_attrs_compat(order_obj: schemas.ShopifyOrder) -> schemas.ShopifyOrder:
    """
    Ensure fields older code expects exist on the object. Prevents AttributeError crashes.
    """
    # pydantic models allow attribute assignment by default (v1 and v2)
    defaults = {
        "cancel_reason": None,
        "paymentGatewayNames": [],  # GraphQL sometimes omits
    }
    for k, v in defaults.items():
        if not hasattr(order_obj, k):
            try:
                setattr(order_obj, k, v)
            except Exception:
                pass
    return order_obj


def _coerce_order(o: Any) -> Optional[schemas.ShopifyOrder]:
    """
    Convert raw dicts to schemas.ShopifyOrder, ensure compatibility attrs, or return None if invalid.
    """
    if isinstance(o, schemas.ShopifyOrder):
        return _ensure_attrs_compat(o)

    if isinstance(o, dict):
        try:
            # Pydantic v2 first
            if hasattr(schemas.ShopifyOrder, "model_validate"):
                obj = schemas.ShopifyOrder.model_validate(o)  # type: ignore[attr-defined]
            else:
                obj = schemas.ShopifyOrder.parse_obj(o)  # v1 fallback
            return _ensure_attrs_compat(obj)
        except Exception:
            return None

    return None


def run_orders_sync_for_store(
    db_factory=SessionLocal,
    store_id: int = 0,
    created_at_min: Optional[str] = None,
    created_at_max: Optional[str] = None,
    task_id: Optional[str] = None,
):
    """
    Background task that syncs orders for ONE store.

    created_at_min / created_at_max are optional ISO-8601 date strings (“YYYY-MM-DD”).
    """
    db: Session = db_factory()
    processed = 0
    try:
        store = crud_store.get_store(db, store_id)
        if not store:
            raise RuntimeError(f"Store id {store_id} not found")

        if task_id:
            sync_tracker.step(task_id, 0, note=f"Fetching orders from {store.shopify_url}...")

        svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)

        # Expect generator of order “pages”
        for page in svc.get_all_orders_and_related_data(created_at_min, created_at_max):
            safe_page: List[schemas.ShopifyOrder] = []
            for raw in page or []:
                obj = _coerce_order(raw)
                if obj:
                    safe_page.append(obj)

            if not safe_page:
                continue

            crud_order.create_or_update_orders(db, orders_data=safe_page, store_id=store_id)
            db.commit()
            processed += len(safe_page)

            if task_id:
                sync_tracker.step(task_id, processed, note=f"Upserted {processed} orders so far")

        if task_id:
            sync_tracker.finish_task(task_id, ok=True, note=f"Completed. Total: {processed}")

    except Exception as e:
        if task_id:
            sync_tracker.finish_task(task_id, ok=False, note=f"Failed after {processed}. {e}")
        print(f"[orders-sync][store={store_id}] ERROR: {e}\n{traceback.format_exc()}")
    finally:
        db.close()


def run_orders_sync_all_stores(
    db_factory=SessionLocal,
    created_at_min: Optional[str] = None,
    created_at_max: Optional[str] = None,
):
    """
    Convenience runner to sync all stores sequentially (non-background).
    """
    db: Session = db_factory()
    try:
        stores = crud_store.get_enabled_stores(db)
        for s in stores:
            run_orders_sync_for_store(db_factory, s.id, created_at_min, created_at_max)
    finally:
        db.close()
