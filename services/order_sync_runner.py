# services/order_sync_runner.py
from __future__ import annotations

from typing import Optional, List, Any
import traceback
import json # Import the json library for pretty-printing

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


# --------- helpers to normalize / harden payloads ---------

def _set_if_missing(obj: Any, target: str, sources: List[str], default: Any = None) -> None:
    """If obj.target is missing, copy from the first available obj.<source> in sources, else default."""
    if hasattr(obj, target):
        return
    for s in sources:
        if hasattr(obj, s):
            try:
                setattr(obj, target, getattr(obj, s))
                return
            except Exception:
                break
    try:
        setattr(obj, target, default)
    except Exception:
        pass


def _ensure_attrs_compat(order_obj: schemas.ShopifyOrder, store_currency: Optional[str]) -> schemas.ShopifyOrder:
    """
    Normalize common GraphQL names to legacy names the CRUD layer uses and
    ensure attributes exist so we don't crash on AttributeError.
    """
    _set_if_missing(order_obj, "created_at", ["createdAt"])
    _set_if_missing(order_obj, "updated_at", ["updatedAt"])
    _set_if_missing(order_obj, "processed_at", ["processedAt"])
    _set_if_missing(order_obj, "closed_at", ["closedAt"])

    if hasattr(order_obj, "canceledAt") and not hasattr(order_obj, "cancelledAt"):
        try:
            setattr(order_obj, "cancelledAt", getattr(order_obj, "canceledAt"))
        except Exception:
            pass
    _set_if_missing(order_obj, "cancelled_at", ["cancelledAt", "canceledAt"])

    _set_if_missing(order_obj, "financial_status", ["financialStatus"])
    _set_if_missing(order_obj, "fulfillment_status", ["fulfillmentStatus"])

    if not hasattr(order_obj, "currency"):
        fallback = None
        if hasattr(order_obj, "currencyCode"):
            fallback = getattr(order_obj, "currencyCode")
        elif hasattr(order_obj, "presentmentCurrencyCode"):
            fallback = getattr(order_obj, "presentmentCurrencyCode")
        elif store_currency:
            fallback = store_currency
        try:
            setattr(obj, target, fallback)
        except Exception:
            pass

    if not hasattr(order_obj, "paymentGatewayNames"):
        try:
            setattr(order_obj, "paymentGatewayNames", [])
        except Exception:
            pass

    if not hasattr(order_obj, "cancel_reason"):
        try:
            setattr(order_obj, "cancel_reason", None)
        except Exception:
            pass

    for name in ("subtotal_price", "total_price", "total_tax", "total_discounts"):
        if not hasattr(order_obj, name):
            try:
                setattr(order_obj, name, None)
            except Exception:
                pass

    return order_obj


def _coerce_order(o: Any, store_currency: Optional[str]) -> Optional[schemas.ShopifyOrder]:
    """
    Convert raw dicts to schemas.ShopifyOrder, ensure compatibility attrs, or return None if invalid.
    """
    if isinstance(o, schemas.ShopifyOrder):
        return _ensure_attrs_compat(o, store_currency)

    if isinstance(o, dict):
        try:
            if hasattr(schemas.ShopifyOrder, "model_validate"):
                obj = schemas.ShopifyOrder.model_validate(o)
            else:
                obj = schemas.ShopifyOrder.parse_obj(o)
            return _ensure_attrs_compat(obj, store_currency)
        except Exception:
            return None

    return None


# --------- runner entrypoints ---------

def run_orders_sync_for_store(
    db_factory=SessionLocal,
    store_id: int = 0,
    created_at_min: Optional[str] = None,
    created_at_max: Optional[str] = None,
    task_id: Optional[str] = None,
):
    """
    Background task that syncs orders for ONE store.
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

        for page in svc.get_all_orders_and_related_data(created_at_min, created_at_max):
            safe_page: List[schemas.ShopifyOrder] = []
            for raw in page or []:
                obj = _coerce_order(raw, getattr(store, "currency", None))
                if obj:
                    safe_page.append(obj)

            if not safe_page:
                continue
            
            # --- START: Diagnostic Logging Block ---
            # This will attempt the operation and, if it fails, print the exact data.
            try:
                crud_order.create_or_update_orders(db, orders_data=safe_page, store_id=store_id)
            except AttributeError as e:
                print("\n--- RAW ORDER PAYLOAD CAUSING CRASH ---")
                print(f"Error: {e}")
                # Convert Pydantic models back to dictionaries for clean printing
                payload_to_print = [order.model_dump(mode='json') for order in safe_page]
                print(json.dumps(payload_to_print, indent=2))
                print("--- END RAW PAYLOAD ---")
                # Re-raise the exception to stop the process and show the original traceback
                raise e
            # --- END: Diagnostic Logging Block ---

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