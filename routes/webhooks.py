# routes/webhooks.py

import base64
import hashlib
import hmac
from types import SimpleNamespace
from typing import Optional, Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# CRUD
from crud import store as crud_store
from crud import product as crud_product
from crud import order as crud_order            # NEW: order upsert
from crud import webhooks as crud_webhook       # optional helpers

# Services
from services import inventory_sync_service
from services import commited_projector as committed_projector  # note one 't'

try:
    import schemas
except Exception:  # pragma: no cover
    schemas = None  # type: ignore

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)


def _verify_hmac(secret: str, raw_body: bytes, header_hmac: Optional[str]) -> None:
    if not header_hmac:
        raise HTTPException(status_code=400, detail="Missing X-Shopify-Hmac-SHA256 header.")
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    if not hmac.compare_digest(computed, header_hmac):
        raise HTTPException(status_code=401, detail="HMAC verification failed.")


def _to_attr(obj: Any) -> Any:
    """Recursively convert dicts → SimpleNamespace for attribute access."""
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: _to_attr(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return [_to_attr(v) for v in obj]
    return obj


@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    # --- load store ---
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")

    secret = getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store API secret not configured.")

    # --- headers & raw body ---
    topic = request.headers.get("x-shopify-topic")
    event_id = request.headers.get("x-shopify-webhook-id")
    raw_body = await request.body()

    # --- verify ---
    _verify_hmac(secret, raw_body, request.headers.get("x-shopify-hmac-sha256"))

    # --- parse payload (dict) ---
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Always ack quickly; heavy processing is best-effort in-process/background.
    response = Response(status_code=200, content="ok")

    # --- app/uninstalled: disable store ---
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # --- ORDERS / FULFILLMENTS ---
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            # 1) Parse to attr-access object or schema
            if schemas and hasattr(schemas, "ShopifyOrderWebhook"):
                try:
                    order_obj = schemas.ShopifyOrderWebhook.parse_obj(payload)
                except Exception:
                    order_obj = _to_attr(payload)
            else:
                order_obj = _to_attr(payload)

            # 2) Save / upsert order in DB (NEW)
            try:
                crud_order.upsert_order_from_webhook(db, store.id, order_obj)
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"[orders][save-error] store={store_id} topic={topic}: {e}")

            # 3) Continue with committed-stock projector for deltas
            committed_projector.process_order_event(db, store_id, topic, order_obj)

        elif topic in {"fulfillments/create", "fulfillments/update"}:
            if schemas and hasattr(schemas, "ShopifyFulfillmentWebhook"):
                try:
                    fulfillment_obj = schemas.ShopifyFulfillmentWebhook.parse_obj(payload)
                except Exception:
                    fulfillment_obj = _to_attr(payload)
            else:
                fulfillment_obj = _to_attr(payload)

            committed_projector.process_fulfillment_event(db, store_id, topic, fulfillment_obj)

        elif topic == "refunds/create":
            # If you later count restocks from refunds, parse & persist similarly.
            pass

    except Exception as e:
        db.rollback()
        print(f"[orders][error] store={store_id} topic={topic}: {e}")

    # --- PRODUCTS ---
    try:
        if topic in {"products/create", "products/update"}:
            if schemas and hasattr(schemas, "ShopifyProductWebhook"):
                try:
                    product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
                except Exception:
                    product_data = payload  # fallback
            else:
                product_data = payload  # fallback

            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)  # type: ignore[arg-type]
            db.commit()

        elif topic == "products/delete":
            delete_id = payload.get("id")
            if delete_id:
                # Optional: soft-delete code here if you keep tombstones
                pass

    except Exception as e:
        db.rollback()
        print(f"[product-upsert][error] store={store_id} topic={topic}: {e}")

    # --- INVENTORY LEVELS → Golden Sync Loop (background) ---
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")
            if inventory_item_id and location_id and event_id:
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db_factory=SessionLocal,            # pass factory; service owns lifecycle
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=int(inventory_item_id),
                    location_id=int(location_id),
                )
        except Exception as e:
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    # inventory_items/update (optional enrichment)
    if topic == "inventory_items/update":
        pass

    return response
