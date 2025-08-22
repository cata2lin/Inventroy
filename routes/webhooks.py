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

# CRUD (prefer package imports)
from crud import store as crud_store
from crud import product as crud_product
from crud import webhooks as crud_webhook  # optional helpers

# Services (force package import so it never falls back to root)
from services import inventory_sync_service
from services import commited_projector as committed_projector  # note 1 't' in filename

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
    """
    Recursively convert dicts → SimpleNamespace so downstream code can use attribute access.
    Lists and scalars are preserved; nested dicts are converted too.
    """
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

    # --- ORDERS / FULFILLMENTS → committed projector ---
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            # Prefer your schema if present; otherwise convert dict → attribute object
            if schemas and hasattr(schemas, "ShopifyOrderWebhook"):
                try:
                    order_obj = schemas.ShopifyOrderWebhook.parse_obj(payload)
                except Exception:
                    order_obj = _to_attr(payload)
            else:
                order_obj = _to_attr(payload)

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
            # If you handle restock deltas off refunds, parse similarly:
            # refund_obj = _to_attr(payload)   # or a schemas model, if you have one
            pass

    except Exception as e:
        db.rollback()
        print(f"[orders][error] store={store_id} topic={topic}: {e}")

    # --- PRODUCTS → upsert + (optional) group updates ---
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

        elif topic == "products/delete":
            # Minimal delete handling (optional)
            delete_id = payload.get("id")
            if delete_id:
                # If you maintain soft-deletes, call your crud here.
                # Example:
                # try: crud_webhook.mark_product_as_deleted(db, product_id=int(delete_id))
                # except Exception: pass
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
        # enrich cost/tracked flags here if desired
        pass

    return response
