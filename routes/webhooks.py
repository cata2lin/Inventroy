# routes/webhooks.py

import base64
import hashlib
import hmac
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# CRUD (package imports)
from crud import store as crud_store
from crud import product as crud_product
try:
    from crud import webhooks as crud_webhook  # optional helpers if you have them
except Exception:  # pragma: no cover
    crud_webhook = None  # type: ignore

# Services (package imports only; no fallback-to-root to avoid reload import errors)
from services import inventory_sync_service
from services import commited_projector as committed_projector  # note single 't' in filename

# Schemas are optional; if missing we fall back to raw dicts
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
    """
    Verify Shopify webhook HMAC using the store's secret.
    Raises HTTPException if verification fails.
    """
    if not header_hmac:
        raise HTTPException(status_code=400, detail="Missing X-Shopify-Hmac-SHA256 header.")
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    if not hmac.compare_digest(computed, header_hmac):
        raise HTTPException(status_code=401, detail="HMAC verification failed.")


def _to_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    """
    Central webhook receiver. Verifies HMAC, routes topics,
    and offloads inventory propagation to the Golden Sync Loop
    via a background task. Always returns 200 quickly so Shopify
    doesn't retry due to our processing latency.
    """
    # --- load store ---
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")

    # Prefer webhook_secret when present; fall back to api_secret
    secret = getattr(store, "webhook_secret", None) or getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store secret not configured.")

    # --- headers & raw body ---
    topic = request.headers.get("x-shopify-topic")
    event_id = request.headers.get("x-shopify-webhook-id")
    raw_body = await request.body()

    # --- verify signature ---
    _verify_hmac(secret, raw_body, request.headers.get("x-shopify-hmac-sha256"))

    # Parse JSON body (best effort)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Return immediately; do the heavy lifting in background/DB tx
    response = Response(status_code=200, content="ok")

    # --- app/uninstalled: disable store to stop all writes ---
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # --- committed-stock projector (orders / fulfillments / refunds) ---
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            committed_projector.process_order_event(db, store_id, topic, payload)

        elif topic in {"fulfillments/create", "fulfillments/update"}:
            committed_projector.process_fulfillment_event(db, store_id, topic, payload)

        elif topic == "refunds/create":
            # If you track restocks from refund lines, handle here as needed
            pass

    except Exception as e:
        db.rollback()
        # Never fail the webhook; just log the projector error
        print(f"[committed_projector][error] store={store_id} topic={topic}: {e}")

    # --- product webhooks -> upsert (schema optional; always pass a dict downstream) ---
    try:
        if topic in {"products/create", "products/update"}:
            if schemas and hasattr(schemas, "ShopifyProductWebhook"):
                try:
                    model_obj = schemas.ShopifyProductWebhook.parse_obj(payload)
                    # Always pass dict to CRUD to avoid attribute errors like
                    # "'ShopifyProductWebhook' object has no attribute 'admin_graphql_api_id'"
                    product_data = model_obj.dict(by_alias=True)
                except Exception:
                    product_data = payload  # fallback to raw dict
            else:
                product_data = payload

            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)  # type: ignore[arg-type]

        elif topic == "products/delete":
            delete_id = payload.get("id")
            if delete_id and crud_webhook and hasattr(crud_webhook, "mark_product_as_deleted"):
                try:
                    crud_webhook.mark_product_as_deleted(db, product_id=_to_int(delete_id) or delete_id)  # type: ignore
                except Exception:
                    # Non-fatal
                    pass

    except Exception as e:
        db.rollback()
        print(f"[product-upsert][error] store={store_id} topic={topic}: {e}")

    # --- inventory_levels/update -> Golden Sync Loop (runs in background) ---
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = _to_int(payload.get("inventory_item_id"))
            location_id = _to_int(payload.get("location_id"))

            if inventory_item_id is not None and location_id is not None and event_id:
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db_factory=SessionLocal,  # pass a *factory*; the service will manage its own session
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=inventory_item_id,
                    location_id=location_id,
                )
        except Exception as e:
            # Never fail the webhook; just log
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    # (Optional) inventory_items/update, products/* other topics can be handled here

    return response
