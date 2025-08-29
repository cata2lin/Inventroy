# routes/webhooks.py

import base64
import hashlib
import hmac
from typing import Optional, Any
import json

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# CRUD
from crud import store as crud_store
from crud import product as crud_product
from crud import order as crud_order
from crud import webhooks as crud_webhook

# Services
from services import committed_projector as committed_projector
from services import inventory_sync_service
# FIX: Import ShopifyService to resolve order_id from fulfillment_order_gid
from shopify_service import ShopifyService, gid_to_id

try:
    import schemas  # optional
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


@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    """
    Central webhook receiver. Verifies HMAC, routes topics, and offloads
    inventory-level work to the Golden Sync Loop in a background task.
    """
    # --- load store ---
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")

    secret = getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store API secret not configured.")

    # --- headers & raw body ---
    topic = request.headers.get("x-shopify-topic") or ""
    event_id = request.headers.get("x-shopify-webhook-id")
    raw_body = await request.body()

    # --- verify ---
    _verify_hmac(secret, raw_body, request.headers.get("x-shopify-hmac-sha256"))

    # --- parse payload ---
    try:
        payload: Any = await request.json()
    except Exception:
        payload = {}

    # Always ack quickly; heavy work must never block Shopify
    response = Response(status_code=200, content="ok")

    # --- app/uninstalled ---
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # --- Persist data and update committed stock (new unified logic) ---
    order_id = payload.get("order_id") or payload.get("id")

    try:
        if topic in {"orders/create", "orders/updated", "orders/edited"}:
            crud_order.create_or_update_order_from_webhook(db, store.id, payload)
        elif topic == "orders/delete":
            # Deleting an order also changes committed stock
            crud_webhook.delete_order_by_id(db, order_id)
        elif topic in {"products/create", "products/update", "products/delete"}:
            crud_product.create_or_update_product_from_webhook(db, store.id, payload)
        elif topic in {"fulfillments/create", "fulfillments/update"}:
            crud_order.create_or_update_fulfillment_from_webhook(db, store.id, payload)
        elif topic == "refunds/create":
            crud_order.create_refund_from_webhook(db, store.id, payload)
        elif topic == "inventory_levels/update":
            # This is handled separately below to trigger the sync loop
            pass
        
        # FIX: The dedicated hold/release logic is now called from here
        elif topic in {"fulfillment_orders/placed_on_hold", "fulfillment_orders/hold_released"}:
            fulfillment_order_data = payload.get("fulfillment_order", {})
            fulfillment_order_gid = fulfillment_order_data.get("id")
            
            if fulfillment_order_gid:
                # Use ShopifyService to get the order_id from the fulfillment_order_gid
                service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
                order_id = service.get_order_id_from_fulfillment_order_gid(fulfillment_order_gid)
                
                if order_id:
                    status = "ON_HOLD" if topic == "fulfillment_orders/placed_on_hold" else "RELEASED"
                    reason = payload.get("reason") # Use the top-level reason from the payload
                    crud_webhook.update_order_fulfillment_status_from_hold(
                        db,
                        order_id=order_id,
                        fulfillment_order_gid=fulfillment_order_gid,
                        status=status,
                        reason=reason
                    )
            else:
                print(f"[holds][error] Could not extract fulfillment_order_gid from payload for topic: {topic}")

    except Exception as e:
        db.rollback()
        print(f"[webhook-handler][error] store={store_id} topic={topic}: {e}")

    # --- inventory_levels/update -> enqueue Golden Sync Loop ---
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = (payload or {}).get("inventory_item_id")
            location_id = (payload or {}).get("location_id")
            if inventory_item_id and location_id and event_id:
                # Pass the session factory to the background task
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=int(inventory_item_id),
                    location_id=int(location_id),
                    db_factory=SessionLocal,
                    # No need to pass a session here, the factory will create one
                )
        except Exception as e:
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    return response