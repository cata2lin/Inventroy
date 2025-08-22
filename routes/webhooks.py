# routes/webhooks.py

import base64
import hashlib
import hmac
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# CRUD (prefer package imports)
from crud import store as crud_store
from crud import product as crud_product
from crud import webhooks as crud_webhook  # optional helpers

# Services (ALWAYS import via package so the reloader resolves modules consistently)
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

@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")

    secret = getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store API secret not configured.")

    topic = request.headers.get("x-shopify-topic")
    event_id = request.headers.get("x-shopify-webhook-id")
    raw_body = await request.body()

    _verify_hmac(secret, raw_body, request.headers.get("x-shopify-hmac-sha256"))

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Return 200 immediately; heavy work goes to background
    response = Response(status_code=200, content="ok")

    # app/uninstalled: disable store
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # committed stock projector (orders / fulfillments / refunds)
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            committed_projector.process_order_event(db, store_id, topic, payload)
        elif topic in {"fulfillments/create", "fulfillments/update"}:
            committed_projector.process_fulfillment_event(db, store_id, topic, payload)
        elif topic == "refunds/create":
            # If you want to count restocks from refund lines, handle here
            pass
    except Exception as e:
        db.rollback()
        print(f"[committed_projector][error] store={store_id} topic={topic}: {e}")

    # product upsert (schema optional & tolerant)
    try:
        if topic in {"products/create", "products/update"}:
            product_data = payload
            if schemas and hasattr(schemas, "ShopifyProductWebhook"):
                try:
                    # Some Shopify webhooks omit admin_graphql_api_id; don't rely on it
                    product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
                except Exception:
                    product_data = payload
            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)  # type: ignore[arg-type]

        elif topic == "products/delete":
            delete_id = payload.get("id")
            if delete_id:
                # optional hook if you maintain soft-deletes
                # crud_webhook.mark_product_as_deleted(db, product_id=delete_id)
                pass
    except Exception as e:
        db.rollback()
        print(f"[product-upsert][error] store={store_id} topic={topic}: {e}")

    # inventory sync (Golden Loop) – always pass a DB *factory* to avoid session reuse issues
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")
            if inventory_item_id and location_id and event_id:
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db_factory=SessionLocal,
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=int(inventory_item_id),
                    location_id=int(location_id),
                )
        except Exception as e:
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    # inventory_items/update (optional enrichment hook for cost/tracked)
    if topic == "inventory_items/update":
        pass

    return response
