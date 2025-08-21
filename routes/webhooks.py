# routes/webhooks.py
from fastapi import APIRouter, Depends, HTTPException, Request, Response, BackgroundTasks
from sqlalchemy.orm import Session
import hmac, hashlib, base64
from database import get_db, SessionLocal
from crud import store as crud_store, webhooks as crud_webhook, product as crud_product, order as crud_order
from services import inventory_sync_service, commited_projector  # note: file name has one 't' in repo
import schemas

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)

def _verify_shopify_hmac(secret: str, raw_body: bytes, header_value: str) -> bool:
    """
    Shopify sends X-Shopify-Hmac-Sha256 as BASE64(SHA256(secret, body)).
    We accept base64 (canonical) and hex (legacy) just in case.
    """
    if not secret or not header_value:
        return False
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    calc_b64 = base64.b64encode(digest).decode()
    return hmac.compare_digest(calc_b64, header_value) or hmac.compare_digest(digest.hex(), header_value)

@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(store_id: int, request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Central Shopify webhook endpoint (per store).
    Verifies HMAC, then routes by topic.
    Idempotency + echo suppression are handled in the services layer.
    """
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    # --- HMAC verification ---
    raw_body = await request.body()
    hmac_header = request.headers.get("x-shopify-hmac-sha256")
    secret = store.webhook_secret or store.api_secret  # prefer webhook_secret if present
    if not _verify_shopify_hmac(secret, raw_body, hmac_header):
        raise HTTPException(status_code=401, detail="HMAC verification failed")

    topic = request.headers.get("x-shopify-topic") or ""
    event_id = request.headers.get("x-shopify-webhook-id") or ""
    # Shop domain header if you need it for logging:
    # shop_domain = request.headers.get("x-shopify-shop-domain") or store.shopify_url

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # --- Topic routing ---
    if topic == "app/uninstalled":
        store.enabled = False
        db.commit()
        return Response(status_code=200, content="ok")

    # Orders / committed projector
    if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
        try:
            commited_projector.process_order_event(db, store_id, topic, payload)
        except Exception:
            pass
        return Response(status_code=200, content="ok")

    if topic in {"fulfillments/create", "fulfillments/update"}:
        try:
            commited_projector.process_fulfillment_event(db, store_id, topic, payload)
        except Exception:
            pass
        return Response(status_code=200, content="ok")

    if topic == "refunds/create":
        # Optional: hook into projector if you adjust committed on restock
        return Response(status_code=200, content="ok")

    if topic in {"products/create", "products/update"}:
        try:
            product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)
        except Exception:
            pass
        return Response(status_code=200, content="ok")

    if topic == "products/delete":
        try:
            delete_id = payload.get("id")
            if delete_id:
                crud_webhook.mark_product_as_deleted(db, product_id=int(delete_id))
        except Exception:
            pass
        return Response(status_code=200, content="ok")

    if topic == "inventory_levels/update":
        inventory_item_id = payload.get("inventory_item_id")
        location_id = payload.get("location_id")
        if not inventory_item_id or not location_id:
            return Response(status_code=200, content="ignored")
        # ✅ Background task uses a SESSION FACTORY, not a live session
        background_tasks.add_task(
            inventory_sync_service.process_inventory_update_event,
            db_factory=SessionLocal,            # pass the factory
            shop_domain=store.shopify_url,
            event_id=event_id or f"inv-{store_id}-{inventory_item_id}-{location_id}-{(hashlib.sha1(raw_body).hexdigest())}",
            inventory_item_id=int(inventory_item_id),
            location_id=int(location_id),
        )
        return Response(status_code=200, content="ok")

    if topic == "inventory_items/update":
        # You can refresh tracked/cost flags here if needed
        return Response(status_code=200, content="ok")

    # Acknowledge unknown topics to stop retries
    return Response(status_code=200, content="ok")
