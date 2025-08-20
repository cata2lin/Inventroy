# routes/webhooks.py

import hmac
import hashlib
import base64
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
from database import get_db
import schemas
from crud import store as crud_store, webhooks as crud_webhook, order as crud_order
from shopify_service import ShopifyService # --- ADDED: Import ShopifyService ---

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)

@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(store_id: int, request: Request, db: Session = Depends(get_db)):
    """
    Receives, verifies, and processes all webhooks from Shopify.
    """
    store = crud_store.get_store(db, store_id=store_id)
    if not store or not store.api_secret:
        raise HTTPException(status_code=404, detail="Store not found or API Secret Key not configured.")

    shopify_hmac = request.headers.get("x-shopify-hmac-sha256")
    if not shopify_hmac:
        raise HTTPException(status_code=400, detail="Missing X-Shopify-Hmac-Sha256 header.")

    raw_body = await request.body()
    try:
        calculated_hmac = base64.b64encode(hmac.new(store.api_secret.encode(), raw_body, hashlib.sha256).digest()).decode()
        if not hmac.compare_digest(calculated_hmac, shopify_hmac):
            raise HTTPException(status_code=401, detail="HMAC verification failed.")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"HMAC verification failed: {e}")

    topic = request.headers.get("x-shopify-topic")
    payload = await request.json()
    print(f"Received webhook for store {store_id} on topic: {topic}")

    # --- Order Topics ---
    if topic in ["orders/create", "orders/updated"]:
        order_data = schemas.ShopifyOrderWebhook.parse_obj(payload)
        crud_order.create_or_update_order_from_webhook(db, store.id, order_data)
    elif topic == "orders/delete":
        delete_data = schemas.DeletePayload.parse_obj(payload)
        crud_webhook.delete_order_by_id(db, order_id=delete_data.id)

    # --- Product & Inventory Topics ---
    elif topic in ["products/create", "products/update"]:
        product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
        crud_webhook.process_product_webhook(db, store.id, product_data)
    elif topic == "products/delete":
        delete_data = schemas.DeletePayload.parse_obj(payload)
        crud_webhook.mark_product_as_deleted(db, product_id=delete_data.id)
    elif topic == "inventory_levels/update":
        crud_webhook.process_inventory_level_update(db, payload)

    # --- Fulfillment Topics ---
    elif topic in ["fulfillments/create", "fulfillments/update"]:
        fulfillment_data = schemas.ShopifyFulfillmentWebhook.parse_obj(payload)
        crud_webhook.process_fulfillment_webhook(db, store.id, fulfillment_data)
    
    # --- Fulfillment Hold Topics ---
    elif topic == "fulfillment_orders/placed_on_hold":
        webhook_data = schemas.FulfillmentOrderWebhook.parse_obj(payload)
        fulfillment_order_gid = webhook_data.fulfillment_order.get("id")
        reason = webhook_data.fulfillment_hold.reason_notes if webhook_data.fulfillment_hold else None
        
        # --- FIX: Use the service to get the order_id ---
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        order_id = service.get_order_id_from_fulfillment_order_gid(fulfillment_order_gid)
        
        if order_id:
            crud_webhook.update_order_fulfillment_status_from_hold(db, order_id, "ON_HOLD", reason)
            
    elif topic == "fulfillment_orders/hold_released":
        webhook_data = schemas.FulfillmentOrderWebhook.parse_obj(payload)
        fulfillment_order_gid = webhook_data.fulfillment_order.get("id")

        # --- FIX: Use the service to get the order_id ---
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        order_id = service.get_order_id_from_fulfillment_order_gid(fulfillment_order_gid)

        if order_id:
            crud_webhook.update_order_fulfillment_status_from_hold(db, order_id, "RELEASED")

    elif topic == "fulfillment_orders/cancellation_request_accepted":
        # This topic indicates a change, but doesn't directly map to a simple status.
        # Often it's followed by another event. For now, we can log it.
        print(f"Fulfillment cancellation for order related to {payload.get('fulfillment_order', {}).get('id')} was accepted.")

    # --- Refund Topic ---
    elif topic == "refunds/create":
        refund_data = schemas.ShopifyRefundWebhook.parse_obj(payload)
        crud_webhook.process_refund_webhook(db, store.id, refund_data)

    else:
        print(f"Received unhandled webhook topic: {topic}")

    return Response(status_code=200, content="Webhook received.")
