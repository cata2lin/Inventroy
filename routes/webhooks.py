# routes/webhooks.py

import hmac
import hashlib
import base64
from fastapi import APIRouter, Depends, HTTPException, Request, Response, BackgroundTasks
from sqlalchemy.orm import Session
from database import get_db, SessionLocal
import schemas
from crud import store as crud_store, webhooks as crud_webhook, order as crud_order, product as crud_product
from services import inventory_sync_service, committed_projector
from shopify_service import ShopifyService

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)

@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(store_id: int, request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
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
    event_id = request.headers.get("x-shopify-webhook-id")
    payload = await request.json()
    print(f"Received webhook for store {store_id} on topic: {topic}")
    
    # --- Idempotency check happens within the service layer now ---

    # --- Routing ---
    db_session = SessionLocal()
    try:
        if topic == "app/uninstalled":
            store.enabled = False
            db.commit()
            print(f"Store {store.name} has uninstalled the app. Writes have been disabled.")

        elif topic in ["orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"]:
            committed_projector.process_order_event(db_session, store_id, topic, payload)
        
        elif topic in ["fulfillments/create", "fulfillments/update"]:
            committed_projector.process_fulfillment_event(db_session, store_id, topic, payload)
        
        elif topic == "refunds/create":
            # Can also trigger committed projector if restocks are counted
            pass
        
        elif topic in ["products/create", "products/update"]:
            product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)

        elif topic == "products/delete":
            delete_data = schemas.DeletePayload.parse_obj(payload)
            crud_webhook.mark_product_as_deleted(db, product_id=delete_data.id)
            
        elif topic == "inventory_levels/update":
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")
            if inventory_item_id and location_id:
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db=db_session,
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=inventory_item_id,
                    location_id=location_id
                )
        
        elif topic == "inventory_items/update":
             # This can be used to update cost/tracked status
            pass

        else:
            print(f"Received unhandled webhook topic: {topic}")

    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {e}")
    finally:
        # For non-background tasks, we close the session here.
        # Background tasks manage their own session lifecycle.
        if not background_tasks.tasks:
            db_session.close()

    return Response(status_code=200, content="Webhook received.")