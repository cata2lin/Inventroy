# routes/webhooks.py
import hmac
import hashlib
import base64
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Header, BackgroundTasks
from sqlalchemy.orm import Session

from database import get_db
import crud.store as crud_store
from services import inventory_sync_service # Main service import

router = APIRouter(prefix="/api/webhooks", tags=["Webhooks"])

def verify_webhook(data: bytes, hmac_header: str, secret: str) -> bool:
    """Verify the HMAC signature of the webhook request."""
    if not secret: return False
    digest = hmac.new(secret.encode('utf-8'), data, digestmod=hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest)
    return hmac.compare_digest(computed_hmac, hmac_header.encode('utf-8'))

@router.post("/{store_id}")
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_hmac_sha256: str = Header(None),
    x_shopify_topic: str = Header(None),
    x_shopify_triggered_at: str = Header(None),
    db: Session = Depends(get_db)
):
    """
    Receives all webhooks, verifies them, and dispatches them to the
    correct background service based on the topic.
    """
    if not x_shopify_hmac_sha256:
        raise HTTPException(status_code=400, detail="Missing HMAC header")

    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    raw_body = await request.body()
    # Use api_secret for HMAC verification, as it's the standard for webhook secrets
    if not verify_webhook(raw_body, x_shopify_hmac_sha256, store.api_secret):
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    payload = await request.json()

    # --- Dispatch to the correct service based on topic ---
    if x_shopify_topic == "inventory_levels/update":
        # This is a high-priority stock sync event
        background_tasks.add_task(
            inventory_sync_service.handle_webhook, 
            store_id, 
            payload,
            x_shopify_triggered_at
        )
    elif x_shopify_topic in ["products/create", "products/update", "products/delete", "inventory_items/update", "inventory_items/delete"]:
        # These are catalog management events
        background_tasks.add_task(
            inventory_sync_service.handle_catalog_webhook,
            store_id,
            x_shopify_topic,
            payload
        )
    else:
        print(f"Received unhandled webhook topic: {x_shopify_topic}")

    return {"status": "ok"}