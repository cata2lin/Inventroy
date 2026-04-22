# routes/webhooks.py
import hmac
import hashlib
import base64
import time
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Header, BackgroundTasks
from sqlalchemy.orm import Session

from database import get_db
import crud.store as crud_store
from services import inventory_sync_service
from services import audit_logger

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
    start_time = time.monotonic()

    if not x_shopify_hmac_sha256:
        audit_logger.log_webhook(store_id, f"store_{store_id}", x_shopify_topic or "unknown",
                                  result="rejected", error="Missing HMAC header")
        raise HTTPException(status_code=400, detail="Missing HMAC header")

    store = crud_store.get_store(db, store_id)
    if not store:
        audit_logger.log_webhook(store_id, f"store_{store_id}", x_shopify_topic or "unknown",
                                  result="rejected", error="Store not found")
        raise HTTPException(status_code=404, detail="Store not found")

    raw_body = await request.body()
    # Use api_secret for HMAC verification, as it's the standard for webhook secrets
    if not verify_webhook(raw_body, x_shopify_hmac_sha256, store.api_secret):
        audit_logger.log_webhook(store.id, store.name, x_shopify_topic or "unknown",
                                  result="rejected", error="Invalid HMAC signature")
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    try:
        payload = await request.json()
    except Exception:
        audit_logger.log_webhook(store.id, store.name, x_shopify_topic or "unknown",
                                  result="rejected", error="Malformed JSON body")
        raise HTTPException(status_code=400, detail="Malformed JSON body")

    duration_ms = int((time.monotonic() - start_time) * 1000)

    # --- Log the webhook acceptance ---
    audit_logger.log_webhook(
        store_id=store.id,
        store_name=store.name,
        topic=x_shopify_topic or "unknown",
        result="accepted",
        duration_ms=duration_ms,
        details={
            "triggered_at": x_shopify_triggered_at,
            "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None,
        }
    )

    # --- Dispatch to the correct service based on topic ---
    if x_shopify_topic == "inventory_levels/update":
        background_tasks.add_task(
            inventory_sync_service.handle_webhook, 
            store_id, 
            payload,
            x_shopify_triggered_at
        )
    elif x_shopify_topic in ["products/create", "products/update", "products/delete", "inventory_items/update", "inventory_items/delete"]:
        background_tasks.add_task(
            inventory_sync_service.handle_catalog_webhook,
            store_id,
            x_shopify_topic,
            payload
        )
    else:
        audit_logger.log_webhook(store.id, store.name, x_shopify_topic or "unknown",
                                  result="unhandled",
                                  details={"note": "No handler for this topic"})

    return {"status": "ok"}