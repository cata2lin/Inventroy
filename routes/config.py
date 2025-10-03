# routes/config.py

from fastapi import APIRouter, Depends, HTTPException, Response, Request
from sqlalchemy.orm import Session
from typing import List

import schemas
import models
from database import get_db
from crud import store as crud_store, webhooks as crud_webhook
from shopify_service import ShopifyService

router = APIRouter(
    prefix="/api/config",
    tags=["Configuration"],
    responses={404: {"description": "Not found"}},
)

# List of all webhook topics the application needs
ESSENTIAL_WEBHOOK_TOPICS = [
    "products/create", "products/update", "products/delete",
    "inventory_levels/update",
]

@router.get("/stores", response_model=List[schemas.Store])
def get_all_stores(db: Session = Depends(get_db)):
    return crud_store.get_all_stores(db)

@router.get("/stores/{store_id}", response_model=schemas.Store)
def get_single_store(store_id: int, db: Session = Depends(get_db)):
    db_store = crud_store.get_store(db, store_id=store_id)
    if not db_store:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store

@router.post("/stores", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    if db.query(models.Store).filter(models.Store.name == store.name).first():
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    return crud_store.create_store(db=db, store=store)

# --- Webhook Management Endpoints ---
@router.get("/stores/{store_id}/webhooks", response_model=List[schemas.Webhook])
def get_store_webhooks(store_id: int, db: Session = Depends(get_db)):
    """Gets all locally registered webhooks for a store."""
    return crud_webhook.get_webhook_registrations_for_store(db, store_id=store_id)

@router.post("/stores/{store_id}/webhooks/create-all", status_code=201)
def create_all_necessary_webhooks(store_id: int, request: Request, db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        base_url = str(request.base_url).replace("http://", "https://")
        correct_address = f"{base_url.rstrip('/')}/api/webhooks/{store_id}"
        
        existing_webhooks = service.get_webhooks()
        existing_webhooks_map = {wh['topic']: {'id': wh['id'], 'address': wh['address']} for wh in existing_webhooks}

        created_count, updated_count = 0, 0

        for topic in ESSENTIAL_WEBHOOK_TOPICS:
            existing = existing_webhooks_map.get(topic)
            if not existing:
                created_webhook = service.create_webhook(topic=topic, address=correct_address)
                crud_webhook.create_webhook_registration(db, store_id=store.id, webhook_data=created_webhook)
                created_count += 1
            elif existing['address'] != correct_address:
                service.delete_webhook(webhook_id=existing['id'])
                crud_webhook.delete_webhook_registration(db, shopify_webhook_id=existing['id'])
                created_webhook = service.create_webhook(topic=topic, address=correct_address)
                crud_webhook.create_webhook_registration(db, store_id=store.id, webhook_data=created_webhook)
                updated_count += 1
        
        message = f"Webhook setup complete. Created: {created_count}, Updated: {updated_count}."
        if created_count == 0 and updated_count == 0:
            message = "All necessary webhooks are already correctly registered."
            
        return {"message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create/verify webhooks: {str(e)}")

@router.delete("/stores/{store_id}/webhooks/{shopify_webhook_id}", status_code=204)
def delete_store_webhook(store_id: int, shopify_webhook_id: int, db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        service.delete_webhook(webhook_id=shopify_webhook_id)
        crud_webhook.delete_webhook_registration(db, shopify_webhook_id=shopify_webhook_id)
        return Response(status_code=204)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete webhook: {str(e)}")