# routes/config.py

from fastapi import APIRouter, Depends, HTTPException
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

@router.get("/stores", response_model=List[schemas.Store])
def get_all_stores(db: Session = Depends(get_db)):
    """Retrieves a list of all configured stores from the database."""
    return crud_store.get_stores(db)

@router.get("/stores/{store_id}", response_model=schemas.Store)
def get_single_store(store_id: int, db: Session = Depends(get_db)):
    """Retrieves a single store by its ID."""
    db_store = crud_store.get_store(db, store_id=store_id)
    if not db_store:
        raise HTTPException(status_code=404, detail="Store not found")
    return db_store

@router.post("/stores", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    """Adds a new Shopify store to the database."""
    db_store = db.query(models.Store).filter(models.Store.name == store.name).first()
    if db_store:
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    return crud_store.create_store(db=db, store=store)

@router.put("/stores/{store_id}", response_model=schemas.Store)
def update_store_details(store_id: int, store_update: schemas.StoreUpdate, db: Session = Depends(get_db)):
    """Updates a store's details."""
    updated_store = crud_store.update_store(db, store_id=store_id, store_update=store_update)
    if not updated_store:
        raise HTTPException(status_code=404, detail="Store not found")
    return updated_store

# --- ADDED: Webhook Management Endpoints ---

@router.get("/stores/{store_id}/webhooks", response_model=List[schemas.Webhook])
def get_store_webhooks(store_id: int, db: Session = Depends(get_db)):
    """Gets all locally registered webhooks for a store."""
    return crud_webhook.get_webhook_registrations_for_store(db, store_id=store_id)

@router.post("/stores/{store_id}/webhooks", response_model=schemas.Webhook)
def create_store_webhook(store_id: int, webhook: schemas.WebhookCreate, db: Session = Depends(get_db)):
    """Creates a webhook in Shopify and registers it locally."""
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        # The address must be a publicly accessible URL
        # e.g., https://your-app-domain.com/api/webhooks/{store_id}
        created_webhook = service.create_webhook(topic=webhook.topic, address=webhook.address)
        
        # Save the registration to our database
        return crud_webhook.create_webhook_registration(db, store_id=store.id, webhook_data=created_webhook)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create webhook: {str(e)}")

@router.delete("/stores/{store_id}/webhooks/{shopify_webhook_id}", status_code=204)
def delete_store_webhook(store_id: int, shopify_webhook_id: int, db: Session = Depends(get_db)):
    """Deletes a webhook from Shopify and removes it locally."""
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        service.delete_webhook(webhook_id=shopify_webhook_id)
        
        # Delete from our database
        crud_webhook.delete_webhook_registration(db, shopify_webhook_id=shopify_webhook_id)
        return Response(status_code=204)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete webhook: {str(e)}")
