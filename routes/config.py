# routes/config.py
from fastapi import APIRouter, Depends, HTTPException, Response, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List

import schemas
import models
from database import get_db
from crud import store as crud_store, webhooks as crud_webhook
from shopify_service import ShopifyService
from services import audit_logger

router = APIRouter(
    prefix="/api/config",
    tags=["Configuration"],
    responses={404: {"description": "Not found"}},
)

# --- Full list of necessary webhooks ---
ESSENTIAL_WEBHOOK_TOPICS = [
    # For real-time inventory sync
    "inventory_levels/update",
    
    # For keeping product catalog and barcode mappings up-to-date
    "products/create",
    "products/update",
    "products/delete",
    "inventory_items/update",
    "inventory_items/delete"
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
    new_store = crud_store.create_store(db=db, store=store)
    audit_logger.log_config_change("admin", "store_created",
                                    f"Store '{store.name}' created ({store.shopify_url})",
                                    store_id=new_store.id, store_name=store.name,
                                    details={"shopify_url": store.shopify_url, "currency": store.currency})
    return new_store

@router.get("/stores/{store_id}/locations")
def get_store_locations(store_id: int, db: Session = Depends(get_db)):
    """Fetches all inventory locations for a given store from Shopify."""
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        locations = service.get_locations()
        return {"locations": locations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch locations: {str(e)}")

class StoreUpdatePayload(BaseModel):
    sync_location_id: int

@router.put("/stores/{store_id}")
def update_store_settings(store_id: int, payload: StoreUpdatePayload, db: Session = Depends(get_db)):
    """Updates the settings for a store, such as the sync location."""
    db_store = crud_store.get_store(db, store_id=store_id)
    if not db_store:
        raise HTTPException(status_code=404, detail="Store not found")

    old_location = db_store.sync_location_id
    db_store.sync_location_id = payload.sync_location_id
    db.commit()
    db.refresh(db_store)
    audit_logger.log_config_change("admin", "store_settings_updated",
                                    f"Store '{db_store.name}' sync location changed: {old_location} → {payload.sync_location_id}",
                                    store_id=store_id, store_name=db_store.name,
                                    details={"old_location": old_location, "new_location": payload.sync_location_id})
    return db_store

# --- Webhook Management Endpoints ---
@router.get("/stores/{store_id}/webhooks", response_model=List[schemas.Webhook])
def get_store_webhooks(store_id: int, db: Session = Depends(get_db)):
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
        audit_logger.log_config_change("admin", "webhooks_verified", message,
                                        store_id=store_id, store_name=store.name,
                                        details={"created": created_count, "updated": updated_count})
        return {"message": message}
    except Exception as e:
        audit_logger.log_error("config.create_all_necessary_webhooks",
                               f"Failed to create webhooks for store {store_id}", exc=e)
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

@router.delete("/webhooks/delete-all", status_code=200)
def delete_all_webhooks_for_all_stores(db: Session = Depends(get_db)):
    stores = crud_store.get_all_stores(db)
    if not stores:
        return {"message": "No stores are configured."}

    deleted_count = 0
    errors = []

    for store in stores:
        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            existing_webhooks = service.get_webhooks()
            
            for webhook in existing_webhooks:
                try:
                    service.delete_webhook(webhook_id=webhook['id'])
                    crud_webhook.delete_webhook_registration(db, shopify_webhook_id=webhook['id'])
                    deleted_count += 1
                except Exception as e:
                    errors.append(f"Failed to delete webhook {webhook['id']} for store '{store.name}': {e}")

        except Exception as e:
            errors.append(f"Could not process webhooks for store '{store.name}': {e}")

    audit_logger.log_config_change("admin", "webhooks_bulk_deleted",
                                    f"Bulk webhook delete: {deleted_count} deleted, {len(errors)} errors",
                                    details={"deleted_count": deleted_count, "errors": errors})

    if errors:
        content = {
            "message": f"Completed with errors. Deleted {deleted_count} webhooks.",
            "errors": errors
        }
        return JSONResponse(content=content, status_code=207)

    return {"message": f"Successfully deleted {deleted_count} webhooks from all stores."}


@router.post("/webhooks/verify-all", status_code=200)
def verify_all_webhooks_for_all_stores(request: Request, db: Session = Depends(get_db)):
    """
    Verify and recreate missing webhooks for ALL enabled stores.
    This is the manual equivalent of the weekly scheduler job.
    """
    from services import webhook_maintenance

    base_url = str(request.base_url).replace("http://", "https://")
    stores = crud_store.get_enabled_stores(db)
    if not stores:
        return {"message": "No enabled stores configured."}

    total_created = 0
    total_updated = 0
    total_verified = 0
    errors = []
    store_results = []

    for store in stores:
        try:
            correct_address = f"{base_url.rstrip('/')}/api/webhooks/{store.id}"
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            existing_webhooks = service.get_webhooks()
            existing_map = {wh['topic']: {'id': wh['id'], 'address': wh['address']} for wh in existing_webhooks}

            created, updated, verified = 0, 0, 0

            for topic in ESSENTIAL_WEBHOOK_TOPICS:
                existing = existing_map.get(topic)
                if not existing:
                    created_webhook = service.create_webhook(topic=topic, address=correct_address)
                    crud_webhook.create_webhook_registration(db, store_id=store.id, webhook_data=created_webhook)
                    created += 1
                elif existing['address'] != correct_address:
                    service.delete_webhook(webhook_id=existing['id'])
                    crud_webhook.delete_webhook_registration(db, shopify_webhook_id=existing['id'])
                    created_webhook = service.create_webhook(topic=topic, address=correct_address)
                    crud_webhook.create_webhook_registration(db, store_id=store.id, webhook_data=created_webhook)
                    updated += 1
                else:
                    verified += 1

            total_created += created
            total_updated += updated
            total_verified += verified
            store_results.append({
                "store": store.name,
                "created": created,
                "updated": updated,
                "verified": verified,
            })

        except Exception as e:
            errors.append(f"Store '{store.name}': {str(e)}")

    audit_logger.log_config_change(
        "admin", "webhooks_verified_all",
        f"Webhook verification: {total_verified} OK, {total_created} created, {total_updated} fixed, {len(errors)} errors",
        details={"store_results": store_results, "errors": errors}
    )

    result = {
        "message": f"Verified {total_verified}, created {total_created}, fixed {total_updated} webhooks across {len(stores)} stores.",
        "store_results": store_results,
    }
    if errors:
        result["errors"] = errors
        return JSONResponse(content=result, status_code=207)

    return result