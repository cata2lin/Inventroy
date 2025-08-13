# routes/orders.py

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List

import schemas, models
from database import get_db, SessionLocal
from shopify_service import ShopifyService
# MODIFIED: Import specific crud modules
from crud import store as crud_store
from crud import order as crud_order

router = APIRouter(
    prefix="/orders",
    tags=["Orders & Sync"],
    responses={404: {"description": "Not found"}},
)

def sync_store_data_task(store_id: int):
    """
    Background task to fetch and progressively save all data for a store.
    """
    db = SessionLocal()
    try:
        print(f"Starting background sync for store ID: {store_id}")
        # MODIFIED: Use crud_store module
        store = crud_store.get_store(db, store_id=store_id)
        if not store:
            print(f"Error: Could not find store with ID {store_id} for background sync.")
            return

        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        for page_of_orders in service.get_all_orders_and_related_data():
            if page_of_orders:
                print(f"Fetched a page with {len(page_of_orders)} orders. Saving to database...")
                # MODIFIED: Use crud_order module
                crud_order.create_or_update_orders(db=db, orders_data=page_of_orders, store_id=store.id)

    except Exception as e:
        print(f"An error occurred during sync for store ID {store_id}: {e}")
    finally:
        db.close()
        print(f"Finished background sync task for store ID: {store_id}.")

@router.post("/sync/{store_id}", status_code=202)
def trigger_order_sync(store_id: int, background_tasks: BackgroundTasks):
    """
    Triggers a full background sync of all orders and related data for a specific store.
    """
    background_tasks.add_task(sync_store_data_task, store_id)
    
    return {"message": f"Full data synchronization started in the background for store ID: {store_id}"}

@router.post("/stores/", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    """
    Adds a new Shopify store to the database.
    """
    db_store = db.query(models.Store).filter(models.Store.name == store.name).first()
    if db_store:
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    # MODIFIED: Use crud_store module
    return crud_store.create_store(db=db, store=store)