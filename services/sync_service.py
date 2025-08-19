# services/sync_service.py

from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from database import SessionLocal
from crud import store as crud_store, product as crud_product, order as crud_order
from shopify_service import ShopifyService

# --- Sync Task Implementations ---

def run_full_product_sync(db: Session, store_ids: Optional[List[int]] = None):
    """
    Runs a full product and variant sync for the specified stores.
    If no store_ids are provided, it syncs all stores.
    """
    stores_to_sync = []
    if store_ids:
        for store_id in store_ids:
            store = crud_store.get_store(db, store_id)
            if store:
                stores_to_sync.append(store)
    else:
        stores_to_sync = crud_store.get_stores(db)

    for store in stores_to_sync:
        print(f"Starting full product sync for store: {store.name}")
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        # Stage 1: Sync products and variants
        for page_of_products in service.get_all_products_and_variants():
            if page_of_products:
                crud_product.create_or_update_products(db=db, products_data=page_of_products, store_id=store.id)
        
        # Stage 2: Enrich with inventory details
        for details_batch in service.get_all_inventory_details():
            if details_batch:
                crud_product.update_inventory_details(db, details_batch)
        print(f"Finished product sync for store: {store.name}")
    return {"message": f"Product sync completed for {len(stores_to_sync)} store(s)."}

def run_full_order_sync(db: Session, store_ids: Optional[List[int]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Runs a full order, fulfillment, and line item sync for the specified stores and timeframe.
    """
    stores_to_sync = []
    if store_ids:
        for store_id in store_ids:
            store = crud_store.get_store(db, store_id)
            if store:
                stores_to_sync.append(store)
    else:
        stores_to_sync = crud_store.get_stores(db)

    for store in stores_to_sync:
        print(f"Starting order sync for store: {store.name}")
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        # Note: The ShopifyService fetches all orders; filtering by date would need to be added
        # to the service layer if precise date-based fetching is required from Shopify's API.
        # For now, we fetch all and could filter before saving, but it's less efficient.
        for page_of_orders in service.get_all_orders_and_related_data():
            if page_of_orders:
                crud_order.create_or_update_orders(db=db, orders_data=page_of_orders, store_id=store.id)
        print(f"Finished order sync for store: {store.name}")
    return {"message": f"Order sync completed for {len(stores_to_sync)} store(s)."}


def run_sync_in_background(target_function, **kwargs):
    """Helper to run a sync function in a background thread-like manner."""
    db = SessionLocal()
    try:
        target_function(db=db, **kwargs)
    finally:
        db.close()