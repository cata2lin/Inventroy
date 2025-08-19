# services/sync_service.py

from sqlalchemy.orm import Session
from typing import List, Optional

from database import SessionLocal
from crud import store as crud_store, product as crud_product, order as crud_order
from shopify_service import ShopifyService

SYNC_STATUS = {}

def get_sync_status(task_id: str):
    return SYNC_STATUS.get(task_id, {"status": "not_found"})

def update_sync_progress(task_id, store_name, progress, total, status="running"):
    if task_id not in SYNC_STATUS:
        SYNC_STATUS[task_id] = {}
    SYNC_STATUS[task_id][store_name] = {"progress": progress, "total": total, "status": status}

def run_full_product_sync(db: Session, task_id: str, store_ids: Optional[List[int]] = None):
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
        total_products = service.get_total_counts()["products"]
        processed_count = 0
        update_sync_progress(task_id, store.name, 0, total_products)
        
        for page_of_products in service.get_all_products_and_variants():
            if page_of_products:
                crud_product.create_or_update_products(db=db, products_data=page_of_products, store_id=store.id)
                processed_count += len(page_of_products)
                update_sync_progress(task_id, store.name, processed_count, total_products)
        
        for details_batch in service.get_all_inventory_details():
            if details_batch:
                crud_product.update_inventory_details(db, details_batch)
        print(f"Finished product sync for store: {store.name}")
    
    update_sync_progress(task_id, "overall", 100, 100, status="completed")

def run_full_order_sync(db: Session, task_id: str, store_ids: Optional[List[int]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
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
        total_orders = service.get_total_counts()["orders"]
        processed_count = 0
        update_sync_progress(task_id, store.name, 0, total_orders)
        
        for page_of_orders in service.get_all_orders_and_related_data():
            if page_of_orders:
                crud_order.create_or_update_orders(db=db, orders_data=page_of_orders, store_id=store.id)
                processed_count += len(page_of_orders)
                update_sync_progress(task_id, store.name, processed_count, total_orders)
        print(f"Finished order sync for store: {store.name}")
    
    update_sync_progress(task_id, "overall", 100, 100, status="completed")

def run_sync_in_background(target_function, **kwargs):
    db = SessionLocal()
    try:
        target_function(db=db, **kwargs)
    finally:
        db.close()