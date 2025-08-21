# services/sync_service.py

from sqlalchemy.orm import Session
from . import sync_tracker
from shopify_service import ShopifyService
from crud import store as crud_store, order as crud_order, product as crud_product

def run_full_order_sync(db: Session, store_id: int, task_id: str, start_date: str = None, end_date: str = None):
    """
    Background task to sync all orders for a single store.
    It uses the database session provided by the wrapper.
    """
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        sync_tracker.fail_task(task_id, f"Store with ID {store_id} not found.")
        return

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    
    total_orders = service.get_total_counts(created_at_min=start_date, created_at_max=end_date)["orders"]
    if total_orders == 0:
        sync_tracker.complete_task(task_id, "No orders found to sync for the selected criteria.")
        return

    processed_orders = 0
    sync_tracker.update_task_progress(task_id, 0, total_orders, "Starting order fetch...")

    for page_of_orders in service.get_all_orders_and_related_data(created_at_min=start_date, created_at_max=end_date):
        if page_of_orders:
            crud_order.create_or_update_orders(db=db, orders_data=page_of_orders, store_id=store.id)
            processed_orders += len(page_of_orders)
            sync_tracker.update_task_progress(task_id, processed_orders, total_orders, f"Processing {processed_orders} of {total_orders} orders...")
    
    sync_tracker.complete_task(task_id, f"Successfully synced {processed_orders} orders.")

def run_full_product_sync(db: Session, store_id: int, task_id: str):
    """
    Background task to sync all products for a single store.
    It uses the database session provided by the wrapper.
    """
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        sync_tracker.fail_task(task_id, f"Store with ID {store_id} not found.")
        return

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    
    total_products = service.get_total_counts()["products"]
    if total_products == 0:
        sync_tracker.complete_task(task_id, "No products found to sync.")
        return

    processed_products = 0
    sync_tracker.update_task_progress(task_id, 0, total_products, "Starting product fetch...")

    for page_of_products in service.get_all_products_and_variants():
        if page_of_products:
            crud_product.create_or_update_products(db=db, products_data=page_of_products, store_id=store.id)
            processed_products += len(page_of_products)
            sync_tracker.update_task_progress(task_id, processed_products, total_products, f"Processing {processed_products} of {total_products} products...")
    
    sync_tracker.complete_task(task_id, f"Successfully synced {processed_products} products.")

def run_sync_in_background(target_function, db: Session, **kwargs):
    """
    A robust wrapper that handles session management for any background task.
    It receives a newly created session, executes the target function within a
    try...finally block, and guarantees the session is closed.
    """
    try:
        # The session is passed in from the route, so we just use it.
        target_function(db=db, **kwargs)
    except Exception as e:
        task_id = kwargs.get("task_id")
        if task_id:
            try:
                sync_tracker.fail_task(task_id, f"A critical background error occurred: {str(e)}")
            except Exception as tracker_e:
                print(f"Error updating sync tracker: {tracker_e}")
        
        print(f"CRITICAL BACKGROUND ERROR in task {task_id}: {e}")
    finally:
        # --- FIX ---
        # This wrapper is now the single source of truth for closing the session.
        # This ensures that even if the target function fails, the session for this
        # specific task is closed, and it doesn't affect subsequent tasks.
        if db.is_active:
            db.close()