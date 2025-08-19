# services/sync_service.py

from sqlalchemy.orm import Session
from tqdm import tqdm
from . import sync_tracker
from shopify_service import ShopifyService
from crud import store as crud_store, order as crud_order

def run_full_order_sync(db: Session, store_id: int, task_id: str):
    """Fetches and saves all orders for a store, with progress tracking."""
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        sync_tracker.fail_task(task_id, f"Store with ID {store_id} not found.")
        return

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    
    try:
        total_orders = service.get_total_counts()["orders"]
        if total_orders == 0:
            sync_tracker.complete_task(task_id, "No orders found to sync.")
            return

        processed_orders = 0
        sync_tracker.update_task_progress(task_id, 0, total_orders, "Starting order fetch...")

        for page_of_orders in service.get_all_orders_and_related_data():
            if page_of_orders:
                crud_order.create_or_update_orders(db=db, orders_data=page_of_orders, store_id=store.id)
                processed_orders += len(page_of_orders)
                progress_percent = (processed_orders / total_orders) * 100
                sync_tracker.update_task_progress(
                    task_id, 
                    processed_orders, 
                    total_orders, 
                    f"Processing {processed_orders} of {total_orders} orders..."
                )
        
        sync_tracker.complete_task(task_id, f"Successfully synced {processed_orders} orders.")
    except Exception as e:
        sync_tracker.fail_task(task_id, f"An error occurred: {str(e)}")


def run_sync_in_background(target_function, db: Session, **kwargs):
    """
    Wrapper to run a sync function. This is the entry point for the background task.
    """
    try:
        target_function(db=db, **kwargs)
    except Exception as e:
        task_id = kwargs.get("task_id")
        if task_id:
            sync_tracker.fail_task(task_id, f"A critical error occurred: {str(e)}")