# services/order_sync_runner.py

from typing import Optional
from sqlalchemy.orm import Session
from database import SessionLocal

from crud import store as crud_store
from crud import order as crud_order
from shopify_service import ShopifyService


def run_orders_sync_for_store(
    db_factory=SessionLocal,
    store_id: int = 0,
    created_at_min: Optional[str] = None,
    created_at_max: Optional[str] = None,
):
    """
    Background task: fetch all orders (optionally within a date range) for a store,
    and upsert them into the DB page by page.
    """
    db: Session = db_factory()
    try:
        store = crud_store.get_store(db, store_id)
        if not store:
            print(f"[orders-sync][abort] store={store_id} not found")
            return

        service = ShopifyService(store.shopify_url, store.api_token)
        print(f"Starting order data fetch from https://{store.shopify_url}/admin/api/{service.api_endpoint.split('/')[-2]}/graphql.json with query window: {created_at_min}..{created_at_max}")

        for page in service.get_all_orders_and_related_data(created_at_min, created_at_max):
            try:
                crud_order.create_or_update_orders(db, page, store_id=store.id)
            except Exception as e:
                db.rollback()
                print(f"[orders-sync][page-error] store={store_id}: {e}")
                # continue with next page
        print(f"[orders-sync] Completed for store={store_id}")
    finally:
        db.close()


def run_orders_sync_for_all_stores(
    db_factory=SessionLocal,
    created_at_min: Optional[str] = None,
    created_at_max: Optional[str] = None,
):
    """
    Background task: run the above for all enabled stores.
    """
    db: Session = db_factory()
    try:
        stores = crud_store.get_all_stores(db)
        for s in stores:
            run_orders_sync_for_store(db_factory, s.id, created_at_min, created_at_max)
    finally:
        db.close()
