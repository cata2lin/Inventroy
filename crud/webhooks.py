# crud/webhooks.py

from sqlalchemy.orm import Session
import models
import schemas
from . import product as crud_product, order as crud_order
from shopify_service import gid_to_id

def create_webhook_registration(db: Session, store_id: int, webhook_data: dict):
    """Saves a webhook registration to the local database."""
    db_webhook = models.Webhook(
        shopify_webhook_id=webhook_data['id'],
        store_id=store_id,
        topic=webhook_data['topic'],
        address=webhook_data['address']
    )
    db.add(db_webhook)
    db.commit()
    db.refresh(db_webhook)
    return db_webhook

def get_webhook_registrations_for_store(db: Session, store_id: int):
    """Retrieves all webhook registrations for a specific store."""
    return db.query(models.Webhook).filter(models.Webhook.store_id == store_id).all()

def delete_webhook_registration(db: Session, shopify_webhook_id: int):
    """Deletes a webhook registration from the local database."""
    db.query(models.Webhook).filter(models.Webhook.shopify_webhook_id == shopify_webhook_id).delete()
    db.commit()

def update_fulfillment_hold_status_by_gid(db: Session, fulfillment_gid: str, status: str):
    """Updates the hold status of a fulfillment based on its Shopify GID."""
    fulfillment = db.query(models.Fulfillment).filter(models.Fulfillment.shopify_gid == fulfillment_gid).first()
    if fulfillment:
        fulfillment.hold_status = status
        db.commit()

def delete_order_by_id(db: Session, order_id: int):
    """Deletes an order by its Shopify legacy ID."""
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if order:
        db.delete(order)
        db.commit()

def delete_product_by_id(db: Session, product_id: int):
    """Deletes a product by its Shopify legacy ID."""
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if product:
        db.delete(product)
        db.commit()

def process_product_webhook(db: Session, store_id: int, product_data: schemas.ShopifyProductWebhook):
    """Processes a product create/update webhook."""
    # This is a simplified version. You might need to adapt your existing
    # `create_or_update_products` logic to handle a single product payload.
    print(f"Processing product webhook for product ID: {product_data.id}")
    # Placeholder for your logic to upsert the product and its variants.

def process_inventory_level_update(db: Session, payload: dict):
    """Processes an inventory level update webhook."""
    inventory_item_id = payload.get("inventory_item_id")
    location_id = payload.get("location_id")
    available = payload.get("available")
    
    if inventory_item_id and location_id is not None and available is not None:
        db.query(models.InventoryLevel).filter(
            models.InventoryLevel.inventory_item_id == inventory_item_id,
            models.InventoryLevel.location_id == location_id
        ).update({"available": available}, synchronize_session=False)
        db.commit()
        print(f"Updated inventory for item {inventory_item_id} at location {location_id} to {available}.")
