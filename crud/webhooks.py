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
    """
    Updates the hold status of a fulfillment and, more importantly,
    updates the parent order's fulfillment_status to 'on_hold'.
    """
    fulfillment = db.query(models.Fulfillment).filter(models.Fulfillment.shopify_gid == fulfillment_gid).first()
    if fulfillment:
        fulfillment.hold_status = status
        
        # --- FIX: Update the parent order's status ---
        order = db.query(models.Order).filter(models.Order.id == fulfillment.order_id).first()
        if order:
            if status == "ON_HOLD":
                order.fulfillment_status = "on_hold"
            # You might add logic here for when a hold is released, e.g., revert to 'unfulfilled'
            elif status == "RELEASED" and order.fulfillment_status == "on_hold":
                order.fulfillment_status = "unfulfilled"
        
        db.commit()

def delete_order_by_id(db: Session, order_id: int):
    """Deletes an order by its Shopify legacy ID."""
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if order:
        db.delete(order)
        db.commit()

def mark_product_as_deleted(db: Session, product_id: int):
    """Marks a product as 'DELETED' in the database."""
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if product:
        product.status = 'DELETED'
        db.commit()

def process_product_webhook(db: Session, store_id: int, product_data: schemas.ShopifyProductWebhook):
    """Processes a product create/update webhook."""
    crud_product.create_or_update_product_from_webhook(db, store_id, product_data)

def process_fulfillment_webhook(db: Session, store_id: int, fulfillment_data: schemas.ShopifyFulfillmentWebhook):
    """Processes a fulfillment create/update webhook."""
    crud_order.create_or_update_fulfillment_from_webhook(db, store_id, fulfillment_data)

def process_refund_webhook(db: Session, store_id: int, refund_data: schemas.ShopifyRefundWebhook):
    """Processes a refund create webhook."""
    crud_order.create_refund_from_webhook(db, store_id, refund_data)

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
