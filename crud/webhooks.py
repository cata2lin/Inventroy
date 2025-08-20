# crud/webhooks.py

from sqlalchemy.orm import Session
from typing import Optional
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

def update_order_fulfillment_status_from_hold(db: Session, order_id: int, status: str, reason: Optional[str] = None):
    """
    Finds an order by its ID and updates its fulfillment status based on a hold event.
    If a fulfillment doesn't exist when a hold is placed, a placeholder is created.
    """
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    
    if order:
        if status == "ON_HOLD":
            order.fulfillment_status = "on_hold"
            
            # Find an existing unfulfilled fulfillment for this order.
            fulfillment = db.query(models.Fulfillment).filter(
                models.Fulfillment.order_id == order_id, 
                models.Fulfillment.status != 'success'
            ).first()
            
            # If no fulfillment exists yet, create a placeholder to store the hold reason.
            if not fulfillment:
                fulfillment = models.Fulfillment(
                    order_id=order_id,
                    status="on_hold", # Set initial status
                    shopify_gid=f"placeholder-gid-for-order-{order_id}" # Use a placeholder GID
                )
                db.add(fulfillment)
            
            # Now that we're sure a fulfillment object exists, update its hold status and reason.
            fulfillment.hold_status = "ON_HOLD"
            fulfillment.hold_reason = reason
        
        elif status == "RELEASED" and order.fulfillment_status == "on_hold":
            # Revert to unfulfilled.
            order.fulfillment_status = "unfulfilled"
            
            # Clear the hold status and reason from any relevant fulfillment.
            db.query(models.Fulfillment).filter(
                models.Fulfillment.order_id == order_id,
                models.Fulfillment.hold_status == "ON_HOLD"
            ).update({"hold_status": "RELEASED", "hold_reason": None}, synchronize_session=False)

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
