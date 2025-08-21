# services/committed_projector.py

from sqlalchemy.orm import Session
import models
import schemas
from crud.order import update_committed_stock_for_order

def process_order_event(db: Session, store_id: int, topic: str, payload: dict):
    """
    Processes all order-related webhooks to update the committed stock projection.
    """
    print(f"Processing committed stock for topic: {topic}")
    order_data = schemas.ShopifyOrderWebhook.parse_obj(payload)
    
    # First, upsert the order and line item data
    # (This is simplified; in production, you'd share this logic with the main webhook handler)
    order_dict = {
        "id": order_data.id, "store_id": store_id, "name": order_data.name,
        "shopify_gid": order_data.admin_graphql_api_id,
        "financial_status": order_data.financial_status,
        "fulfillment_status": order_data.fulfillment_status,
        "cancelled_at": order_data.cancelled_at,
        # ... other fields
    }
    # This part needs a full upsert implementation like in crud/order.py
    
    # After saving, recalculate
    order = db.query(models.Order).filter_by(id=order_data.id).first()
    if order:
        # This will need to be adapted to decrement stock if an order is cancelled
        update_committed_stock_for_order(db, order)

    db.commit()


def process_fulfillment_event(db: Session, store_id: int, topic: str, payload: dict):
    """
    Processes fulfillment webhooks to potentially adjust committed stock.
    """
    # When an order is fulfilled, its status changes, and the update_committed_stock_for_order
    # function will automatically exclude it from the count. We just need to ensure
    # the order's fulfillment_status is updated correctly.
    fulfillment_data = schemas.ShopifyFulfillmentWebhook.parse_obj(payload)
    order = db.query(models.Order).filter_by(id=fulfillment_data.order_id).first()
    if order:
        # A more advanced version would check all fulfillments for an order
        # to determine if it is 'fulfilled' or 'partially_fulfilled'
        order.fulfillment_status = 'fulfilled' 
        db.commit()