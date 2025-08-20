# crud/order.py

from sqlalchemy.orm import Session, joinedload
from typing import List, Dict, Any

import models
import schemas
from shopify_service import gid_to_id
from .utils import upsert_batch

def create_or_update_order_from_webhook(db: Session, store_id: int, order_data: schemas.ShopifyOrderWebhook):
    """
    Upserts a single order and its line items from a webhook payload.
    This is separate from the GraphQL sync due to structural differences in the payload.
    """
    order_dict = {
        "id": order_data.id,
        "shopify_gid": order_data.admin_graphql_api_id,
        "store_id": store_id,
        "name": order_data.name,
        "email": order_data.email,
        "phone": order_data.phone,
        "created_at": order_data.created_at,
        "updated_at": order_data.updated_at,
        "cancelled_at": order_data.cancelled_at,
        "cancel_reason": order_data.cancel_reason,
        "closed_at": order_data.closed_at,
        "processed_at": order_data.processed_at,
        "financial_status": order_data.financial_status,
        "fulfillment_status": order_data.fulfillment_status,
        "currency": order_data.currency,
        "payment_gateway_names": ", ".join(order_data.payment_gateway_names) if order_data.payment_gateway_names else None,
        "note": order_data.note,
        "tags": order_data.tags,
        "total_price": order_data.total_price,
        "subtotal_price": order_data.subtotal_price,
        "total_tax": order_data.total_tax,
        "total_discounts": order_data.total_discounts,
        "total_shipping_price": order_data.total_shipping_price_set['shop_money']['amount']
    }
    upsert_batch(db, models.Order, [order_dict], ['id'])

    line_items_list = []
    for item in order_data.line_items:
        line_items_list.append({
            "id": item.id,
            "order_id": order_data.id,
            "variant_id": item.variant_id,
            "product_id": item.product_id,
            "title": item.title,
            "quantity": item.quantity,
            "sku": item.sku,
            "vendor": item.vendor,
            "price": item.price,
            "total_discount": item.total_discount,
            "taxable": item.taxable,
            "shopify_gid": f"gid://shopify/LineItem/{item.id}"
        })
    
    if line_items_list:
        upsert_batch(db, models.LineItem, line_items_list, ['id'])

    db.commit()


def create_or_update_fulfillment_from_webhook(db: Session, store_id: int, fulfillment_data: schemas.ShopifyFulfillmentWebhook):
    """
    Upserts a single fulfillment from a webhook payload. If the parent order does not
    exist, a placeholder is created to prevent foreign key violations.
    """
    # --- FIX: Check for parent order and create a placeholder if it doesn't exist ---
    order = db.query(models.Order).filter(models.Order.id == fulfillment_data.order_id).first()
    if not order:
        placeholder_order = {
            "id": fulfillment_data.order_id,
            "store_id": store_id,
            "name": f"#{fulfillment_data.order_id}", # Use a placeholder name
            "shopify_gid": f"gid://shopify/Order/{fulfillment_data.order_id}"
        }
        upsert_batch(db, models.Order, [placeholder_order], ['id'])

    fulfillment_dict = {
        "id": fulfillment_data.id,
        "order_id": fulfillment_data.order_id,
        "status": fulfillment_data.status,
        "created_at": fulfillment_data.created_at,
        "updated_at": fulfillment_data.updated_at,
        "tracking_company": fulfillment_data.tracking_company,
        "tracking_number": fulfillment_data.tracking_number,
        "tracking_url": str(fulfillment_data.tracking_url) if fulfillment_data.tracking_url else None,
        "shopify_gid": f"gid://shopify/Fulfillment/{fulfillment_data.id}"
    }
    upsert_batch(db, models.Fulfillment, [fulfillment_dict], ['id'])
    
    # Refresh the order's fulfillment status
    order_to_update = db.query(models.Order).filter(models.Order.id == fulfillment_data.order_id).first()
    if order_to_update:
        # A simple update based on the fulfillment status. A more complex logic
        # might be needed for partial fulfillments if line items are sent.
        if fulfillment_data.status == 'success':
             order_to_update.fulfillment_status = 'fulfilled'
        else:
             order_to_update.fulfillment_status = fulfillment_data.status
        db.commit()


def create_refund_from_webhook(db: Session, store_id: int, refund_data: schemas.ShopifyRefundWebhook):
    """
    Creates refund records from a webhook payload. If the parent order does not
    exist, a placeholder is created.
    """
    # --- FIX: Check for parent order and create a placeholder if it doesn't exist ---
    order = db.query(models.Order).filter(models.Order.id == refund_data.order_id).first()
    if not order:
        placeholder_order = {
            "id": refund_data.order_id,
            "store_id": store_id,
            "name": f"#{refund_data.order_id}",
            "shopify_gid": f"gid://shopify/Order/{refund_data.order_id}"
        }
        upsert_batch(db, models.Order, [placeholder_order], ['id'])

    total_refunded = 0.0
    currency = "USD"
    for transaction in refund_data.transactions:
        if transaction.get('kind') == 'refund' and transaction.get('status') == 'success':
            total_refunded += float(transaction.get('amount', 0.0))
            currency = transaction.get('currency', currency)

    refund_dict = {
        "id": refund_data.id,
        "order_id": refund_data.order_id,
        "created_at": refund_data.created_at,
        "note": refund_data.note,
        "total_refunded": total_refunded,
        "currency": currency,
        "shopify_gid": f"gid://shopify/Refund/{refund_data.id}"
    }
    upsert_batch(db, models.Refund, [refund_dict], ['id'])
    
    db_refund = db.query(models.Refund).filter(models.Refund.id == refund_data.id).one()

    refund_line_items_list = []
    for item in refund_data.refund_line_items:
        refund_line_items_list.append({
            "id": item.id,
            "refund_id": db_refund.id,
            "line_item_id": item.line_item_id,
            "quantity": item.quantity,
            "subtotal": item.subtotal,
            "total_tax": item.total_tax
        })
    
    if refund_line_items_list:
        upsert_batch(db, models.RefundLineItem, refund_line_items_list, ['id'])

    order_to_update = db.query(models.Order).filter(models.Order.id == refund_data.order_id).first()
    if order_to_update and order_to_update.total_price is not None:
        if total_refunded >= float(order_to_update.total_price):
             order_to_update.financial_status = 'refunded'
        else:
             order_to_update.financial_status = 'partially_refunded'

    db.commit()


def create_or_update_orders(db: Session, orders_data: List[schemas.ShopifyOrder], store_id: int):
    """
    Takes a list of Pydantic ShopifyOrder objects from GraphQL and upserts them.
    """
    if not orders_data: return

    all_products, all_variants, all_inventory_levels, all_locations = [], [], [], []
    all_orders, all_line_items, all_fulfillments, all_fulfillment_events = [], [], [], []
    processed_product_ids, processed_variant_ids, processed_location_ids, processed_line_item_ids = set(), set(), set(), set()

    for order in orders_data:
        payment_gateway_str = ", ".join(order.paymentGatewayNames) if order.paymentGatewayNames else None
        
        all_orders.append({
            "id": order.legacy_resource_id, "shopify_gid": order.id, "store_id": store_id, "name": order.name, 
            "email": order.email, "phone": order.phone, "created_at": order.created_at, "updated_at": order.updated_at, 
            "cancelled_at": order.cancelled_at, "cancel_reason": order.cancel_reason, "closed_at": order.closed_at, 
            "processed_at": order.processed_at, "financial_status": order.financial_status, 
            "fulfillment_status": order.fulfillment_status, "currency": order.currency, 
            "payment_gateway_names": payment_gateway_str,
            "note": order.note, "tags": ", ".join(order.tags), 
            "total_price": order.total_price.amount, "subtotal_price": order.subtotal_price.amount if order.subtotal_price else None, 
            "total_tax": order.total_tax.amount if order.total_tax else None, "total_discounts": order.total_discounts.amount, 
            "total_shipping_price": order.total_shipping_price.amount
        })

        for item in order.line_items:
            line_item_id = gid_to_id(item.id)
            if not line_item_id or line_item_id in processed_line_item_ids: continue
            processed_line_item_ids.add(line_item_id)
            
            if item.variant:
                variant = item.variant
                product = variant.product
                
                if product and product.legacy_resource_id not in processed_product_ids:
                    processed_product_ids.add(product.legacy_resource_id)
                    all_products.append({
                        "id": product.legacy_resource_id, "shopify_gid": product.id, "store_id": store_id, "title": product.title, 
                        "body_html": product.body_html, "vendor": product.vendor, "product_type": product.product_type,
                        "product_category": product.category.name if product.category else None, "created_at": product.created_at, 
                        "handle": product.handle, "updated_at": product.updated_at, "published_at": product.published_at, 
                        "status": product.status, "tags": ", ".join(product.tags), "image_url": str(product.featured_image.url) if product.featured_image else None
                    })
                if variant.legacy_resource_id not in processed_variant_ids:
                    processed_variant_ids.add(variant.legacy_resource_id)
                    inv_item = variant.inventory_item
                    all_variants.append({
                        "id": variant.legacy_resource_id, "shopify_gid": variant.id, "product_id": product.legacy_resource_id if product else None, 
                        "title": variant.title, "price": variant.price, "sku": variant.sku, "position": variant.position, 
                        "inventory_policy": variant.inventory_policy, "compare_at_price": variant.compare_at_price, "barcode": variant.barcode, 
                        "inventory_item_id": inv_item.legacy_resource_id, "inventory_quantity": variant.inventory_quantity, 
                        "created_at": variant.created_at, "updated_at": variant.updated_at,
                        "cost": inv_item.unit_cost.amount if inv_item.unit_cost else None,
                        "inventory_management": "shopify" if inv_item.tracked else "not_tracked"
                    })
                    for level in inv_item.inventory_levels:
                        loc = level.location
                        if loc.legacy_resource_id not in processed_location_ids:
                            processed_location_ids.add(loc.legacy_resource_id)
                            all_locations.append({"id": loc.legacy_resource_id, "name": loc.name, "store_id": store_id})
                        available_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'available'), None)
                        on_hand_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'on_hand'), None)
                        all_inventory_levels.append({"inventory_item_id": inv_item.legacy_resource_id, "location_id": loc.legacy_resource_id, "available": available_qty, "on_hand": on_hand_qty, "updated_at": level.updated_at})
            all_line_items.append({"id": line_item_id, "shopify_gid": item.id, "order_id": order.legacy_resource_id, "variant_id": item.variant.legacy_resource_id if item.variant else None, "product_id": item.variant.product.legacy_resource_id if item.variant and item.variant.product else None, "title": item.title, "quantity": item.quantity, "sku": item.sku, "vendor": item.vendor, "price": item.price.amount if item.price else None, "total_discount": item.total_discount.amount if item.total_discount else None, "taxable": item.taxable})
        
        for fulfillment in order.fulfillments:
            all_fulfillments.append({"id": fulfillment.legacy_resource_id, "shopify_gid": fulfillment.id, "order_id": order.legacy_resource_id, "status": fulfillment.status, "created_at": fulfillment.created_at, "updated_at": fulfillment.updated_at, "tracking_company": fulfillment.tracking_company, "tracking_number": fulfillment.tracking_number, "tracking_url": str(fulfillment.tracking_url) if fulfillment.tracking_url else None})
            for event in fulfillment.events:
                event_id = gid_to_id(event.id)
                if event_id:
                    all_fulfillment_events.append({"id": event_id, "shopify_gid": event.id, "fulfillment_id": fulfillment.legacy_resource_id, "status": event.status, "happened_at": event.happened_at, "description": event.description})
    
    upsert_batch(db, models.Location, all_locations, ['id'])
    upsert_batch(db, models.Product, all_products, ['id'])
    upsert_batch(db, models.ProductVariant, all_variants, ['id'])
    upsert_batch(db, models.InventoryLevel, all_inventory_levels, ['inventory_item_id', 'location_id'])
    upsert_batch(db, models.Order, all_orders, ['id'])
    upsert_batch(db, models.LineItem, all_line_items, ['id'])
    upsert_batch(db, models.Fulfillment, all_fulfillments, ['id'])
    upsert_batch(db, models.FulfillmentEvent, all_fulfillment_events, ['id'])
    
    db.commit()
    print("Database synchronization complete.")

def get_orders_by_store(db: Session, store_id: int):
    return db.query(models.Order).filter(models.Order.store_id == store_id).order_by(models.Order.created_at.desc()).all()

def get_fulfillments_by_store(db: Session, store_id: int):
    return db.query(
        models.Fulfillment.id,
        models.Fulfillment.created_at,
        models.Fulfillment.tracking_company,
        models.Fulfillment.tracking_number,
        models.Fulfillment.status,
        models.Order.name.label("order_name")
    ).join(models.Order, models.Fulfillment.order_id == models.Order.id)\
     .filter(models.Order.store_id == store_id)\
     .order_by(models.Fulfillment.created_at.desc())\
     .all()
