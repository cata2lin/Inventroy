# crud/order.py

from sqlalchemy.orm import Session
from typing import List, Dict, Any

import models
import schemas
from shopify_service import gid_to_id
from .utils import upsert_batch

def create_or_update_orders(db: Session, orders_data: List[schemas.ShopifyOrder], store_id: int):
    """
    Takes a list of Pydantic ShopifyOrder objects and upserts them and all related data.
    """
    if not orders_data: return

    all_products, all_variants, all_inventory_levels, all_locations = [], [], [], []
    all_orders, all_line_items, all_fulfillments, all_fulfillment_events = [], [], [], []
    processed_product_ids, processed_variant_ids, processed_location_ids, processed_line_item_ids = set(), set(), set(), set()

    for order in orders_data:
        # MODIFIED: Added 'payment_gateway' to the data being saved
        all_orders.append({
            "id": order.legacy_resource_id, "shopify_gid": order.id, "store_id": store_id, 
            "name": order.name, "email": order.email, "phone": order.phone, 
            "created_at": order.created_at, "updated_at": order.updated_at, 
            "cancelled_at": order.cancelled_at, "cancel_reason": order.cancel_reason, 
            "closed_at": order.closed_at, "processed_at": order.processed_at, 
            "financial_status": order.financial_status, "fulfillment_status": order.fulfillment_status, 
            "currency": order.currency, "note": order.note, "tags": ", ".join(order.tags),
            "payment_gateway": order.gateway,
            "total_price": order.total_price.amount, 
            "subtotal_price": order.subtotal_price.amount if order.subtotal_price else None, 
            "total_tax": order.total_tax.amount if order.total_tax else None, 
            "total_discounts": order.total_discounts.amount, 
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
                        "id": product.legacy_resource_id, "shopify_gid": product.id, 
                        "store_id": store_id, "title": product.title, 
                        "body_html": product.body_html, "vendor": product.vendor, 
                        "product_type": product.product_type,
                        "product_category": product.category.name if product.category else None,
                        "created_at": product.created_at, "handle": product.handle, 
                        "updated_at": product.updated_at, "published_at": product.published_at, 
                        "status": product.status, "tags": ", ".join(product.tags),
                        "image_url": str(product.featured_image.url) if product.featured_image else None
                    })
                if variant.legacy_resource_id not in processed_variant_ids:
                    processed_variant_ids.add(variant.legacy_resource_id)
                    inv_item = variant.inventory_item
                    all_variants.append({
                        "id": variant.legacy_resource_id, "shopify_gid": variant.id, 
                        "product_id": product.legacy_resource_id if product else None, 
                        "title": variant.title, "price": variant.price, "sku": variant.sku, 
                        "position": variant.position, "inventory_policy": variant.inventory_policy, 
                        "compare_at_price": variant.compare_at_price, "barcode": variant.barcode, 
                        "inventory_item_id": inv_item.legacy_resource_id, 
                        "inventory_quantity": variant.inventory_quantity, 
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