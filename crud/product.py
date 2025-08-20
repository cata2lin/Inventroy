# crud/product.py

from sqlalchemy.orm import Session, joinedload
from typing import List, Dict, Any

import models
import schemas
from .utils import upsert_batch

def create_or_update_product_from_webhook(db: Session, store_id: int, product_data: schemas.ShopifyProductWebhook):
    """
    Upserts a single product and its variants from a webhook payload.
    """
    product_dict = {
        "id": product_data.id,
        "store_id": store_id,
        "title": product_data.title,
        "body_html": product_data.body_html,
        "vendor": product_data.vendor,
        "product_type": product_data.product_type,
        "created_at": product_data.created_at,
        "handle": product_data.handle,
        "updated_at": product_data.updated_at,
        "published_at": product_data.published_at,
        "status": product_data.status,
        "tags": product_data.tags,
        "shopify_gid": f"gid://shopify/Product/{product_data.id}"
    }
    upsert_batch(db, models.Product, [product_dict], ['id'])

    variants_list = []
    for variant_data in product_data.variants:
        variants_list.append({
            "id": variant_data['id'],
            "product_id": product_data.id,
            "title": variant_data['title'],
            "price": variant_data['price'],
            "sku": variant_data['sku'],
            "position": variant_data['position'],
            "inventory_policy": variant_data['inventory_policy'],
            "compare_at_price": variant_data.get('compare_at_price'),
            "barcode": variant_data.get('barcode'),
            "inventory_item_id": variant_data['inventory_item_id'],
            "inventory_quantity": variant_data['inventory_quantity'],
            "created_at": variant_data['created_at'],
            "updated_at": variant_data['updated_at'],
            "shopify_gid": f"gid://shopify/ProductVariant/{variant_data['id']}"
        })
    if variants_list:
        upsert_batch(db, models.ProductVariant, variants_list, ['id'])
    
    db.commit()

def update_inventory_details(db: Session, inventory_data: List[Dict[str, Any]]):
    """
    Updates variant details (cost, inventory management) based on a list of inventory items.
    """
    if not inventory_data: return
    
    print(f"Enriching data for {len(inventory_data)} inventory items...")
    for item in inventory_data:
        inventory_item_legacy_id = item.get('legacyResourceId')
        if not inventory_item_legacy_id: continue

        update_payload = {}
        if item.get('unitCost') and item['unitCost'].get('amount') is not None:
            update_payload['cost'] = item['unitCost']['amount']
        
        if item.get('tracked') is not None:
            update_payload['inventory_management'] = 'shopify' if item['tracked'] else 'not_tracked'

        if update_payload:
            db.query(models.ProductVariant).\
                filter(models.ProductVariant.inventory_item_id == inventory_item_legacy_id).\
                update(update_payload, synchronize_session=False)
    db.commit()
    print("Finished enriching variant data.")

def create_or_update_products(db: Session, products_data: List[Dict[str, Any]], store_id: int):
    """
    Takes a list of product and variant data from the Shopify service and upserts them.
    """
    all_products, all_variants, all_inventory_levels, all_locations = [], [], [], []
    
    for item in products_data:
        product = item['product']
        all_products.append({
            "id": product.legacy_resource_id, "shopify_gid": product.id, 
            "store_id": store_id, "title": product.title, "body_html": product.body_html, 
            "vendor": product.vendor, "product_type": product.product_type,
            "product_category": product.category.name if product.category else None,
            "created_at": product.created_at, "handle": product.handle, 
            "updated_at": product.updated_at, "published_at": product.published_at, 
            "status": product.status, "tags": ", ".join(product.tags),
            "image_url": str(product.featured_image.url) if product.featured_image else None
        })

        for variant in item['variants']:
            inv_item = variant.inventory_item
            all_variants.append({
                "id": variant.legacy_resource_id, "shopify_gid": variant.id, 
                "product_id": product.legacy_resource_id, "title": variant.title, 
                "price": variant.price, "sku": variant.sku, "position": variant.position, 
                "inventory_policy": variant.inventory_policy, 
                "compare_at_price": variant.compare_at_price, "barcode": variant.barcode, 
                "inventory_item_id": inv_item.legacy_resource_id, 
                "inventory_quantity": variant.inventory_quantity, 
                "created_at": variant.created_at, "updated_at": variant.updated_at,
                "cost": inv_item.unit_cost.amount if inv_item.unit_cost else None
            })
            
            for level in inv_item.inventory_levels:
                loc = level.location
                all_locations.append({"id": loc.legacy_resource_id, "name": loc.name, "store_id": store_id})
                
                available_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'available'), None)
                on_hand_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'on_hand'), None)
                
                all_inventory_levels.append({"inventory_item_id": inv_item.legacy_resource_id, "location_id": loc.legacy_resource_id, "available": available_qty, "on_hand": on_hand_qty, "updated_at": level.updated_at})
    
    print("Upserting locations from product sync...")
    upsert_batch(db, models.Location, all_locations, ['id'])
    print("Upserting products...")
    upsert_batch(db, models.Product, all_products, ['id'])
    print("Upserting variants...")
    upsert_batch(db, models.ProductVariant, all_variants, ['id'])
    print("Upserting inventory levels from product sync...")
    upsert_batch(db, models.InventoryLevel, all_inventory_levels, ['inventory_item_id', 'location_id'])
    
    db.commit()

def get_variants_by_store(db: Session, store_id: int):
    """
    Fetches all product variants for a specific store, eagerly loading related data.
    """
    return db.query(models.ProductVariant)\
        .join(models.Product)\
        .outerjoin(models.InventoryLevel)\
        .outerjoin(models.Location)\
        .filter(models.Product.store_id == store_id)\
        .options(
            joinedload(models.ProductVariant.product),
            joinedload(models.ProductVariant.inventory_levels).joinedload(models.InventoryLevel.location)
        )\
        .order_by(models.Product.title, models.ProductVariant.title)\
        .all()

def get_variant_with_inventory(db: Session, variant_id: int):
    """
    Fetches a single product variant with its inventory levels and locations.
    """
    return db.query(models.ProductVariant)\
        .filter(models.ProductVariant.id == variant_id)\
        .options(joinedload(models.ProductVariant.inventory_levels))\
        .first()

def set_primary_variant(db: Session, barcode: str, variant_id: int):
    """
    Sets a specific variant as the primary for a barcode group.
    """
    db.query(models.ProductVariant).filter(
        models.ProductVariant.barcode == barcode
    ).update({"is_primary_variant": False}, synchronize_session=False)

    db.query(models.ProductVariant).filter(
        models.ProductVariant.id == variant_id
    ).update({"is_primary_variant": True}, synchronize_session=False)

    db.commit()
