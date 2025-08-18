# crud/bulk_update.py

from sqlalchemy.orm import Session, joinedload
import models

def get_all_variants_for_bulk_edit(db: Session):
    """
    Fetches a comprehensive list of all product variants from all stores,
    including all necessary related data for the bulk edit page.
    """
    results = db.query(
        models.ProductVariant,
        models.Product,
        models.Store,
        models.InventoryLevel
    ).join(
        models.Product, models.ProductVariant.product_id == models.Product.id
    ).join(
        models.Store, models.Product.store_id == models.Store.id
    ).outerjoin(
        models.InventoryLevel, 
        models.ProductVariant.inventory_item_id == models.InventoryLevel.inventory_item_id
    ).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.inventory_levels)
    ).order_by(models.Store.name, models.Product.title, models.ProductVariant.title).all()

    # Flatten the data into a list of dictionaries for easier frontend consumption
    variants_list = []
    for variant, product, store, inventory_level in results:
        variants_list.append({
            "variant_id": variant.id,
            "product_id": product.id,
            "store_id": store.id,
            "store_name": store.name,
            "product_title": product.title,
            "variant_title": variant.title,
            "sku": variant.sku,
            "barcode": variant.barcode,
            "product_type": product.product_type,
            "product_category": product.product_category,
            "price": float(variant.price) if variant.price is not None else None,
            "cost": float(variant.cost) if variant.cost is not None else None,
            "on_hand": inventory_level.on_hand if inventory_level else None,
            "available": inventory_level.available if inventory_level else None,
        })
    return variants_list

def get_variant_for_update(db: Session, variant_id: int):
    """
    Fetches a single variant with all relationships needed for an update operation.
    """
    return db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id).options(
        joinedload(models.ProductVariant.product),
        joinedload(models.ProductVariant.inventory_levels)
    ).first()