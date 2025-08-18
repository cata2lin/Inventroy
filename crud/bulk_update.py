# crud/bulk_update.py

from sqlalchemy.orm import Session, joinedload
import models

def get_all_variants_for_bulk_edit(db: Session):
    """
    Fetches a comprehensive list of all product variants from all stores,
    including all necessary related data for the bulk edit page.
    """
    # FIXED: Replaced the complex query with a more robust and efficient one
    all_variants = db.query(models.ProductVariant).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.inventory_levels)
    ).order_by(
        models.ProductVariant.product.has(models.Product.store.has(models.Store.name)), # Custom sort for relationship
        models.ProductVariant.product_id, 
        models.ProductVariant.title
    ).all()

    # Flatten the data into a list of dictionaries for easier frontend consumption
    variants_list = []
    for variant in all_variants:
        # Get the primary inventory level, if it exists
        primary_inventory_level = variant.inventory_levels[0] if variant.inventory_levels else None
        
        variants_list.append({
            "variant_id": variant.id,
            "product_id": variant.product.id,
            "store_id": variant.product.store.id,
            "store_name": variant.product.store.name,
            "product_title": variant.product.title,
            "variant_title": variant.title,
            "sku": variant.sku,
            "barcode": variant.barcode,
            "product_type": variant.product.product_type,
            "product_category": variant.product.product_category,
            "price": float(variant.price) if variant.price is not None else None,
            "cost": float(variant.cost) if variant.cost is not None else None,
            "onHand": primary_inventory_level.on_hand if primary_inventory_level else None,
            "available": primary_inventory_level.available if primary_inventory_level else None,
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