# crud/bulk_update.py

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_
from typing import List, Optional
import models

def get_all_variants_for_bulk_edit(
    db: Session,
    search: Optional[str] = None,
    store_ids: Optional[List[int]] = None,
    product_types: Optional[List[str]] = None,
    has_no_barcode: bool = False
):
    """
    Fetches a comprehensive list of all product variants, applying advanced filters.
    """
    query = db.query(models.ProductVariant).join(
        models.ProductVariant.product
    ).join(
        models.Product.store
    ).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.inventory_levels)
    )

    # --- FILTERING LOGIC ---
    if search:
        search_terms = [term.strip() for term in search.split(',') if term.strip()]
        if search_terms:
            search_filters = []
            for term in search_terms:
                search_ilike = f"%{term}%"
                search_filters.append(models.Product.title.ilike(search_ilike))
                search_filters.append(models.ProductVariant.sku.ilike(search_ilike))
                search_filters.append(models.ProductVariant.barcode.ilike(search_ilike))
            query = query.filter(or_(*search_filters))

    if store_ids:
        query = query.filter(models.Product.store_id.in_(store_ids))
    
    if product_types:
        query = query.filter(models.Product.product_type.in_(product_types))

    # MODIFIED: This now checks for both NULL and empty strings for a more reliable filter.
    if has_no_barcode:
        query = query.filter(or_(models.ProductVariant.barcode.is_(None), models.ProductVariant.barcode == ''))

    all_variants = query.order_by(
        models.Store.name,
        models.Product.title,
        models.ProductVariant.title
    ).all()

    variants_list = []
    for variant in all_variants:
        primary_inventory_level = variant.inventory_levels[0] if variant.inventory_levels else None
        
        variants_list.append({
            "variant_id": variant.id,
            "product_id": variant.product.id,
            "store_id": variant.product.store.id,
            "store_name": variant.product.store.name,
            "product_title": variant.product.title,
            "image_url": variant.product.image_url, 
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

def update_local_variant(db: Session, variant_id: int, changes: dict):
    """
    Updates a single variant record and its related product in the local database.
    This should be called AFTER a successful Shopify update to ensure data consistency.
    """
    db_variant = db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id).first()
    if not db_variant:
        print(f"Warning: Could not find variant with ID {variant_id} in local DB to update.")
        return

    if 'product_title' in changes or 'product_type' in changes:
        db_product = db.query(models.Product).filter(models.Product.id == db_variant.product_id).first()
        if db_product:
            if 'product_title' in changes:
                db_product.title = changes['product_title']
            if 'product_type' in changes:
                db_product.product_type = changes['product_type']

    for key, value in changes.items():
        if hasattr(db_variant, key):
            setattr(db_variant, key, value)
    
    db.commit()

def get_variants_by_skus(db: Session, skus: list[str]):
    """
    Fetches all product variants that match a given list of SKUs.
    Eagerly loads relationships needed for processing updates.
    """
    if not skus:
        return []
    return db.query(models.ProductVariant).filter(
        models.ProductVariant.sku.in_(skus)
    ).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.inventory_levels)
    ).all()