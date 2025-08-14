# crud/inventory.py

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, or_
from typing import Optional

import models

def get_inventory_report(
    db: Session, 
    skip: int, 
    limit: int, 
    view: str = 'grouped',
    search: Optional[str] = None,
    sort_by: str = 'on_hand',
    sort_order: str = 'desc'
):
    """
    Fetches a paginated, sorted, and filtered inventory report, either grouped by barcode or as individual variants.
    """
    # 1. Subquery to calculate committed stock for each SKU
    committed_sq = db.query(
        models.LineItem.sku,
        func.sum(models.LineItem.quantity).label("committed")
    ).join(models.Order).filter(
        models.Order.fulfillment_status.in_(['unfulfilled', 'partially_fulfilled', 'scheduled']),
        models.Order.cancelled_at.is_(None)
    ).group_by(models.LineItem.sku).subquery('committed_sq')

    # 2. Base query for all variants, joining with products and committed stock
    base_query = db.query(
        models.ProductVariant,
        models.Product,
        func.coalesce(committed_sq.c.committed, 0).label("committed_qty")
    ).join(
        models.Product, models.ProductVariant.product_id == models.Product.id
    ).outerjoin(
        committed_sq, models.ProductVariant.sku == committed_sq.c.sku
    )

    # 3. Apply search filter if provided
    if search:
        search_ilike = f"%{search}%"
        base_query = base_query.filter(
            or_(
                models.ProductVariant.sku.ilike(search_ilike),
                models.ProductVariant.barcode.ilike(search_ilike),
                models.Product.product_type.ilike(search_ilike),
                models.Product.product_category.ilike(search_ilike)
            )
        )
    
    # 4. Handle the two different views
    if view == 'individual':
        # --- INDIVIDUAL VIEW ---
        on_hand_col = func.coalesce(models.ProductVariant.inventory_quantity, 0)
        committed_col = func.coalesce(committed_sq.c.committed, 0)
        available_col = on_hand_col - committed_col
        retail_value_col = on_hand_col * func.coalesce(models.ProductVariant.price, 0)
        inventory_value_col = on_hand_col * func.coalesce(models.ProductVariant.cost, 0)

        sort_column_map = {
            'on_hand': on_hand_col,
            'committed': committed_col,
            'available': available_col,
            'retail_value': retail_value_col,
            'inventory_value': inventory_value_col,
            'price': models.ProductVariant.price,
            'cost': models.ProductVariant.cost
        }
        sort_column = sort_column_map.get(sort_by, on_hand_col)

        order_func = sort_column.desc() if sort_order == 'desc' else sort_column.asc()
        
        totals = base_query.with_entities(
            func.sum(retail_value_col).label("total_retail_value"),
            func.sum(inventory_value_col).label("total_inventory_value")
        ).one()

        total_count = base_query.count()
        results = base_query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()

        inventory_list = []
        for variant, product, committed in results:
            on_hand = variant.inventory_quantity or 0
            committed_val = int(committed)
            price = float(variant.price or 0)
            cost = float(variant.cost or 0)
            inventory_list.append({
                "barcode": variant.barcode, "sku": variant.sku,
                "on_hand": on_hand, "committed": committed_val, "available": on_hand - committed_val,
                "image_url": product.image_url, "title": variant.title,
                "product_title": product.title, "category": product.product_category,
                "type": product.product_type, "status": product.status,
                "price": price, "cost": cost,
                "retail_value": on_hand * price,
                "inventory_value": on_hand * cost
            })
        
        return {
            "total_count": total_count, 
            "inventory": inventory_list,
            "total_retail_value": float(totals.total_retail_value or 0),
            "total_inventory_value": float(totals.total_inventory_value or 0)
        }

    else:
        # --- GROUPED VIEW (REFACTORED AND FIXED) ---
        
        # Base for query, filtered by search term
        query_base = base_query.filter(models.ProductVariant.barcode.isnot(None))

        # Correctly get the total count of unique barcodes
        total_count = query_base.distinct(models.ProductVariant.barcode).count()

        # Subquery to find the primary variant ID for each barcode
        primary_variant_id_sq = db.query(
            models.ProductVariant.barcode,
            func.min(models.ProductVariant.id).over(
                partition_by=models.ProductVariant.barcode,
                order_by=models.ProductVariant.is_primary_variant.desc()
            ).label("primary_variant_id")
        ).filter(models.ProductVariant.barcode.isnot(None)).distinct().subquery('primary_variant_id_sq')
        
        PrimaryProduct = aliased(models.Product)
        PrimaryVariant = aliased(models.ProductVariant)

        # Main aggregation query
        on_hand_agg = func.max(models.ProductVariant.inventory_quantity).label("on_hand")
        committed_agg = func.sum(func.coalesce(committed_sq.c.committed, 0)).label("committed")

        agg_query = query_base.join(
            primary_variant_id_sq, 
            primary_variant_id_sq.c.barcode == models.ProductVariant.barcode
        ).join(
            PrimaryVariant, PrimaryVariant.id == primary_variant_id_sq.c.primary_variant_id
        ).join(
            PrimaryProduct, PrimaryProduct.id == PrimaryVariant.product_id
        ).with_entities(
            models.ProductVariant.barcode,
            on_hand_agg,
            committed_agg,
            (on_hand_agg - committed_agg).label("available"),
            func.json_agg(
                func.json_build_object('sku', models.ProductVariant.sku, 'title', models.Product.title, 'variant_id', models.ProductVariant.id)
            ).label("variants_json"),
            PrimaryProduct.title.label("primary_title"),
            PrimaryProduct.image_url.label("primary_image_url"),
            PrimaryProduct.product_type.label("primary_type"),
            PrimaryProduct.product_category.label("primary_category"),
            PrimaryProduct.status.label("primary_status")
        ).group_by(
            models.ProductVariant.barcode,
            PrimaryProduct.title,
            PrimaryProduct.image_url,
            PrimaryProduct.product_type,
            PrimaryProduct.product_category,
            PrimaryProduct.status
        )

        # Dynamic Sorting
        sort_column_map = {
            'on_hand': on_hand_agg,
            'committed': committed_agg,
            'available': (on_hand_agg - committed_agg)
        }
        sort_column = sort_column_map.get(sort_by, on_hand_agg)
        order_func = sort_column.desc() if sort_order == 'desc' else sort_column.asc()

        results = agg_query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()

        inventory_list = []
        for row in results:
            on_hand = row.on_hand or 0
            committed = int(row.committed or 0)
            inventory_list.append({
                "barcode": row.barcode,
                "on_hand": on_hand,
                "committed": committed,
                "available": on_hand - committed,
                "variants": row.variants_json or [],
                "image_url": row.primary_image_url,
                "title": row.primary_title,
                "type": row.primary_type,
                "category": row.primary_category,
                "status": row.primary_status
            })

        return {"total_count": total_count, "inventory": inventory_list}

def get_inventory_by_store(db, store_id: int):
    """
    Returns all inventory items for a given store_id.
    """
    return db.query(models.ProductVariant).filter(models.ProductVariant.store_id == store_id).all()