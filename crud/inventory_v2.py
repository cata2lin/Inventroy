# crud/inventory_v2.py

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, or_, desc, asc
from typing import Optional

import models

def get_inventory_report(
    db: Session, 
    skip: int, 
    limit: int, 
    view: str = 'individual', # ADDED view parameter
    search: Optional[str] = None,
    product_type: Optional[str] = None,
    category: Optional[str] = None,
    min_retail: Optional[float] = None,
    max_retail: Optional[float] = None,
    min_inventory: Optional[float] = None,
    max_inventory: Optional[float] = None,
    sort_by: str = 'on_hand',
    sort_order: str = 'desc'
):
    """
    Fetches a comprehensively filtered, sorted, and paginated inventory report,
    supporting both individual and barcode-grouped views.
    """
    committed_sq = db.query(
        models.LineItem.sku,
        func.sum(models.LineItem.quantity).label("committed")
    ).join(models.Order).filter(
        models.Order.fulfillment_status.in_(['unfulfilled', 'partially_fulfilled', 'scheduled']),
        models.Order.cancelled_at.is_(None)
    ).group_by(models.LineItem.sku).subquery('committed_sq')

    base_query = db.query(
        models.ProductVariant,
        models.Product,
        func.coalesce(committed_sq.c.committed, 0).label("committed_qty")
    ).join(
        models.Product, models.ProductVariant.product_id == models.Product.id
    ).outerjoin(
        committed_sq, models.ProductVariant.sku == committed_sq.c.sku
    )

    # --- FILTERING (applies to both views) ---
    if search:
        search_ilike = f"%{search}%"
        base_query = base_query.filter(
            or_(
                models.ProductVariant.sku.ilike(search_ilike),
                models.ProductVariant.barcode.ilike(search_ilike),
                models.Product.title.ilike(search_ilike)
            )
        )
    if product_type:
        base_query = base_query.filter(models.Product.product_type == product_type)
    if category:
        base_query = base_query.filter(models.Product.product_category == category)
    
    # Column definitions for calculations
    on_hand_col = func.coalesce(models.ProductVariant.inventory_quantity, 0)
    committed_col = func.coalesce(committed_sq.c.committed, 0)
    retail_value_col = on_hand_col * func.coalesce(models.ProductVariant.price, 0)
    inventory_value_col = on_hand_col * func.coalesce(models.ProductVariant.cost, 0)

    # --- VIEW-SPECIFIC LOGIC ---
    if view == 'grouped':
        # --- GROUPED VIEW ---
        query_base = base_query.filter(models.ProductVariant.barcode.isnot(None))
        total_count = query_base.distinct(models.ProductVariant.barcode).count()
        
        on_hand_agg = func.max(on_hand_col).label("on_hand")
        committed_agg = func.sum(committed_col).label("committed")

        # Complex subquery to get details from the "primary" variant for each group
        primary_variant_sq = db.query(
            models.ProductVariant.barcode,
            func.min(models.Product.id).over(partition_by=models.ProductVariant.barcode, order_by=models.ProductVariant.is_primary_variant.desc(), rows=(None,None)).label('primary_product_id')
        ).filter(models.ProductVariant.barcode.isnot(None)).join(models.Product).distinct().subquery('primary_variant_sq')
        
        PrimaryProduct = aliased(models.Product)

        agg_query = query_base.join(
            primary_variant_sq, primary_variant_sq.c.barcode == models.ProductVariant.barcode
        ).join(
            PrimaryProduct, PrimaryProduct.id == primary_variant_sq.c.primary_product_id
        ).with_entities(
            models.ProductVariant.barcode,
            on_hand_agg,
            committed_agg,
            (on_hand_agg - committed_agg).label("available"),
            func.json_agg(func.json_build_object('sku', models.ProductVariant.sku, 'title', models.Product.title)).label("variants_json"),
            PrimaryProduct.title.label("primary_title"),
            PrimaryProduct.image_url.label("primary_image_url")
        ).group_by(
            models.ProductVariant.barcode,
            PrimaryProduct.title,
            PrimaryProduct.image_url,
        )

        sort_column_map = {'on_hand': on_hand_agg, 'committed': committed_agg, 'available': (on_hand_agg - committed_agg)}
        sort_column = sort_column_map.get(sort_by, on_hand_agg)
        order_func = sort_column.desc() if sort_order == 'desc' else sort_column.asc()
        
        results = agg_query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()

        inventory_list = [dict(row._mapping) for row in results]
        return {"total_count": total_count, "inventory": inventory_list}

    else:
        # --- INDIVIDUAL VIEW ---
        if min_retail is not None: base_query = base_query.filter(retail_value_col >= min_retail)
        if max_retail is not None: base_query = base_query.filter(retail_value_col <= max_retail)
        if min_inventory is not None: base_query = base_query.filter(inventory_value_col >= min_inventory)
        if max_inventory is not None: base_query = base_query.filter(inventory_value_col <= max_inventory)

        aggregates = base_query.with_entities(
            func.sum(retail_value_col).label("total_retail_value"),
            func.sum(inventory_value_col).label("total_inventory_value"),
            func.sum(on_hand_col).label("total_on_hand")
        ).one()
        total_count = base_query.count()

        sort_column_map = {
            'price': models.ProductVariant.price, 'cost': models.ProductVariant.cost, 'on_hand': on_hand_col,
            'committed': committed_col, 'available': on_hand_col - committed_col, 'retail_value': retail_value_col,
            'inventory_value': inventory_value_col, 'product_title': models.Product.title, 'sku': models.ProductVariant.sku,
            'barcode': models.ProductVariant.barcode, 'type': models.Product.product_type, 'category': models.Product.product_category,
            'status': models.Product.status
        }
        sort_column = sort_column_map.get(sort_by, on_hand_col)
        order_func = sort_column.desc() if sort_order == 'desc' else sort_column.asc()
        
        results = base_query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()

        inventory_list = []
        for variant, product, committed in results:
            on_hand = variant.inventory_quantity or 0
            price = float(variant.price or 0)
            cost = float(variant.cost or 0)
            inventory_list.append({
                "image_url": product.image_url, "product_title": product.title, "variant_title": variant.title,
                "sku": variant.sku, "barcode": variant.barcode, "type": product.product_type,
                "category": product.product_category, "status": product.status, "price": price, "cost": cost,
                "on_hand": on_hand, "committed": int(committed), "available": on_hand - int(committed),
                "retail_value": on_hand * price, "inventory_value": on_hand * cost
            })
        
        return {
            "total_count": total_count,
            "total_retail_value": float(aggregates.total_retail_value or 0),
            "total_inventory_value": float(aggregates.total_inventory_value or 0),
            "total_on_hand": int(aggregates.total_on_hand or 0),
            "inventory": inventory_list
        }

def get_filter_options(db: Session):
    types = db.query(models.Product.product_type).distinct().all()
    categories = db.query(models.Product.product_category).distinct().all()
    return {
        "types": [t[0] for t in types if t[0]],
        "categories": [c[0] for c in categories if c[0]]
    }