# crud/inventory_v2.py

from sqlalchemy.orm import Session, aliased, joinedload
from sqlalchemy import func, or_, desc, asc
from typing import Optional, List

import models

def get_product_details_by_barcode(db: Session, barcode: str):
    """
    Gathers comprehensive details for a barcode group for the details modal.
    """
    # 1. Get all variant SKUs in this barcode group
    variants_in_group = db.query(models.ProductVariant.sku).filter(
        models.ProductVariant.barcode_normalized == barcode
    ).all()
    
    if not variants_in_group:
        return None
    
    skus_in_group = [sku[0] for sku in variants_in_group if sku[0]]

    # 2. Get Committed Orders (open orders containing these SKUs)
    committed_orders = db.query(models.Order, models.LineItem.quantity, models.Store.shopify_url).join(
        models.LineItem, models.Order.id == models.LineItem.order_id
    ).join(
        models.Store, models.Order.store_id == models.Store.id
    ).filter(
        models.LineItem.sku.in_(skus_in_group),
        models.Order.fulfillment_status.in_(['unfulfilled', 'partially_fulfilled', 'scheduled']),
        models.Order.cancelled_at.is_(None)
    ).order_by(models.Order.created_at.desc()).all()

    # 3. Get All Orders (historical)
    all_orders = db.query(models.Order, models.LineItem.quantity, models.Store.shopify_url).join(
        models.LineItem, models.Order.id == models.LineItem.order_id
    ).join(
        models.Store, models.Order.store_id == models.Store.id
    ).filter(
        models.LineItem.sku.in_(skus_in_group)
    ).order_by(models.Order.created_at.desc()).limit(100).all() # Limit for performance

    # 4. Get Stock Movement History
    stock_movements = db.query(models.StockMovement).filter(
        models.StockMovement.product_sku.in_(skus_in_group)
    ).order_by(models.StockMovement.created_at.desc()).limit(100).all() # Limit for performance

    return {
        "committed_orders": [{**order.__dict__, "quantity": qty, "shopify_url": url} for order, qty, url in committed_orders],
        "all_orders": [{**order.__dict__, "quantity": qty, "shopify_url": url} for order, qty, url in all_orders],
        "stock_movements": [move.__dict__ for move in stock_movements]
    }

def get_inventory_report(
    db: Session, 
    skip: int, 
    limit: int, 
    view: str = 'individual',
    store_ids: Optional[List[int]] = None,
    search: Optional[str] = None,
    product_type: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    min_retail: Optional[float] = None,
    max_retail: Optional[float] = None,
    min_inventory: Optional[float] = None,
    max_inventory: Optional[float] = None,
    sort_by: str = 'on_hand',
    sort_order: str = 'desc'
):
    """
    Fetches a comprehensively filtered, sorted, and paginated inventory report,
    supporting both individual and barcode-grouped views with accurate metrics for both.
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
        models.Store.name.label("store_name"),
        func.coalesce(committed_sq.c.committed, 0).label("committed_qty")
    ).join(
        models.Product, models.ProductVariant.product_id == models.Product.id
    ).join(
        models.Store, models.Product.store_id == models.Store.id
    ).outerjoin(
        committed_sq, models.ProductVariant.sku == committed_sq.c.sku
    )

    # --- FILTERING ---
    if store_ids:
        base_query = base_query.filter(models.Product.store_id.in_(store_ids))
    if search:
        search_ilike = f"%{search}%"
        base_query = base_query.filter(or_(models.ProductVariant.sku.ilike(search_ilike), models.ProductVariant.barcode.ilike(search_ilike), models.Product.title.ilike(search_ilike)))
    if product_type:
        base_query = base_query.filter(models.Product.product_type == product_type)
    if category:
        base_query = base_query.filter(models.Product.product_category == category)
    if status:
        base_query = base_query.filter(models.Product.status == status)
    
    on_hand_col = func.coalesce(models.ProductVariant.inventory_quantity, 0)
    retail_value_col = on_hand_col * func.coalesce(models.ProductVariant.price, 0)
    inventory_value_col = on_hand_col * func.coalesce(models.ProductVariant.cost, 0)

    if min_retail is not None: base_query = base_query.filter(retail_value_col >= min_retail)
    if max_retail is not None: base_query = base_query.filter(retail_value_col <= max_retail)
    if min_inventory is not None: base_query = base_query.filter(inventory_value_col >= min_inventory)
    if max_inventory is not None: base_query = base_query.filter(inventory_value_col <= max_inventory)

    aggregates = base_query.with_entities(
        func.sum(retail_value_col).label("total_retail_value"),
        func.sum(inventory_value_col).label("total_inventory_value"),
        func.sum(on_hand_col).label("total_on_hand")
    ).one()

    if view == 'grouped':
        query_base = base_query.filter(models.ProductVariant.barcode.isnot(None))
        total_count = query_base.distinct(models.ProductVariant.barcode).count()
        
        grouped_data_sq = query_base.group_by(models.ProductVariant.barcode).with_entities(
            models.ProductVariant.barcode.label("barcode"),
            func.max(on_hand_col).label("on_hand"),
            func.sum(func.coalesce(committed_sq.c.committed, 0)).label("committed"),
            func.json_agg(func.json_build_object('variant_id', models.ProductVariant.id, 'sku', models.ProductVariant.sku, 'store_name', models.Store.name, 'status', models.Product.status, 'is_primary', models.ProductVariant.is_primary_variant)).label("variants_json")
        ).subquery('grouped_data_sq')

        row_num_sq = db.query(
            models.ProductVariant.id,
            models.ProductVariant.barcode,
            func.row_number().over(
                partition_by=models.ProductVariant.barcode,
                order_by=[models.ProductVariant.is_primary_variant.desc(), models.ProductVariant.id.asc()]
            ).label('rn')
        ).filter(models.ProductVariant.barcode.isnot(None)).subquery('row_num_sq')

        primary_variant_sq = db.query(
            row_num_sq.c.id.label('primary_variant_id'),
            row_num_sq.c.barcode
        ).filter(row_num_sq.c.rn == 1).subquery('primary_variant_sq')

        primary_details_sq = db.query(
            primary_variant_sq.c.barcode,
            models.Product.title.label("primary_title"),
            models.Store.name.label("primary_store"),
            models.Product.image_url.label("primary_image_url")
        ).join(models.ProductVariant, models.ProductVariant.id == primary_variant_sq.c.primary_variant_id)\
         .join(models.Product, models.Product.id == models.ProductVariant.product_id)\
         .join(models.Store, models.Store.id == models.Product.store_id)\
         .subquery('primary_details_sq')
        
        final_query = db.query(
            grouped_data_sq.c.barcode, grouped_data_sq.c.on_hand, grouped_data_sq.c.committed,
            (grouped_data_sq.c.on_hand - grouped_data_sq.c.committed).label("available"),
            grouped_data_sq.c.variants_json, primary_details_sq.c.primary_title,
            primary_details_sq.c.primary_store, primary_details_sq.c.primary_image_url
        ).join(primary_details_sq, primary_details_sq.c.barcode == grouped_data_sq.c.barcode)

        sort_column_map = {'on_hand': 'on_hand', 'committed': 'committed', 'available': 'available', 'primary_title': 'primary_title', "barcode": "barcode"}
        sort_column = sort_column_map.get(sort_by, 'on_hand')
        order_func = asc(sort_column) if sort_order == 'asc' else desc(sort_column)
        results = final_query.order_by(order_func).offset(skip).limit(limit).all()
        inventory_list = [dict(row._mapping) for row in results]
    else:
        total_count = base_query.count()
        sort_column_map = {
            'price': models.ProductVariant.price, 'cost': models.ProductVariant.cost, 'on_hand': on_hand_col, 'committed': func.coalesce(committed_sq.c.committed, 0),
            'available': on_hand_col - func.coalesce(committed_sq.c.committed, 0), 'retail_value': retail_value_col, 'inventory_value': inventory_value_col,
            'product_title': models.Product.title, 'sku': models.ProductVariant.sku, 'barcode': models.ProductVariant.barcode,
            'type': models.Product.product_type, 'category': models.Product.product_category, 'status': models.Product.status,
            'store_name': models.Store.name
        }
        sort_column = sort_column_map.get(sort_by, on_hand_col)
        order_func = asc(sort_column) if sort_order == 'asc' else desc(sort_column)
        results = base_query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()
        inventory_list = [
            {"image_url": p.image_url, "product_title": p.title, "variant_title": v.title, "sku": v.sku, "barcode": v.barcode,
             "store_name": s_name, "type": p.product_type, "category": p.product_category, "status": p.status, "price": float(v.price or 0), "cost": float(v.cost or 0),
             "on_hand": v.inventory_quantity or 0, "committed": int(c), "available": (v.inventory_quantity or 0) - int(c),
             "retail_value": (v.inventory_quantity or 0) * float(v.price or 0), "inventory_value": (v.inventory_quantity or 0) * float(v.cost or 0)}
            for v, p, s_name, c in results
        ]
    
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
    return { "types": [t[0] for t in types if t[0]], "categories": [c[0] for c in categories if c[0]] }

def set_primary_variant(db: Session, barcode: str, variant_id: int):
    db.query(models.ProductVariant).filter(models.ProductVariant.barcode == barcode).update({"is_primary_variant": False}, synchronize_session=False)
    db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id).update({"is_primary_variant": True}, synchronize_session=False)
    db.commit()
    return {"message": "Primary variant updated successfully."}