# crud/forecasting.py

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, case, and_
from datetime import datetime, timedelta
import models

def get_forecasting_data(
    db: Session,
    search: str,
    lead_time: int,
    coverage_period: int,
    store_ids: list[int] = None,
    product_types: list[str] = None,
    reorder_start_date: str = None,
    reorder_end_date: str = None,
    use_custom_velocity: bool = False,
    velocity_start_date: str = None,
    velocity_end_date: str = None,
    velocity_metric: str = 'period', # 'period' or 'lifetime'
):
    """
    Generates a forecasting report with custom velocity period support.
    """
    today = datetime.utcnow().date()
    
    # Define a robust grouping key: barcode, or SKU if barcode is null/empty
    group_key = func.coalesce(
        func.nullif(models.ProductVariant.barcode, ''),
        models.ProductVariant.sku
    ).label("group_key")

    # Subquery to find the primary variant for each group
    RowNumber = func.row_number().over(
        partition_by=group_key,
        order_by=[models.ProductVariant.is_primary_variant.desc(), models.ProductVariant.id.asc()]
    ).label("rn")
    
    PrimaryVariantSelector = db.query(
        models.ProductVariant.id.label("primary_variant_id"),
        group_key
    ).add_column(RowNumber).subquery()
    
    PrimaryVariant = aliased(models.ProductVariant)
    PrimaryProduct = aliased(models.Product)

    # Base query to get stock and primary product info
    base_query = db.query(
        group_key,
        func.min(models.ProductVariant.inventory_quantity).label('total_stock'),
        PrimaryProduct.title.label("product_title"),
        PrimaryVariant.sku.label("sku"),
        PrimaryProduct.image_url.label("image_url")
    ).join(models.Product, models.ProductVariant.product_id == models.Product.id).join(
        PrimaryVariantSelector,
        and_(
            group_key == PrimaryVariantSelector.c.group_key,
            PrimaryVariantSelector.c.rn == 1
        )
    ).join(
        PrimaryVariant, PrimaryVariant.id == PrimaryVariantSelector.c.primary_variant_id
    ).join(
        PrimaryProduct, PrimaryProduct.id == PrimaryVariant.product_id
    ).filter(
        group_key.isnot(None)
    )
    
    if search:
        search_term = f"%{search}%"
        base_query = base_query.filter(
            or_(
                PrimaryProduct.title.ilike(search_term),
                PrimaryVariant.sku.ilike(search_term)
            )
        )

    if store_ids:
        base_query = base_query.filter(models.Product.store_id.in_(store_ids))
    if product_types:
        base_query = base_query.filter(models.Product.product_type.in_(product_types))

    product_groups = base_query.group_by(
        group_key,
        PrimaryProduct.title,
        PrimaryVariant.sku,
        PrimaryProduct.image_url
    ).all()

    group_keys_for_sales = [p.group_key for p in product_groups]
    
    sales_variants_map = {}
    if group_keys_for_sales:
        sales_variants_query = db.query(models.ProductVariant.id, group_key).filter(group_key.in_(group_keys_for_sales))
        for vid, gkey in sales_variants_query.all():
            if gkey not in sales_variants_map: sales_variants_map[gkey] = []
            sales_variants_map[gkey].append(vid)

    def get_sales_and_first_date(start_date=None, end_date=None):
        all_variant_ids = [v for sublist in sales_variants_map.values() for v in sublist]
        if not all_variant_ids: return {}, {}
        
        group_key_expr = func.coalesce(func.nullif(models.ProductVariant.barcode, ''), models.ProductVariant.sku)

        sales_query = db.query(
            func.sum(models.LineItem.quantity).label('total_sales'),
            func.min(models.Order.created_at).label('first_sale_date'),
            group_key_expr.label("group_key")
        ).join(
            models.ProductVariant, models.ProductVariant.id == models.LineItem.variant_id
        ).join(
            models.Order, models.Order.id == models.LineItem.order_id
        ).filter(
            models.LineItem.variant_id.in_(all_variant_ids)
        )
        if start_date and end_date:
            sales_query = sales_query.filter(models.Order.created_at.between(start_date, end_date))
        
        sales_data = sales_query.group_by(group_key_expr).all()

        sales = {s.group_key: s.total_sales for s in sales_data}
        first_dates = {s.group_key: s.first_sale_date for s in sales_data}

        return sales, first_dates

    sales_map_7d, _ = get_sales_and_first_date(today - timedelta(days=7), today)
    sales_map_30d, _ = get_sales_and_first_date(today - timedelta(days=30), today)
    
    sales_map_period = {}
    period_days = 0
    if use_custom_velocity and velocity_start_date and velocity_end_date:
        start = datetime.fromisoformat(velocity_start_date)
        end = datetime.fromisoformat(velocity_end_date)
        period_days = (end - start).days + 1
        sales_map_period, _ = get_sales_and_first_date(start, end)

    lifetime_sales_map, first_sale_dates_map = get_sales_and_first_date()

    report = []
    for product in product_groups:
        velocity_7d = (sales_map_7d.get(product.group_key, 0) or 0) / 7
        velocity_30d = (sales_map_30d.get(product.group_key, 0) or 0) / 30
        
        velocity_period = 0
        if use_custom_velocity and period_days > 0:
            velocity_period = (sales_map_period.get(product.group_key, 0) or 0) / period_days

        velocity_lifetime = 0
        first_sale_date = first_sale_dates_map.get(product.group_key)
        if first_sale_date:
            lifetime_days = (today - first_sale_date.date()).days + 1
            if lifetime_days > 0:
                total_sales = lifetime_sales_map.get(product.group_key, 0) or 0
                velocity_lifetime = total_sales / lifetime_days

        active_velocity = 0
        if velocity_metric == 'lifetime':
            active_velocity = velocity_lifetime
        elif use_custom_velocity and period_days > 0:
            active_velocity = velocity_period
        else:
            active_velocity = velocity_30d
        
        days_of_stock = 0
        if active_velocity > 0 and product.total_stock is not None and product.total_stock > 0:
            days_of_stock = int(product.total_stock / active_velocity)

        stock_status = "slow_mover"
        if active_velocity > 0:
            if days_of_stock < 7: stock_status = "urgent"
            elif days_of_stock < 14: stock_status = "warning"
            elif days_of_stock < 30: stock_status = "watch"
            elif days_of_stock <= 90: stock_status = "healthy"
            else: stock_status = "overstocked"
        
        reorder_date = None
        if days_of_stock is not None:
            reorder_date = (today + timedelta(days=days_of_stock - lead_time)).strftime('%Y-%m-%d')
        
        reorder_qty = int(active_velocity * coverage_period)

        report.append({
            "product_title": product.product_title, "sku": product.sku, "image_url": product.image_url,
            "total_stock": product.total_stock, "velocity_7d": velocity_7d, "velocity_30d": velocity_30d,
            "velocity_period": velocity_period, "velocity_lifetime": velocity_lifetime,
            "days_of_stock": days_of_stock,
            "stock_status": stock_status, "reorder_date": reorder_date, "reorder_qty": reorder_qty
        })
    
    if reorder_start_date and reorder_end_date:
        report = [
            item for item in report 
            if item['reorder_date'] and reorder_start_date <= item['reorder_date'] <= reorder_end_date
        ]
        
    return report

def get_forecasting_filters(db: Session):
    stores = db.query(models.Store.name).distinct().all()
    product_types = db.query(models.Product.product_type).distinct().all()
    return {
        "stores": [s[0] for s in stores if s[0]],
        "product_types": [pt[0] for pt in product_types if pt[0]],
    }