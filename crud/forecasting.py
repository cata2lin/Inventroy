# crud/forecasting.py

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, case, and_, text
from datetime import datetime, timedelta
import models

def get_forecasting_data(
    db: Session,
    lead_time: int,
    coverage_period: int,
    store_ids: list[int] = None,
    product_types: list[str] = None,
    reorder_start_date: str = None,
    reorder_end_date: str = None,
):
    """
    Generates a forecasting report by calculating stock levels, sales velocities,
    and reorder points for each product group.
    Groups by barcode, falling back to SKU for products without a barcode.
    """
    today = datetime.utcnow().date()
    start_date_7d = today - timedelta(days=7)
    start_date_30d = today - timedelta(days=30)

    # Define a robust grouping key: barcode, or SKU if barcode is null/empty
    group_key = func.coalesce(
        func.nullif(models.ProductVariant.barcode, ''),
        models.ProductVariant.sku
    ).label("group_key")

    # Subquery to find the primary variant for each group
    # The primary is the one marked as such, or the one with the lowest ID
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

    # Base query to get stock and primary product info for each group
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

    # Get all variant IDs for sales velocity calculation
    group_keys_for_sales = [p.group_key for p in product_groups]
    sales_variants_query = db.query(
        models.ProductVariant.id,
        group_key
    ).filter(group_key.in_(group_keys_for_sales))

    sales_variants_map = {}
    for vid, gkey in sales_variants_query.all():
        if gkey not in sales_variants_map:
            sales_variants_map[gkey] = []
        sales_variants_map[gkey].append(vid)

    # Calculate sales velocity
    def get_sales(start_date):
        sales_data = db.query(
            func.sum(models.LineItem.quantity).label('total_sales'),
            text("CASE WHEN line_items.variant_id IS NOT NULL THEN pv.barcode ELSE pv.sku END as group_key")
        ).join(
            models.ProductVariant, models.ProductVariant.id == models.LineItem.variant_id
        ).alias("pv").join(
            models.Order, models.Order.id == models.LineItem.order_id
        ).filter(
            models.LineItem.variant_id.in_([v for sublist in sales_variants_map.values() for v in sublist]),
            models.Order.created_at >= start_date
        ).group_by("group_key").all()
        return {s.group_key: s.total_sales for s in sales_data}

    sales_map_7d = get_sales(start_date_7d)
    sales_map_30d = get_sales(start_date_30d)

    report = []
    for product in product_groups:
        total_sales_7d = sales_map_7d.get(product.group_key, 0)
        total_sales_30d = sales_map_30d.get(product.group_key, 0)
        
        velocity_7d = total_sales_7d / 7
        velocity_30d = total_sales_30d / 30

        days_of_stock = None
        if velocity_30d > 0 and product.total_stock is not None:
            days_of_stock = int(product.total_stock / velocity_30d)

        stock_status = "slow_mover"
        if days_of_stock is not None:
            if days_of_stock < 7: stock_status = "urgent"
            elif days_of_stock < 14: stock_status = "warning"
            elif days_of_stock < 30: stock_status = "watch"
            elif days_of_stock <= 90: stock_status = "healthy"
            else: stock_status = "overstocked"
        
        reorder_date = None
        if days_of_stock is not None:
            reorder_date = (today + timedelta(days=days_of_stock - lead_time)).strftime('%Y-%m-%d')
        
        reorder_qty = int(velocity_30d * coverage_period)

        report.append({
            "product_title": product.product_title,
            "sku": product.sku,
            "image_url": product.image_url,
            "total_stock": product.total_stock,
            "velocity_7d": velocity_7d,
            "velocity_30d": velocity_30d,
            "days_of_stock": days_of_stock,
            "stock_status": stock_status,
            "reorder_date": reorder_date,
            "reorder_qty": reorder_qty
        })
    
    # Filter by reorder date range if provided
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