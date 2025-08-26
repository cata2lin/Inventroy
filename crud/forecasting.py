# crud/forecasting.py

from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_
from datetime import datetime, timedelta
import models

def get_forecasting_data(
    db: Session,
    lead_time: int,
    coverage_period: int,
    store_ids: list[int] = None,
    product_types: list[str] = None,
    reorder_before: str = None
):
    """
    Generates a forecasting report by calculating stock levels, sales velocities,
    and reorder points for each product group (grouped by barcode).
    """
    today = datetime.utcnow().date()
    start_date_7d = today - timedelta(days=7)
    start_date_30d = today - timedelta(days=30)

    # Base query for product variants
    base_query = db.query(
        models.ProductVariant.barcode,
        func.min(models.ProductVariant.inventory_quantity).label('total_stock'),
        models.Product.title,
        models.ProductVariant.sku,
        models.Product.image_url
    ).join(models.Product).filter(
        models.ProductVariant.barcode.isnot(None)
    )

    if store_ids:
        base_query = base_query.filter(models.Product.store_id.in_(store_ids))
    if product_types:
        base_query = base_query.filter(models.Product.product_type.in_(product_types))

    product_groups = base_query.group_by(
        models.ProductVariant.barcode,
        models.Product.title,
        models.ProductVariant.sku,
        models.Product.image_url
    ).all()

    # Get all variant IDs for sales velocity calculation
    variant_ids = db.query(models.ProductVariant.id).filter(
        models.ProductVariant.barcode.in_([p.barcode for p in product_groups])
    ).all()
    variant_ids = [v[0] for v in variant_ids]

    # Calculate sales velocity
    sales_7d = db.query(
        models.ProductVariant.barcode,
        func.sum(models.LineItem.quantity).label('total_sales')
    ).join(models.LineItem).join(models.Order).filter(
        models.ProductVariant.id.in_(variant_ids),
        models.Order.created_at >= start_date_7d
    ).group_by(models.ProductVariant.barcode).all()

    sales_30d = db.query(
        models.ProductVariant.barcode,
        func.sum(models.LineItem.quantity).label('total_sales')
    ).join(models.LineItem).join(models.Order).filter(
        models.ProductVariant.id.in_(variant_ids),
        models.Order.created_at >= start_date_30d
    ).group_by(models.ProductVariant.barcode).all()

    velocity_map_7d = {s.barcode: s.total_sales / 7 for s in sales_7d}
    velocity_map_30d = {s.barcode: s.total_sales / 30 for s in sales_30d}

    report = []
    for product in product_groups:
        velocity_7d = velocity_map_7d.get(product.barcode, 0)
        velocity_30d = velocity_map_30d.get(product.barcode, 0)

        days_of_stock = None
        if velocity_30d > 0:
            days_of_stock = int(product.total_stock / velocity_30d)

        stock_status = "slow_mover"
        if days_of_stock is not None:
            if days_of_stock < 7:
                stock_status = "urgent"
            elif days_of_stock < 14:
                stock_status = "warning"
            elif days_of_stock < 30:
                stock_status = "watch"
            elif days_of_stock <= 90:
                stock_status = "healthy"
            else:
                stock_status = "overstocked"
        
        reorder_date = None
        if days_of_stock is not None:
            reorder_date = (today + timedelta(days=days_of_stock - lead_time)).strftime('%Y-%m-%d')
        
        reorder_qty = int(velocity_30d * coverage_period)

        report.append({
            "product_title": product.title,
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
    
    if reorder_before:
        report = [item for item in report if item['reorder_date'] and item['reorder_date'] <= reorder_before]
        
    return report

def get_forecasting_filters(db: Session):
    stores = db.query(models.Store.name).distinct().all()
    product_types = db.query(models.Product.product_type).distinct().all()
    
    return {
        "stores": [s[0] for s in stores if s[0]],
        "product_types": [pt[0] for pt in product_types if pt[0]],
    }