# crud/dashboard.py

from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
from typing import List, Optional

import models

def get_orders_for_dashboard(
    db: Session,
    skip: int = 0,
    limit: int = 50,
    store_ids: Optional[List[int]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = 'created_at',
    sort_order: str = 'desc'
):
    """
    Fetches a paginated and comprehensively filtered/sorted list of orders for the dashboard.
    """
    
    # Base query joining Orders and Stores to allow sorting/filtering by store name
    query = db.query(
        models.Order,
        models.Store.name.label("store_name")
    ).join(models.Store, models.Order.store_id == models.Store.id)

    # --- FILTERING ---
    if store_ids:
        query = query.filter(models.Order.store_id.in_(store_ids))
    if start_date:
        query = query.filter(models.Order.created_at >= start_date)
    if end_date:
        # To make the end date inclusive, we can add a day or use '<' with the next day
        from datetime import datetime, timedelta
        end_date_dt = datetime.fromisoformat(end_date) + timedelta(days=1)
        query = query.filter(models.Order.created_at < end_date_dt.isoformat())
    if financial_status:
        query = query.filter(models.Order.financial_status == financial_status)
    if fulfillment_status:
        query = query.filter(models.Order.fulfillment_status == fulfillment_status)
    if search:
        query = query.filter(models.Order.name.ilike(f"%{search}%"))

    # --- AGGREGATES ---
    # Calculate aggregates based on the *filtered* query before pagination
    aggregates_query = query.with_entities(
        func.count(models.Order.id).label("total_count"),
        func.sum(models.Order.total_price).label("total_value"),
        func.sum(models.Order.total_shipping_price).label("total_shipping")
    )
    aggregates = aggregates_query.first()

    # --- SORTING ---
    sort_column_map = {
        'order_name': models.Order.name,
        'created_at': models.Order.created_at,
        'total_price': models.Order.total_price,
        'financial_status': models.Order.financial_status,
        'fulfillment_status': models.Order.fulfillment_status,
        'store_name': 'store_name' # Handle aliased column
    }
    
    sort_column = sort_column_map.get(sort_by, models.Order.created_at)
    
    if sort_order.lower() == 'asc':
        order_func = asc(sort_column)
    else:
        order_func = desc(sort_column)

    # Apply sorting and pagination to the main query
    results = query.order_by(order_func).offset(skip).limit(limit).all()

    # The result is a list of tuples (Order, store_name), we need to combine them
    orders_list = []
    for order, store_name in results:
        order_dict = order.__dict__
        order_dict['store_name'] = store_name
        orders_list.append(order_dict)

    return {
        "total_count": aggregates.total_count or 0,
        "total_value": float(aggregates.total_value or 0),
        "total_shipping": float(aggregates.total_shipping or 0),
        "orders": orders_list
    }