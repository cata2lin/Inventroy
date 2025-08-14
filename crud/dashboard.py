# crud/dashboard.py

from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
from typing import List, Optional
from datetime import datetime, timedelta

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
    has_note: Optional[bool] = None, # ADDED
    tags: Optional[str] = None, # ADDED
    search: Optional[str] = None,
    sort_by: str = 'created_at',
    sort_order: str = 'desc'
):
    """
    Fetches a paginated and comprehensively filtered/sorted list of orders for the dashboard.
    """
    
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
        end_date_dt = datetime.fromisoformat(end_date) + timedelta(days=1)
        query = query.filter(models.Order.created_at < end_date_dt.isoformat())
    if financial_status:
        query = query.filter(models.Order.financial_status == financial_status)
    if fulfillment_status:
        query = query.filter(models.Order.fulfillment_status == fulfillment_status)
    
    # --- NEW FILTERS ---
    if has_note is not None:
        if has_note:
            query = query.filter(models.Order.note.isnot(None))
        else:
            query = query.filter(models.Order.note.is_(None))
            
    if tags:
        # Assumes tags are stored as a comma-separated string in the database
        search_tags = [tag.strip() for tag in tags.split(',')]
        for tag in search_tags:
            query = query.filter(models.Order.tags.ilike(f"%{tag}%"))

    if search:
        query = query.filter(models.Order.name.ilike(f"%{search}%"))

    # --- AGGREGATES ---
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
        'store_name': 'store_name',
        'note': models.Order.note
    }
    
    sort_column = sort_column_map.get(sort_by, models.Order.created_at)
    
    order_func = asc(sort_column) if sort_order.lower() == 'asc' else desc(sort_column)
    
    results = query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()

    orders_list = []
    for order, store_name in results:
        order_dict = {
            "id": order.id,
            "name": order.name,
            "created_at": order.created_at,
            "financial_status": order.financial_status,
            "fulfillment_status": order.fulfillment_status,
            "total_price": order.total_price,
            "currency": order.currency,
            "store_name": store_name,
            "cancelled": order.cancelled_at is not None, # ADDED
            "cancel_reason": order.cancel_reason, # ADDED
            "note": order.note, # ADDED
            "tags": order.tags, # ADDED
        }
        orders_list.append(order_dict)

    return {
        "total_count": aggregates.total_count or 0,
        "total_value": float(aggregates.total_value or 0),
        "total_shipping": float(aggregates.total_shipping or 0),
        "orders": orders_list
    }