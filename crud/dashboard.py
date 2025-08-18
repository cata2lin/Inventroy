# crud/dashboard.py

from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
from typing import List, Optional
from datetime import datetime, timedelta
import pandas as pd
import io

import models

def _get_filtered_query(db: Session, store_ids, start_date, end_date, financial_status, fulfillment_status, has_note, tags, search):
    """Helper function to build the base filtered query."""
    query = db.query(
        models.Order,
        models.Store.name.label("store_name")
    ).join(models.Store, models.Order.store_id == models.Store.id)

    if store_ids:
        query = query.filter(models.Order.store_id.in_(store_ids))
    if start_date:
        query = query.filter(models.Order.created_at >= start_date)
    if end_date:
        end_date_dt = datetime.fromisoformat(end_date) + timedelta(days=1)
        query = query.filter(models.Order.created_at < end_date_dt.isoformat())
    
    # MODIFIED: Use the 'in_' operator for list-based filtering
    if financial_status:
        query = query.filter(models.Order.financial_status.in_(financial_status))
    if fulfillment_status:
        query = query.filter(models.Order.fulfillment_status.in_(fulfillment_status))
        
    if has_note is not None:
        query = query.filter(models.Order.note.isnot(None) if has_note else models.Order.note.is_(None))
    if tags:
        for tag in [t.strip() for t in tags.split(',') if t.strip()]:
            query = query.filter(models.Order.tags.ilike(f"%{tag}%"))
    if search:
        query = query.filter(models.Order.name.ilike(f"%{search}%"))
    
    return query

def get_orders_for_dashboard(
    db: Session,
    skip: int = 0, limit: int = 50,
    store_ids: Optional[List[int]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None,
    financial_status: Optional[List[str]] = None, fulfillment_status: Optional[List[str]] = None,
    has_note: Optional[bool] = None, tags: Optional[str] = None, search: Optional[str] = None,
    sort_by: str = 'created_at', sort_order: str = 'desc'
):
    query = _get_filtered_query(db, store_ids, start_date, end_date, financial_status, fulfillment_status, has_note, tags, search)

    aggregates_query = query.with_entities(
        func.count(models.Order.id).label("total_count"),
        func.sum(models.Order.total_price).label("total_value"),
        func.sum(models.Order.total_shipping_price).label("total_shipping"),
        func.mode().within_group(models.Order.currency).label("currency")
    )
    aggregates = aggregates_query.first()

    sort_column_map = {
        'order_name': models.Order.name, 'store_name': 'store_name', 'created_at': models.Order.created_at,
        'total_price': models.Order.total_price, 'financial_status': models.Order.financial_status,
        'fulfillment_status': models.Order.fulfillment_status, 'cancelled': models.Order.cancelled_at,
        'note': models.Order.note, 'tags': models.Order.tags
    }
    sort_column = sort_column_map.get(sort_by, models.Order.created_at)
    order_func = asc(sort_column) if sort_order.lower() == 'asc' else desc(sort_column)
    
    results = query.order_by(order_func.nulls_last()).offset(skip).limit(limit).all()
    orders_list = [{"id": order.id, "name": order.name, "created_at": order.created_at, "financial_status": order.financial_status,
                    "fulfillment_status": order.fulfillment_status, "total_price": order.total_price, "currency": order.currency,
                    "store_name": store_name, "cancelled": order.cancelled_at is not None, "cancel_reason": order.cancel_reason,
                    "note": order.note, "tags": order.tags} for order, store_name in results]

    return {"total_count": aggregates.total_count or 0, "total_value": float(aggregates.total_value or 0),
            "total_shipping": float(aggregates.total_shipping or 0), "currency": aggregates.currency or "RON", "orders": orders_list}

def export_orders_for_dashboard(
    db: Session,
    store_ids: Optional[List[int]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None,
    financial_status: Optional[List[str]] = None, fulfillment_status: Optional[List[str]] = None,
    has_note: Optional[bool] = None, tags: Optional[str] = None, search: Optional[str] = None,
    visible_columns: Optional[List[str]] = None
):
    query = _get_filtered_query(db, store_ids, start_date, end_date, financial_status, fulfillment_status, has_note, tags, search)
    results = query.order_by(desc(models.Order.created_at)).all()

    data_to_export = []
    for order, store_name in results:
        data_to_export.append({
            "Order": order.name, "Store": store_name, "Date": order.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            "Total": f"{order.total_price} {order.currency}", "Financial Status": order.financial_status,
            "Fulfillment": order.fulfillment_status, "Cancelled": f"Yes ({order.cancel_reason})" if order.cancelled_at else "No",
            "Note": order.note, "Tags": order.tags
        })

    if not data_to_export:
        return None

    df = pd.DataFrame(data_to_export)
    
    column_map = {
        'order_name': 'Order', 'store_name': 'Store', 'created_at': 'Date', 'total_price': 'Total',
        'financial_status': 'Financial Status', 'fulfillment_status': 'Fulfillment',
        'cancelled': 'Cancelled', 'note': 'Note', 'tags': 'Tags'
    }
    
    df_columns = [column_map[col] for col in visible_columns if col in column_map] if visible_columns else list(column_map.values())
    df_columns_exist = [col for col in df_columns if col in df.columns]
    df = df[df_columns_exist]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Orders')
    
    return output.getvalue()