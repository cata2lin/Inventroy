# routes/dashboard_v2.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
from crud import dashboard as crud_dashboard

router = APIRouter(
    prefix="/api/v2/dashboard",
    tags=["Dashboard V2"],
    responses={404: {"description": "Not found"}},
)

@router.get("/orders/")
def get_dashboard_orders(
    skip: int = 0,
    limit: int = 50,
    store_ids: Optional[List[int]] = Query(None),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    has_note: Optional[bool] = None, # ADDED
    tags: Optional[str] = None, # ADDED
    search: Optional[str] = None,
    sort_by: str = 'created_at',
    sort_order: str = 'desc',
    db: Session = Depends(get_db)
):
    """
    Provides data for the new dashboard with advanced filtering and sorting.
    """
    return crud_dashboard.get_orders_for_dashboard(
        db=db,
        skip=skip,
        limit=limit,
        store_ids=store_ids,
        start_date=start_date,
        end_date=end_date,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        has_note=has_note,
        tags=tags,
        search=search,
        sort_by=sort_by,
        sort_order=sort_order
    )