# routes/dashboard_v2.py

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
from crud import dashboard as crud_dashboard

router = APIRouter(
    prefix="/api/v2/dashboard",
    tags=["Dashboard V2"],
    responses={404: {"description": "Not found"}},
)

@router.get("/filters/")
def get_dashboard_filters(db: Session = Depends(get_db)):
    """
    Endpoint to get all unique, non-null status values for dashboard filters.
    """
    return crud_dashboard.get_status_filters(db)

@router.get("/orders/")
def get_dashboard_orders(
    skip: int = 0, limit: int = 50,
    store_ids: Optional[List[int]] = Query(None),
    start_date: Optional[str] = None, end_date: Optional[str] = None,
    financial_status: Optional[List[str]] = Query(None), 
    fulfillment_status: Optional[List[str]] = Query(None),
    has_note: Optional[bool] = None, tags: Optional[str] = None, search: Optional[str] = None,
    sort_by: str = 'created_at', sort_order: str = 'desc',
    db: Session = Depends(get_db)
):
    return crud_dashboard.get_orders_for_dashboard(
        db=db, skip=skip, limit=limit, store_ids=store_ids, start_date=start_date, end_date=end_date,
        financial_status=financial_status, fulfillment_status=fulfillment_status, has_note=has_note,
        tags=tags, search=search, sort_by=sort_by, sort_order=sort_order
    )

@router.get("/export/")
def export_dashboard_orders(
    store_ids: Optional[List[int]] = Query(None),
    start_date: Optional[str] = None, end_date: Optional[str] = None,
    financial_status: Optional[List[str]] = Query(None), 
    fulfillment_status: Optional[List[str]] = Query(None),
    has_note: Optional[bool] = None, tags: Optional[str] = None, search: Optional[str] = None,
    visible_columns: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db)
):
    excel_data = crud_dashboard.export_orders_for_dashboard(
        db=db, store_ids=store_ids, start_date=start_date, end_date=end_date,
        financial_status=financial_status, fulfillment_status=fulfillment_status,
        has_note=has_note, tags=tags, search=search, visible_columns=visible_columns
    )
    if excel_data is None:
        return StreamingResponse(iter([b"No data to export."]), media_type="text/plain")

    return StreamingResponse(
        iter([excel_data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=orders_export.xlsx"}
    )
