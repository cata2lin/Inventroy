# routes/sales_analytics.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from database import get_db
from crud import sales_analytics as crud_sales_analytics

router = APIRouter(
    prefix="/api/analytics",
    tags=["Sales Analytics"],
)

@router.get("/sales-by-product")
def get_sales_by_product(
    db: Session = Depends(get_db),
    start: date = Query(...),
    end: date = Query(...),
    stores: Optional[List[int]] = Query(None),
    only_paid: bool = Query(False),
    exclude_canceled: bool = Query(False),
    search: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0)
):
    """
    Provides aggregated sales data grouped by product barcode.
    """
    return crud_sales_analytics.get_sales_by_product_data(
        db, start, end, stores, only_paid, exclude_canceled, search, limit, offset
    )

@router.get("/inventory-for-barcode")
def get_inventory_for_barcode(
    barcode: str,
    db: Session = Depends(get_db)
):
    """
    Returns the inventory breakdown for a specific barcode.
    """
    return crud_sales_analytics.get_inventory_for_barcode_data(db, barcode)