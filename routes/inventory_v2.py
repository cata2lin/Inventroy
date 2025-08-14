# routes/inventory_v2.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from database import get_db
from crud import inventory_v2 as crud_inventory

router = APIRouter(
    prefix="/api/v2/inventory",
    tags=["Inventory V2"],
    responses={404: {"description": "Not found"}},
)

@router.get("/report/")
def get_inventory_report_data(
    skip: int = 0, limit: int = 50,
    view: str = 'individual',
    store_ids: Optional[List[int]] = Query(None), # ADDED
    search: Optional[str] = None,
    product_type: Optional[str] = None,
    category: Optional[str] = None,
    min_retail: Optional[float] = None,
    max_retail: Optional[float] = None,
    min_inventory: Optional[float] = None,
    max_inventory: Optional[float] = None,
    sort_by: str = 'on_hand',
    sort_order: str = 'desc',
    db: Session = Depends(get_db)
):
    return crud_inventory.get_inventory_report(
        db, skip=skip, limit=limit, view=view, store_ids=store_ids, search=search, 
        product_type=product_type, category=category, min_retail=min_retail, max_retail=max_retail,
        min_inventory=min_inventory, max_inventory=max_inventory,
        sort_by=sort_by, sort_order=sort_order
    )

@router.get("/filters/")
def get_filter_data(db: Session = Depends(get_db)):
    return crud_inventory.get_filter_options(db)