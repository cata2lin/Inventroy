# routes/inventory_v2.py

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from pydantic import BaseModel

from database import get_db
from crud import inventory_v2 as crud_inventory

router = APIRouter(
    prefix="/api/v2/inventory",
    tags=["Inventory V2"],
    responses={404: {"description": "Not found"}},
)

class SetPrimaryVariantRequest(BaseModel):
    barcode: str
    variant_id: int

@router.get("/report/")
def get_inventory_report_data(
    skip: int = 0, limit: int = 50,
    view: str = 'individual',
    store_ids: Optional[List[int]] = Query(None),
    search: Optional[str] = None,
    product_type: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
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
        product_type=product_type, category=category, status=status,
        min_retail=min_retail, max_retail=max_retail,
        min_inventory=min_inventory, max_inventory=max_inventory,
        sort_by=sort_by, sort_order=sort_order
    )

@router.get("/filters/")
def get_filter_data(db: Session = Depends(get_db)):
    return crud_inventory.get_filter_options(db)

@router.post("/set-primary-variant/")
def set_primary_variant_endpoint(request: SetPrimaryVariantRequest, db: Session = Depends(get_db)):
    try:
        return crud_inventory.set_primary_variant(db, barcode=request.barcode, variant_id=request.variant_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEW: Endpoint to get detailed history for a barcode group ---
@router.get("/product-details/{barcode}")
def get_product_details(barcode: str, db: Session = Depends(get_db)):
    """
    Fetches detailed information for a barcode group, including committed orders,
    all historical orders, and stock movements.
    """
    details = crud_inventory.get_product_details_by_barcode(db, barcode=barcode)
    if not details:
        raise HTTPException(status_code=404, detail="No details found for this barcode.")
    return details