# routes/products.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional

import schemas
from database import get_db
from crud import product as crud_product

router = APIRouter(
    prefix="/api/products",
    tags=["Products"],
)

@router.get("/", response_model=schemas.ProductResponse)
def get_products(
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = Query(None),
    search: Optional[str] = Query(None),
):
    """
    Get a paginated and filterable list of products.
    """
    products, total_count = crud_product.get_products(
        db, skip=skip, limit=limit, store_id=store_id, search=search
    )
    return {"total_count": total_count, "products": products}