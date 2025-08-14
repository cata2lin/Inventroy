# routes/mutations.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List

from database import get_db
from product_service import ProductService
from crud import mutations as crud_mutations, store as crud_store

router = APIRouter(
    prefix="/api/mutations",
    tags=["Product Mutations"],
    responses={404: {"description": "Not found"}},
)

# --- Pydantic Models ---
class ProductUpdatePayload(BaseModel):
    title: Optional[str] = None
    bodyHtml: Optional[str] = None
    vendor: Optional[str] = None
    productType: Optional[str] = None
    status: Optional[str] = None
    tags: Optional[str] = None # Sent as a comma-separated string

class ProductResponse(BaseModel):
    id: int
    shopify_gid: str
    store_id: int
    title: str
    body_html: Optional[str]
    vendor: Optional[str]
    product_type: Optional[str]
    status: Optional[str]
    tags: Optional[str]

    class Config:
        from_attributes = True

# --- API Endpoints ---
@router.get("/product/{product_id}", response_model=ProductResponse)
def get_product_details(product_id: int, db: Session = Depends(get_db)):
    """
    Retrieves all editable details for a single product from the database.
    """
    product = crud_mutations.get_product_by_id(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

@router.post("/product/{product_id}", status_code=200)
def update_product_details(product_id: int, payload: ProductUpdatePayload, db: Session = Depends(get_db)):
    """
    Receives product updates, sends them to the ProductService to execute
    the Shopify mutation, and returns the result.
    """
    product_db = crud_mutations.get_product_by_id(db, product_id)
    if not product_db:
        raise HTTPException(status_code=404, detail="Product not found in local database.")

    store = crud_store.get_store(db, store_id=product_db.store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Associated store not found.")

    service = ProductService(store_url=store.shopify_url, token=store.api_token)

    # Prepare the input for the Shopify API
    update_input = payload.dict(exclude_unset=True)
    if 'tags' in update_input and update_input['tags']:
        update_input['tags'] = [tag.strip() for tag in update_input['tags'].split(',')]

    try:
        response = service.update_product(product_gid=product_db.shopify_gid, product_input=update_input)
        
        # Optionally, you can trigger a background task here to re-sync this specific product
        # to update your local database with the absolute latest from Shopify.

        return {"message": "Product updated successfully in Shopify.", "response": response}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")