# routes/mutations.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any

from database import get_db
from crud import store as crud_store
from shopify_service import ShopifyService
import schemas

router = APIRouter(
    prefix="/api/mutations",
    tags=["Mutations"],
)

@router.post("/execute/{store_id}")
def execute_mutation(
    store_id: int,
    payload: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """
    Execute a GraphQL mutation for a specific store.
    """
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    mutation_name = payload.get("mutation_name")
    variables = payload.get("variables")

    if not mutation_name or not variables:
        raise HTTPException(status_code=400, detail="Missing mutation_name or variables")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        result = service.execute_mutation(mutation_name, variables)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/find-categories/{store_id}")
def find_categories(
    store_id: int,
    payload: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """
    Find Shopify Taxonomy Category IDs.
    """
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    query = payload.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Missing query")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        result = service.find_categories(query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))