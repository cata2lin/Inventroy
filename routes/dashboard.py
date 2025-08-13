# routes/dashboard.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from crud import store as crud_store
from crud import order as crud_order
from crud import inventory as crud_inventory # CORRECTED IMPORT
from database import get_db
from schemas import Store, Order as OrderSchema, Fulfillment as FulfillmentSchema, Inventory as InventorySchema

router = APIRouter(
    tags=["Dashboard"],
    responses={404: {"description": "Not found"}},
)

@router.get("/stores", response_model=List[Store])
def get_all_stores(db: Session = Depends(get_db)):
    """
    Retrieves a list of all configured stores from the database.
    """
    return crud_store.get_stores(db)

@router.get("/orders/{store_id}", response_model=List[OrderSchema])
def get_orders_for_store(store_id: int, db: Session = Depends(get_db)):
    """
    Retrieves all orders for a given store from the database.
    """
    orders = crud_order.get_orders_by_store(db, store_id=store_id)
    if not orders:
        return []
    return orders

@router.get("/fulfillments/{store_id}", response_model=List[FulfillmentSchema])
def get_fulfillments_for_store(store_id: int, db: Session = Depends(get_db)):
    """
    Retrieves all fulfillments for a given store from the database.
    """
    fulfillments = crud_order.get_fulfillments_by_store(db, store_id=store_id)
    if not fulfillments:
        return []
    return fulfillments

@router.get("/inventory/{store_id}", response_model=List[InventorySchema])
def get_inventory_for_store(store_id: int, db: Session = Depends(get_db)):
    """
    Retrieves all inventory levels for a given store from the database.
    """
    # MODIFIED: Use crud_inventory module
    inventory = crud_inventory.get_inventory_by_store(db, store_id=store_id)
    if not inventory:
        return []
    return inventory