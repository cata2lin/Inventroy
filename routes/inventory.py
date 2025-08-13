# routes/inventory.py

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import List, Optional, Any
import decimal

import schemas
from database import get_db, SessionLocal
from product_service import ProductService
from shopify_service import ShopifyService
from inventory_service import InventoryService
from crud import product as crud_product, inventory as crud_inventory, store as crud_store

router = APIRouter(
    tags=["Inventory & Variants"],
    responses={404: {"description": "Not found"}},
)

# --- Pydantic Models ---
class VariantResponse(BaseModel):
    id: int
    gid: str
    product_id: str
    product_title: str
    product_status: str
    title: str
    sku: Optional[str]
    barcode: Optional[str]
    price: Optional[float]
    compare_at_price: Optional[float] = Field(None, alias="compareAtPrice")
    cost: Optional[float] = None
    inventory_management: Optional[str]
    inventory_item_id: str
    location_id: Optional[str]
    available_quantity: Optional[int]
    on_hand_quantity: Optional[int]
    class Config:
        from_attributes = True
        populate_by_name = True

class FieldUpdateRequest(BaseModel):
    variant_id: int
    field: str
    value: Any

class SetPrimaryVariantRequest(BaseModel):
    barcode: str
    variant_id: int

class AdjustInventoryRequest(BaseModel):
    barcode: str
    quantity: int
    reason: str
    source_info: str

# --- Background Task ---
def sync_inventory_task(store_id: int):
    """Background task to fetch and save all products, variants, and inventory for a store."""
    db = SessionLocal()
    try:
        print(f"Starting background inventory sync for store ID: {store_id}")
        store = crud_store.get_store(db, store_id=store_id)
        if not store:
            print(f"Error: Could not find store with ID {store_id} for inventory sync.")
            return

        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        # CORRECTED: This now uses a single, correct query to get all data at once.
        for page_of_products in service.get_all_products_and_variants():
            if page_of_products:
                crud_product.create_or_update_products(db=db, products_data=page_of_products, store_id=store.id)
                print(f"Processed a batch of {len(page_of_products)} products.")
    
    except Exception as e:
        print(f"An error occurred during inventory sync for store ID {store_id}: {e}")
    finally:
        db.close()
        print(f"Finished background inventory sync for store ID: {store_id}")

# --- API Endpoints ---
@router.post("/sync/{store_id}", status_code=202)
def trigger_inventory_sync(store_id: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_inventory_task, store_id)
    return {"message": f"Inventory synchronization started in the background for store ID: {store_id}"}

@router.get("/variants/{store_id}", response_model=List[VariantResponse])
def get_inventory_variants(store_id: int, db: Session = Depends(get_db)):
    variants_from_db = crud_product.get_variants_by_store(db, store_id=store_id)
    response_data = []
    for v in variants_from_db:
        primary_level = v.inventory_levels[0] if v.inventory_levels else None
        response_data.append(
            VariantResponse(
                id=v.id, gid=v.shopify_gid, product_id=v.product.shopify_gid,
                product_title=v.product.title, product_status=v.product.status,
                title=v.title, sku=v.sku, barcode=v.barcode,
                cost=float(v.cost) if isinstance(v.cost, decimal.Decimal) else v.cost,
                inventory_management=v.inventory_management,
                price=float(v.price) if isinstance(v.price, decimal.Decimal) else v.price,
                compareAtPrice=float(v.compare_at_price) if isinstance(v.compare_at_price, decimal.Decimal) else v.compare_at_price,
                inventory_item_id=f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                location_id=f"gid://shopify/Location/{primary_level.location_id}" if primary_level else None,
                available_quantity=primary_level.available if primary_level else None,
                on_hand_quantity=primary_level.on_hand if primary_level else None
            )
        )
    return response_data

@router.post("/variants/update-field/{store_id}", status_code=200)
def update_variant_field(store_id: int, request: FieldUpdateRequest, db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id=store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    variant_db = crud_product.get_variant_with_inventory(db, variant_id=request.variant_id)
    if not variant_db:
        raise HTTPException(status_code=404, detail="Variant not found in local database.")

    service = ProductService(store_url=store.shopify_url, token=store.api_token)
    try:
        if request.field in ["title", "sku", "barcode", "price", "compareAtPrice", "cost"]:
            variant_updates = { "id": variant_db.shopify_gid }
            if request.field == "cost":
                variant_updates["inventoryItem"] = {"cost": request.value}
            elif request.field == "compareAtPrice":
                 variant_updates["compareAtPrice"] = request.value
            else:
                 variant_updates[request.field] = request.value
            response = service.update_variant_details(product_id=variant_db.product.shopify_gid, variant_updates=variant_updates)
            return {"message": f"Successfully updated {request.field}.", "response": response}
        elif request.field == "available":
            if not variant_db.inventory_levels:
                raise HTTPException(status_code=400, detail="Cannot update quantity for variant with no inventory location.")
            current_quantity = variant_db.inventory_levels[0].available if variant_db.inventory_levels else 0
            delta = int(request.value) - (current_quantity or 0)
            if delta == 0:
                return {"message": "No change in available quantity."}
            location_gid = f"gid://shopify/Location/{variant_db.inventory_levels[0].location_id}"
            inventory_item_gid = f"gid://shopify/InventoryItem/{variant_db.inventory_item_id}"
            response = service.adjust_inventory_quantity(inventory_item_id=inventory_item_gid, location_id=location_gid, available_delta=delta)
            return {"message": f"Successfully adjusted available quantity.", "response": response}
        elif request.field == "onHand":
            if not variant_db.inventory_levels:
                raise HTTPException(status_code=400, detail="Cannot update quantity for variant with no inventory location.")
            location_gid = f"gid://shopify/Location/{variant_db.inventory_levels[0].location_id}"
            inventory_item_gid = f"gid://shopify/InventoryItem/{variant_db.inventory_item_id}"
            response = service.set_on_hand_quantity(inventory_item_id=inventory_item_gid, location_id=location_gid, on_hand_quantity=int(request.value))
            return {"message": f"Successfully set on-hand quantity.", "response": response}
        else:
            raise HTTPException(status_code=400, detail=f"Field '{request.field}' is not updatable.")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.get("/grouped/")
def get_grouped_inventory_data(
    skip: int = 0,
    limit: int = 50,
    view: str = 'grouped',
    search: Optional[str] = None,
    sort_by: str = 'on_hand',
    sort_order: str = 'desc',
    db: Session = Depends(get_db)
):
    """
    Endpoint to get inventory with grouping, filtering, sorting, and pagination.
    """
    return crud_inventory.get_inventory_report(
        db, 
        skip=skip, 
        limit=limit, 
        view=view,
        search=search,
        sort_by=sort_by,
        sort_order=sort_order
    )

@router.post("/set-primary-variant")
def set_primary_variant(request: SetPrimaryVariantRequest, db: Session = Depends(get_db)):
    try:
        crud_product.set_primary_variant(db, barcode=request.barcode, variant_id=request.variant_id)
        return {"message": f"Primary variant for barcode {request.barcode} has been updated."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/set_quantity")
def set_inventory_quantity(request: AdjustInventoryRequest, db: Session = Depends(get_db)):
    try:
        service = InventoryService(db)
        return service.set_inventory(barcode=request.barcode, quantity=request.quantity, reason=request.reason, source_info=request.source_info)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/add_quantity")
def add_inventory_quantity(request: AdjustInventoryRequest, db: Session = Depends(get_db)):
    try:
        service = InventoryService(db)
        return service.add_inventory(barcode=request.barcode, quantity=request.quantity, reason=request.reason, source_info=request.source_info)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/subtract_quantity")
def subtract_inventory_quantity(request: AdjustInventoryRequest, db: Session = Depends(get_db)):
    try:
        service = InventoryService(db)
        return service.subtract_inventory(barcode=request.barcode, quantity=request.quantity, reason=request.reason, source_info=request.source_info)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))