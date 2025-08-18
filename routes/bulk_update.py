# routes/bulk_update.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from database import get_db
from crud import bulk_update as crud_bulk_update, store as crud_store
from product_service import ProductService

router = APIRouter(
    prefix="/api/bulk-update",
    tags=["Bulk Update"],
    responses={404: {"description": "Not found"}},
)

# --- Pydantic Models ---
class VariantUpdatePayload(BaseModel):
    variant_id: int
    store_id: int
    changes: Dict[str, Any]

class BulkUpdatePayload(BaseModel):
    updates: List[VariantUpdatePayload]

# --- API Endpoints ---
@router.get("/variants/")
def get_all_variants_for_bulk_edit(db: Session = Depends(get_db)):
    """
    Fetches a flat list of all product variants from all stores,
    optimized for the bulk editing page.
    """
    return crud_bulk_update.get_all_variants_for_bulk_edit(db)

@router.post("/variants/", status_code=200)
def process_bulk_updates(payload: BulkUpdatePayload, db: Session = Depends(get_db)):
    """
    Receives and processes a list of updates for various product variants.
    """
    results = {"success": [], "errors": []}
    
    # Group updates by store to use one service instance per store
    updates_by_store: Dict[int, List[VariantUpdatePayload]] = {}
    for update in payload.updates:
        if update.store_id not in updates_by_store:
            updates_by_store[update.store_id] = []
        updates_by_store[update.store_id].append(update)

    for store_id, updates in updates_by_store.items():
        store = crud_store.get_store(db, store_id=store_id)
        if not store:
            for update in updates:
                results["errors"].append(f"Store not found for variant ID {update.variant_id}")
            continue

        service = ProductService(store_url=store.shopify_url, token=store.api_token)
        
        for update_data in updates:
            variant_db = crud_bulk_update.get_variant_for_update(db, update_data.variant_id)
            if not variant_db:
                results["errors"].append(f"Variant ID {update_data.variant_id} not found in database.")
                continue

            try:
                # Process product-level updates (title, type)
                product_changes = {k: v for k, v in update_data.changes.items() if k in ["title", "productType"]}
                if product_changes:
                    # Rename 'title' to 'title' for product update if present
                    if 'title' in product_changes:
                         product_changes['title'] = product_changes.pop('title')
                    service.update_product(product_gid=variant_db.product.shopify_gid, product_input=product_changes)

                # Process variant-level updates (sku, barcode, price, compareAtPrice)
                variant_changes = {k: v for k, v in update_data.changes.items() if k in ["sku", "barcode", "price", "compareAtPrice"]}
                if variant_changes:
                    variant_updates = {"id": variant_db.shopify_gid, **variant_changes}
                    service.update_variant_details(product_id=variant_db.product.shopify_gid, variant_updates=variant_updates)

                # Process inventory item updates (cost)
                if 'cost' in update_data.changes:
                    cost_update = {"id": variant_db.shopify_gid, "inventoryItem": {"cost": update_data.changes['cost']}}
                    service.update_variant_details(product_id=variant_db.product.shopify_gid, variant_updates=cost_update)

                # Process inventory level updates (available, onHand)
                location_gid = f"gid://shopify/Location/{variant_db.inventory_levels[0].location_id}" if variant_db.inventory_levels else None
                inventory_item_gid = f"gid://shopify/InventoryItem/{variant_db.inventory_item_id}"
                
                if location_gid:
                    if 'available' in update_data.changes:
                        current_qty = variant_db.inventory_levels[0].available or 0
                        delta = int(update_data.changes['available']) - current_qty
                        if delta != 0:
                            service.adjust_inventory_quantity(inventory_item_id=inventory_item_gid, location_id=location_gid, available_delta=delta)
                    
                    if 'onHand' in update_data.changes:
                         service.set_on_hand_quantity(inventory_item_id=inventory_item_gid, location_id=location_gid, on_hand_quantity=int(update_data.changes['onHand']))

                results["success"].append(f"Successfully updated variant ID {update_data.variant_id}")

            except Exception as e:
                results["errors"].append(f"Failed to update variant ID {update_data.variant_id}: {str(e)}")

    if results["errors"]:
        # Raise an exception if any updates failed to provide clear feedback
        raise HTTPException(status_code=400, detail={"message": "Some updates failed.", "details": results})

    return {"message": "Bulk update processed successfully.", "details": results}