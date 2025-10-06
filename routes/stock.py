# routes/stock.py
from typing import List, Dict, Any
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

from database import get_db
import models
import crud.store as crud_store
from shopify_service import ShopifyService

router = APIRouter(prefix="/api/stock", tags=["Stock Management"])

# --- NEW CRUD LOGIC (included directly for simplicity) ---
def set_primary_for_barcode(db: Session, variant_id_to_set: int):
    """Sets a variant as the primary for its barcode, and unsets all others."""
    variant = db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id_to_set).first()
    if not variant or not variant.barcode:
        raise HTTPException(status_code=404, detail="Variant with that barcode not found.")

    # Unset all other variants with the same barcode in the same store
    db.query(models.ProductVariant).filter(
        models.ProductVariant.store_id == variant.store_id,
        models.ProductVariant.barcode == variant.barcode
    ).update({"is_barcode_primary": False}, synchronize_session=False)

    # Set the target variant as primary
    variant.is_barcode_primary = True
    db.commit()
    return variant

# --- API ENDPOINTS ---

@router.get("/by-barcode/{store_id}")
def get_stock_grouped_by_barcode(store_id: int, db: Session = Depends(get_db)):
    """
    Returns a list of products, grouped by barcode, for a specific store.
    Includes primary designation and image URLs.
    """
    variants_with_barcode = (
        db.query(models.ProductVariant)
        .filter(
            models.ProductVariant.store_id == store_id,
            models.ProductVariant.barcode != None,
            models.ProductVariant.barcode != ''
        )
        .options(
            joinedload(models.ProductVariant.product),
            joinedload(models.ProductVariant.inventory_levels).joinedload(models.InventoryLevel.location)
        )
        .order_by(models.ProductVariant.barcode, models.ProductVariant.is_barcode_primary.desc())
        .all()
    )

    grouped_by_barcode: Dict[str, Dict[str, Any]] = {}
    for variant in variants_with_barcode:
        barcode = variant.barcode
        if barcode not in grouped_by_barcode:
            # The first variant we see for a barcode becomes the temporary primary for display
            # if none is explicitly set, thanks to the ORDER BY clause above.
            grouped_by_barcode[barcode] = {
                "barcode": barcode,
                "primary_image_url": variant.product.image_url,
                "primary_title": variant.product.title,
                "variants": []
            }

        total_available = sum(level.available for level in variant.inventory_levels if level.available is not None)

        grouped_by_barcode[barcode]["variants"].append({
            "variant_id": variant.id, # <-- Important for making updates
            "product_title": variant.product.title,
            "variant_title": variant.title,
            "sku": variant.sku,
            "image_url": variant.product.image_url,
            "inventory_item_gid": f"gid://shopify/InventoryItem/{variant.inventory_item_id}",
            "is_barcode_primary": variant.is_barcode_primary, # <-- Pass to frontend
            "total_available": total_available,
            "locations": [
                {"name": lvl.location.name, "location_gid": lvl.location.shopify_gid, "available": lvl.available}
                for lvl in variant.inventory_levels if lvl.location
            ]
        })
    return list(grouped_by_barcode.values())

class PrimaryVariantPayload(BaseModel):
    variant_id: int

@router.post("/set-primary")
def set_primary_variant(payload: PrimaryVariantPayload, db: Session = Depends(get_db)):
    """Sets a specific variant as the primary for its barcode group."""
    try:
        set_primary_for_barcode(db, payload.variant_id)
        return {"status": "ok", "message": "Primary variant updated successfully."}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class BulkStockUpdatePayload(BaseModel):
    barcode: str
    quantity: int
    location_gid: str

@router.post("/bulk-update/{store_id}")
def bulk_update_stock_by_barcode(
    store_id: int,
    payload: BulkStockUpdatePayload,
    db: Session = Depends(get_db)
):
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    variants_to_update = (
        db.query(models.ProductVariant)
        .filter(
            models.ProductVariant.store_id == store_id,
            models.ProductVariant.barcode == payload.barcode
        )
        .all()
    )

    if not variants_to_update:
        raise HTTPException(status_code=404, detail="No variants found with that barcode")

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    
    quantities_payload = [
        {
            "inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
            "locationId": payload.location_gid,
            "quantity": payload.quantity,
        }
        for v in variants_to_update if v.inventory_item_id
    ]

    if not quantities_payload:
         raise HTTPException(status_code=400, detail="Variants have no inventory items to update")

    variables = { "input": { "reason": "correction", "quantities": quantities_payload } }

    try:
        result = service.execute_mutation("inventorySetQuantities", variables)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))