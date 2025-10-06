# routes/stock.py
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_

from database import get_db
import models
import crud.product as crud_product
from shopify_service import ShopifyService, gid_to_id

router = APIRouter(prefix="/api/stock", tags=["Stock Management"])

# --- CRUD LOGIC ---
def set_primary_for_barcode(db: Session, variant_id_to_set: int):
    variant = db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id_to_set).first()
    if not variant or not variant.barcode:
        raise HTTPException(status_code=404, detail="Variant with that barcode not found.")
    db.query(models.ProductVariant).filter(
        models.ProductVariant.barcode == variant.barcode
    ).update({"is_barcode_primary": False}, synchronize_session=False)
    variant.is_barcode_primary = True
    db.commit()
    return variant

# --- API ENDPOINTS ---

@router.get("/by-barcode")
def get_stock_grouped_by_barcode(search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    """
    Returns a list of products grouped by barcode from ALL stores,
    with a smart search capability.
    """
    query = (
        db.query(models.ProductVariant)
        .filter(models.ProductVariant.barcode != None, models.ProductVariant.barcode != '')
        .options(
            joinedload(models.ProductVariant.product).joinedload(models.Product.store),
            joinedload(models.ProductVariant.inventory_levels).joinedload(models.InventoryLevel.location)
        )
    )

    if search:
        # Smart Search Logic: Find all barcodes associated with the searched product titles,
        # then get all variants for those barcodes.
        search_term = f"%{search.lower()}%"
        matching_barcodes = (
            db.query(models.ProductVariant.barcode)
            .join(models.Product)
            .filter(or_(
                func.lower(models.Product.title).like(search_term),
                func.lower(models.ProductVariant.sku).like(search_term)
            ))
            .distinct()
        )
        query = query.filter(models.ProductVariant.barcode.in_(matching_barcodes))

    variants_with_barcode = query.order_by(
        models.ProductVariant.barcode, 
        models.ProductVariant.is_barcode_primary.desc()
    ).all()

    grouped_by_barcode: Dict[str, Dict[str, Any]] = {}
    for variant in variants_with_barcode:
        barcode = variant.barcode
        if barcode not in grouped_by_barcode:
            grouped_by_barcode[barcode] = {
                "barcode": barcode,
                "primary_image_url": variant.product.image_url,
                "primary_title": variant.product.title,
                "variants": []
            }
        
        total_available = sum(level.available for level in variant.inventory_levels if level.available is not None)
        
        grouped_by_barcode[barcode]["variants"].append({
            "variant_id": variant.id,
            "product_title": variant.product.title,
            "variant_title": variant.title,
            "sku": variant.sku,
            "store_name": variant.product.store.name,
            "inventory_item_gid": f"gid://shopify/InventoryItem/{variant.inventory_item_id}",
            "is_barcode_primary": variant.is_barcode_primary,
            "total_available": total_available,
        })
    return list(grouped_by_barcode.values())

class PrimaryVariantPayload(BaseModel):
    variant_id: int

@router.post("/set-primary")
def set_primary_variant(payload: PrimaryVariantPayload, db: Session = Depends(get_db)):
    try:
        set_primary_for_barcode(db, payload.variant_id)
        return {"status": "ok", "message": "Primary variant updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class BulkStockUpdatePayload(BaseModel):
    barcode: str
    quantity: int

@router.post("/bulk-update")
def bulk_update_stock(payload: BulkStockUpdatePayload, db: Session = Depends(get_db)):
    """
    Updates stock for all variants sharing a barcode across all stores,
    using the pre-configured sync location for each store.
    """
    all_variants = db.query(models.ProductVariant).filter(
        models.ProductVariant.barcode == payload.barcode
    ).options(joinedload(models.ProductVariant.product).joinedload(models.Product.store)).all()

    if not all_variants:
        raise HTTPException(status_code=404, detail="No variants found with that barcode")

    variants_by_store: Dict[int, List[models.ProductVariant]] = {}
    for v in all_variants:
        store_id = v.product.store.id
        if store_id not in variants_by_store: variants_by_store[store_id] = []
        variants_by_store[store_id].append(v)
    
    errors = []
    success_updates = []

    for store_id, variants in variants_by_store.items():
        store = variants[0].product.store
        if not store.sync_location_id:
            errors.append(f"Store '{store.name}' has no sync location configured.")
            continue

        location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        quantities_payload = [
            {"inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}", "locationId": location_gid, "quantity": payload.quantity}
            for v in variants if v.inventory_item_id
        ]
        
        if not quantities_payload: continue

        variables = {"input": {"reason": "correction", "quantities": quantities_payload}}

        try:
            result = service.execute_mutation("inventorySetQuantities", variables)
            if result.get("inventorySetQuantities", {}).get("userErrors", []):
                errors.append(f"Store {store.name}: {result['inventorySetQuantities']['userErrors'][0]['message']}")
            else:
                variant_ids = [v.id for v in variants]
                success_updates.append({"variant_ids": variant_ids, "location_id": store.sync_location_id, "quantity": payload.quantity})
        except Exception as e:
            errors.append(f"Store {store.name}: {str(e)}")

    if success_updates:
        for update in success_updates:
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=update["variant_ids"], location_id=update["location_id"], new_quantity=update["quantity"]
            )

    if errors:
        raise HTTPException(status_code=422, detail={"message": "Completed with errors.", "errors": errors})

    return {"status": "ok", "message": "Stock updated successfully for all applicable stores."}