# routes/bulk_update.py

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd

from database import get_db
from crud import bulk_update as crud_bulk_update, store as crud_store
from product_service import ProductService
from utils import generate_ean13

router = APIRouter(
    prefix="/api/bulk-update",
    tags=["Bulk Update"],
    responses={404: {"description": "Not found"}},
)

# --- Pydantic Models ---
class VariantUpdatePayload(BaseModel):
    variant_id: int
    store_id: int
    product_id: int
    changes: Dict[str, Any]

class BulkUpdatePayload(BaseModel):
    updates: List[VariantUpdatePayload]

class BarcodeGenerationRequest(BaseModel):
    variant_ids: List[int]
    mode: str

# --- API Endpoints ---
@router.get("/variants/")
def get_all_variants_for_bulk_edit(
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    store_ids: Optional[List[int]] = Query(None),
    product_types: Optional[List[str]] = Query(None),
    statuses: Optional[List[str]] = Query(None),
    has_no_barcode: bool = Query(False)
):
    return crud_bulk_update.get_all_variants_for_bulk_edit(
        db,
        search=search,
        store_ids=store_ids,
        product_types=product_types,
        statuses=statuses,
        has_no_barcode=has_no_barcode
    )

@router.post("/generate-barcode/")
def generate_barcodes_endpoint(request: BarcodeGenerationRequest, db: Session = Depends(get_db)):
    try:
        if request.mode == 'unique':
            barcodes = {variant_id: generate_ean13(db) for variant_id in request.variant_ids}
            return barcodes
        elif request.mode == 'same':
            if not request.variant_ids:
                return {}
            barcode = generate_ean13(db)
            barcodes = {variant_id: barcode for variant_id in request.variant_ids}
            return barcodes
        else:
            raise HTTPException(status_code=400, detail="Invalid generation mode. Use 'unique' or 'same'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during barcode generation: {str(e)}")

@router.post("/upload-excel/")
async def upload_excel_for_bulk_update(db: Session = Depends(get_db), file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an .xlsx file.")

    try:
        df = pd.read_excel(file.file, dtype={'sku': str, 'barcode': str})
        df.columns = df.columns.str.lower()
        
        required_columns = {'sku'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns. The Excel file must contain at least a 'sku' column.")

        all_skus = df['sku'].dropna().tolist()
        variants_to_update = crud_bulk_update.get_variants_by_skus(db, all_skus)
        
        variants_map = {v.sku: v for v in variants_to_update}
        updates_payload = []

        for _, row in df.iterrows():
            sku = row.get('sku')
            if pd.isna(sku) or sku not in variants_map:
                continue

            variant = variants_map[sku]
            changes = {}
            possible_fields = ['barcode', 'price', 'cost', 'onHand']
            
            for field in possible_fields:
                if field in row and pd.notna(row[field]):
                    changes[field] = row[field]
            
            if changes:
                updates_payload.append(
                    VariantUpdatePayload(
                        variant_id=variant.id,
                        store_id=variant.product.store_id,
                        product_id=variant.product.id,
                        changes=changes
                    )
                )
        
        if not updates_payload:
            return {"message": "No valid product updates found in the uploaded file."}

        return process_bulk_updates(BulkUpdatePayload(updates=updates_payload), db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred processing the file: {str(e)}")


@router.post("/variants/", status_code=200)
def process_bulk_updates(payload: BulkUpdatePayload, db: Session = Depends(get_db)):
    results = {"success": [], "errors": []}
    
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
                changes = {k: (v if v != "" else None) for k, v in update_data.changes.items()}
                
                # --- Shopify Update Logic ---
                product_changes = {}
                variant_changes = {"id": variant_db.shopify_gid}
                inventory_changes = {}

                if 'status' in changes:
                    product_changes['status'] = changes.pop('status')
                if 'product_title' in changes:
                    product_changes['title'] = changes.pop('product_title')
                if 'product_type' in changes:
                    product_changes['productType'] = changes.pop('product_type')
                
                if 'onHand' in changes:
                    inventory_changes['onHand'] = changes.pop('onHand')
                if 'available' in changes:
                    inventory_changes['available'] = changes.pop('available')

                if product_changes:
                    service.product_update({"id": variant_db.product.shopify_gid, **product_changes})

                for key, value in changes.items():
                    if key == 'cost':
                        variant_changes['inventoryItem'] = {'cost': value}
                    elif key in ["price", "barcode", "sku", "title"]:
                        variant_changes[key] = value

                if len(variant_changes) > 1:
                    service.variants_bulk_update(variant_db.product.shopify_gid, [variant_changes])

                # FIX: Handle inventory updates for both onHand and available with only one API call
                if inventory_changes and variant_db.inventory_levels:
                    location_gid = f"gid://shopify/Location/{variant_db.inventory_levels[0].location_id}"
                    inventory_item_gid = f"gid://shopify/InventoryItem/{variant_db.inventory_item_id}"
                    
                    if 'available' in inventory_changes:
                        current_qty = variant_db.inventory_levels[0].available or 0
                        delta = int(inventory_changes['available']) - current_qty
                        if delta != 0:
                            service.adjust_inventory_quantity(inventory_item_gid, location_gid, delta)
                    
                    elif 'onHand' in inventory_changes:
                        # Only adjust based on onHand if available was not changed
                        current_on_hand = variant_db.inventory_levels[0].on_hand or 0
                        delta = int(inventory_changes['onHand']) - current_on_hand
                        if delta != 0:
                            service.adjust_inventory_quantity(inventory_item_gid, location_gid, delta)

                # --- Local Database Update ---
                crud_bulk_update.update_local_variant(db, update_data.variant_id, {**product_changes, **changes, **inventory_changes})
                results["success"].append(f"Successfully updated variant ID {update_data.variant_id}")

            except Exception as e:
                results["errors"].append(f"Failed to update variant ID {update_data.variant_id}: {str(e)}")

    if results["errors"]:
        raise HTTPException(status_code=400, detail={"message": "Some updates failed.", "details": results})

    return {"message": "Bulk update processed successfully.", "details": results}