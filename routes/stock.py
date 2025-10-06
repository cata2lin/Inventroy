# routes/stock.py
from typing import List, Dict, Any, Optional
from pantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, func
from unidecode import unidecode
import requests

from database import get_db
import models
import crud.product as crud_product
from shopify_service import ShopifyService, gid_to_id

router = APIRouter(prefix="/api/stock", tags=["Stock Management"])

# --- Currency Conversion ---
def get_exchange_rates(base_currency: str = "RON") -> Dict[str, float]:
    try:
        # Using a reliable public API for exchange rates
        response = requests.get(f"https://api.exchangerate-api.com/v4/latest/{base_currency}")
        response.raise_for_status()
        data = response.json()
        rates = data.get("rates", {})
        rates[base_currency] = 1.0
        return rates
    except Exception:
        # Hardcoded fallback rates in case the API fails
        return {"RON": 1.0, "EUR": 5.0, "USD": 4.6}

# --- Helper for Smart Search ---
def normalize_and_split(text: str) -> List[str]:
    if not text:
        return []
    return [unidecode(word) for word in text.lower().split()]

# --- API ENDPOINTS ---

@router.get("/by-barcode")
def get_stock_grouped_by_barcode(
    search: Optional[str] = Query(None),
    store_id: Optional[int] = Query(None),
    min_stock: Optional[int] = Query(None),
    max_stock: Optional[int] = Query(None),
    min_retail: Optional[float] = Query(None),
    max_retail: Optional[float] = Query(None),
    db: Session = Depends(get_db)
):
    base_query = (
        db.query(models.ProductVariant)
        .filter(models.ProductVariant.barcode != None, models.ProductVariant.barcode != '')
        .options(
            joinedload(models.ProductVariant.product).joinedload(models.Product.store),
            joinedload(models.ProductVariant.inventory_levels)
        )
    )

    if store_id:
        base_query = base_query.filter(models.ProductVariant.store_id == store_id)

    all_variants = base_query.all()
    
    if search:
        search_terms = normalize_and_split(search)
        matching_barcodes = set()
        for v in all_variants:
            full_text = f"{v.product.title} {v.sku}"
            normalized_full_text = normalize_and_split(full_text)
            if all(term in normalized_full_text for term in search_terms):
                matching_barcodes.add(v.barcode)
        
        all_variants = [v for v in all_variants if v.barcode in matching_barcodes]

    exchange_rates = get_exchange_rates("RON")
    
    grouped_by_barcode: Dict[str, Dict[str, Any]] = {}
    for variant in all_variants:
        barcode = variant.barcode
        if barcode not in grouped_by_barcode:
            grouped_by_barcode[barcode] = { "barcode": barcode, "variants": [], "total_stock": 0, "total_retail_value_ron": 0.0, "total_inventory_value_ron": 0.0 }

        store_currency = variant.product.store.currency
        rate = exchange_rates.get(store_currency, 1.0)

        variant_stock = sum(level.available for level in variant.inventory_levels if level.available is not None)
        variant_retail_value = variant_stock * float(variant.price or 0)
        variant_inventory_value = variant_stock * float(variant.cost_per_item or 0)

        grouped_by_barcode[barcode]["variants"].append({
            "variant_id": variant.id,
            "product_title": variant.product.title,
            "image_url": variant.product.image_url,
            "sku": variant.sku,
            "store_name": variant.product.store.name,
            "is_barcode_primary": variant.is_barcode_primary,
        })
        
        grouped_by_barcode[barcode]["total_stock"] += variant_stock
        grouped_by_barcode[barcode]["total_retail_value_ron"] += variant_retail_value * rate
        grouped_by_barcode[barcode]["total_inventory_value_ron"] += variant_inventory_value * rate

    filtered_groups = list(grouped_by_barcode.values())
    if min_stock is not None:
        filtered_groups = [g for g in filtered_groups if g["total_stock"] >= min_stock]
    if max_stock is not None:
        filtered_groups = [g for g in filtered_groups if g["total_stock"] <= max_stock]
    if min_retail is not None:
        filtered_groups = [g for g in filtered_groups if g["total_retail_value_ron"] >= min_retail]
    if max_retail is not None:
        filtered_groups = [g for g in filtered_groups if g["total_retail_value_ron"] <= max_retail]
        
    final_results = []
    for group in filtered_groups:
        primary_variant = next((v for v in group["variants"] if v["is_barcode_primary"]), group["variants"][0])
        final_results.append({
            "barcode": group["barcode"],
            "primary_image_url": primary_variant["image_url"],
            "primary_title": primary_variant["product_title"],
            "variants": group["variants"],
            "total_stock": group["total_stock"],
            "total_retail_value": round(group["total_retail_value_ron"], 2),
            "total_inventory_value": round(group["total_inventory_value_ron"], 2),
            "currency": "RON"
        })
    
    grand_total_stock = sum(g['total_stock'] for g in final_results)
    grand_total_retail = sum(g['total_retail_value'] for g in final_results)
    grand_total_inventory = sum(g['total_inventory_value'] for g in final_results)

    return {
        "metrics": {
            "total_stock": grand_total_stock,
            "total_retail_value": round(grand_total_retail, 2),
            "total_inventory_value": round(grand_total_inventory, 2)
        },
        "results": final_results
    }

class PrimaryVariantPayload(BaseModel):
    variant_id: int

@router.post("/set-primary")
def set_primary_variant(payload: PrimaryVariantPayload, db: Session = Depends(get_db)):
    variant = db.query(models.ProductVariant).filter(models.ProductVariant.id == payload.variant_id).first()
    if not variant or not variant.barcode:
        raise HTTPException(status_code=404, detail="Variant with that barcode not found.")
    db.query(models.ProductVariant).filter(
        models.ProductVariant.barcode == variant.barcode
    ).update({"is_barcode_primary": False}, synchronize_session=False)
    variant.is_barcode_primary = True
    db.commit()
    return {"status": "ok", "message": "Primary variant updated successfully."}

class BulkStockUpdatePayload(BaseModel):
    barcode: str
    quantity: int

@router.post("/bulk-update")
def bulk_update_stock(payload: BulkStockUpdatePayload, db: Session = Depends(get_db)):
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