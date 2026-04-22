# routes/stock.py
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, func
from unidecode import unidecode
import requests
import threading

from database import get_db
import models
import crud.product as crud_product
from shopify_service import ShopifyService, gid_to_id
from services import audit_logger

router = APIRouter(prefix="/api/stock", tags=["Stock Management"])

# --- BUG-28 FIX: Cached Currency Conversion ---
_exchange_rate_cache: Dict[str, Any] = {"rates": None, "fetched_at": None}
_exchange_rate_lock = threading.Lock()
EXCHANGE_RATE_TTL_SECONDS = 3600  # 1 hour

def get_exchange_rates(base_currency: str = "RON") -> Dict[str, float]:
    """Fetch exchange rates with 1-hour TTL cache to avoid blocking on every page load."""
    with _exchange_rate_lock:
        now = datetime.now(timezone.utc)
        if _exchange_rate_cache["rates"] and _exchange_rate_cache["fetched_at"]:
            age = (now - _exchange_rate_cache["fetched_at"]).total_seconds()
            if age < EXCHANGE_RATE_TTL_SECONDS:
                return _exchange_rate_cache["rates"]

    try:
        response = requests.get(f"https://api.exchangerate-api.com/v4/latest/{base_currency}", timeout=10)
        response.raise_for_status()
        data = response.json()
        rates = data.get("rates", {})
        rates[base_currency] = 1.0

        with _exchange_rate_lock:
            _exchange_rate_cache["rates"] = rates
            _exchange_rate_cache["fetched_at"] = datetime.now(timezone.utc)

        return rates
    except Exception:
        # BUG-21 NOTE: These fallback values are store_currency→RON conversion factors.
        # e.g., 1 EUR * 5.0 = 5 RON. They intentionally differ from the API's format
        # (which returns 1 RON = 0.2 EUR). Do NOT "fix" them to match the API.
        return {"RON": 1.0, "EUR": 5.0, "USD": 4.6, "BGN": 2.5, "PLN": 1.1, "CZK": 0.2}

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
    sort_field: str = Query("title"),
    sort_order: str = Query("asc"),
    db: Session = Depends(get_db)
):
    base_query = (
        db.query(models.ProductVariant)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.ProductVariant.barcode != None,
            models.ProductVariant.barcode != '',
            # BUG-24 FIX: Exclude soft-deleted products from stock view
            models.Product.deleted_at.is_(None)
        )
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
            # BUG-26 FIX: Guard against None values in title, sku, barcode
            title = v.product.title if v.product else ""
            full_text = f"{title or ''} {v.sku or ''} {v.barcode or ''}"
            normalized_full_text = normalize_and_split(full_text)
            if all(term in normalized_full_text for term in search_terms):
                matching_barcodes.add(v.barcode)
        all_variants = [v for v in all_variants if v.barcode in matching_barcodes]

    exchange_rates = get_exchange_rates("RON")

    grouped_by_barcode: Dict[str, Dict[str, Any]] = {}
    for variant in all_variants:
        barcode = variant.barcode
        if barcode not in grouped_by_barcode:
            grouped_by_barcode[barcode] = { "barcode": barcode, "variants": [] }

        store_currency = variant.product.store.currency if variant.product and variant.product.store else "RON"
        rate = exchange_rates.get(store_currency, 1.0)
        variant_stock = sum(level.available for level in variant.inventory_levels if level.available is not None)

        grouped_by_barcode[barcode]["variants"].append({
            "variant_id": variant.id, "product_title": variant.product.title if variant.product else "Unknown",
            "image_url": variant.product.image_url if variant.product else None,
            "sku": variant.sku, "store_name": variant.product.store.name if variant.product and variant.product.store else "Unknown",
            "is_barcode_primary": variant.is_barcode_primary,
            "stock": variant_stock, "retail_value_ron": (variant_stock * float(variant.price or 0)) * rate,
            "inventory_value_ron": (variant_stock * float(variant.cost_per_item or 0)) * rate,
        })

    final_groups = []
    for barcode, group in grouped_by_barcode.items():
        if not group["variants"]: continue

        primary_variant = next((v for v in group["variants"] if v["is_barcode_primary"]), group["variants"][0])
        representative_stock = primary_variant["stock"]

        total_retail_value = sum(v["retail_value_ron"] for v in group["variants"])
        total_inventory_value = sum(v["inventory_value_ron"] for v in group["variants"])

        if min_stock is not None and representative_stock < min_stock: continue
        if max_stock is not None and representative_stock > max_stock: continue
        if min_retail is not None and total_retail_value < min_retail: continue
        if max_retail is not None and total_retail_value > max_retail: continue

        final_groups.append({
            "barcode": barcode, "primary_image_url": primary_variant["image_url"], "primary_title": primary_variant["product_title"],
            "variants": group["variants"], "total_stock": representative_stock,
            "total_retail_value": round(total_retail_value, 2), "total_inventory_value": round(total_inventory_value, 2),
            "currency": "RON"
        })

    # Apply sorting
    sort_keys = {
        "stock": lambda x: x["total_stock"],
        "retail": lambda x: x["total_retail_value"],
        "barcode": lambda x: x["barcode"],
        "title": lambda x: (x["primary_title"] or "").lower(),
    }
    sort_key = sort_keys.get(sort_field, sort_keys["title"])
    reverse = sort_order.lower() == "desc"
    final_groups.sort(key=sort_key, reverse=reverse)

    grand_total_stock = sum(g['total_stock'] for g in final_groups)
    grand_total_retail = sum(g['total_retail_value'] for g in final_groups)
    grand_total_inventory = sum(g['total_inventory_value'] for g in final_groups)

    return {
        "metrics": {
            "total_stock": grand_total_stock, "total_retail_value": round(grand_total_retail, 2),
            "total_inventory_value": round(grand_total_inventory, 2)
        },
        "results": final_groups
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
    all_variants = (
        db.query(models.ProductVariant)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.ProductVariant.barcode == payload.barcode,
            models.Product.deleted_at.is_(None)
        )
        .options(joinedload(models.ProductVariant.product).joinedload(models.Product.store))
        .all()
    )

    if not all_variants:
        raise HTTPException(status_code=404, detail="No variants found with that barcode")

    variants_by_store: Dict[int, List[models.ProductVariant]] = {}
    for v in all_variants:
        store_id = v.product.store.id
        if store_id not in variants_by_store: variants_by_store[store_id] = []
        variants_by_store[store_id].append(v)

    # BUG-25 FIX: Create WriteIntents BEFORE calling Shopify to suppress echo webhooks
    _create_bulk_update_write_intents(db, payload.barcode, payload.quantity, variants_by_store.keys())

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

        variables = {
            "input": {
                "name": "available", "reason": "correction", "ignoreCompareQuantity": True,
                "quantities": quantities_payload
            }
        }

        try:
            result = service.execute_mutation("inventorySetQuantities", variables)
            if result.get("inventorySetQuantities", {}).get("userErrors", []):
                errors.append(f"Store {store.name}: {result['inventorySetQuantities']['userErrors'][0]['message']}")
            else:
                variant_ids = [v.id for v in variants]
                success_updates.append({"variant_ids": variant_ids, "location_id": store.sync_location_id, "quantity": payload.quantity})
        except Exception as e:
            errors.append(f"Store {store.name}: {str(e)}")

    # After all API calls, update the local database for the successful ones.
    if success_updates:
        for update in success_updates:
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=update["variant_ids"], location_id=update["location_id"], new_quantity=update["quantity"]
            )

    if errors:
        audit_logger.log_stock_change(payload.barcode, 0, "Manual", 0, payload.quantity,
                                       source="manual_bulk_update",
                                       details={"errors": errors, "success_count": len(success_updates)})
        raise HTTPException(status_code=422, detail={"message": "Completed with partial success.", "errors": errors})

    audit_logger.log_stock_change(payload.barcode, 0, "Manual", 0, payload.quantity,
                                   source="manual_bulk_update",
                                   details={"stores_updated": len(success_updates), "variant_count": len(all_variants)})
    return {"status": "ok", "message": "Stock updated successfully for all applicable stores."}


def _create_bulk_update_write_intents(db: Session, barcode: str, quantity: int, store_ids):
    """BUG-25 FIX: Create WriteIntents for all stores before bulk stock update."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=60)

    version_obj = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()
    version = version_obj.version if version_obj else 0

    for store_id in store_ids:
        intent = models.WriteIntent(
            barcode=barcode,
            target_store_id=store_id,
            quantity=quantity,
            barcode_version=version,
            expires_at=expires
        )
        db.add(intent)

    try:
        db.commit()
    except Exception:
        db.rollback()