
from fastapi import APIRouter, Depends, HTTPException, Body
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timezone

from sqlalchemy.orm import Session

import models
from app.api.deps import get_db
from shopify_service import ShopifyService, gid_to_id

router = APIRouter()

class MutationRequest(BaseModel):
    mutation_name: str
    variables: Dict[str, Any]

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _update_local_after_product_update(db: Session, variables: Dict[str, Any], data: Dict[str, Any]) -> None:
    # Handles: setProductCategory, updateProductType (both use productUpdate envelope)
    node = data.get("productUpdate", {})
    prod = (node.get("product") or {})
    prod_gid: Optional[str] = (variables.get("product") or {}).get("id")
    if not prod_gid:
        prod_gid = prod.get("id")
    if not prod_gid:
        return
    q = db.query(models.Product).filter(models.Product.shopify_gid == prod_gid)
    # Update product_type if present
    if "productType" in prod and prod["productType"] is not None:
        q.update({models.Product.product_type: prod["productType"]})
    # Update product_category using category.fullName if present
    cat = prod.get("category")
    if cat and isinstance(cat, dict):
        full_name = cat.get("fullName")
        if full_name:
            q.update({models.Product.product_category: full_name})
    db.commit()

def _update_local_after_variants_bulk(db: Session, mutation_name: str, variables: Dict[str, Any], data: Dict[str, Any]) -> None:
    # Handles: updateVariantPrices, updateVariantCompareAt, updateVariantBarcode, updateVariantCosts
    node = data.get("productVariantsBulkUpdate", {})
    # Prefer authoritative values from input variables
    incoming: List[Dict[str, Any]] = variables.get("variants") or []
    for v in incoming:
        v_gid = v.get("id")
        if not v_gid:
            continue
        q = db.query(models.ProductVariant).filter(models.ProductVariant.shopify_gid == v_gid)
        updates = {}
        if mutation_name == "updateVariantPrices" and v.get("price") is not None:
            updates[models.ProductVariant.price] = Decimal(str(v["price"]))
        if mutation_name == "updateVariantCompareAt" and v.get("compareAtPrice") is not None:
            updates[models.ProductVariant.compare_at_price] = Decimal(str(v["compareAtPrice"]))
        if mutation_name == "updateVariantBarcode" and v.get("barcode") is not None:
            updates[models.ProductVariant.barcode] = v["barcode"]
        if mutation_name == "updateVariantCosts":
            inv = v.get("inventoryItem") or {}
            if inv.get("cost") is not None:
                updates[models.ProductVariant.cost_per_item] = Decimal(str(inv["cost"]))
        if updates:
            q.update(updates)
    db.commit()

def _update_local_after_inventory_item_update(db: Session, variables: Dict[str, Any]) -> None:
    # Handles: updateInventoryCost (InventoryItemInput)
    inv_gid = variables.get("id")
    input_ = variables.get("input") or {}
    if not inv_gid or "cost" not in input_:
        return
    inv_id = gid_to_id(inv_gid)
    if not inv_id:
        return
    cost_val = Decimal(str(input_["cost"])) if input_["cost"] is not None else None
    q = db.query(models.ProductVariant).filter(models.ProductVariant.inventory_item_id == inv_id)
    if cost_val is not None:
        q.update({models.ProductVariant.cost_per_item: cost_val})
        db.commit()

def _update_local_after_set_quantities(db: Session, variables: Dict[str, Any]) -> None:
    # Handles: inventorySetQuantities (absolute set)
    input_ = variables.get("input") or {}
    items = input_.get("quantities") or []
    for item in items:
        inv_gid = item.get("inventoryItemId")
        loc_gid = item.get("locationId")
        qty = item.get("quantity")
        if not inv_gid or not loc_gid or qty is None:
            continue
        inv_id = gid_to_id(inv_gid)
        loc_id = gid_to_id(loc_gid)
        if not inv_id or not loc_id:
            continue
        # Find variant via inventory_item_id
        variant = db.query(models.ProductVariant).filter(models.ProductVariant.inventory_item_id == inv_id).first()
        if not variant:
            continue
        # Upsert inventory level for (variant_id, location_id)
        lvl = db.query(models.InventoryLevel).filter(
            models.InventoryLevel.variant_id == variant.id,
            models.InventoryLevel.location_id == loc_id
        ).first()
        if lvl:
            lvl.available = qty
            lvl.updated_at = _now_utc()
            lvl.last_fetched_at = _now_utc()
        else:
            lvl = models.InventoryLevel(
                variant_id=variant.id,
                location_id=loc_id,
                inventory_item_id=inv_id,
                available=qty,
                on_hand=qty,
                updated_at=_now_utc(),
                last_fetched_at=_now_utc()
            )
            db.add(lvl)
    db.commit()

@router.post("/stores/{store_id}/mutations")
def execute_mutation(store_id: int, payload: MutationRequest = Body(...), db: Session = Depends(get_db)):
    store = db.query(models.Store).filter(models.Store.id == store_id).first()
    if not store or not store.enabled:
        raise HTTPException(status_code=404, detail="Store not found or disabled")

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token, api_version="2025-10")
    data = service.execute_mutation(payload.mutation_name, payload.variables)

    # Surface userErrors if present
    envelopes = [
        "productUpdate",
        "productVariantsBulkUpdate",
        "inventoryItemUpdate",
        "inventorySetQuantities",
        "quantityRulesAdd",
    ]
    for key in envelopes:
        if key in data and isinstance(data[key], dict):
            errs = data[key].get("userErrors") or []
            if errs:
                raise HTTPException(status_code=422, detail={"userErrors": errs, "data": data})
            break

    # Persist to local DB on success
    name = payload.mutation_name
    if name in ("setProductCategory", "updateProductType"):
        _update_local_after_product_update(db, payload.variables, data)
    elif name in ("updateVariantPrices", "updateVariantCompareAt", "updateVariantBarcode", "updateVariantCosts"):
        _update_local_after_variants_bulk(db, name, payload.variables, data)
    elif name == "updateInventoryCost":
        _update_local_after_inventory_item_update(db, payload.variables)
    elif name == "inventorySetQuantities":
        _update_local_after_set_quantities(db, payload.variables)

    return data

class FindCategoriesRequest(BaseModel):
    query: str

@router.post("/stores/{store_id}/categories/find")
def find_categories(store_id: int, payload: FindCategoriesRequest = Body(...), db: Session = Depends(get_db)):
    store = db.query(models.Store).filter(models.Store.id == store_id).first()
    if not store or not store.enabled:
        raise HTTPException(status_code=404, detail="Store not found or disabled")
    service = ShopifyService(store_url=store.shopify_url, token=store.api_token, api_version="2025-10")
    data = service.find_categories(payload.query)
    return data
