# routes/mutations.py
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from crud import store as crud_store
import models
from shopify_service import ShopifyService, gid_to_id

router = APIRouter(prefix="/api/mutations", tags=["Mutations"])

# ---------- helpers ----------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _raise_if_user_errors(result: Dict[str, Any]) -> None:
    for key in (
        "productUpdate",
        "productVariantsBulkUpdate",
        "inventoryItemUpdate",
        "inventorySetQuantities",
        "quantityRulesAdd",
    ):
        node = result.get(key)
        if isinstance(node, dict):
            errs = node.get("userErrors") or []
            if errs:
                raise HTTPException(status_code=422, detail={"userErrors": errs, "data": result})
            break

def _persist_product_update(db: Session, variables: Dict[str, Any], data: Dict[str, Any]) -> None:
    node = data.get("productUpdate") or {}
    prod = node.get("product") or {}
    prod_gid: Optional[str] = (variables.get("product") or {}).get("id") or prod.get("id")
    if not prod_gid:
        return
    updates = {}
    if "productType" in prod and prod["productType"] is not None:
        updates[models.Product.product_type] = prod["productType"]
    cat = prod.get("category")
    if isinstance(cat, dict) and cat.get("fullName"):
        updates[models.Product.product_category] = cat["fullName"]
    if updates:
        (db.query(models.Product)
           .filter(models.Product.shopify_gid == prod_gid)
           .update(updates, synchronize_session=False))
        db.commit()

def _persist_variants_bulk(db: Session, mutation_name: str, variables: Dict[str, Any]) -> None:
    incoming: List[Dict[str, Any]] = variables.get("variants") or []
    for v in incoming:
        v_gid = v.get("id")
        if not v_gid:
            continue
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
            (db.query(models.ProductVariant)
               .filter(models.ProductVariant.shopify_gid == v_gid)
               .update(updates, synchronize_session=False))
    if incoming:
        db.commit()

def _persist_inventory_item_update(db: Session, variables: Dict[str, Any]) -> None:
    inv_gid = variables.get("id")
    input_ = variables.get("input") or {}
    if not inv_gid or "cost" not in input_:
        return
    inv_id = gid_to_id(inv_gid)
    if not inv_id:
        return
    cost_val = input_.get("cost")
    if cost_val is None:
        return
    (db.query(models.ProductVariant)
       .filter(models.ProductVariant.inventory_item_id == inv_id)
       .update({models.ProductVariant.cost_per_item: Decimal(str(cost_val))},
               synchronize_session=False))
    db.commit()

def _ensure_location(db: Session, loc_gid: str, store_id: int) -> Optional[int]:
    loc_id = gid_to_id(loc_gid)
    if not loc_id:
        return None
    loc = db.query(models.Location).filter(models.Location.id == loc_id).first()
    if not loc:
        db.add(models.Location(id=loc_id, store_id=store_id, name="", shopify_gid=loc_gid))
        try:
            db.commit()
        except Exception:
            db.rollback()
            return None
    return loc_id

def _persist_set_quantities(db: Session, variables: Dict[str, Any]) -> None:
    input_ = variables.get("input") or {}
    items = input_.get("quantities") or []
    for item in items:
        inv_gid = item.get("inventoryItemId")
        loc_gid = item.get("locationId")
        qty = item.get("quantity")
        if not inv_gid or not loc_gid or qty is None:
            continue
        inv_id = gid_to_id(inv_gid)
        if not inv_id:
            continue
        variant = (db.query(models.ProductVariant)
                     .filter(models.ProductVariant.inventory_item_id == inv_id)
                     .first())
        if not variant:
            continue
        loc_id = _ensure_location(db, loc_gid, store_id=variant.store_id)
        if not loc_id:
            continue
        lvl = (db.query(models.InventoryLevel)
                 .filter(models.InventoryLevel.variant_id == variant.id,
                         models.InventoryLevel.location_id == loc_id)
                 .first())
        if lvl:
            lvl.available = int(qty)
            if lvl.on_hand is None:
                lvl.on_hand = int(qty)
            lvl.updated_at = _now_utc()
            lvl.last_fetched_at = _now_utc()
        else:
            db.add(models.InventoryLevel(
                variant_id=variant.id,
                location_id=loc_id,
                inventory_item_id=inv_id,
                available=int(qty),
                on_hand=int(qty),
                updated_at=_now_utc(),
                last_fetched_at=_now_utc(),
            ))
    if items:
        db.commit()

# ---------- endpoints ----------

@router.post("/execute/{store_id}")
def execute_mutation(store_id: int, payload: Dict[str, Any], db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    mutation_name = payload.get("mutation_name")
    variables = payload.get("variables")
    if not mutation_name or not isinstance(variables, dict):
        raise HTTPException(status_code=400, detail="Missing mutation_name or variables")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        result = service.execute_mutation(mutation_name, variables)

        _raise_if_user_errors(result)

        if mutation_name in ("setProductCategory", "updateProductType"):
            _persist_product_update(db, variables, result)
        elif mutation_name in ("updateVariantPrices", "updateVariantCompareAt", "updateVariantBarcode", "updateVariantCosts"):
            _persist_variants_bulk(db, mutation_name, variables)
        elif mutation_name == "updateInventoryCost":
            _persist_inventory_item_update(db, variables)
        elif mutation_name == "inventorySetQuantities":
            _persist_set_quantities(db, variables)

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/find-categories/{store_id}")
def find_categories(store_id: int, payload: Dict[str, Any], db: Session = Depends(get_db)):
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    query = payload.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Missing query")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        return service.find_categories(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
