# routes/mutations.py
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from crud import store as crud_store
import models
from shopify_service import ShopifyService, gid_to_id

logger = logging.getLogger("mutations")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.DEBUG)  # set to INFO in prod if too chatty

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
            logger.debug("Shopify envelope=%s userErrors=%s", key, errs)
            if errs:
                raise HTTPException(status_code=422, detail={"userErrors": errs, "data": result})
            break

def _persist_product_update(db: Session, variables: Dict[str, Any], data: Dict[str, Any]) -> None:
    node = data.get("productUpdate") or {}
    prod = node.get("product") or {}
    prod_gid: Optional[str] = (variables.get("product") or {}).get("id") or prod.get("id")
    logger.debug("Persist productUpdate prod_gid=%s payload.product=%s resp.product=%s",
                 prod_gid, variables.get("product"), prod)
    if not prod_gid:
        logger.debug("Skip productUpdate: no product GID")
        return
    updates = {}
    if "productType" in prod and prod["productType"] is not None:
        updates[models.Product.product_type] = prod["productType"]
    cat = prod.get("category")
    if isinstance(cat, dict) and cat.get("fullName"):
        updates[models.Product.product_category] = cat["fullName"]
    if updates:
        count = (db.query(models.Product)
                   .filter(models.Product.shopify_gid == prod_gid)
                   .update(updates, synchronize_session=False))
        logger.debug("productUpdate DB rows updated=%s for prod_gid=%s fields=%s", count, prod_gid, list(updates.keys()))
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            logger.exception("Commit failed on productUpdate: %s", e)
            raise

def _persist_variants_bulk(db: Session, mutation_name: str, variables: Dict[str, Any]) -> None:
    incoming: List[Dict[str, Any]] = variables.get("variants") or []
    logger.debug("Persist variantsBulk mutation=%s count=%d", mutation_name, len(incoming))
    total_updated = 0
    for v in incoming:
        v_gid = v.get("id")
        if not v_gid:
            logger.debug("Skip variant with missing id payload=%s", v)
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
            count = (db.query(models.ProductVariant)
                       .filter(models.ProductVariant.shopify_gid == v_gid)
                       .update(updates, synchronize_session=False))
            total_updated += count
            logger.debug("variantsBulk updated=%d for v_gid=%s fields=%s", count, v_gid, list(updates.keys()))
        else:
            logger.debug("variantsBulk no updates computed for v_gid=%s payload=%s", v_gid, v)
    if incoming:
        try:
            db.commit()
            logger.debug("variantsBulk commit ok total_rows_updated=%d", total_updated)
        except Exception as e:
            db.rollback()
            logger.exception("Commit failed on variantsBulk: %s", e)
            raise

def _persist_inventory_item_update(db: Session, variables: Dict[str, Any]) -> None:
    inv_gid = variables.get("id")
    input_ = variables.get("input") or {}
    logger.debug("Persist inventoryItemUpdate inv_gid=%s input=%s", inv_gid, input_)
    if not inv_gid or "cost" not in input_:
        logger.debug("Skip inventoryItemUpdate: missing id or cost")
        return
    inv_id = gid_to_id(inv_gid)
    if not inv_id:
        logger.debug("Skip inventoryItemUpdate: could not parse inv_id from %s", inv_gid)
        return
    cost_val = input_.get("cost")
    if cost_val is None:
        logger.debug("Skip inventoryItemUpdate: cost is None")
        return
    count = (db.query(models.ProductVariant)
               .filter(models.ProductVariant.inventory_item_id == inv_id)
               .update({models.ProductVariant.cost_per_item: Decimal(str(cost_val))},
                       synchronize_session=False))
    logger.debug("inventoryItemUpdate rows updated=%d for inv_id=%s", count, inv_id)
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.exception("Commit failed on inventoryItemUpdate: %s", e)
        raise

def _ensure_location(db: Session, loc_gid: str, store_id: int) -> Optional[int]:
    loc_id = gid_to_id(loc_gid)
    logger.debug("Ensure location loc_gid=%s parsed loc_id=%s store_id=%s", loc_gid, loc_id, store_id)
    if not loc_id:
        return None
    loc = db.query(models.Location).filter(models.Location.id == loc_id).first()
    if not loc:
        db.add(models.Location(id=loc_id, store_id=store_id, name="", shopify_gid=loc_gid))
        try:
            db.commit()
            logger.debug("Inserted stub location id=%s for store_id=%s", loc_id, store_id)
        except Exception as e:
            db.rollback()
            logger.exception("Failed to insert stub location id=%s: %s", loc_id, e)
            return None
    return loc_id

def _persist_set_quantities(db: Session, variables: Dict[str, Any]) -> None:
    input_ = variables.get("input") or {}
    items = input_.get("quantities") or []
    logger.debug("Persist inventorySetQuantities items=%s", items)

    upserts = 0
    for item in items:
        inv_gid = item.get("inventoryItemId")
        loc_gid = item.get("locationId")
        qty = item.get("quantity")
        logger.debug("Item inv_gid=%s loc_gid=%s qty=%s", inv_gid, loc_gid, qty)
        if not inv_gid or not loc_gid or qty is None:
            logger.debug("Skip item: missing inv_gid/loc_gid/qty")
            continue

        inv_id = gid_to_id(inv_gid)
        if not inv_id:
            logger.debug("Skip item: could not parse inv_id from %s", inv_gid)
            continue

        variant = (db.query(models.ProductVariant)
                     .filter(models.ProductVariant.inventory_item_id == inv_id)
                     .first())
        if not variant:
            logger.debug("No variant found for inv_id=%s", inv_id)
            continue

        loc_id = _ensure_location(db, loc_gid, store_id=variant.store_id)
        if not loc_id:
            logger.debug("Cannot ensure location for loc_gid=%s", loc_gid)
            continue

        lvl = (db.query(models.InventoryLevel)
                 .filter(models.InventoryLevel.variant_id == variant.id,
                         models.InventoryLevel.location_id == loc_id)
                 .first())
        if lvl:
            logger.debug("Update inventory_level existing variant_id=%s location_id=%s from avail=%s to=%s",
                         variant.id, loc_id, lvl.available, qty)
            lvl.available = int(qty)
            if lvl.on_hand is None:
                lvl.on_hand = int(qty)
            lvl.updated_at = _now_utc()
            lvl.last_fetched_at = _now_utc()
            upserts += 1
        else:
            logger.debug("Insert inventory_level new variant_id=%s location_id=%s qty=%s", variant.id, loc_id, qty)
            db.add(models.InventoryLevel(
                variant_id=variant.id,
                location_id=loc_id,
                inventory_item_id=inv_id,
                available=int(qty),
                on_hand=int(qty),
                updated_at=_now_utc(),
                last_fetched_at=_now_utc(),
            ))
            upserts += 1

    if items:
        try:
            db.commit()
            logger.debug("inventorySetQuantities commit ok rows_touched=%d", upserts)
        except Exception as e:
            db.rollback()
            logger.exception("Commit failed on inventorySetQuantities: %s", e)
            raise

# ---------- endpoints ----------

@router.post("/execute/{store_id}")
def execute_mutation(store_id: int, payload: Dict[str, Any], db: Session = Depends(get_db)):
    """
    Execute a GraphQL mutation and persist success locally.
    """
    logger.debug("execute_mutation store_id=%s payload_keys=%s", store_id, list(payload.keys()))
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    mutation_name = payload.get("mutation_name")
    variables = payload.get("variables")
    logger.debug("mutation_name=%s variables=%s", mutation_name, variables)
    if not mutation_name or not isinstance(variables, dict):
        raise HTTPException(status_code=400, detail="Missing mutation_name or variables")

    try:
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        result = service.execute_mutation(mutation_name, variables)
        logger.debug("Shopify result keys=%s", list(result.keys()))

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
        logger.exception("execute_mutation failed: %s", e)
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
        logger.exception("find_categories failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
