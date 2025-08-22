# crud/product.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

import models
import schemas

# Prefer the shared helper; if not present (tests), use a local fallback
try:
    from shopify_service import gid_to_id  # type: ignore
except Exception:  # pragma: no cover
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid:
            return None
        try:
            return int(str(gid).split("/")[-1])
        except Exception:
            return None

# ---------- helpers ----------

def _norm_barcode(b: Optional[str]) -> Optional[str]:
    if not b:
        return None
    # normalize: trim, remove spaces, uppercase
    v = b.strip().replace(" ", "")
    return v.upper() or None

def _coalesce(new, old):
    return new if new is not None else old

def _product_status_to_db(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    return str(val).upper()

def _ensure_group_for_barcode(db: Session, barcode_norm: str) -> models.BarcodeGroup:
    grp = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.id == barcode_norm).first()
    if not grp:
        grp = models.BarcodeGroup(id=barcode_norm, status="active", pool_available=0)
        db.add(grp)
        db.flush()
    return grp

def _ensure_membership(db: Session, variant_id: int, group_id: str) -> None:
    exists = (
        db.query(models.GroupMembership)
        .filter(
            models.GroupMembership.variant_id == variant_id,
            models.GroupMembership.group_id == group_id,
        )
        .first()
    )
    if exists:
        return
    db.add(models.GroupMembership(variant_id=variant_id, group_id=group_id))


# ---------- upsert from full product + variants (GraphQL pagination job) ----------

def create_or_update_products(
    db: Session,
    store_id: int,
    items: List[Dict[str, Any]],
) -> None:
    """
    Upsert products + variants from the GraphQL bulk product fetch.
    items = [{ "product": schemas.Product, "variants": [schemas.ProductVariant, ...] }, ...]
    """
    now = datetime.utcnow()

    for bundle in items:
        p: schemas.Product = bundle["product"]
        vs: List[schemas.ProductVariant] = bundle.get("variants", []) or []

        pid = int(p.legacyResourceId or gid_to_id(p.id))  # numeric
        prod = db.query(models.Product).filter(models.Product.id == pid).first()
        if not prod:
            prod = models.Product(id=pid, store_id=store_id)
            db.add(prod)

        # update columns (do not blindly overwrite with None)
        prod.shopify_gid = _coalesce(p.id, prod.shopify_gid)
        prod.store_id = store_id
        prod.title = _coalesce(p.title, prod.title)
        prod.body_html = _coalesce(p.bodyHtml, prod.body_html)
        prod.vendor = _coalesce(p.vendor, prod.vendor)
        prod.product_type = _coalesce(p.productType, prod.product_type)
        prod.product_category = _coalesce((p.category.name if p.category else None), prod.product_category)
        prod.created_at = _coalesce(p.createdAt, prod.created_at)
        prod.handle = _coalesce(p.handle, prod.handle)
        prod.updated_at = _coalesce(p.updatedAt, prod.updated_at)
        prod.published_at = _coalesce(p.publishedAt, prod.published_at)
        prod.status = _coalesce(_product_status_to_db(p.status), prod.status)
        # keep the first non-empty featured image URL we know
        if p.featuredImage and p.featuredImage.url:
            prod.image_url = p.featuredImage.url
        prod.tags = _coalesce(",".join(p.tags) if isinstance(p.tags, list) else p.tags, prod.tags)
        prod.last_fetched_at = now

        # variants
        for v in vs:
            vid = int(v.legacyResourceId or gid_to_id(v.id))
            var = db.query(models.ProductVariant).filter(models.ProductVariant.id == vid).first()
            if not var:
                var = models.ProductVariant(id=vid, product_id=pid, store_id=store_id)
                db.add(var)

            var.shopify_gid = _coalesce(v.id, var.shopify_gid)
            var.product_id = pid
            var.store_id = store_id
            var.title = _coalesce(v.title, var.title)
            var.price = _coalesce(v.price, var.price)
            var.compare_at_price = _coalesce(v.compareAtPrice, var.compare_at_price)
            var.sku = _coalesce(v.sku, var.sku)
            var.position = _coalesce(v.position, var.position)
            var.inventory_policy = _coalesce(v.inventoryPolicy, var.inventory_policy)
            var.fulfillment_service = _coalesce(v.fulfillmentService, var.fulfillment_service)
            var.inventory_management = _coalesce(
                ("SHOPIFY" if v.inventoryItem else None), var.inventory_management
            )
            var.barcode = _coalesce(v.barcode, var.barcode)
            var.barcode_normalized = _norm_barcode(var.barcode)
            var.weight = _coalesce(v.weight, var.weight)
            var.weight_unit = _coalesce(v.weightUnit, var.weight_unit)
            var.inventory_item_id = _coalesce(
                (int(v.inventoryItem.legacyResourceId) if v.inventoryItem and v.inventoryItem.legacyResourceId else None),
                var.inventory_item_id,
            )
            var.inventory_quantity = _coalesce(v.inventoryQuantity, var.inventory_quantity)
            var.created_at = _coalesce(v.createdAt, var.created_at)
            var.updated_at = _coalesce(v.updatedAt, var.updated_at)
            var.last_fetched_at = now
            # cost precedence: unitCost > cost (legacy)
            unit_cost = None
            if v.inventoryItem and v.inventoryItem.unitCost and v.inventoryItem.unitCost.amount is not None:
                try:
                    unit_cost = float(v.inventoryItem.unitCost.amount)
                except Exception:
                    unit_cost = None
            var.cost_per_item = _coalesce(unit_cost, var.cost_per_item)
            var.tracked = True if v.inventoryItem else var.tracked

            # Ensure group by barcode
            if var.barcode_normalized:
                grp = _ensure_group_for_barcode(db, var.barcode_normalized)
                _ensure_membership(db, var.id, grp.id)

            # Optional: seed inventory level snapshot(s) if GraphQL gave us levels
            if v.inventoryItem and v.inventoryItem.inventoryLevels:
                for lvl in v.inventoryItem.inventoryLevels:
                    loc_legacy = gid_to_id(lvl.location.id) if lvl.location and lvl.location.id else None
                    if not loc_legacy:
                        continue
                    # quantities is a list of {"name": "...", "quantity": X}
                    q = {x.name: int(x.quantity) for x in (lvl.quantities or [])}
                    avail = q.get("available")
                    on_hand = q.get("on_hand", avail)
                    snap = (
                        db.query(models.InventoryLevel)
                        .filter(
                            models.InventoryLevel.inventory_item_id == var.inventory_item_id,
                            models.InventoryLevel.location_id == int(loc_legacy),
                        )
                        .first()
                    )
                    if snap:
                        if avail is not None:
                            snap.available = avail
                        if on_hand is not None:
                            snap.on_hand = on_hand
                        snap.last_fetched_at = now
                    else:
                        db.add(
                            models.InventoryLevel(
                                inventory_item_id=var.inventory_item_id,
                                location_id=int(loc_legacy),
                                available=avail if avail is not None else 0,
                                on_hand=on_hand if on_hand is not None else (avail or 0),
                                last_fetched_at=now,
                            )
                        )

    # commit is controlled by caller (jobs, routes)
    # db.commit()


# ---------- upsert from product webhook (REST-ish payload) ----------

def create_or_update_product_from_webhook(
    db: Session,
    store_id: int,
    payload: schemas.ShopifyProductWebhook,
) -> None:
    """
    Shopify product webhook upsert (products/create|update).
    Maps REST/webhook fields to our DB (no use of admin_graphql_api_id).
    """
    now = datetime.utcnow()

    pid = int(payload.id)
    prod = db.query(models.Product).filter(models.Product.id == pid).first()
    if not prod:
        prod = models.Product(id=pid, store_id=store_id)
        db.add(prod)

    prod.shopify_gid = payload.admin_graphql_api_id or prod.shopify_gid
    prod.store_id = store_id
    prod.title = _coalesce(payload.title, prod.title)
    prod.body_html = _coalesce(payload.body_html, prod.body_html)
    prod.vendor = _coalesce(payload.vendor, prod.vendor)
    prod.product_type = _coalesce(payload.product_type, prod.product_type)
    prod.product_category = _coalesce(
        payload.product_category.get("name") if payload.product_category else None, prod.product_category
    )
    prod.created_at = _coalesce(payload.created_at, prod.created_at)
    prod.handle = _coalesce(payload.handle, prod.handle)
    prod.updated_at = _coalesce(payload.updated_at, prod.updated_at)
    prod.published_at = _coalesce(payload.published_at, prod.published_at)
    prod.status = _coalesce(_product_status_to_db(payload.status), prod.status)
    prod.tags = _coalesce(",".join(payload.tags) if isinstance(payload.tags, list) else payload.tags, prod.tags)
    if payload.image and payload.image.get("src"):
        prod.image_url = payload.image["src"]
    prod.last_fetched_at = now

    # Variants from webhook
    for v in payload.variants or []:
        vid = int(v.get("id"))
        var = db.query(models.ProductVariant).filter(models.ProductVariant.id == vid).first()
        if not var:
            var = models.ProductVariant(id=vid, product_id=pid, store_id=store_id)
            db.add(var)

        var.product_id = pid
        var.store_id = store_id
        var.title = _coalesce(v.get("title"), var.title)
        var.price = _coalesce(v.get("price"), var.price)
        var.compare_at_price = _coalesce(v.get("compare_at_price"), var.compare_at_price)
        var.sku = _coalesce(v.get("sku"), var.sku)
        var.position = _coalesce(v.get("position"), var.position)
        var.inventory_policy = _coalesce(v.get("inventory_policy"), var.inventory_policy)
        var.fulfillment_service = _coalesce(v.get("fulfillment_service"), var.fulfillment_service)
        var.inventory_management = _coalesce(v.get("inventory_management"), var.inventory_management)
        var.barcode = _coalesce(v.get("barcode"), var.barcode)
        var.barcode_normalized = _norm_barcode(var.barcode)
        var.weight = _coalesce(v.get("weight"), var.weight)
        var.weight_unit = _coalesce(v.get("weight_unit"), var.weight_unit)
        inv_item_id = v.get("inventory_item_id")
        if inv_item_id is not None:
            try:
                var.inventory_item_id = int(inv_item_id)
            except Exception:
                pass
        var.inventory_quantity = _coalesce(v.get("inventory_quantity"), var.inventory_quantity)
        var.created_at = _coalesce(v.get("created_at"), var.created_at)
        var.updated_at = _coalesce(v.get("updated_at"), var.updated_at)
        var.last_fetched_at = now

        # cost precedence: unit_cost (from inventory webhook) > cost
        # Webhook variant usually contains "cost" (if present)
        try:
            vc = v.get("cost")
            if vc is not None:
                var.cost_per_item = float(vc)
        except Exception:
            pass

        # "tracked" defaults to True unless the variant explicitly says otherwise
        tracked_flag = v.get("tracked")
        if tracked_flag is not None:
            var.tracked = bool(tracked_flag)

        # ensure group by barcode
        if var.barcode_normalized:
            grp = _ensure_group_for_barcode(db, var.barcode_normalized)
            _ensure_membership(db, var.id, grp.id)

    # db.commit() controlled by caller
