# crud/product.py

from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
import models

# -------- helpers --------

def _nz(v):
    """Return None if falsy empty string; otherwise the value."""
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v

def _merge_set(obj, attr, value):
    """
    Only set obj.attr if 'value' is not None.
    Prevents overwriting DB with NULL from partial webhooks.
    """
    if value is not None:
        setattr(obj, attr, value)

def normalize_barcode(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = "".join(ch for ch in str(raw).strip() if ch.isalnum())
    return s if s else None

def _ensure_group(db: Session, group_id: str) -> models.BarcodeGroup:
    grp = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.id == group_id).first()
    if not grp:
        grp = models.BarcodeGroup(id=group_id, status="active", pool_available=0)
        db.add(grp)
        db.flush()
    return grp

def _set_variant_group(db: Session, variant: models.ProductVariant, group_id: str) -> bool:
    """
    Idempotent: attach variant to group_id if not already attached.
    Returns True only when a new membership row was created or moved.
    """
    if not group_id:
        return False

    # existing membership?
    gm = db.query(models.GroupMembership).filter(
        models.GroupMembership.variant_id == variant.id
    ).first()

    if gm:
        if gm.group_id == group_id:
            # already in this group; no-op
            return False
        # move to new group
        gm.group_id = group_id
        return True

    # fresh attach
    db.add(models.GroupMembership(variant_id=variant.id, group_id=group_id))
    return True

# -------- core upserts --------

def upsert_product_from_rest_webhook(db: Session, store_id: int, payload: Dict[str, Any]) -> None:
    """
    Merge-safe upsert for REST product webhooks (products/create, products/update).
    - Never overwrite DB values with NULL from partial payloads.
    - Idempotent group membership from barcode on each variant.
    """
    if not payload or "id" not in payload:
        return

    p_id = int(payload["id"])
    product = db.query(models.Product).filter(models.Product.id == p_id).first()
    is_new = product is None

    # Extract fields (REST webhook shape)
    title          = _nz(payload.get("title"))
    body_html      = _nz(payload.get("body_html"))
    vendor         = _nz(payload.get("vendor"))
    product_type   = _nz(payload.get("product_type"))
    handle         = _nz(payload.get("handle"))
    status         = _nz(payload.get("status"))  # Shopify may send "active"/"ACTIVE"
    tags           = _nz(payload.get("tags"))
    created_at     = payload.get("created_at")
    updated_at     = payload.get("updated_at")
    published_at   = payload.get("published_at")
    image_url      = None
    if payload.get("image") and payload["image"].get("src"):
        image_url = _nz(payload["image"]["src"])

    if is_new:
        product = models.Product(
            id=p_id,
            shopify_gid=str(payload.get("admin_graphql_api_id") or payload.get("admin_graphql_api_id") or f"gid://shopify/Product/{p_id}"),
            store_id=store_id,
            title=title or "Untitled",
        )
        db.add(product)

    # Merge-safe updates
    _merge_set(product, "title", title)
    _merge_set(product, "body_html", body_html)
    _merge_set(product, "vendor", vendor)
    _merge_set(product, "product_type", product_type)
    _merge_set(product, "handle", handle)
    _merge_set(product, "status", status)
    _merge_set(product, "tags", tags)
    # Timestamps
    if created_at:
        _merge_set(product, "created_at", _coerce_dt(created_at))
    if updated_at:
        _merge_set(product, "updated_at", _coerce_dt(updated_at))
    if published_at:
        _merge_set(product, "published_at", _coerce_dt(published_at))
    # Image only if present (don't null it out)
    if image_url:
        product.image_url = image_url

    # --- Variants merge ---
    variants = payload.get("variants") or []
    for v in variants:
        v_id = int(v["id"])
        variant = db.query(models.ProductVariant).filter(models.ProductVariant.id == v_id).first()
        if not variant:
            variant = models.ProductVariant(
                id=v_id,
                shopify_gid=str(v.get("admin_graphql_api_id") or f"gid://shopify/ProductVariant/{v_id}"),
                product_id=p_id,
                store_id=store_id,
            )
            db.add(variant)

        # REST fields
        _merge_set(variant, "title", _nz(v.get("title")))
        _merge_set(variant, "sku", _nz(v.get("sku")))
        _merge_set(variant, "price", _nz(v.get("price")))
        _merge_set(variant, "compare_at_price", _nz(v.get("compare_at_price")))
        _merge_set(variant, "inventory_policy", _nz(v.get("inventory_policy")))
        _merge_set(variant, "position", _nz(v.get("position")))
        _merge_set(variant, "fulfillment_service", _nz(v.get("fulfillment_service")))
        _merge_set(variant, "inventory_management", _nz(v.get("inventory_management")))
        _merge_set(variant, "cost_per_item", _nz(v.get("cost")))
        # times
        if v.get("created_at"):
            _merge_set(variant, "created_at", _coerce_dt(v["created_at"]))
        if v.get("updated_at"):
            _merge_set(variant, "updated_at", _coerce_dt(v["updated_at"]))

        # IDs
        if v.get("inventory_item_id") is not None:
            _merge_set(variant, "inventory_item_id", int(v["inventory_item_id"]))

        # inventory_quantity in variant webhook is global; DO NOT write to inventory_levels here.
        if v.get("inventory_quantity") is not None:
            _merge_set(variant, "inventory_quantity", int(v["inventory_quantity"]))

        # barcode/group
        raw_barcode = _nz(v.get("barcode"))
        norm = normalize_barcode(raw_barcode)
        _merge_set(variant, "barcode", raw_barcode)
        _merge_set(variant, "barcode_normalized", norm)

        # update group only if we have a barcode
        if norm:
            _ensure_group(db, norm)
            moved = _set_variant_group(db, variant, norm)
            if moved:
                print(f"Variant {variant.id} linked to group '{norm}'")

    # touch product last_fetched_at (auto via model onupdate, but ensure changed)
    product.last_fetched_at = func.now()
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # re-try minimal: dedupe membership if conflict
        _cleanup_dupe_memberships(db, p_id)
        db.commit()


def _coerce_dt(dt_val: Any) -> Optional[datetime]:
    """Accept ISO strings or datetime; return datetime or None."""
    if dt_val is None:
        return None
    if isinstance(dt_val, datetime):
        return dt_val
    try:
        # let PostgreSQL parse text if needed, but best effort here:
        from dateutil.parser import isoparse  # type: ignore
        return isoparse(str(dt_val))
    except Exception:
        return None


def _cleanup_dupe_memberships(db: Session, product_id: int):
    """
    Defensive cleanup if there was a unique conflict on group_membership.
    Ensures one row per variant.
    """
    # find all variants of this product with >1 memberships (shouldn't happen with our logic)
    q = db.query(models.ProductVariant.id).filter(models.ProductVariant.product_id == product_id).all()
    variant_ids = [r[0] for r in q]
    for vid in variant_ids:
        rows = db.query(models.GroupMembership).filter(models.GroupMembership.variant_id == vid).all()
        if len(rows) > 1:
            # keep the most recent, delete the rest
            keep = rows[0]
            for r in rows[1:]:
                db.delete(r)
    db.flush()
