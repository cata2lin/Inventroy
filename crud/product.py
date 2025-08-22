# crud/product.py
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session

import models

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


# ---------- generic helpers (robust to dicts or pydantic models) ----------

def _get(obj: Any, *path: str, default=None):
    """Safe getter that works with both dicts and objects."""
    cur = obj
    for key in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(key, default if key == path[-1] else None)
        else:
            cur = getattr(cur, key, default if key == path[-1] else None)
    return cur


def _to_dt(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        # Accept naive or aware ISO strings
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


def _norm_barcode(b: Optional[str]) -> Optional[str]:
    if not b:
        return None
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


# ---------- field extraction (handles aliases + shapes) ----------

def _extract_product_fields(prod: Any) -> Dict[str, Any]:
    # id
    pid = (
        _get(prod, "legacyResourceId")
        or _get(prod, "legacy_resource_id")
        or gid_to_id(_get(prod, "id"))
        or gid_to_id(_get(prod, "shopify_gid"))
    )
    if pid is None:
        raise ValueError("Unable to extract numeric product id.")

    tags_val = _get(prod, "tags")
    if isinstance(tags_val, list):
        tags_val = ",".join(tags_val)

    image_url = _get(prod, "featuredImage", "url") or _get(prod, "image_url")

    return {
        "id": int(pid),
        "shopify_gid": _get(prod, "id") or _get(prod, "shopify_gid"),
        "title": _get(prod, "title"),
        "body_html": _get(prod, "bodyHtml") or _get(prod, "body_html"),
        "vendor": _get(prod, "vendor"),
        "product_type": _get(prod, "productType") or _get(prod, "product_type"),
        "product_category": _get(prod, "category", "name") or _get(prod, "product_category"),
        "created_at": _to_dt(_get(prod, "createdAt")),
        "handle": _get(prod, "handle"),
        "updated_at": _to_dt(_get(prod, "updatedAt")),
        "published_at": _to_dt(_get(prod, "publishedAt")),
        "status": _product_status_to_db(_get(prod, "status")),
        "tags": tags_val,
        "image_url": image_url,
    }


def _extract_variant_fields(variant: Any, product_id: int) -> Dict[str, Any]:
    vid = (
        _get(variant, "legacyResourceId")
        or _get(variant, "legacy_resource_id")
        or gid_to_id(_get(variant, "id"))
    )
    if vid is None:
        raise ValueError("Unable to extract numeric variant id.")

    inv_item_id = (
        _get(variant, "inventoryItem", "legacyResourceId")
        or _get(variant, "inventory_item_id")
        or gid_to_id(_get(variant, "inventoryItem", "id"))
    )

    # unit cost
    unit_cost = None
    amount = _get(variant, "inventoryItem", "unitCost", "amount")
    if amount is not None:
        try:
            unit_cost = float(amount)
        except Exception:
            unit_cost = None

    return {
        "id": int(vid),
        "shopify_gid": _get(variant, "id"),
        "product_id": product_id,
        "title": _get(variant, "title"),
        "sku": _get(variant, "sku"),
        "barcode": _get(variant, "barcode"),
        "price": _get(variant, "price"),
        "compare_at_price": _get(variant, "compareAtPrice") or _get(variant, "compare_at_price"),
        "position": _get(variant, "position"),
        "inventory_item_id": int(inv_item_id) if inv_item_id is not None else None,
        "inventory_quantity": _get(variant, "inventoryQuantity") or _get(variant, "inventory_quantity"),
        "created_at": _to_dt(_get(variant, "createdAt") or _get(variant, "created_at")),
        "updated_at": _to_dt(_get(variant, "updatedAt") or _get(variant, "updated_at")),
        "inventory_policy": _get(variant, "inventoryPolicy") or _get(variant, "inventory_policy"),
        "fulfillment_service": _get(variant, "fulfillmentService") or _get(variant, "fulfillment_service"),
        "inventory_management": (
            "SHOPIFY" if _get(variant, "inventoryItem") else _get(variant, "inventory_management")
        ),
        "weight": _get(variant, "weight"),
        "weight_unit": _get(variant, "weightUnit") or _get(variant, "weight_unit"),
        "cost_per_item": unit_cost,
        "tracked": True if _get(variant, "inventoryItem") else _get(variant, "tracked"),
        "inventory_levels": _get(variant, "inventoryItem", "inventoryLevels"),
    }


# ---------- upsert from full product + variants (GraphQL pagination job) ----------

def create_or_update_products(
    db: Session,
    store_id: int,
    items: List[Any],
) -> None:
    """
    Upsert products + variants from the GraphQL product fetch.

    Accepts pages in either of these shapes:
      - [{ "product": ProductModel|dict, "variants": [VariantModel|dict, ...] }, ...]
      - [ProductModel|dict, ...]  (we'll try to read .variants if present)
    """
    now = datetime.now(timezone.utc)

    for bundle in items or []:
        # Unwrap to product + variants
        if isinstance(bundle, dict) and ("product" in bundle or "variants" in bundle):
            p = bundle.get("product", bundle.get("Product", bundle.get("PRODUCT")))
            vs = bundle.get("variants") or []
        else:
            p = bundle
            vs = _get(bundle, "variants") or []
            if not isinstance(vs, list):
                vs = []

        # --- product fields & upsert ---
        fields = _extract_product_fields(p)
        pid = fields["id"]

        prod = db.query(models.Product).filter(models.Product.id == pid).first()
        if not prod:
            prod = models.Product(id=pid, store_id=store_id)
            db.add(prod)

        prod.store_id = store_id
        prod.shopify_gid = _coalesce(fields["shopify_gid"], prod.shopify_gid)
        prod.title = _coalesce(fields["title"], prod.title)
        prod.body_html = _coalesce(fields["body_html"], prod.body_html)
        prod.vendor = _coalesce(fields["vendor"], prod.vendor)
        prod.product_type = _coalesce(fields["product_type"], prod.product_type)
        prod.product_category = _coalesce(fields["product_category"], prod.product_category)
        prod.created_at = _coalesce(fields["created_at"], prod.created_at)
        prod.handle = _coalesce(fields["handle"], prod.handle)
        prod.updated_at = _coalesce(fields["updated_at"], prod.updated_at)
        prod.published_at = _coalesce(fields["published_at"], prod.published_at)
        prod.status = _coalesce(fields["status"], prod.status)
        prod.tags = _coalesce(fields["tags"], prod.tags)
        if fields["image_url"]:
            prod.image_url = fields["image_url"]
        prod.last_fetched_at = now

        # --- variants upsert ---
        for v in vs:
            try:
                v_fields = _extract_variant_fields(v, product_id=pid)
            except Exception:
                # Skip malformed variant rather than failing the whole page
                continue

            vid = v_fields["id"]
            var = db.query(models.ProductVariant).filter(models.ProductVariant.id == vid).first()
            if not var:
                var = models.ProductVariant(id=vid, product_id=pid, store_id=store_id)
                db.add(var)

            var.product_id = pid
            var.store_id = store_id
            var.shopify_gid = _coalesce(v_fields["shopify_gid"], var.shopify_gid)
            var.title = _coalesce(v_fields["title"], var.title)
            var.sku = _coalesce(v_fields["sku"], var.sku)
            var.barcode = _coalesce(v_fields["barcode"], var.barcode)
            var.barcode_normalized = _norm_barcode(var.barcode)
            var.price = _coalesce(v_fields["price"], var.price)
            var.compare_at_price = _coalesce(v_fields["compare_at_price"], var.compare_at_price)
            var.position = _coalesce(v_fields["position"], var.position)
            var.inventory_policy = _coalesce(v_fields["inventory_policy"], var.inventory_policy)
            var.fulfillment_service = _coalesce(v_fields["fulfillment_service"], var.fulfillment_service)
            var.inventory_management = _coalesce(v_fields["inventory_management"], var.inventory_management)
            if v_fields["inventory_item_id"] is not None:
                var.inventory_item_id = v_fields["inventory_item_id"]
            var.inventory_quantity = _coalesce(v_fields["inventory_quantity"], var.inventory_quantity)
            var.created_at = _coalesce(v_fields["created_at"], var.created_at)
            var.updated_at = _coalesce(v_fields["updated_at"], var.updated_at)
            var.last_fetched_at = now
            if v_fields["cost_per_item"] is not None:
                var.cost_per_item = v_fields["cost_per_item"]
            if v_fields["tracked"] is not None:
                var.tracked = bool(v_fields["tracked"])

            # Ensure group membership by barcode
            if var.barcode_normalized:
                grp = _ensure_group_for_barcode(db, var.barcode_normalized)
                _ensure_membership(db, var.id, grp.id)

            # Optional: seed inventory level snapshots if present on the variant
            levels = v_fields["inventory_levels"]
            if levels:
                for lvl in levels:
                    # shapes: lvl["location"]["id"] or lvl.location.id
                    loc_gid = _get(lvl, "location", "id")
                    loc_legacy = gid_to_id(loc_gid) if loc_gid else None
                    if not loc_legacy:
                        continue
                    # quantities: list of { name, quantity }
                    quantities = _get(lvl, "quantities") or []
                    qmap = {}
                    for q in quantities:
                        name = _get(q, "name")
                        qty = _get(q, "quantity")
                        if name is not None and qty is not None:
                            try:
                                qmap[str(name)] = int(qty)
                            except Exception:
                                pass
                    avail = qmap.get("available")
                    on_hand = qmap.get("on_hand", avail)

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
                                on_hand=on_hand if on_hand is not None else (avail or 0) or 0,
                                last_fetched_at=now,
                            )
                        )
    # commit controlled by caller


# ---------- upsert from product webhook (tolerant to shapes) ----------

def create_or_update_product_from_webhook(
    db: Session,
    store_id: int,
    payload: Any,
) -> None:
    """
    Shopify product webhook upsert (products/create|update).

    Accepts pydantic model or dict. Extracts id from either:
      • numeric id
      • admin_graphql_api_id (gid://) -> converted to numeric
      • legacyResourceId / legacy_resource_id
    """
    now = datetime.now(timezone.utc)

    # Some webhooks wrap in {"product": {...}}
    prod = payload.get("product") if isinstance(payload, dict) and "product" in payload else payload

    pid = (
        _get(prod, "id") if isinstance(_get(prod, "id"), int) else None
    ) or _get(prod, "legacyResourceId") or _get(prod, "legacy_resource_id") or gid_to_id(_get(prod, "admin_graphql_api_id"))

    if pid is None:
        # also accept GraphQL product id in "id": "gid://shopify/Product/123"
        pid = gid_to_id(_get(prod, "id"))

    if pid is None:
        raise ValueError("Webhook product payload missing usable id.")

    pid = int(pid)

    db_prod = db.query(models.Product).filter(models.Product.id == pid).first()
    if not db_prod:
        db_prod = models.Product(id=pid, store_id=store_id)
        db.add(db_prod)

    db_prod.store_id = store_id
    db_prod.shopify_gid = _coalesce(_get(prod, "admin_graphql_api_id") or _get(prod, "id"), db_prod.shopify_gid)
    db_prod.title = _coalesce(_get(prod, "title"), db_prod.title)
    db_prod.body_html = _coalesce(_get(prod, "body_html"), db_prod.body_html)
    db_prod.vendor = _coalesce(_get(prod, "vendor"), db_prod.vendor)
    db_prod.product_type = _coalesce(_get(prod, "product_type"), db_prod.product_type)
    db_prod.product_category = _coalesce(_get(prod, "product_category", "name"), db_prod.product_category)
    db_prod.created_at = _coalesce(_to_dt(_get(prod, "created_at")), db_prod.created_at)
    db_prod.handle = _coalesce(_get(prod, "handle"), db_prod.handle)
    db_prod.updated_at = _coalesce(_to_dt(_get(prod, "updated_at")), db_prod.updated_at)
    db_prod.published_at = _coalesce(_to_dt(_get(prod, "published_at")), db_prod.published_at)
    db_prod.status = _coalesce(_product_status_to_db(_get(prod, "status")), db_prod.status)

    tags_val = _get(prod, "tags")
    if isinstance(tags_val, list):
        tags_val = ",".join(tags_val)
    db_prod.tags = _coalesce(tags_val, db_prod.tags)

    image_src = _get(prod, "image", "src")
    if image_src:
        db_prod.image_url = image_src

    db_prod.last_fetched_at = now

    # Variants (if present)
    variants = _get(prod, "variants") or []
    if not isinstance(variants, list):
        variants = []

    for v in variants:
        vid = _get(v, "id")
        if vid is None:
            # last resort: GraphQL id on webhook variant
            vid = gid_to_id(_get(v, "admin_graphql_api_id"))
        if vid is None:
            continue
        vid = int(vid)

        var = db.query(models.ProductVariant).filter(models.ProductVariant.id == vid).first()
        if not var:
            var = models.ProductVariant(id=vid, product_id=pid, store_id=store_id)
            db.add(var)

        var.product_id = pid
        var.store_id = store_id
        var.shopify_gid = _coalesce(_get(v, "admin_graphql_api_id") or _get(v, "id"), var.shopify_gid)
        var.title = _coalesce(_get(v, "title"), var.title)
        var.price = _coalesce(_get(v, "price"), var.price)
        var.compare_at_price = _coalesce(_get(v, "compare_at_price"), var.compare_at_price)
        var.sku = _coalesce(_get(v, "sku"), var.sku)
        var.position = _coalesce(_get(v, "position"), var.position)
        var.inventory_policy = _coalesce(_get(v, "inventory_policy"), var.inventory_policy)
        var.fulfillment_service = _coalesce(_get(v, "fulfillment_service"), var.fulfillment_service)
        var.inventory_management = _coalesce(_get(v, "inventory_management"), var.inventory_management)
        var.barcode = _coalesce(_get(v, "barcode"), var.barcode)
        var.barcode_normalized = _norm_barcode(var.barcode)
        var.weight = _coalesce(_get(v, "weight"), var.weight)
        var.weight_unit = _coalesce(_get(v, "weight_unit"), var.weight_unit)

        inv_item_id = _get(v, "inventory_item_id")
        if inv_item_id is not None:
            try:
                var.inventory_item_id = int(inv_item_id)
            except Exception:
                pass

        var.inventory_quantity = _coalesce(_get(v, "inventory_quantity"), var.inventory_quantity)
        var.created_at = _coalesce(_to_dt(_get(v, "created_at")), var.created_at)
        var.updated_at = _coalesce(_to_dt(_get(v, "updated_at")), var.updated_at)
        var.last_fetched_at = now

        # Cost if present on webhook
        try:
            vc = _get(v, "cost")
            if vc is not None:
                var.cost_per_item = float(vc)
        except Exception:
            pass

        tracked_flag = _get(v, "tracked")
        if tracked_flag is not None:
            var.tracked = bool(tracked_flag)

        if var.barcode_normalized:
            grp = _ensure_group_for_barcode(db, var.barcode_normalized)
            _ensure_membership(db, var.id, grp.id)

    # commit controlled by caller
