# crud/product.py

from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
import models
import schemas
from .sync import normalize_barcode, update_variant_group_membership

def _set_if_not_none(obj, **kwargs):
    """Update SQLAlchemy obj attributes only when value is not None."""
    for k, v in kwargs.items():
        if v is not None:
            setattr(obj, k, v)

def _upsert_product(db: Session, store_id: int, p: schemas.Product) -> models.Product:
    prod = db.query(models.Product).filter(models.Product.id == p.id).first()
    if not prod:
        prod = models.Product(
            id=p.id,
            shopify_gid=p.id_gid if hasattr(p, "id_gid") else p.shopify_gid if hasattr(p, "shopify_gid") else p.id,
            store_id=store_id,
            title=p.title,
            body_html=p.body_html,
            vendor=p.vendor,
            product_type=p.product_type,
            created_at=p.created_at,
            handle=p.handle,
            updated_at=p.updated_at,
            published_at=p.published_at,
            status=p.status.upper() if isinstance(p.status, str) else p.status,
            tags=",".join(p.tags) if isinstance(p.tags, list) else p.tags,
            image_url=getattr(p, "featured_image_url", None) or getattr(p, "image_url", None),
            product_category=getattr(p, "product_category", None) or getattr(p, "category", None),
        )
        db.add(prod)
    else:
        # preserve existing if incoming is None
        _set_if_not_none(
            prod,
            title=p.title,
            body_html=p.body_html,
            vendor=p.vendor,
            product_type=p.product_type,
            created_at=p.created_at,
            handle=p.handle,
            updated_at=p.updated_at,
            published_at=p.published_at,
            status=p.status.upper() if isinstance(p.status, str) else p.status,
            tags=",".join(p.tags) if isinstance(p.tags, list) else p.tags,
        )
        # these are often missing in webhook payloads; only update if provided
        img = getattr(p, "featured_image_url", None) or getattr(p, "image_url", None)
        cat = getattr(p, "product_category", None) or getattr(p, "category", None)
        if img: prod.image_url = img
        if cat: prod.product_category = cat
        if not prod.store_id: prod.store_id = store_id
    return prod

def _extract_variant_inventory_fields(v: schemas.ProductVariant) -> Dict[str, Any]:
    inv = getattr(v, "inventoryItem", None) or {}
    # Flatten inventory levels
    levels = getattr(inv, "inventoryLevels", None) or getattr(inv, "levels", None) or []
    level_rows = []
    for lvl in levels:
        # lvl may be dict or Pydantic model
        loc = getattr(lvl, "location", None)
        quantities = getattr(lvl, "quantities", None)
        if isinstance(lvl, dict):
            loc_id = int(lvl.get("location", {}).get("legacyResourceId") or lvl.get("location_id") or 0)
            qs = lvl.get("quantities", [])
        else:
            loc_id = int(getattr(loc, "legacyResourceId", 0) or 0)
            qs = quantities or []
        available = 0
        on_hand = 0
        for q in qs or []:
            name = q["name"] if isinstance(q, dict) else getattr(q, "name", None)
            qty = q["quantity"] if isinstance(q, dict) else getattr(q, "quantity", 0)
            if name == "available":
                available = int(qty or 0)
            elif name == "on_hand":
                on_hand = int(qty or 0)
        if loc_id:
            level_rows.append({"location_id": loc_id, "available": available, "on_hand": on_hand})
    unit_cost = None
    if isinstance(inv, dict):
        amt = inv.get("unitCost", {}).get("amount")
        unit_cost = float(amt) if amt is not None else None
    else:
        uc = getattr(inv, "unitCost", None)
        unit_cost = float(getattr(uc, "amount", 0)) if uc and getattr(uc, "amount", None) is not None else None
    return {"levels": level_rows, "unit_cost": unit_cost}

def _upsert_variant(db: Session, store_id: int, p: models.Product, v: schemas.ProductVariant) -> models.ProductVariant:
    # Normalize barcode
    bc = v.barcode if hasattr(v, "barcode") else getattr(v, "barcode", None)
    norm_bc = normalize_barcode(bc) if bc else None

    var = db.query(models.ProductVariant).filter(models.ProductVariant.id == v.id).first()
    if not var:
        var = models.ProductVariant(
            id=v.id,
            shopify_gid=v.id_gid if hasattr(v, "id_gid") else v.shopify_gid if hasattr(v, "shopify_gid") else v.id,
            product_id=p.id,
            title=v.title,
            price=v.price,
            sku=v.sku,
            position=v.position,
            inventory_policy=v.inventory_policy if hasattr(v, "inventory_policy") else v.inventoryPolicy if hasattr(v, "inventoryPolicy") else None,
            compare_at_price=v.compare_at_price if hasattr(v, "compare_at_price") else v.compareAtPrice if hasattr(v, "compareAtPrice") else None,
            fulfillment_service=None,  # GraphQL doesn't expose this
            inventory_management=None,  # GraphQL path differs
            barcode=bc,
            barcode_normalized=norm_bc,
            grams=getattr(v, "grams", None),
            weight=getattr(v, "weight", None),
            weight_unit=getattr(v, "weight_unit", None) or getattr(v, "weightUnit", None),
            inventory_item_id=v.inventory_item_id if hasattr(v, "inventory_item_id") else getattr(v, "inventoryItem", {}).get("legacyResourceId", None) if isinstance(getattr(v, "inventoryItem", None), dict) else getattr(getattr(v, "inventoryItem", None), "legacyResourceId", None),
            inventory_quantity=v.inventory_quantity if hasattr(v, "inventory_quantity") else getattr(v, "inventoryQuantity", None),
            created_at=v.created_at if hasattr(v, "created_at") else getattr(v, "createdAt", None),
            updated_at=v.updated_at if hasattr(v, "updated_at") else getattr(v, "updatedAt", None),
            cost=None,  # deprecated
            store_id=store_id,
            tracked=True,  # default; will adjust from InventoryItem.tracked if you sync details later
            cost_per_item=None,
        )
        db.add(var)
    else:
        _set_if_not_none(
            var,
            product_id=p.id,
            title=v.title,
            price=v.price,
            sku=v.sku,
            position=v.position,
            inventory_policy=v.inventory_policy if hasattr(v, "inventory_policy") else v.inventoryPolicy if hasattr(v, "inventoryPolicy") else None,
            compare_at_price=v.compare_at_price if hasattr(v, "compare_at_price") else v.compareAtPrice if hasattr(v, "compareAtPrice") else None,
            barcode=bc,
            barcode_normalized=norm_bc,
            grams=getattr(v, "grams", None),
            weight=getattr(v, "weight", None),
            weight_unit=getattr(v, "weight_unit", None) or getattr(v, "weightUnit", None),
            inventory_item_id=v.inventory_item_id if hasattr(v, "inventory_item_id") else getattr(v, "inventoryItem", {}).get("legacyResourceId", None) if isinstance(getattr(v, "inventoryItem", None), dict) else getattr(getattr(v, "inventoryItem", None), "legacyResourceId", None),
            inventory_quantity=v.inventory_quantity if hasattr(v, "inventory_quantity") else getattr(v, "inventoryQuantity", None),
            created_at=v.created_at if hasattr(v, "created_at") else getattr(v, "createdAt", None),
            updated_at=v.updated_at if hasattr(v, "updated_at") else getattr(v, "updatedAt", None),
        )
        if not var.store_id:
            var.store_id = store_id

    # cost_per_item from InventoryItem
    inv_fields = _extract_variant_inventory_fields(v)
    if inv_fields["unit_cost"] is not None:
        var.cost_per_item = inv_fields["unit_cost"]

    # Upsert inventory level snapshots we got on this page (best-effort)
    for lvl in inv_fields["levels"]:
        il = db.query(models.InventoryLevel).filter(
            models.InventoryLevel.inventory_item_id == var.inventory_item_id,
            models.InventoryLevel.location_id == int(lvl["location_id"])
        ).first()
        if il:
            il.available = lvl["available"]
            il.on_hand = lvl["on_hand"]
        else:
            db.add(models.InventoryLevel(
                inventory_item_id=var.inventory_item_id,
                location_id=int(lvl["location_id"]),
                available=lvl["available"],
                on_hand=lvl["on_hand"],
            ))
    # Group membership (by normalized barcode)
    if norm_bc:
        update_variant_group_membership(db, var, norm_bc)

    return var

def create_or_update_products(db: Session, products_data: List[Dict[str, Any]], store_id: int):
    """
    Upserts products and variants from a GraphQL page returned by ShopifyService.get_all_products_and_variants().
    products_data: List[{"product": schemas.Product, "variants": List[schemas.ProductVariant]}]
    """
    for item in products_data:
        p: schemas.Product = item["product"]
        variants: List[schemas.ProductVariant] = item.get("variants", [])
        prod_row = _upsert_product(db, store_id, p)
        # Flush to ensure product_id foreign key is valid
        db.flush()
        for v in variants:
            _upsert_variant(db, store_id, prod_row, v)
        # Optional: mark primary variant if one was set in Shopify (not exposed here)
        db.commit()

def mark_products_deleted(db: Session, store_id: int, product_ids: List[int]):
    """Mark products as deleted by removing from DB (optional soft-delete could be implemented)."""
    # For safety, don't hard delete here; leave to your chosen policy
    pass

def set_primary_variant(db: Session, barcode: str, variant_id: int):
    # Keep this helper available for UI
    db.query(models.ProductVariant).filter(models.ProductVariant.barcode == barcode).update(
        {"is_primary_variant": False}, synchronize_session=False
    )
    db.query(models.ProductVariant).filter(models.ProductVariant.id == variant_id).update(
        {"is_primary_variant": True}, synchronize_session=False
    )
    db.commit()

def create_or_update_product_from_webhook(db: Session, store_id: int, product_data: schemas.ShopifyProductWebhook):
    """
    Lightweight upsert from a REST webhook (products/create|update).
    """
    # Map incoming to schemas.Product-like for reuse
    p = schemas.Product(
        id=product_data.id,
        shopify_gid=product_data.admin_graphql_api_id,
        title=product_data.title,
        body_html=product_data.body_html,
        vendor=product_data.vendor,
        product_type=product_data.product_type,
        created_at=product_data.created_at,
        updated_at=product_data.updated_at,
        published_at=product_data.published_at,
        status=product_data.status,
        handle=product_data.handle,
        tags=",".join(product_data.tags) if isinstance(product_data.tags, list) else product_data.tags,
        image_url=(product_data.image.get("src") if getattr(product_data, "image", None) else None),
        product_category=None
    )
    prod_row = _upsert_product(db, store_id, p)
    db.flush()

    # Variants
    for v in (product_data.variants or []):
        # Build a minimal schemas.ProductVariant-like object
        class _V:
            pass
        _v = _V()
        _v.id = v.id
        _v.title = v.title
        _v.price = v.price
        _v.sku = v.sku
        _v.position = v.position
        _v.inventoryPolicy = v.inventory_policy if hasattr(v, "inventory_policy") else None
        _v.compareAtPrice = v.compare_at_price
        _v.barcode = v.barcode
        _v.inventoryQuantity = v.inventory_quantity
        _v.createdAt = v.created_at
        _v.updatedAt = v.updated_at
        _v.inventoryItem = {"legacyResourceId": v.inventory_item_id}
        _upsert_variant(db, store_id, prod_row, _v)

    db.commit()
