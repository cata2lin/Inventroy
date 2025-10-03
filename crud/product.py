# crud/product.py

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models
try:
    from shopify_service import gid_to_id
except ImportError:
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid: return None
        try: return int(str(gid).split('/')[-1])
        except (IndexError, ValueError): return None

# --- Helper Functions (mostly unchanged) ---
def _get(obj: Any, *path: str, default=None):
    cur = obj
    for key in path:
        if cur is None: return default
        cur = cur.get(key, default) if isinstance(cur, dict) else getattr(cur, key, default)
    return cur

def _to_dt(val) -> Optional[datetime]:
    if not val: return None
    if isinstance(val, datetime): return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    s = str(val).strip().replace(" ", "T").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s); return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception: return None

def _first_image_url(prod: Any) -> Optional[str]:
    return _get(prod, "featuredImage", "url") or _get(prod, "image", "src")

# --- Data Extraction (with SKU normalization) ---
def _extract_product_fields(prod: Any) -> Dict[str, Any]:
    pid = _get(prod, "legacyResourceId") or gid_to_id(_get(prod, "id"))
    if pid is None: raise ValueError("Unable to extract numeric product id.")
    return {
        "id": int(pid), "shopify_gid": _get(prod, "id"), "title": _get(prod, "title"),
        "body_html": _get(prod, "bodyHtml"), "vendor": _get(prod, "vendor"),
        "product_type": _get(prod, "productType"), "product_category": _get(prod, "category", "name"),
        "created_at": _to_dt(_get(prod, "createdAt")), "handle": _get(prod, "handle"),
        "updated_at": _to_dt(_get(prod, "updatedAt")), "published_at": _to_dt(_get(prod, "publishedAt")),
        "status": str(_get(prod, "status")).upper() if _get(prod, "status") else None,
        "tags": ",".join(_get(prod, "tags", default=[])), "image_url": _first_image_url(prod),
    }

def _extract_variant_fields(variant: Any, product_id: int) -> Dict[str, Any]:
    vid = _get(variant, "legacyResourceId") or gid_to_id(_get(variant, "id"))
    if vid is None: raise ValueError("Unable to extract numeric variant id.")
    
    # *** FIX: Normalize blank SKUs to NULL ***
    sku = _get(variant, "sku")
    normalized_sku = sku.strip() if sku else None
    if not normalized_sku:
        sku = None

    return {
        "id": int(vid), "shopify_gid": _get(variant, "id"), "product_id": product_id,
        "title": _get(variant, "title"), "sku": sku, "barcode": _get(variant, "barcode"),
        "price": _get(variant, "price"), "compare_at_price": _get(variant, "compareAtPrice"),
        "position": _get(variant, "position"),
        "inventory_item_id": gid_to_id(_get(variant, "inventoryItem", "id")),
        "inventory_quantity": _get(variant, "inventoryQuantity"),
        "created_at": _to_dt(_get(variant, "createdAt")), "updated_at": _to_dt(_get(variant, "updatedAt")),
        "inventory_policy": _get(variant, "inventoryPolicy"),
        "cost_per_item": _get(variant, "inventoryItem", "unitCost", "amount"),
        "inventory_levels": _get(variant, "inventoryItem", "inventoryLevels", default=[]),
    }

# --- Robust Upsert Logic ---
def create_or_update_products(db: Session, store_id: int, items: List[Any]):
    now = datetime.now(timezone.utc)
    prod_rows, var_rows, loc_rows, inv_level_rows = [], [], [], []

    for bundle in items or []:
        p, vs = bundle.get("product", {}), bundle.get("variants", [])
        try:
            pf = _extract_product_fields(p)
            pf.update({"store_id": store_id, "last_fetched_at": now})
            prod_rows.append(pf)
            for v in vs:
                vf = _extract_variant_fields(v, product_id=pf["id"])
                vf.update({"store_id": store_id, "last_fetched_at": now})
                levels = vf.pop("inventory_levels", [])
                var_rows.append(vf)
                for lvl in levels:
                    loc = _get(lvl, "location", default={})
                    loc_id = gid_to_id(_get(loc, "id"))
                    if not loc_id: continue
                    loc_rows.append({"id": loc_id, "store_id": store_id, "name": _get(loc, "name")})
                    qmap = {q["name"]: q["quantity"] for q in _get(lvl, "quantities", default=[])}
                    inv_level_rows.append({
                        "variant_id": vf["id"], # *** FIX: Use variant_id for the new FK ***
                        "inventory_item_id": vf["inventory_item_id"], 
                        "location_id": loc_id,
                        "available": qmap.get("available", 0),
                        "on_hand": qmap.get("on_hand", qmap.get("available", 0)),
                        "last_fetched_at": now,
                    })
        except Exception as e:
            print(f"SKIPPING product due to data extraction error: {e}")

    # --- Database Operations ---
    if not prod_rows: return

    # Upsert products by their unique Shopify ID
    stmt_products = pg_insert(models.Product).values(prod_rows)
    stmt_products = stmt_products.on_conflict_do_update(
        index_elements=['id'],
        set_={c.name: getattr(stmt_products.excluded, c.name) for c in stmt_products.excluded if c.name != 'id'}
    )
    db.execute(stmt_products)

    # Upsert variants by their unique Shopify ID
    if var_rows:
        stmt_variants = pg_insert(models.ProductVariant).values(var_rows)
        stmt_variants = stmt_variants.on_conflict_do_update(
            index_elements=['id'],
            set_={c.name: getattr(stmt_variants.excluded, c.name) for c in stmt_variants.excluded if c.name != 'id'}
        )
        db.execute(stmt_variants)

    # Upsert locations by their unique Shopify ID
    if loc_rows:
        stmt_locations = pg_insert(models.Location).values(list({v['id']:v for v in loc_rows}.values()))
        stmt_locations = stmt_locations.on_conflict_do_nothing(index_elements=['id'])
        db.execute(stmt_locations)

    # Upsert inventory levels based on the new composite primary key
    if inv_level_rows:
        stmt_inv = pg_insert(models.InventoryLevel).values(inv_level_rows)
        stmt_inv = stmt_inv.on_conflict_do_update(
            index_elements=['variant_id', 'location_id'],
            set_={
                'available': stmt_inv.excluded.available,
                'on_hand': stmt_inv.excluded.on_hand,
                'last_fetched_at': stmt_inv.excluded.last_fetched_at
            }
        )
        db.execute(stmt_inv)

    db.commit()