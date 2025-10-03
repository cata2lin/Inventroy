from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
import models
from shopify_service import gid_to_id

# Helper functions for safe data extraction
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
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _first_image_url(prod: Any) -> Optional[str]:
    return _get(prod, "featuredImage", "url") or _get(prod, "image", "src")

# Main data extraction functions
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
    return {
        "id": int(vid), "shopify_gid": _get(variant, "id"), "product_id": product_id,
        "title": _get(variant, "title"), "sku": _get(variant, "sku"), "barcode": _get(variant, "barcode"),
        "price": _get(variant, "price"), "compare_at_price": _get(variant, "compareAtPrice"),
        "position": _get(variant, "position"),
        "inventory_item_id": gid_to_id(_get(variant, "inventoryItem", "id")),
        "inventory_quantity": _get(variant, "inventoryQuantity"),
        "created_at": _to_dt(_get(variant, "createdAt")), "updated_at": _to_dt(_get(variant, "updatedAt")),
        "inventory_policy": _get(variant, "inventoryPolicy"),
        "cost_per_item": _get(variant, "inventoryItem", "unitCost", "amount"),
        "inventory_levels": _get(variant, "inventoryItem", "inventoryLevels", default=[]),
    }

def _pg_upsert(db: Session, table, rows: List[Dict[str, Any]], conflict_cols: Iterable[str]):
    if not rows: return
    unique_rows = {tuple(row.get(col) for col in conflict_cols): row for row in rows}.values()
    stmt = pg_insert(table).values(list(unique_rows))
    update_cols = {c.name: getattr(stmt.excluded, c.name) for c in stmt.excluded if c.name not in conflict_cols}
    stmt = stmt.on_conflict_do_update(index_elements=list(conflict_cols), set_=update_cols)
    db.execute(stmt)

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
                        "inventory_item_id": vf["inventory_item_id"], "location_id": loc_id,
                        "available": qmap.get("available", 0), "on_hand": qmap.get("on_hand", 0),
                        "last_fetched_at": now,
                    })
        except Exception as e:
            print(f"Skipping product due to extraction error: {e}")

    _pg_upsert(db, models.Product.__table__, prod_rows, conflict_cols=("id",))
    _pg_upsert(db, models.ProductVariant.__table__, var_rows, conflict_cols=("id",))
    _pg_upsert(db, models.Location.__table__, loc_rows, conflict_cols=("id",))
    _pg_upsert(db, models.InventoryLevel.__table__, inv_level_rows, conflict_cols=("inventory_item_id", "location_id"))
    db.commit()