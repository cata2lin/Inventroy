from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
import models

try:
    from shopify_service import gid_to_id
except ImportError:
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid: return None
        try: return int(str(gid).split("/")[-1])
        except (IndexError, ValueError): return None

def get_products(
    db: Session, skip: int = 0, limit: int = 100, store_id: Optional[int] = None, search: Optional[str] = None
) -> Tuple[List[models.Product], int]:
    query = db.query(models.Product).options(joinedload(models.Product.variants))
    if store_id:
        query = query.filter(models.Product.store_id == store_id)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            or_(
                models.Product.title.ilike(search_term),
                models.Product.variants.any(models.ProductVariant.sku.ilike(search_term)),
            )
        )
    total_count = query.count()
    products = query.order_by(models.Product.title).offset(skip).limit(limit).all()
    return products, total_count

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
    sku = _get(variant, "sku")
    if sku and not sku.strip(): sku = None
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
                        "variant_id": vf["id"],
                        "inventory_item_id": vf["inventory_item_id"], 
                        "location_id": loc_id,
                        "available": qmap.get("available", 0),
                        "on_hand": qmap.get("on_hand", qmap.get("available", 0)),
                        "last_fetched_at": now,
                    })
        except Exception as e:
            print(f"SKIPPING product due to data extraction error: {e}")

    if not prod_rows: return

    # Upsert data using ON CONFLICT
    def pg_upsert(table, rows, conflict_cols):
        if not rows: return
        unique_rows = list({tuple(row.get(col) for col in conflict_cols): row for row in rows}.values())
        stmt = pg_insert(table).values(unique_rows)
        update_cols = {c.name: getattr(stmt.excluded, c.name) for c in stmt.excluded if c.name not in conflict_cols}
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
        db.execute(stmt)

    pg_upsert(models.Product.__table__, prod_rows, ('id',))
    pg_upsert(models.ProductVariant.__table__, var_rows, ('id',))
    pg_upsert(models.Location.__table__, loc_rows, ('id',))
    pg_upsert(models.InventoryLevel.__table__, inv_level_rows, ('variant_id', 'location_id'))
    
    db.commit()