from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models

try:
    from shopify_service import gid_to_id
except Exception:
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid:
            return None
        try:
            return int(str(gid).split("/")[-1])
        except Exception:
            return None

def _get(obj: Any, *path: str, default=None):
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
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _first_image_url(prod: Any) -> Optional[str]:
    url = _get(prod, "featuredImage", "url")
    if url:
        return url
    url = _get(prod, "image_url")
    if url:
        return url
    images = _get(prod, "images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            return first.get("src") or first.get("url")
    edges = _get(prod, "images", "edges")
    if isinstance(edges, list) and edges:
        node = _get(edges[0], "node")
        if isinstance(node, dict):
            return node.get("url")
    return None

def _extract_product_fields(prod: Any) -> Dict[str, Any]:
    pid = (
        _get(prod, "legacyResourceId")
        or gid_to_id(_get(prod, "id"))
    )
    if pid is None:
        raise ValueError("Unable to extract numeric product id.")

    tags_val = _get(prod, "tags")
    if isinstance(tags_val, list):
        tags_val = ",".join(tags_val)

    return {
        "id": int(pid),
        "shopify_gid": _get(prod, "id"),
        "title": _get(prod, "title"),
        "body_html": _get(prod, "bodyHtml"),
        "vendor": _get(prod, "vendor"),
        "product_type": _get(prod, "productType"),
        "product_category": _get(prod, "category", "name"),
        "created_at": _to_dt(_get(prod, "createdAt")),
        "handle": _get(prod, "handle"),
        "updated_at": _to_dt(_get(prod, "updatedAt")),
        "published_at": _to_dt(_get(prod, "publishedAt")),
        "status": str(_get(prod, "status")).upper() if _get(prod, "status") else None,
        "tags": tags_val,
        "image_url": _first_image_url(prod),
    }

def _extract_variant_fields(variant: Any, product_id: int) -> Dict[str, Any]:
    vid = (
        _get(variant, "legacyResourceId")
        or gid_to_id(_get(variant, "id"))
    )
    if vid is None:
        raise ValueError("Unable to extract numeric variant id.")

    inv_item_id = (
        _get(variant, "inventoryItem", "legacyResourceId")
        or gid_to_id(_get(variant, "inventoryItem", "id"))
    )

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
        "compare_at_price": _get(variant, "compareAtPrice"),
        "position": _get(variant, "position"),
        "inventory_item_id": int(inv_item_id) if inv_item_id is not None else None,
        "inventory_quantity": _get(variant, "inventoryQuantity"),
        "created_at": _to_dt(_get(variant, "createdAt")),
        "updated_at": _to_dt(_get(variant, "updatedAt")),
        "inventory_policy": _get(variant, "inventoryPolicy"),
        "cost_per_item": unit_cost,
        "inventory_levels": _get(variant, "inventoryItem", "inventoryLevels") or [],
    }

def _pg_upsert(
    db: Session,
    table,
    rows: List[Dict[str, Any]],
    conflict_cols: Iterable[str],
    exclude_from_update: Iterable[str] = (),
):
    if not rows:
        return

    unique_rows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(col) for col in conflict_cols)
        unique_rows[key] = row
    
    deduplicated_rows = list(unique_rows.values())

    if not deduplicated_rows:
        return

    all_cols = set()
    for r in deduplicated_rows:
        all_cols.update(r.keys())

    stmt = pg_insert(table).values(deduplicated_rows)
    excl = set(exclude_from_update) | set(conflict_cols)
    update_cols = {}
    for c in all_cols - excl:
        update_cols[c] = func.coalesce(getattr(stmt.excluded, c), getattr(table.c, c))

    stmt = stmt.on_conflict_do_update(index_elements=list(conflict_cols), set_=update_cols)
    db.execute(stmt)

def create_or_update_products(
    db: Session,
    store_id: int,
    items: List[Any],
) -> None:
    now = datetime.now(timezone.utc)
    prod_rows, var_rows, inv_level_rows = [], [], []

    for bundle in items or []:
        p = bundle.get("product", bundle)
        vs = _get(p, "variants") or []
        
        try:
            pf = _extract_product_fields(p)
        except Exception:
            continue

        pf["store_id"] = store_id
        pf["last_fetched_at"] = now
        prod_rows.append(pf)

        for v in vs:
            try:
                vf = _extract_variant_fields(v, product_id=pf["id"])
            except Exception:
                continue
            vf["store_id"] = store_id
            vf["last_fetched_at"] = now
            
            levels = vf.pop("inventory_levels", []) or []
            var_rows.append(vf)

            if levels and vf.get("inventory_item_id") is not None:
                for lvl in levels:
                    loc_gid = _get(lvl, "location", "id")
                    loc_legacy = gid_to_id(loc_gid) if loc_gid else None
                    if not loc_legacy:
                        continue
                    qmap = {}
                    for q in (_get(lvl, "quantities") or []):
                        name = _get(q, "name")
                        qty = _get(q, "quantity")
                        if name is not None and qty is not None:
                            try:
                                qmap[str(name)] = int(qty)
                            except Exception:
                                pass
                    avail = qmap.get("available")
                    on_hand = qmap.get("on_hand", avail)
                    inv_level_rows.append({
                        "inventory_item_id": int(vf["inventory_item_id"]),
                        "location_id": int(loc_legacy),
                        "available": int(avail if avail is not None else 0),
                        "on_hand": int(on_hand if on_hand is not None else (avail or 0) or 0),
                        "last_fetched_at": now,
                    })

    _pg_upsert(db, models.Product.__table__, prod_rows, conflict_cols=("store_id", "id"))
    _pg_upsert(db, models.ProductVariant.__table__, var_rows, conflict_cols=("store_id", "id"))
    _pg_upsert(db, models.InventoryLevel.__table__, inv_level_rows, conflict_cols=("inventory_item_id", "location_id"))
    
    db.commit()