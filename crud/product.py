# crud/product.py

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
import models
import json

try:
    from shopify_service import gid_to_id
except ImportError:
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid: return None
        try: return int(str(gid).split("/")[-1])
        except (IndexError, ValueError): return None

# --- Main function to get products for UI ---
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
                models.Product.variants.any(models.ProductVariant.barcode.ilike(search_term)),
            )
        )

    total_count = query.count()
    products = query.order_by(models.Product.title).offset(skip).limit(limit).all()

    return products, total_count

def get_product(db: Session, product_id: int) -> Optional[models.Product]:
    return db.query(models.Product).options(
        joinedload(models.Product.variants).joinedload(models.ProductVariant.inventory_levels).joinedload(models.InventoryLevel.location)
    ).filter(models.Product.id == product_id).first()


# --- Helper functions ---
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

def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def log_dead_letter(db: Session, store_id: int, run_id: int, payload: Dict, reason: str):
    try:
        payload_str = json.dumps(payload, default=json_serial, indent=2)
        payload_json = json.loads(payload_str)

        dead_letter = models.SyncDeadLetter(
            store_id=store_id,
            run_id=run_id,
            payload=payload_json,
            reason=reason
        )
        db.add(dead_letter)
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"FATAL: Could not write to dead letter table. Reason: {e}")


# --- Data Extraction ---
def _extract_product_fields(p_data: Any, store_id: int, last_seen_at: datetime) -> Dict:
    pid = gid_to_id(p_data.get("id"))
    if not pid: raise ValueError("Missing product ID")
    tags = p_data.get("tags", [])
    return {
        "id": pid, "store_id": store_id, "shopify_gid": p_data.get("id"),
        "title": p_data.get("title"), "body_html": p_data.get("bodyHtml"),
        "vendor": p_data.get("vendor"), "product_type": p_data.get("productType"),
        "status": p_data.get("status"), "handle": p_data.get("handle"),
        "tags": ",".join(tags if tags is not None else []),
        "image_url": _first_image_url(p_data),
        "created_at": _to_dt(p_data.get("createdAt")), "updated_at": _to_dt(p_data.get("updatedAt")),
        "published_at": _to_dt(p_data.get("publishedAt")), "last_seen_at": last_seen_at,
    }

def _extract_variant_fields(v_data: Any, product_id: int, store_id: int, last_seen_at: datetime) -> Dict:
    vid = gid_to_id(v_data.get("id"))
    if not vid: raise ValueError("Missing variant ID")
    
    sku = v_data.get("sku")
    if sku is not None and not sku.strip():
        sku = None

    inventory_item = v_data.get("inventoryItem", {})
    inventory_item_id = gid_to_id(inventory_item.get("id")) or v_data.get("inventory_item_id")

    return {
        "id": vid, "product_id": product_id, "store_id": store_id, "shopify_gid": v_data.get("id"),
        "title": v_data.get("title"), "sku": sku, "barcode": v_data.get("barcode"),
        "price": v_data.get("price"), "compare_at_price": v_data.get("compareAtPrice"),
        "inventory_item_id": inventory_item_id,
        "inventory_quantity": _get(v_data, "inventoryQuantity"),
        "inventory_policy": _get(v_data, "inventoryPolicy"), "position": _get(v_data, "position"),
        "cost_per_item": _get(inventory_item, "unitCost", "amount"),
        "created_at": _to_dt(v_data.get("createdAt")), "updated_at": _to_dt(v_data.get("updatedAt")),
        "last_seen_at": last_seen_at,
    }

# --- Robust Upsert Logic ---
def create_or_update_products(db: Session, store_id: int, run_id: int, items: List[Any], last_seen_at: datetime):
    if not items:
        return
    now = datetime.now(timezone.utc)

    for bundle in items:
        try:
            p_data = bundle
            p_row = _extract_product_fields(p_data, store_id, last_seen_at)
            
            product_stmt = pg_insert(models.Product).values(p_row)
            product_stmt = product_stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={k: getattr(product_stmt.excluded, k) for k in p_row if k not in ['id', 'store_id']}
            )
            db.execute(product_stmt)
            
            v_data_list = p_data.get("variants", [])
            if isinstance(v_data_list, dict) and "edges" in v_data_list:
                 v_data_list = [edge['node'] for edge in v_data_list['edges']]

            if v_data_list:
                loc_rows_map = {}
                inv_level_rows = []
                
                for v_data in v_data_list:
                    v_row = _extract_variant_fields(v_data, p_row["id"], store_id, last_seen_at)
                    
                    variant_stmt = pg_insert(models.ProductVariant).values(v_row)
                    variant_stmt = variant_stmt.on_conflict_do_update(
                        index_elements=['id'],
                        set_={k: getattr(variant_stmt.excluded, k) for k in v_row if k != 'id'}
                    )
                    db.execute(variant_stmt)
                    
                    inventory_levels = _get(v_data, "inventoryItem", "inventoryLevels", "edges", default=[])
                    for lvl_edge in inventory_levels:
                        lvl = lvl_edge.get("node", lvl_edge)
                        loc_gid = _get(lvl, "location", "id")
                        loc_id = gid_to_id(loc_gid)
                        if not loc_id or not loc_gid: continue
                        
                        loc_rows_map[loc_id] = { "id": loc_id, "shopify_gid": loc_gid, "store_id": store_id, "name": _get(lvl, "location", "name") }
                        
                        qmap = {q["name"]: q["quantity"] for q in _get(lvl, "quantities", default=[])}
                        inv_level_rows.append({
                            "variant_id": v_row["id"], "location_id": loc_id,
                            "inventory_item_id": v_row["inventory_item_id"],
                            "available": qmap.get("available", 0), "on_hand": qmap.get("on_hand", qmap.get("available", 0)),
                            "last_fetched_at": now,
                        })

                if loc_rows_map:
                    loc_rows = list(loc_rows_map.values())
                    loc_stmt = pg_insert(models.Location).values(loc_rows)
                    loc_stmt = loc_stmt.on_conflict_do_update(
                        index_elements=['id'], 
                        set_={ "name": loc_stmt.excluded.name, "shopify_gid": loc_stmt.excluded.shopify_gid }
                    )
                    db.execute(loc_stmt)
                if inv_level_rows:
                    inv_stmt = pg_insert(models.InventoryLevel).values(inv_level_rows).on_conflict_do_update(
                        index_elements=['variant_id', 'location_id'],
                        set_={ "available": pg_insert(models.InventoryLevel).excluded.available, "on_hand": pg_insert(models.InventoryLevel).excluded.on_hand, "last_fetched_at": pg_insert(models.InventoryLevel).excluded.last_fetched_at }
                    )
                    db.execute(inv_stmt)
            db.commit()
        except IntegrityError as e:
            db.rollback()
            log_dead_letter(db, store_id, run_id, bundle, f"Data integrity error: {e.orig}")
        except Exception as e:
            db.rollback()
            log_dead_letter(db, store_id, run_id, bundle, f"A general error occurred: {e}")

# --- WEBHOOK PROCESSING FUNCTIONS ---
def create_or_update_product_from_webhook(db: Session, store_id: int, payload: Dict[str, Any]):
    now = datetime.now(timezone.utc)
    create_or_update_products(db, store_id, run_id=0, items=[payload], last_seen_at=now)
    print(f"[DB-UPDATE] Created/Updated product '{payload.get('title')}' from webhook.")

def delete_product_from_webhook(db: Session, payload: Dict[str, Any]):
    product_id = payload.get("id")
    if not product_id: return
    db.query(models.Product).filter(models.Product.id == product_id).delete()
    db.commit()
    print(f"[DB-UPDATE] Deleted product ID {product_id} from webhook.")

def update_variant_from_webhook(db: Session, payload: Dict[str, Any]):
    inventory_item_id = payload.get("id")
    variant = db.query(models.ProductVariant).filter(models.ProductVariant.inventory_item_id == inventory_item_id).first()
    if not variant: return
    if 'barcode' in payload and variant.barcode != payload['barcode']:
        variant.barcode = payload['barcode']
        db.commit()
        print(f"[DB-UPDATE] Updated barcode for variant SKU {variant.sku} to {variant.barcode}.")

def delete_inventory_item_from_webhook(db: Session, payload: Dict[str, Any]):
    inventory_item_id = payload.get("id")
    if not inventory_item_id: return
    db.query(models.ProductVariant).filter(models.ProductVariant.inventory_item_id == inventory_item_id).delete()
    db.commit()
    print(f"[DB-UPDATE] Deleted variant with inventory_item_id {inventory_item_id} from webhook.")

def update_inventory_levels_for_variants(db: Session, variant_ids: List[int], location_id: int, new_quantity: int):
    now = datetime.now(timezone.utc)
    db.query(models.InventoryLevel).filter(
        models.InventoryLevel.variant_id.in_(variant_ids),
        models.InventoryLevel.location_id == location_id
    ).update({
        models.InventoryLevel.available: new_quantity,
        models.InventoryLevel.updated_at: now,
        models.InventoryLevel.last_fetched_at: now
    }, synchronize_session=False)
    db.commit()