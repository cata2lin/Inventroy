# crud/product.py

from typing import Any, Dict, List, Optional, Tuple
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

# --- Main function to get products for the UI ---
def get_products(
    db: Session, skip: int = 0, limit: int = 100, store_id: Optional[int] = None, search: Optional[str] = None
) -> Tuple[List[models.Product], int]:
    """
    Fetches a paginated list of products with optional filtering.
    """
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

# --- Helper functions for data extraction and normalization ---
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

def normalize_sku(val: Optional[str]) -> Optional[str]:
    s = (val or "").strip().lower()
    return s if s else None

def log_dead_letter(db: Session, store_id: int, run_id: int, payload: Dict, reason: str):
    dead_letter = models.SyncDeadLetter(
        store_id=store_id,
        run_id=run_id,
        payload=payload,
        reason=reason
    )
    db.add(dead_letter)

# --- Data Extraction from Shopify Payload ---
def _extract_product_fields(p_data: Any, store_id: int, last_seen_at: datetime) -> Dict:
    pid = gid_to_id(p_data.get("id"))
    if not pid: raise ValueError("Missing product ID")
    return {
        "id": pid, "store_id": store_id, "shopify_gid": p_data.get("id"),
        "title": p_data.get("title"), "body_html": p_data.get("bodyHtml"),
        "vendor": p_data.get("vendor"), "product_type": p_data.get("productType"),
        "status": p_data.get("status"), "handle": p_data.get("handle"),
        "tags": ",".join(p_data.get("tags", [])), "image_url": _first_image_url(p_data),
        "created_at": _to_dt(p_data.get("createdAt")), "updated_at": _to_dt(p_data.get("updatedAt")),
        "published_at": _to_dt(p_data.get("publishedAt")), "last_seen_at": last_seen_at,
    }

def _extract_variant_fields(v_data: Any, product_id: int, store_id: int, last_seen_at: datetime) -> Dict:
    vid = gid_to_id(v_data.get("id"))
    if not vid: raise ValueError("Missing variant ID")
    return {
        "id": vid, "product_id": product_id, "store_id": store_id, "shopify_gid": v_data.get("id"),
        "title": v_data.get("title"), "sku": v_data.get("sku"), "barcode": v_data.get("barcode"),
        "price": v_data.get("price"), "compare_at_price": v_data.get("compareAtPrice"),
        "inventory_item_id": gid_to_id(_get(v_data, "inventoryItem", "id")),
        "inventory_quantity": _get(v_data, "inventoryQuantity"),
        "inventory_policy": _get(v_data, "inventoryPolicy"), "position": _get(v_data, "position"),
        "cost_per_item": _get(v_data, "inventoryItem", "unitCost", "amount"),
        "created_at": _to_dt(v_data.get("createdAt")), "updated_at": _to_dt(v_data.get("updatedAt")),
        "last_seen_at": last_seen_at,
    }

# --- Robust Upsert Logic ---
def create_or_update_products(db: Session, store_id: int, run_id: int, items: List[Any], last_seen_at: datetime):
    prod_rows, var_rows, loc_rows, inv_level_rows = [], [], [], []
    now = datetime.now(timezone.utc)

    for bundle in items or []:
        try:
            p_data, v_data_list = bundle, bundle.get("variants", [])
            
            p_row = _extract_product_fields(p_data, store_id, last_seen_at)
            prod_rows.append(p_row)

            for v_data in v_data_list:
                v_row = _extract_variant_fields(v_data, p_row["id"], store_id, last_seen_at)
                var_rows.append(v_row)
                
                for lvl in _get(v_data, "inventoryItem", "inventoryLevels", default=[]):
                    loc_id = gid_to_id(_get(lvl, "location", "id"))
                    if not loc_id: continue
                    loc_rows.append({"id": loc_id, "store_id": store_id, "name": _get(lvl, "location", "name")})
                    qmap = {q["name"]: q["quantity"] for q in _get(lvl, "quantities", default=[])}
                    inv_level_rows.append({
                        "variant_id": v_row["id"],
                        "location_id": loc_id,
                        "inventory_item_id": v_row["inventory_item_id"],
                        "available": qmap.get("available", 0),
                        "on_hand": qmap.get("on_hand", qmap.get("available", 0)),
                        "last_fetched_at": now,
                    })
        except Exception as e:
            log_dead_letter(db, store_id, run_id, bundle, f"Data extraction failed: {e}")

    if not prod_rows: return

    # --- De-duplication logic for variants before insertion ---
    unique_variants_map = {}
    variants_with_null_sku = []

    for v_row in var_rows:
        sku = v_row.get("sku")
        if sku and sku.strip():
            # Keep only the last seen variant for a given SKU in the batch
            unique_variants_map[(store_id, sku)] = v_row
        else:
            # Collect variants with no SKU to be inserted/updated by their ID
            variants_with_null_sku.append(v_row)
    
    # Reconstruct the list of variants to be processed
    deduplicated_var_rows = list(unique_variants_map.values()) + variants_with_null_sku

    def pg_upsert(table, rows, conflict_cols, update_cols):
        if not rows: return
        # The primary key (id) is always unique within the batch from Shopify
        stmt = pg_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_cols)
        db.execute(stmt)

    try:
        # Upsert all data in the correct order
        pg_upsert(models.Product.__table__, prod_rows, ['id'], {
            "title": pg_insert(models.Product).excluded.title,
            # Add other fields to update on conflict as needed
            "last_seen_at": pg_insert(models.Product).excluded.last_seen_at
        })
        
        # Upsert variants using the de-duplicated list
        pg_upsert(models.ProductVariant.__table__, deduplicated_var_rows, ['id'], {
            "sku": pg_insert(models.ProductVariant).excluded.sku,
            "price": pg_insert(models.ProductVariant).excluded.price,
            "inventory_quantity": pg_insert(models.ProductVariant).excluded.inventory_quantity,
            # Add other fields to update on conflict as needed
            "last_seen_at": pg_insert(models.ProductVariant).excluded.last_seen_at
        })

        pg_upsert(models.Location.__table__, loc_rows, ['id'], {
            "name": pg_insert(models.Location).excluded.name
        })
        
        pg_upsert(models.InventoryLevel.__table__, inv_level_rows, ['variant_id', 'location_id'], {
            "available": pg_insert(models.InventoryLevel).excluded.available,
            "on_hand": pg_insert(models.InventoryLevel).excluded.on_hand,
            "last_fetched_at": pg_insert(models.InventoryLevel).excluded.last_fetched_at
        })
        
        db.commit()

    except Exception as e:
        db.rollback()
        # Log the entire batch that failed for inspection
        log_dead_letter(db, store_id, run_id, {"products": prod_rows, "variants": var_rows}, f"Page processing failed: {e}")
        db.commit()