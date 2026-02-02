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
    def gid_to_id(gid) -> Optional[int]:
        if gid is None: return None
        if isinstance(gid, int): return gid
        try: return int(str(gid).strip().split("/")[-1])
        except (IndexError, ValueError): return None

# --- Main function to get products for UI ---
def get_products(
    db: Session, 
    skip: int = 0, 
    limit: int = 100, 
    store_id: Optional[int] = None, 
    search: Optional[str] = None,
    sort_col: str = "title",
    sort_order: str = "asc",
) -> Tuple[List[models.Product], int]:
    """
    Get products with fuzzy multi-word search and sorting.
    Search matches if ALL words are found in title, SKU, or barcode (any order).
    """
    # Load variants with their inventory levels to compute accurate stock
    query = db.query(models.Product).options(
        joinedload(models.Product.variants).joinedload(models.ProductVariant.inventory_levels)
    )
    
    if store_id:
        query = query.filter(models.Product.store_id == store_id)
    
    # Fuzzy multi-word search: ALL words must match (in any order)
    if search:
        search_text = search.strip()
        words = [w.strip().lower() for w in search_text.split() if w.strip()]
        
        for word in words:
            word_pattern = f"%{word}%"
            query = query.filter(
                or_(
                    func.lower(models.Product.title).like(word_pattern),
                    models.Product.variants.any(func.lower(models.ProductVariant.sku).like(word_pattern)),
                    models.Product.variants.any(func.lower(models.ProductVariant.barcode).like(word_pattern)),
                )
            )
    
    total_count = query.count()
    
    # Sorting
    valid_sort_cols = {
        "title": models.Product.title,
        "status": models.Product.status,
        "created_at": models.Product.created_at,
        "updated_at": models.Product.updated_at,
    }
    sort_column = valid_sort_cols.get(sort_col, models.Product.title)
    
    if sort_order.lower() == "desc":
        query = query.order_by(sort_column.desc())
    else:
        query = query.order_by(sort_column.asc())
    
    products = query.offset(skip).limit(limit).all()
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
        db_run_id = run_id if run_id != 0 else None
        dead_letter = models.SyncDeadLetter(store_id=store_id, run_id=db_run_id, payload=payload_json, reason=reason)
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
        "product_category": _get(p_data, "category", "name"),
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
    inventory_item_id = gid_to_id(_get(inventory_item, "id")) or v_data.get("inventory_item_id")

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
            
            if not isinstance(v_data_list, list): v_data_list = []

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
                    
                    inventory_levels = _get(v_data, "inventoryItem", "inventoryLevels", default=[])
                    if isinstance(inventory_levels, dict) and "edges" in inventory_levels:
                        inventory_levels = [edge['node'] for edge in inventory_levels['edges']]

                    if not isinstance(inventory_levels, list): inventory_levels = []

                    for lvl in inventory_levels:
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
                    loc_stmt = loc_stmt.on_conflict_do_update(index_elements=['id'], set_={ "name": loc_stmt.excluded.name, "shopify_gid": loc_stmt.excluded.shopify_gid })
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

# --- START: NEW AND MODIFIED WEBHOOK FUNCTIONS ---

def normalize_webhook_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a Shopify REST webhook payload to use the same field names
    as GraphQL responses. This allows the same processing logic to work
    for both sync (GraphQL) and webhooks (REST).
    
    REST webhooks use snake_case, GraphQL uses camelCase.
    """
    if not payload:
        return payload
    
    # Field mappings: REST (snake_case) -> GraphQL (camelCase)
    field_map = {
        # Product fields
        "body_html": "bodyHtml",
        "product_type": "productType",
        "created_at": "createdAt",
        "updated_at": "updatedAt",
        "published_at": "publishedAt",
        # Variant fields
        "compare_at_price": "compareAtPrice",
        "inventory_item_id": "inventoryItemId",
        "inventory_quantity": "inventoryQuantity",
        "inventory_policy": "inventoryPolicy",
        "inventory_management": "inventoryManagement",
        # Featured image
        "featured_image": "featuredImage",
        "image": "featuredImage",
    }
    
    normalized = {}
    for key, value in payload.items():
        # Map the key if it's in our mapping, otherwise keep as-is
        new_key = field_map.get(key, key)
        
        # Handle nested structures
        if key == "variants" and isinstance(value, list):
            normalized[new_key] = [normalize_webhook_payload(v) for v in value]
        elif key == "image" and isinstance(value, dict):
            # REST sends {"src": "..."}, normalize to {"url": "..."}
            normalized["featuredImage"] = {"url": value.get("src")}
        elif key == "images" and isinstance(value, list) and len(value) > 0:
            # Use first image if no featured image
            if "featuredImage" not in normalized:
                normalized["featuredImage"] = {"url": value[0].get("src")}
        elif isinstance(value, dict):
            normalized[new_key] = normalize_webhook_payload(value)
        else:
            normalized[new_key] = value
    
    return normalized

def _get_field(payload: Dict[str, Any], *keys, default=None):
    """Get a field from payload, trying multiple possible key names."""
    for key in keys:
        if key in payload:
            return payload[key]
    return default

def patch_product_from_webhook(db: Session, store_id: int, raw_payload: Dict[str, Any]):
    """
    Safely updates a product record from a webhook payload by only
    updating the fields that are present in the payload. This prevents
    overwriting complete data with partial data.
    
    Handles both REST (snake_case) and GraphQL (camelCase) field names.
    """
    # Normalize the payload to use consistent field names
    payload = normalize_webhook_payload(raw_payload)
    
    product_id = gid_to_id(payload.get("id"))
    if not product_id:
        print(f"[WEBHOOK] Ignoring product update - no valid ID in payload")
        return

    db_product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not db_product:
        # If the product doesn't exist, create it
        print(f"[WEBHOOK] Product {product_id} not found, creating from webhook")
        create_or_update_product_from_webhook(db, store_id, raw_payload)
        return

    # Build update dictionary for fields that are present
    updates = {}
    
    if "title" in payload:
        updates[models.Product.title] = payload["title"]
    if "bodyHtml" in payload:
        updates[models.Product.body_html] = payload["bodyHtml"]
    if "vendor" in payload:
        updates[models.Product.vendor] = payload["vendor"]
    if "productType" in payload:
        updates[models.Product.product_type] = payload["productType"]
    if "status" in payload:
        updates[models.Product.status] = payload["status"]
    if "handle" in payload:
        updates[models.Product.handle] = payload["handle"]
    
    # Handle tags - REST sends comma-separated string, GraphQL sends list
    if "tags" in payload:
        tags = payload["tags"]
        if isinstance(tags, list):
            updates[models.Product.tags] = ",".join(tags)
        elif isinstance(tags, str):
            updates[models.Product.tags] = tags
    
    if "updatedAt" in payload:
        updates[models.Product.updated_at] = _to_dt(payload["updatedAt"])
    
    # Update featured image if present
    if "featuredImage" in payload and payload["featuredImage"]:
        img_url = payload["featuredImage"].get("url") if isinstance(payload["featuredImage"], dict) else None
        if img_url:
            updates[models.Product.image_url] = img_url

    if updates:
        updates[models.Product.last_seen_at] = datetime.now(timezone.utc)
        (db.query(models.Product)
           .filter(models.Product.id == product_id)
           .update(updates, synchronize_session=False))
        db.commit()
        print(f"[DB-UPDATE] Patched product ID {product_id} from webhook with {len(updates)} fields")

    # Process variants incrementally if they are present
    variants = payload.get("variants", [])
    if variants:
        _update_variants_incrementally(db, product_id, store_id, variants)

def _update_variants_incrementally(db: Session, product_id: int, store_id: int, variants: List[Dict[str, Any]]):
    """
    Update variants incrementally from webhook data.
    Only updates fields that are present in the payload, preserving existing data.
    Creates new variants if they don't exist.
    """
    now = datetime.now(timezone.utc)
    
    for v_data in variants:
        variant_id = gid_to_id(v_data.get("id"))
        if not variant_id:
            continue
        
        # Check if variant exists
        db_variant = db.query(models.ProductVariant).filter(
            models.ProductVariant.id == variant_id
        ).first()
        
        if db_variant:
            # Update existing variant
            updates = {}
            if "title" in v_data:
                updates[models.ProductVariant.title] = v_data["title"]
            if "sku" in v_data:
                updates[models.ProductVariant.sku] = v_data["sku"] or None
            if "barcode" in v_data:
                updates[models.ProductVariant.barcode] = v_data["barcode"]
            if "price" in v_data:
                updates[models.ProductVariant.price] = v_data["price"]
            if "compareAtPrice" in v_data:
                updates[models.ProductVariant.compare_at_price] = v_data["compareAtPrice"]
            if "position" in v_data:
                updates[models.ProductVariant.position] = v_data["position"]
            if "inventoryQuantity" in v_data:
                updates[models.ProductVariant.inventory_quantity] = v_data["inventoryQuantity"]
            
            if updates:
                updates[models.ProductVariant.last_seen_at] = now
                (db.query(models.ProductVariant)
                   .filter(models.ProductVariant.id == variant_id)
                   .update(updates, synchronize_session=False))
        else:
            # Create new variant
            sku = v_data.get("sku")
            new_variant = models.ProductVariant(
                id=variant_id,
                product_id=product_id,
                store_id=store_id,
                shopify_gid=v_data.get("admin_graphql_api_id", f"gid://shopify/ProductVariant/{variant_id}"),
                title=v_data.get("title"),
                sku=sku if sku and sku.strip() else None,
                barcode=v_data.get("barcode"),
                price=v_data.get("price"),
                compare_at_price=v_data.get("compareAtPrice"),
                position=v_data.get("position"),
                inventory_item_id=gid_to_id(v_data.get("inventory_item_id")),
                inventory_quantity=v_data.get("inventoryQuantity"),
                last_seen_at=now,
            )
            db.add(new_variant)
            print(f"[DB-UPDATE] Created new variant {variant_id} from webhook")
    
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        print(f"[WEBHOOK-ERROR] Failed to update variants: {e}")

# --- END: WEBHOOK PAYLOAD NORMALIZATION ---

def create_or_update_product_from_webhook(db: Session, store_id: int, raw_payload: Dict[str, Any]):
    """
    Handles the 'products/create' webhook. For 'products/update', the new
    patch function should be used to avoid data loss.
    """
    # Normalize the payload to use consistent field names
    payload = normalize_webhook_payload(raw_payload)
    
    now = datetime.now(timezone.utc)
    # This function is now primarily for *creating* products from webhooks
    create_or_update_products(db, store_id, run_id=0, items=[payload], last_seen_at=now)
    print(f"[DB-UPDATE] Created/Updated product '{payload.get('title')}' from webhook.")

# --- END: NEW AND MODIFIED WEBHOOK FUNCTIONS ---


def delete_product_from_webhook(db: Session, payload: Dict[str, Any]):
    product_id = gid_to_id(payload.get("id"))
    if not product_id: return
    db.query(models.Product).filter(models.Product.id == product_id).delete()
    db.commit()
    print(f"[DB-UPDATE] Deleted product ID {product_id} from webhook.")

def update_variant_from_webhook(db: Session, payload: Dict[str, Any]):
    inventory_item_id = payload.get("id")
    variant = db.query(models.ProductVariant).filter(models.ProductVariant.inventory_item_id == inventory_item_id).first()
    if not variant: return
    
    # Update barcode if present and changed
    if 'barcode' in payload and variant.barcode != payload['barcode']:
        variant.barcode = payload['barcode']
    
    # Update cost if present and changed
    cost_info = payload.get('cost')
    if cost_info is not None and variant.cost_per_item != cost_info:
        variant.cost_per_item = cost_info

    # Update SKU if present and changed
    sku_info = payload.get('sku')
    if sku_info is not None and variant.sku != sku_info:
        variant.sku = sku_info

    db.commit()
    print(f"[DB-UPDATE] Updated variant details for inventory_item_id {inventory_item_id} from webhook.")


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