# crud/product.py

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
import models
import json # Importăm json pentru serializare

try:
    from shopify_service import gid_to_id
except ImportError:
    def gid_to_id(gid: Optional[str]) -> Optional[int]:
        if not gid: return None
        try: return int(str(gid).split("/")[-1])
        except (IndexError, ValueError): return None

# --- Funcție principală pentru a obține produse pentru UI ---
def get_products(
    db: Session, skip: int = 0, limit: int = 100, store_id: Optional[int] = None, search: Optional[str] = None
) -> Tuple[List[models.Product], int]:
    """
    Obține o listă paginată de produse cu filtrare opțională.
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

# --- Funcții ajutătoare pentru extragerea și normalizarea datelor ---
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

# Funcție pentru a face obiectele datetime serializabile JSON
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def log_dead_letter(db: Session, store_id: int, run_id: int, payload: Dict, reason: str):
    """
    Înregistrează un payload eșuat în tabela `sync_dead_letters`, asigurându-se că este serializabil JSON.
    """
    try:
        # Serializăm payload-ul folosind handler-ul nostru custom pentru datetime
        payload_str = json.dumps(payload, default=json_serial)
        payload_json = json.loads(payload_str) # Re-încărcăm pentru a fi un obiect dict valid pentru inserare

        dead_letter = models.SyncDeadLetter(
            store_id=store_id,
            run_id=run_id,
            payload=payload_json,
            reason=reason
        )
        db.add(dead_letter)
        db.commit() # Comitem imediat înregistrarea în dead letter
    except Exception as e:
        db.rollback()
        print(f"FATAL: Could not log to dead letter table. Reason: {e}")


# --- Extragerea datelor din payload-ul Shopify ---
def _extract_product_fields(p_data: Any, store_id: int, last_seen_at: datetime) -> Dict:
    pid = gid_to_id(p_data.get("id"))
    if not pid: raise ValueError("Missing product ID")
    return {
        "id": pid, "store_id": store_id, "shopify_gid": p_data.get("id"),
        "title": p_data.get("title"), "body_html": p_data.get("bodyHtml"),
        "vendor": p_data.get("vendor"), "product_type": p_data.get("productType"),
        "status": p_data.get("status"), "handle": p_data.get("handle"),
        "tags": ",".join(p_data.get("tags", []) if p_data.get("tags") is not None else []),
        "image_url": _first_image_url(p_data),
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

# --- Logică robustă de Upsert ---
def create_or_update_products(db: Session, store_id: int, run_id: int, items: List[Any], last_seen_at: datetime):
    if not items:
        return

    now = datetime.now(timezone.utc)

    # Procesăm fiecare produs individual pentru a evita erorile de cardinalitate
    for bundle in items:
        try:
            # Extragem datele pentru produs și variante
            p_data = bundle
            v_data_list = p_data.get("variants", [])

            p_row = _extract_product_fields(p_data, store_id, last_seen_at)

            # Upsert pentru produs
            product_stmt = pg_insert(models.Product).values(p_row)
            product_update_stmt = product_stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    "title": product_stmt.excluded.title,
                    "status": product_stmt.excluded.status,
                    "vendor": product_stmt.excluded.vendor,
                    "updated_at": product_stmt.excluded.updated_at,
                    "last_seen_at": product_stmt.excluded.last_seen_at
                }
            )
            db.execute(product_update_stmt)

            # Procesăm și facem upsert pentru fiecare variantă individual
            if v_data_list:
                loc_rows_map = {}
                inv_level_rows = []
                unique_skus_in_bundle = set()

                for v_data in v_data_list:
                    # De-duplicare SKU în cadrul aceluiași produs
                    sku = v_data.get("sku")
                    if sku is not None and sku in unique_skus_in_bundle:
                        continue # Sarim peste SKU-urile duplicate din același produs
                    if sku is not None:
                        unique_skus_in_bundle.add(sku)

                    v_row = _extract_variant_fields(v_data, p_row["id"], store_id, last_seen_at)

                    variant_stmt = pg_insert(models.ProductVariant).values(v_row)
                    variant_update_stmt = variant_stmt.on_conflict_do_update(
                        index_elements=['id'],
                        set_={
                            "sku": variant_stmt.excluded.sku,
                            "price": variant_stmt.excluded.price,
                            "barcode": variant_stmt.excluded.barcode,
                            "inventory_quantity": variant_stmt.excluded.inventory_quantity,
                            "updated_at": variant_stmt.excluded.updated_at,
                            "last_seen_at": variant_stmt.excluded.last_seen_at
                        }
                    )
                    # Adăugăm o clauză de conflict secundară pentru constrângerea (sku, store_id)
                    variant_update_stmt = variant_update_stmt.on_conflict_do_update(
                        constraint='product_variants_sku_store_id_key',
                        set_={
                             "price": variant_stmt.excluded.price,
                             "barcode": variant_stmt.excluded.barcode,
                             "inventory_quantity": variant_stmt.excluded.inventory_quantity,
                             "updated_at": variant_stmt.excluded.updated_at,
                             "last_seen_at": variant_stmt.excluded.last_seen_at
                        }
                    )
                    db.execute(variant_update_stmt)

                    # Colectăm nivelurile de inventar și locațiile unice
                    for lvl in _get(v_data, "inventoryItem", "inventoryLevels", default=[]):
                        loc_id = gid_to_id(_get(lvl, "location", "id"))
                        if not loc_id: continue
                        
                        # Folosim un dicționar pentru a asigura unicitatea locațiilor
                        if loc_id not in loc_rows_map:
                            loc_rows_map[loc_id] = {"id": loc_id, "store_id": store_id, "name": _get(lvl, "location", "name")}

                        qmap = {q["name"]: q["quantity"] for q in _get(lvl, "quantities", default=[])}
                        inv_level_rows.append({
                            "variant_id": v_row["id"], "location_id": loc_id,
                            "inventory_item_id": v_row["inventory_item_id"],
                            "available": qmap.get("available", 0),
                            "on_hand": qmap.get("on_hand", qmap.get("available", 0)),
                            "last_fetched_at": now,
                        })

                # Upsert pentru locații (în lot, acum sunt unice)
                loc_rows = list(loc_rows_map.values())
                if loc_rows:
                    loc_stmt = pg_insert(models.Location).values(loc_rows)
                    loc_update_stmt = loc_stmt.on_conflict_do_update(
                        index_elements=['id'],
                        set_={"name": loc_stmt.excluded.name}
                    )
                    db.execute(loc_update_stmt)

                # Upsert pentru nivelurile de inventar (în lot)
                if inv_level_rows:
                    inv_stmt = pg_insert(models.InventoryLevel).values(inv_level_rows)
                    inv_update_stmt = inv_stmt.on_conflict_do_update(
                        index_elements=['variant_id', 'location_id'],
                        set_={
                            "available": inv_stmt.excluded.available,
                            "on_hand": inv_stmt.excluded.on_hand,
                            "last_fetched_at": inv_stmt.excluded.last_fetched_at
                        }
                    )
                    db.execute(inv_update_stmt)

            # Comitem tranzacția pentru fiecare produs în parte
            db.commit()

        except Exception as e:
            db.rollback() # Anulăm tranzacția pentru produsul curent care a eșuat
            log_dead_letter(db, store_id, run_id, bundle, f"Product processing failed: {e}")
            continue # Continuăm cu următorul produs din lot