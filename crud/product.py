# crud/product.py
from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
    """
    Parse Shopify/ISO timestamps and return TZ-aware UTC datetimes.
    """
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


def _norm_text_empty_to_none(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _norm_barcode(b: Optional[str]) -> Optional[str]:
    if not b:
        return None
    v = str(b).strip().replace(" ", "")
    return v.upper() or None


def _product_status_to_db(val: Optional[str]) -> Optional[str]:
    return str(val).upper() if val else None


def _first_image_url(prod: Any) -> Optional[str]:
    """
    Try multiple shapes:
      • GraphQL: product.featuredImage.url
      • Already stored: product.image_url
      • REST webhook: product.image.src
      • REST list: product.images[0].src / .url
      • GraphQL: product.images.edges[0].node.url
    """
    url = _get(prod, "featuredImage", "url")
    if url:
        return url
    url = _get(prod, "image_url")
    if url:
        return url
    url = _get(prod, "image", "src")
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


# ---------- field extraction ----------

def _extract_product_fields(prod: Any) -> Dict[str, Any]:
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
        "published_at": _to_dt(_get(prod, "PublishedAt") or _get(prod, "publishedAt")),
        "status": _product_status_to_db(_get(prod, "status")),
        "tags": tags_val,
        "image_url": _first_image_url(prod),
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

    unit_cost = None
    amount = _get(variant, "inventoryItem", "unitCost", "amount")
    if amount is not None:
        try:
            unit_cost = float(amount)
        except Exception:
            unit_cost = None

    sku = _norm_text_empty_to_none(_get(variant, "sku"))
    barcode = _norm_text_empty_to_none(_get(variant, "barcode"))

    return {
        "id": int(vid),
        "shopify_gid": _get(variant, "id"),
        "product_id": product_id,
        "title": _get(variant, "title"),
        "sku": sku,
        "barcode": barcode,
        "barcode_normalized": _norm_barcode(barcode),
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
        "inventory_levels": _get(variant, "inventoryItem", "inventoryLevels") or [],
    }


# ---------- core bulk-upsert helpers (Postgres) ----------

def _pg_upsert(
    db: Session,
    table,
    rows: List[Dict[str, Any]],
    conflict_cols: Iterable[str],
    exclude_from_update: Iterable[str] = (),
):
    """
    Generic ON CONFLICT upsert:
    - Updates use COALESCE(excluded.col, table.col) so NULLs from input never clobber existing values.
    - Special rule: inventory_item_id is only set if currently NULL.
    """
    if not rows:
        return
    # union of keys across all rows
    all_cols = set()
    for r in rows:
        all_cols.update(r.keys())

    stmt = pg_insert(table).values(rows)
    excl = set(exclude_from_update) | set(conflict_cols)
    update_cols = {}
    for c in all_cols - excl:
        update_cols[c] = func.coalesce(getattr(stmt.excluded, c), getattr(table.c, c))

    # never rebind inventory_item_id after first set
    if "inventory_item_id" in table.c:
        update_cols["inventory_item_id"] = func.coalesce(
            table.c.inventory_item_id, getattr(stmt.excluded, "inventory_item_id")
        )

    stmt = stmt.on_conflict_do_update(index_elements=list(conflict_cols), set_=update_cols)
    db.execute(stmt)


# ---------- upsert from full product + variants (GraphQL pagination job) ----------

def create_or_update_products(
    db: Session,
    store_id: int,
    items: List[Any],
) -> None:
    """
    Fast & safe multi-store sync:
      - Products upserted by (store_id, id)
      - Variants resolved in 4 buckets with deterministic SKU ownership:
          A) owner-by-id -> upsert by (store_id,id)
          B) owner-new   -> insert by (store_id,id)
          C) non-owner existing-by-id -> upsert by (store_id,id) skipping sku update
          D) non-owner new -> merge by (store_id,sku) (id never changes)
      - Inventory levels upserted by (inventory_item_id, location_id)
      - Barcode groups + memberships kept in sync
    """
    now = datetime.now(timezone.utc)

    # --- Collect rows
    prod_rows: List[Dict[str, Any]] = []
    var_rows: List[Dict[str, Any]] = []
    inv_level_rows: List[Dict[str, Any]] = []

    for bundle in items or []:
        p = bundle.get("product", bundle) if isinstance(bundle, dict) else bundle
        vs = _get(p, "variants") or (bundle.get("variants") if isinstance(bundle, dict) else []) or []
        if not isinstance(vs, list):
            vs = []

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

            # peel inventory levels out
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

    # --- Upsert products by (store_id, id)
    _pg_upsert(
        db,
        models.Product.__table__,
        prod_rows,
        conflict_cols=("store_id", "id"),
        exclude_from_update=("store_id", "id"),
    )

    # --- Preload existing variants for this store (by id and sku)
    incoming_ids = [v["id"] for v in var_rows]
    incoming_skus = [v["sku"] for v in var_rows if v.get("sku")]

    existing = db.execute(
        select(models.ProductVariant.id, models.ProductVariant.sku)
        .where(
            models.ProductVariant.store_id == store_id,
            (models.ProductVariant.id.in_(incoming_ids)) |
            (models.ProductVariant.sku.in_(incoming_skus))
        )
    ).all()

    existing_by_id = {row.id: row.sku for row in existing}
    existing_owner_by_sku = {row.sku: row.id for row in existing if row.sku}

    # --- Decide a single “owner id” for each incoming SKU (avoid in-batch collisions)
    # Prefer an existing DB owner; otherwise pick the lowest variant id among incoming
    sku_to_incoming_ids: Dict[str, List[int]] = {}
    for v in var_rows:
        s = v.get("sku")
        if s:
            sku_to_incoming_ids.setdefault(s, []).append(v["id"])

    owner_for_sku: Dict[str, int] = {}
    for sku, vids in sku_to_incoming_ids.items():
        if sku in existing_owner_by_sku:
            owner_for_sku[sku] = int(existing_owner_by_sku[sku])
        else:
            owner_for_sku[sku] = int(min(vids))

    # --- Bucketize variants
    upsert_by_id: List[Dict[str, Any]] = []           # owner exists-by-id
    upsert_by_id_skip_sku: List[Dict[str, Any]] = []   # non-owner exists-by-id (avoid sku update)
    insert_new_by_id: List[Dict[str, Any]] = []        # owner not-exist-by-id
    upsert_by_sku: List[Dict[str, Any]] = []           # non-owner not-exist-by-id (merge by sku)

    for v in var_rows:
        vid = v["id"]
        vsku = v.get("sku")
        exists_by_id = vid in existing_by_id

        if not vsku:
            # No SKU -> pure id path
            if exists_by_id:
                upsert_by_id.append(v)
            else:
                insert_new_by_id.append(v)
            continue

        owner_id = owner_for_sku.get(vsku, vid)

        if vid == owner_id:
            # This row owns the SKU
            if exists_by_id:
                upsert_by_id.append(v)
            else:
                insert_new_by_id.append(v)
        else:
            # This row does NOT own the SKU
            if exists_by_id:
                upsert_by_id_skip_sku.append(v)  # keep its other fields, do NOT change sku
            else:
                upsert_by_sku.append(v)          # merge into owner row by (store,sku), id untouched

    # --- Apply upserts in a safe order to satisfy in-batch dependencies
    # 1) Insert new owners (by id)
    _pg_upsert(
        db,
        models.ProductVariant.__table__,
        insert_new_by_id,
        conflict_cols=("store_id", "id"),
        exclude_from_update=("store_id", "id"),
    )

    # 2) Merge non-owners by (store, sku) – never touch id/sku/store_id
    _pg_upsert(
        db,
        models.ProductVariant.__table__,
        upsert_by_sku,
        conflict_cols=("store_id", "sku"),
        exclude_from_update=("id", "store_id", "sku"),
    )

    # 3) Update existing owners by id (normal) – allow sku change unless it would collide (already handled by ownership)
    _pg_upsert(
        db,
        models.ProductVariant.__table__,
        upsert_by_id,
        conflict_cols=("store_id", "id"),
        exclude_from_update=("store_id", "id"),
    )

    # 4) Update existing non-owners by id but skip sku (avoid unique (store,sku))
    _pg_upsert(
        db,
        models.ProductVariant.__table__,
        upsert_by_id_skip_sku,
        conflict_cols=("store_id", "id"),
        exclude_from_update=("store_id", "id", "sku"),
    )
    
    # *** FIX: Commit variants before inserting inventory levels ***
    db.commit()

    # --- Inventory levels (inventory_item_id, location_id)
    _pg_upsert(
        db,
        models.InventoryLevel.__table__,
        inv_level_rows,
        conflict_cols=("inventory_item_id", "location_id"),
        exclude_from_update=("inventory_item_id", "location_id"),
    )

    # --- Barcode groups & memberships (keep in sync)
    touched_variant_ids = [v["id"] for v in var_rows]
    # Collect desired memberships for those variants
    desired_members: List[Dict[str, Any]] = []
    groups_needed: Dict[str, Dict[str, Any]] = {}

    for v in var_rows:
        bc_norm = v.get("barcode_normalized")
        if bc_norm:
            desired_members.append({"variant_id": v["id"], "group_id": bc_norm})
            groups_needed[bc_norm] = {"id": bc_norm, "status": "active", "pool_available": 0}

    if groups_needed:
        db.execute(
            pg_insert(models.BarcodeGroup.__table__)
            .values(list(groups_needed.values()))
            .on_conflict_do_nothing()
        )

    if touched_variant_ids:
        db.execute(
            delete(models.GroupMembership)
            .where(models.GroupMembership.variant_id.in_(touched_variant_ids))
        )

    if desired_members:
        db.execute(
            pg_insert(models.GroupMembership.__table__)
            .values(desired_members)
            .on_conflict_do_nothing()
        )

    db.commit()


# ---------- webhook: reuse the same pipeline for identical behavior ----------

def create_or_update_product_from_webhook(
    db: Session,
    store_id: int,
    payload: Any,
) -> None:
    prod = payload.get("product") if isinstance(payload, dict) and "product" in payload else payload
    if not prod:
        return
    create_or_update_products(db, store_id, items=[prod])