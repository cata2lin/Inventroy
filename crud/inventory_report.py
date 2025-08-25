# crud/inventory_report.py
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

from sqlalchemy import and_, func, literal, or_
from sqlalchemy.orm import Session

import models


# ---------- helpers ----------

def _normalize_store_ids(store_ids: Optional[List[int]]) -> Optional[List[int]]:
    if not store_ids:
        return None
    ids: List[int] = []
    for s in store_ids:
        if s is None:
            continue
        s_str = str(s).strip()
        if not s_str:
            continue
        try:
            ids.append(int(s_str))
        except ValueError:
            continue
    return list(sorted(set(ids))) or None


def _normalize_str_list(values: Optional[List[str]]) -> Optional[List[str]]:
    if not values:
        return None
    out: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out.append(s)
    return list(sorted(set(out))) or None


def _group_key_expr(GM, Variant):
    """
    Prefer explicit barcode group (group_id). Fallback order:
      normalized_barcode -> barcode -> "UNKNOWN"
    """
    return func.coalesce(
        GM.group_id,
        Variant.barcode_normalized,
        Variant.barcode,
        literal("UNKNOWN"),
    )


def _apply_common_filters(
    filters: Dict[str, Any],
    clauses: List[Any],
    Location,
    Variant,
    Product,
):
    # store filter
    store_ids = _normalize_store_ids(filters.get("store_ids"))
    if store_ids:
        clauses.append(Location.store_id.in_(store_ids))

    # product status filter
    statuses = _normalize_str_list(filters.get("statuses"))
    if statuses:
        clauses.append(Product.status.in_(statuses))

    # product type filter
    types_ = _normalize_str_list(filters.get("product_types"))
    if types_:
        clauses.append(Product.product_type.in_(types_))

    # text search
    search = (filters or {}).get("search")
    if search:
        q = f"%{search.strip()}%"
        GM = models.GroupMembership
        clauses.append(
            or_(
                Product.title.ilike(q),
                Variant.title.ilike(q),
                Variant.sku.ilike(q),
                Variant.barcode.ilike(q),
                Variant.barcode_normalized.ilike(q),
                GM.group_id.ilike(q),
            )
        )


# ---------- query builders ----------

def _build_group_aggregate_query(db: Session, filters: Dict[str, Any]):
    """
    One row per barcode *group*.

    We show:
      - available_dedup = MIN(available per store) to avoid triple counting
      - committed_total = SUM( committed per store )
      - total_stock     = available_dedup + committed_total  (equivalent to on_hand across stores)
      - valuation       = based on available_dedup (requested behavior for grouped rows)
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership
    IL = models.InventoryLevel
    Loc = models.Location
    Product = models.Product
    Store = models.Store

    # unified cost accessor: prefer cost_per_item, fall back to legacy cost
    cost_expr = func.coalesce(Variant.cost_per_item, Variant.cost, 0.0)

    group_key = _group_key_expr(GM, Variant)
    where_clauses: List[Any] = []
    _apply_common_filters(filters, where_clauses, Loc, Variant, Product)

    # ---- stage 1: per (group_id, store_id) rollup
    # Explicit left side avoids ambiguous-join errors in SQLAlchemy
    per_store_q = (
        db.query(
            group_key.label("group_id"),
            Loc.store_id.label("store_id"),
            func.sum(func.coalesce(IL.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(IL.available, 0)).label("available"),
        )
        .select_from(Variant)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .join(Product, Product.id == Variant.product_id)
    )
    if where_clauses:
        per_store_q = per_store_q.filter(and_(*where_clauses))
    per_store_q = per_store_q.group_by(group_key, Loc.store_id).subquery()

    # ---- stage 2: sample/display fields (per group)
    sample_q = (
        db.query(
            group_key.label("group_id"),
            func.max(Product.title).label("product_title"),
            func.max(Variant.title).label("variant_title"),
            func.max(Product.status).label("status"),
            func.max(Product.product_type).label("product_type"),
            func.max(Variant.sku).label("sku"),
            func.max(Variant.barcode).label("barcode"),
            func.max(Product.image_url).label("primary_image_url"),
            func.max(Store.name).label("primary_store"),
            func.max(func.coalesce(cost_expr, 0.0)).label("max_cost"),
            func.max(func.coalesce(Variant.price, 0.0)).label("max_price"),
        )
        .select_from(Variant)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .join(Store, Store.id == Loc.store_id)
        .join(Product, Product.id == Variant.product_id)
    )
    if where_clauses:
        sample_q = sample_q.filter(and_(*where_clauses))
    sample_q = sample_q.group_by(group_key).subquery()

    # ---- final grouped aggregation
    grouped = (
        db.query(
            sample_q.c.group_id,
            sample_q.c.product_title,
            sample_q.c.variant_title,
            sample_q.c.status,
            sample_q.c.product_type,
            sample_q.c.sku,
            sample_q.c.barcode,
            sample_q.c.primary_image_url,
            sample_q.c.primary_store,
            sample_q.c.max_cost,
            sample_q.c.max_price,
            # Availability / commitment
            func.min(per_store_q.c.available).label("available"),  # dedup
            # committed_total across stores = SUM(on_hand - available)
            func.sum(per_store_q.c.on_hand - per_store_q.c.available).label("committed_total"),
        )
        .join(per_store_q, per_store_q.c.group_id == sample_q.c.group_id)
        .group_by(
            sample_q.c.group_id,
            sample_q.c.product_title,
            sample_q.c.variant_title,
            sample_q.c.status,
            sample_q.c.product_type,
            sample_q.c.sku,
            sample_q.c.barcode,
            sample_q.c.primary_image_url,
            sample_q.c.primary_store,
            sample_q.c.max_cost,
            sample_q.c.max_price,
        )
    )
    return grouped


def _build_variant_aggregate_query(db: Session, filters: Dict[str, Any]):
    """
    One row per *variant* (aggregated across its locations).
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership
    IL = models.InventoryLevel
    Loc = models.Location
    Product = models.Product
    Store = models.Store

    where_clauses: List[Any] = []
    _apply_common_filters(filters, where_clauses, Loc, Variant, Product)

    group_key = _group_key_expr(GM, Variant)
    cost_expr = func.coalesce(Variant.cost_per_item, Variant.cost, 0.0)

    q = (
        db.query(
            Variant.id.label("variant_id"),
            group_key.label("group_id"),
            func.max(Product.title).label("product_title"),
            func.max(Variant.title).label("variant_title"),
            func.max(Product.status).label("status"),
            func.max(Product.product_type).label("product_type"),
            func.max(Variant.sku).label("sku"),
            func.max(Variant.barcode).label("barcode"),
            func.max(Store.name).label("store_name"),
            func.max(Product.image_url).label("image_url"),
            func.sum(func.coalesce(IL.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(IL.available, 0)).label("available"),
            func.max(func.coalesce(cost_expr, 0.0)).label("cost_per_item"),
            func.max(func.coalesce(Variant.price, 0.0)).label("price"),
        )
        .select_from(Variant)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .join(Store, Store.id == Loc.store_id)
        .join(Product, Product.id == Variant.product_id)
        .group_by(Variant.id, group_key)
    )
    if where_clauses:
        q = q.filter(and_(*where_clauses))
    return q


def _map_sort(sort_by: str, sort_order: str, columns: Dict[str, Any]):
    """
    Return a SQLAlchemy sort expression without evaluating a SQL clause in boolean context.
    Maps common aliases between grouped/individual views.
    """
    desc = (sort_order or "").lower() == "desc"
    key = (sort_by or "on_hand").lower()

    # Normalize aliases
    alias_map = {
        "cost": "cost_per_item",
        "committed": "committed_total",
        "inventoryvalue": "inventory_value",
        "retailvalue": "retail_value",
        "totalstock": "total_stock",
    }
    key = alias_map.get(key, key)

    # Fallback chain
    candidates: List[str] = [key]
    if key == "price" and "price" not in columns and "max_price" in columns:
        candidates.append("max_price")
    if key in ("cost_per_item",) and key not in columns:
        for alt in ("max_cost", "cost_per_item"):
            if alt not in candidates:
                candidates.append(alt)
    if key == "on_hand" and "on_hand" not in columns and "total_stock" in columns:
        candidates.append("total_stock")

    col = None
    for c in candidates:
        if c in columns:
            col = columns[c]
            break
    if col is None:
        col = columns.get("available") or columns.get("on_hand")
    if col is None:
        col = next(iter(columns.values()))

    return col.desc() if desc else col.asc()


# ---------- public API ----------

def get_inventory_report(
    db: Session,
    *,
    skip: int = 0,
    limit: int = 50,
    sort_by: str = "on_hand",
    sort_order: str = "desc",
    view: str = "individual",
    search: Optional[str] = None,
    store_ids: Optional[List[int]] = None,
    statuses: Optional[List[str]] = None,
    product_types: Optional[List[str]] = None,
    totals_mode: str = "grouped",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], int]:
    """
    Returns (rows, totals, total_count).

    rows:
      - view='grouped'    -> one row per barcode group
      - view='individual' -> one row per variant
    totals:
      - computed from grouped-deduped data
    """
    filters: Dict[str, Any] = {
        "search": search,
        "store_ids": store_ids,
        "statuses": statuses,
        "product_types": product_types,
    }

    # ------------- GROUPED base (for totals and, if requested, for rows)
    g_base = _build_group_aggregate_query(db, filters).subquery()

    # totals based on available_dedup and prices/costs
    totals_row = (
        db.query(
            func.coalesce(func.sum(g_base.c.available), 0).label("available_sum"),
            func.coalesce(func.sum(g_base.c.committed_total), 0).label("committed_sum"),
            func.coalesce(
                func.sum(g_base.c.available * func.coalesce(g_base.c.max_cost, 0.0)), 0.0
            ).label("inventory_value"),
            func.coalesce(
                func.sum(g_base.c.available * func.coalesce(g_base.c.max_price, 0.0)), 0.0
            ).label("retail_value"),
        )
        .select_from(g_base)
        .one()
    )

    totals = {
        # keep key name 'on_hand' for compatibility with the UI top metric
        "on_hand": int(totals_row.available_sum or 0),
        "available": int(totals_row.available_sum or 0),
        "committed": int(totals_row.committed_sum or 0),
        "inventory_value": float(totals_row.inventory_value or 0.0),
        "retail_value": float(totals_row.retail_value or 0.0),
        "mode": "grouped",
    }

    rows: List[Dict[str, Any]] = []
    total_count = 0

    if (view or "").lower() == "grouped":
        # Add computed valuations and total_stock
        # NOTE: inventory/retail values use available_dedup as requested
        g_subq = db.query(
            g_base.c.group_id,
            g_base.c.product_title,
            g_base.c.variant_title,
            g_base.c.status,
            g_base.c.product_type,
            g_base.c.sku,
            g_base.c.barcode,
            g_base.c.primary_image_url,
            g_base.c.primary_store,
            g_base.c.available,
            g_base.c.committed_total,
            g_base.c.max_cost,
            g_base.c.max_price,
            (g_base.c.available + g_base.c.committed_total).label("total_stock"),
            (g_base.c.available * g_base.c.max_cost).label("inventory_value"),
            (g_base.c.available * g_base.c.max_price).label("retail_value"),
        ).subquery()

        columns = {
            "group_id": g_subq.c.group_id,
            "product_title": g_subq.c.product_title,
            "variant_title": g_subq.c.variant_title,
            "status": g_subq.c.status,
            "product_type": g_subq.c.product_type,
            "sku": g_subq.c.sku,
            "barcode": g_subq.c.barcode,
            "primary_image_url": g_subq.c.primary_image_url,
            "primary_store": g_subq.c.primary_store,
            "available": g_subq.c.available,
            "committed_total": g_subq.c.committed_total,
            "total_stock": g_subq.c.total_stock,
            "max_cost": g_subq.c.max_cost,
            "max_price": g_subq.c.max_price,
            "inventory_value": g_subq.c.inventory_value,
            "retail_value": g_subq.c.retail_value,
        }

        total_count = db.query(func.count(literal(1))).select_from(g_subq).scalar() or 0

        q = (
            db.query(
                g_subq.c.group_id,
                g_subq.c.product_title,
                g_subq.c.variant_title,
                g_subq.c.status,
                g_subq.c.product_type,
                g_subq.c.sku,
                g_subq.c.barcode,
                g_subq.c.primary_image_url,
                g_subq.c.primary_store,
                g_subq.c.available,
                g_subq.c.committed_total,
                g_subq.c.total_stock,
                g_subq.c.max_cost,
                g_subq.c.max_price,
                g_subq.c.inventory_value,
                g_subq.c.retail_value,
            )
            .select_from(g_subq)
            .order_by(_map_sort(sort_by, sort_order, columns))
        )

        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        recs = q.all()

        # attach member variants for each group (expanders)
        rows_map: Dict[str, Dict[str, Any]] = {}
        group_ids: List[str] = []
        for rec in recs:
            gid = rec.group_id
            group_ids.append(gid)
            rows_map[gid] = {
                "group_id": rec.group_id,
                "primary_title": rec.product_title,
                "product_title": rec.product_title,       # legacy
                "variant_title": rec.variant_title,
                "status": rec.status,
                "product_type": rec.product_type,
                "primary_image_url": rec.primary_image_url,
                "image_url": rec.primary_image_url,       # legacy
                "primary_store": rec.primary_store,
                "store_name": rec.primary_store,          # legacy
                "sku": rec.sku,
                "barcode": rec.barcode,
                "available": int(rec.available or 0),                     # renamed in UI
                "committed": int(rec.committed_total or 0),               # total across stores
                "total_stock": int(rec.total_stock or 0),                 # available + committed
                "cost_per_item": float(rec.max_cost or 0.0),
                "cost": float(rec.max_cost or 0.0),
                "price": float(rec.max_price or 0.0),
                "inventory_value": float(rec.inventory_value or 0.0),
                "retail_value": float(rec.retail_value or 0.0),
                # ensure arrays exist for UI .map
                "variants_json": [],
                "variants": [],
                "members": [],
            }

        if group_ids:
            Variant = models.ProductVariant
            GM = models.GroupMembership
            Store = models.Store
            Product = models.Product
            group_key = _group_key_expr(GM, Variant)

            members_q = (
                db.query(
                    group_key.label("group_id"),
                    Variant.id.label("variant_id"),
                    Variant.sku.label("sku"),
                    Store.name.label("store_name"),
                    Product.status.label("status"),
                    Variant.is_primary_variant.label("is_primary"),
                )
                .select_from(Variant)
                .outerjoin(GM, GM.variant_id == Variant.id)
                .join(Store, Store.id == Variant.store_id)
                .join(Product, Product.id == Variant.product_id)
                .filter(group_key.in_(group_ids))
                .all()
            )
            bucket: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
            for m in members_q:
                bucket[m.group_id].append(
                    {
                        "variant_id": int(m.variant_id),
                        "sku": m.sku,
                        "store_name": m.store_name,
                        "status": m.status,
                        "is_primary": bool(m.is_primary),
                    }
                )
            for gid in group_ids:
                members = bucket.get(gid, [])
                if gid in rows_map:
                    rows_map[gid]["variants_json"] = members
                    rows_map[gid]["variants"] = members
                    rows_map[gid]["members"] = members

        rows = list(rows_map.values())

    else:
        v_subq = _build_variant_aggregate_query(db, filters).subquery()

        columns = {
            "variant_id": v_subq.c.variant_id,
            "group_id": v_subq.c.group_id,
            "product_title": v_subq.c.product_title,
            "variant_title": v_subq.c.variant_title,
            "status": v_subq.c.status,
            "product_type": v_subq.c.product_type,
            "sku": v_subq.c.sku,
            "barcode": v_subq.c.barcode,
            "image_url": v_subq.c.image_url,
            "store_name": v_subq.c.store_name,
            "on_hand": v_subq.c.on_hand,
            "available": v_subq.c.available,
            "cost_per_item": v_subq.c.cost_per_item,
            "price": v_subq.c.price,
            "inventory_value": v_subq.c.on_hand * v_subq.c.cost_per_item,
            "retail_value": v_subq.c.on_hand * v_subq.c.price,
        }

        total_count = db.query(func.count(literal(1))).select_from(v_subq).scalar() or 0

        q = (
            db.query(
                v_subq.c.variant_id,
                v_subq.c.group_id,
                v_subq.c.product_title,
                v_subq.c.variant_title,
                v_subq.c.status,
                v_subq.c.product_type,
                v_subq.c.sku,
                v_subq.c.barcode,
                v_subq.c.image_url,
                v_subq.c.store_name,
                v_subq.c.on_hand,
                v_subq.c.available,
                v_subq.c.cost_per_item,
                v_subq.c.price,
                (v_subq.c.on_hand * v_subq.c.cost_per_item).label("inventory_value"),
                (v_subq.c.on_hand * v_subq.c.price).label("retail_value"),
            )
            .select_from(v_subq)
            .order_by(_map_sort(sort_by, sort_order, columns))
        )

        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        for rec in q.all():
            committed = int((rec.on_hand or 0) - (rec.available or 0))
            rows.append(
                {
                    "variant_id": int(rec.variant_id),
                    "group_id": rec.group_id,
                    "product_title": rec.product_title,
                    "variant_title": rec.variant_title,
                    "status": rec.status,
                    "product_type": rec.product_type,
                    "sku": rec.sku,
                    "barcode": rec.barcode,
                    "image_url": rec.image_url,
                    "store_name": rec.store_name,
                    "on_hand": int(rec.on_hand or 0),
                    "available": int(rec.available or 0),
                    "committed": committed,
                    "cost_per_item": float(rec.cost_per_item or 0.0),
                    "cost": float(rec.cost_per_item or 0.0),
                    "price": float(rec.price or 0.0),
                    "inventory_value": float(rec.inventory_value or 0.0),
                    "retail_value": float(rec.retail_value or 0.0),
                }
            )

    return rows, totals, int(total_count)
