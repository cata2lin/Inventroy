# crud/inventory_report.py

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, literal, or_
from sqlalchemy.orm import Session

import models


def _normalize_store_ids(store_ids: Optional[List[int]]) -> Optional[List[int]]:
    if not store_ids:
        return None
    ids = []
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


def _group_key_expr(GM, Variant):
    """
    Prefer explicit barcode group (group_id). Fallback order:
      normalized_barcode -> barcode -> "UNKNOWN"
    """
    return func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode, literal("UNKNOWN"))


def _apply_common_filters(
    filters: Dict[str, Any],
    clauses: List[Any],
    Location,
    Variant,
):
    # store filter
    store_ids = _normalize_store_ids(filters.get("store_ids"))
    if store_ids:
        clauses.append(Location.store_id.in_(store_ids))

    # text search
    search = (filters or {}).get("search")
    if search:
        q = f"%{search.strip()}%"
        Product = models.Product
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


def _build_group_aggregate_query(db: Session, filters: Dict[str, Any]):
    """
    One row per barcode *group*, DEDUPED across stores.

    Strategy:
      1) Aggregate per (group_id, store_id): SUM(on_hand), SUM(available).
      2) Aggregate per group_id: MIN across stores to avoid triple counting.
      3) Join "sample" display fields + MAX(cost_per_item/price) for valuation.
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership
    IL = models.InventoryLevel
    Loc = models.Location
    Product = models.Product
    Store = models.Store

    group_key = _group_key_expr(GM, Variant)
    where_clauses: List[Any] = []
    _apply_common_filters(filters, where_clauses, Loc, Variant)

    # ---- stage 1: per (group_id, store_id)
    per_store_q = (
        db.query(
            group_key.label("group_id"),
            Variant.store_id.label("store_id"),
            func.sum(func.coalesce(IL.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(IL.available, 0)).label("available"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
    )
    if where_clauses:
        per_store_q = per_store_q.filter(and_(*where_clauses))
    per_store_q = per_store_q.group_by(group_key, Variant.store_id).subquery()

    # ---- stage 2: sample/display fields (per group)
    sample_q = (
        db.query(
            group_key.label("group_id"),
            func.max(Product.title).label("product_title"),
            func.max(Variant.title).label("variant_title"),
            func.max(Variant.sku).label("sku"),
            func.max(Variant.barcode).label("barcode"),
            func.max(Product.image_url).label("primary_image_url"),
            func.max(Store.name).label("primary_store"),
            func.max(func.coalesce(Variant.cost_per_item, 0.0)).label("max_cost"),
            func.max(func.coalesce(Variant.price, 0.0)).label("max_price"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .join(Store, Store.id == Loc.store_id)
        .outerjoin(Product, Product.id == Variant.product_id)
    )
    if where_clauses:
        sample_q = sample_q.filter(and_(*where_clauses))
    sample_q = sample_q.group_by(group_key).subquery()

    # ---- final grouped: MIN across stores + sample fields
    grouped = (
        db.query(
            sample_q.c.group_id,
            sample_q.c.product_title,
            sample_q.c.variant_title,
            sample_q.c.sku,
            sample_q.c.barcode,
            sample_q.c.primary_image_url,
            sample_q.c.primary_store,
            sample_q.c.max_cost,
            sample_q.c.max_price,
            func.min(per_store_q.c.on_hand).label("on_hand"),
            func.min(per_store_q.c.available).label("available"),
        )
        .join(per_store_q, per_store_q.c.group_id == sample_q.c.group_id)
        .group_by(
            sample_q.c.group_id,
            sample_q.c.product_title,
            sample_q.c.variant_title,
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
    _apply_common_filters(filters, where_clauses, Loc, Variant)

    group_key = _group_key_expr(GM, Variant)

    q = (
        db.query(
            Variant.id.label("variant_id"),
            group_key.label("group_id"),
            func.max(Product.title).label("product_title"),
            func.max(Variant.title).label("variant_title"),
            func.max(Variant.sku).label("sku"),
            func.max(Variant.barcode).label("barcode"),
            func.max(Store.name).label("store_name"),
            func.max(Product.image_url).label("image_url"),
            func.sum(func.coalesce(IL.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(IL.available, 0)).label("available"),
            func.max(func.coalesce(Variant.cost_per_item, 0.0)).label("cost_per_item"),
            func.max(func.coalesce(Variant.price, 0.0)).label("price"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .join(Store, Store.id == Loc.store_id)
        .outerjoin(Product, Product.id == Variant.product_id)
        .group_by(Variant.id, group_key)
    )
    if where_clauses:
        q = q.filter(and_(*where_clauses))
    return q


def _map_sort(sort_by: str, sort_order: str, columns: Dict[str, Any]):
    desc = (sort_order or "").lower() == "desc"
    key = (sort_by or "on_hand").lower()
    alias_map = {
        "retail_value": "retail_value",
        "inventory_value": "inventory_value",
        "price": "price",
        "cost": "cost_per_item",
        "on_hand": "on_hand",
        "available": "available",
    }
    key = alias_map.get(key, key)
    col = columns.get(key) or next(iter(columns.values()))
    return col.desc() if desc else col.asc()


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
    totals_mode: str = "grouped",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], int]:
    """
    Returns (rows, totals, total_count).

    rows:
      - view='grouped'    -> one row per barcode group (deduped across stores)
      - view='individual' -> one row per variant (aggregated across its locations)
    totals:
      - ALWAYS computed from the grouped-deduped subquery so a product present
        in multiple stores is *not* triple-counted.
    """
    filters = {"search": search, "store_ids": store_ids}

    # ---------- Totals (DEDUPED by group) ----------
    g_subq = _build_group_aggregate_query(db, filters).subquery()

    totals_row = db.query(
        func.coalesce(func.sum(g_subq.c.on_hand), 0).label("on_hand"),
        func.coalesce(func.sum(g_subq.c.available), 0).label("available"),
        func.coalesce(func.sum(g_subq.c.on_hand * func.coalesce(g_subq.c.max_cost, 0.0)), 0.0).label("inventory_value"),
        func.coalesce(func.sum(g_subq.c.on_hand * func.coalesce(g_subq.c.max_price, 0.0)), 0.0).label("retail_value"),
    ).one()

    totals = {
        "on_hand": int(totals_row.on_hand or 0),
        "available": int(totals_row.available or 0),
        "inventory_value": float(totals_row.inventory_value or 0.0),
        "retail_value": float(totals_row.retail_value or 0.0),
        "mode": "grouped",
    }

    # ---------- Rows (VIEW) ----------
    rows: List[Dict[str, Any]] = []
    total_count = 0

    if (view or "").lower() == "grouped":
        columns = {
            "group_id": g_subq.c.group_id,
            "product_title": g_subq.c.product_title,
            "variant_title": g_subq.c.variant_title,
            "sku": g_subq.c.sku,
            "barcode": g_subq.c.barcode,
            "primary_image_url": g_subq.c.primary_image_url,
            "primary_store": g_subq.c.primary_store,
            "on_hand": g_subq.c.on_hand,
            "available": g_subq.c.available,
            "max_cost": g_subq.c.max_cost,
            "max_price": g_subq.c.max_price,
            "inventory_value": g_subq.c.on_hand * func.coalesce(g_subq.c.max_cost, 0.0),
            "retail_value": g_subq.c.on_hand * func.coalesce(g_subq.c.max_price, 0.0),
        }

        # count groups
        total_count = db.query(func.count(literal(1))).select_from(g_subq).scalar() or 0

        q = db.query(
            g_subq.c.group_id,
            g_subq.c.product_title,
            g_subq.c.variant_title,
            g_subq.c.sku,
            g_subq.c.barcode,
            g_subq.c.primary_image_url,
            g_subq.c.primary_store,
            g_subq.c.on_hand,
            g_subq.c.available,
            g_subq.c.max_cost,
            g_subq.c.max_price,
            (g_subq.c.on_hand * g_subq.c.max_cost).label("inventory_value"),
            (g_subq.c.on_hand * g_subq.c.max_price).label("retail_value"),
        ).select_from(g_subq).order_by(_map_sort(sort_by, sort_order, columns))

        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        for rec in q.all():
            committed = int((rec.on_hand or 0) - (rec.available or 0))
            rows.append({
                "group_id": rec.group_id,
                "primary_title": rec.product_title,
                "primary_image_url": rec.primary_image_url,
                "primary_store": rec.primary_store,
                "sku": rec.sku,
                "barcode": rec.barcode,
                "on_hand": int(rec.on_hand or 0),
                "available": int(rec.available or 0),
                "committed": committed,
                "max_cost": float(rec.max_cost or 0.0),
                "max_price": float(rec.max_price or 0.0),
                "inventory_value": float(rec.inventory_value or 0.0),
                "retail_value": float(rec.retail_value or 0.0),
            })

    else:
        v_subq = _build_variant_aggregate_query(db, filters).subquery()

        columns = {
            "variant_id": v_subq.c.variant_id,
            "group_id": v_subq.c.group_id,
            "product_title": v_subq.c.product_title,
            "variant_title": v_subq.c.variant_title,
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

        # count variants
        total_count = db.query(func.count(literal(1))).select_from(v_subq).scalar() or 0

        q = db.query(
            v_subq.c.variant_id,
            v_subq.c.group_id,
            v_subq.c.product_title,
            v_subq.c.variant_title,
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
        ).select_from(v_subq).order_by(_map_sort(sort_by, sort_order, columns))

        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        for rec in q.all():
            committed = int((rec.on_hand or 0) - (rec.available or 0))
            rows.append({
                "variant_id": int(rec.variant_id),
                "group_id": rec.group_id,
                "product_title": rec.product_title,
                "variant_title": rec.variant_title,
                "sku": rec.sku,
                "barcode": rec.barcode,
                "image_url": rec.image_url,
                "store_name": rec.store_name,
                "on_hand": int(rec.on_hand or 0),
                "available": int(rec.available or 0),
                "committed": committed,
                "cost_per_item": float(rec.cost_per_item or 0.0),
                "cost": float(rec.cost_per_item or 0.0),  # UI alias
                "price": float(rec.price or 0.0),
                "inventory_value": float(rec.inventory_value or 0.0),
                "retail_value": float(rec.retail_value or 0.0),
            })

    return rows, totals, int(total_count)
