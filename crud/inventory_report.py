# crud/inventory_report.py

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, literal, and_, or_

import models


def _parse_store_ids(store_ids: Optional[List[int]]) -> Optional[List[int]]:
    if not store_ids:
        return None
    ids = [int(s) for s in store_ids if s is not None and str(s).strip() != ""]
    return list(sorted(set(ids))) or None


def _apply_search(filters: Dict[str, Any], clauses: List[Any], Product, Variant, GM):
    """
    Adds fuzzy search over product/variant fields & group (barcode) id.
    """
    search = (filters or {}).get("search")
    if not search:
        return
    q = f"%{search.strip()}%"
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


def _base_join(db: Session):
    """
    Returns (query, aliases) with the canonical joins used by all report queries.
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership
    IL = models.InventoryLevel
    Loc = models.Location
    Product = models.Product

    q = (
        db.query(Variant, GM, IL, Loc, Product)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(IL, IL.inventory_item_id == Variant.inventory_item_id)
        .join(Loc, Loc.id == IL.location_id)
        .outerjoin(Product, Product.id == Variant.product_id)
    )
    return q, Variant, GM, IL, Loc, Product


def _filters_where(filters: Dict[str, Any], Loc, Variant) -> List[Any]:
    """
    Converts incoming filters into SQLAlchemy filter clauses.
    """
    clauses: List[Any] = []

    # store_ids filter: use the store attached to Location
    store_ids = _parse_store_ids(filters.get("store_ids"))
    if store_ids:
        clauses.append(Loc.store_id.in_(store_ids))

    return clauses


def _group_key_expr(GM, Variant):
    """
    Barcode group key. For variants without a barcode/group, isolate them
    as unique groups ('VARIANT:{id}') to avoid accidental cross-counting.
    """
    return func.coalesce(GM.group_id, func.concat(literal("VARIANT:"), models.ProductVariant.id))


def _build_group_aggregate_query(db: Session, filters: Dict[str, Any]):
    """
    Builds a grouped-by-barcode query that aggregates:
      - group_id
      - on_hand (SUM)
      - available (SUM)
      - max_cost (MAX over cost_per_item)
      - max_price (MAX over price)
      - a 'sample' product & variant title/sku/barcode (for display)
    """
    group_key = _group_key_expr(models.GroupMembership, models.ProductVariant)

    grouped = (
        db.query(
            group_key.label("group_id"),
            func.max(models.Product.title).label("product_title"),
            func.max(models.ProductVariant.title).label("variant_title"),
            func.max(models.ProductVariant.sku).label("sku"),
            func.max(models.ProductVariant.barcode).label("barcode"),
            func.sum(func.coalesce(models.InventoryLevel.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(models.InventoryLevel.available, 0)).label("available"),
            func.max(models.ProductVariant.cost_per_item).label("max_cost"),
            func.max(models.ProductVariant.price).label("max_price"),
        )
        .outerjoin(models.GroupMembership, models.GroupMembership.variant_id == models.ProductVariant.id)
        .join(models.InventoryLevel, models.InventoryLevel.inventory_item_id == models.ProductVariant.inventory_item_id)
        .join(models.Location, models.Location.id == models.InventoryLevel.location_id)
        .outerjoin(models.Product, models.Product.id == models.ProductVariant.product_id)
    )

    where_clauses = _filters_where(filters, models.Location, models.ProductVariant)
    if (filters or {}).get("search"):
        _apply_search(filters, where_clauses, models.Product, models.ProductVariant, models.GroupMembership)

    if where_clauses:
        grouped = grouped.filter(and_(*where_clauses))

    grouped = grouped.group_by(group_key)
    return grouped


def _build_variant_aggregate_query(db: Session, filters: Dict[str, Any]):
    """
    Aggregates at variant-level (for `view=individual`) while still joining
    inventory levels and product for display.
    """
    q, Variant, GM, IL, Loc, Product = _base_join(db)

    where_clauses = _filters_where(filters, Loc, Variant)
    _apply_search(filters, where_clauses, Product, Variant, GM)

    if where_clauses:
        q = q.filter(and_(*where_clauses))

    variant_rows = (
        db.query(
            Variant.id.label("variant_id"),
            Variant.product_id.label("product_id"),
            Variant.store_id.label("store_id"),
            func.coalesce(GM.group_id, Variant.barcode_normalized).label("group_id"),
            Product.title.label("product_title"),
            Variant.title.label("variant_title"),
            Variant.sku.label("sku"),
            Variant.barcode.label("barcode"),
            func.coalesce(Variant.cost_per_item, 0.0).label("cost_per_item"),
            func.coalesce(Variant.price, 0.0).label("price"),
            func.sum(func.coalesce(IL.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(IL.available, 0)).label("available"),
        )
        .outerjoin(models.GroupMembership, models.GroupMembership.variant_id == Variant.id)
        .join(models.InventoryLevel, models.InventoryLevel.inventory_item_id == Variant.inventory_item_id)
        .join(models.Location, models.Location.id == models.InventoryLevel.location_id)
        .outerjoin(models.Product, models.Product.id == Variant.product_id)
        .group_by(
            Variant.id,
            Variant.product_id,
            Variant.store_id,
            models.GroupMembership.group_id,
            Variant.barcode_normalized,
            Product.title,
            Variant.title,
            Variant.sku,
            Variant.barcode,
            Variant.cost_per_item,
            Variant.price,
        )
    )

    return variant_rows


def _map_sort(sort_by: str, sort_order: str, columns: Dict[str, Any]):
    """
    Returns a SQLAlchemy ORDER BY expression based on the requested column.
    Supports 'on_hand', 'available', 'inventory_value', 'retail_value', 'price', 'cost'.
    """
    dir_desc = (sort_order or "").lower() == "desc"
    key = (sort_by or "").lower()

    # Accept a few aliases
    if key == "cost":
        key = "cost_per_item"

    col = columns.get(key)
    if col is None:
        col = columns.get("on_hand")

    # Defensive fallback in case nothing matched
    if col is None:
        # arbitrary stable fallback: first value
        col = next(iter(columns.values()))

    return col.desc() if dir_desc else col.asc()


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

    - rows:
        view='grouped'    -> one row per barcode group (deduped)
        view='individual' -> one row per variant (aggregated across its locations)
    - totals: ALWAYS deduped by barcode group:
        SUM(on_hand) and SUM(available) across groups, and
        SUM(on_hand * MAX(cost_per_item)) and SUM(on_hand * MAX(price))
    """
    filters = {
        "search": search,
        "store_ids": store_ids,
    }

    # ---------- Totals (DEDUPED by group) ----------
    grouped = _build_group_aggregate_query(db, filters)
    g_subq = grouped.subquery()

    totals_query = db.query(
        func.coalesce(func.sum(g_subq.c.on_hand), 0).label("on_hand"),
        func.coalesce(func.sum(g_subq.c.available), 0).label("available"),
        func.coalesce(func.sum(g_subq.c.on_hand * func.coalesce(g_subq.c.max_cost, 0.0)), 0.0).label("inventory_value"),
        func.coalesce(func.sum(g_subq.c.on_hand * func.coalesce(g_subq.c.max_price, 0.0)), 0.0).label("retail_value"),
    )

    totals_row = totals_query.one()
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
        # build grouped rows with computed values per group
        columns = {
            "group_id": g_subq.c.group_id,
            "product_title": g_subq.c.product_title,
            "variant_title": g_subq.c.variant_title,
            "sku": g_subq.c.sku,
            "barcode": g_subq.c.barcode,
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
            g_subq.c.on_hand,
            g_subq.c.available,
            g_subq.c.max_cost,
            g_subq.c.max_price,
            (g_subq.c.on_hand * func.coalesce(g_subq.c.max_cost, 0.0)).label("inventory_value"),
            (g_subq.c.on_hand * func.coalesce(g_subq.c.max_price, 0.0)).label("retail_value"),
        ).select_from(g_subq)

        # Apply sort
        sort_expr = _map_sort(sort_by, sort_order, columns)
        q = q.order_by(sort_expr)

        # Pagination
        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        for rec in q.all():
            rows.append({
                "group_id": rec.group_id,
                "product_title": rec.product_title,
                "variant_title": rec.variant_title,
                "sku": rec.sku,
                "barcode": rec.barcode,
                "on_hand": int(rec.on_hand or 0),
                "available": int(rec.available or 0),
                "max_cost": float(rec.max_cost or 0.0),
                "max_price": float(rec.max_price or 0.0),
                "inventory_value": float(rec.inventory_value or 0.0),
                "retail_value": float(rec.retail_value or 0.0),
            })

    else:
        # view='individual' (default): one row per variant, aggregate its levels
        vq = _build_variant_aggregate_query(db, filters)
        v_subq = vq.subquery()

        # columns inc. computed values for sorting
        columns = {
            "variant_id": v_subq.c.variant_id,
            "group_id": v_subq.c.group_id,
            "product_title": v_subq.c.product_title,
            "variant_title": v_subq.c.variant_title,
            "sku": v_subq.c.sku,
            "barcode": v_subq.c.barcode,
            "on_hand": v_subq.c.on_hand,
            "available": v_subq.c.available,
            "cost_per_item": v_subq.c.cost_per_item,
            "price": v_subq.c.price,
            "inventory_value": v_subq.c.on_hand * v_subq.c.cost_per_item,
            "retail_value": v_subq.c.on_hand * v_subq.c.price,
        }

        # count variants
        count_q = db.query(func.count(literal(1))).select_from(v_subq)
        total_count = count_q.scalar() or 0

        # sorting
        sort_expr = _map_sort(sort_by, sort_order, columns)

        q = db.query(
            v_subq.c.variant_id,
            v_subq.c.group_id,
            v_subq.c.product_title,
            v_subq.c.variant_title,
            v_subq.c.sku,
            v_subq.c.barcode,
            v_subq.c.on_hand,
            v_subq.c.available,
            v_subq.c.cost_per_item,
            v_subq.c.price,
            (v_subq.c.on_hand * v_subq.c.cost_per_item).label("inventory_value"),
            (v_subq.c.on_hand * v_subq.c.price).label("retail_value"),
        ).select_from(v_subq).order_by(sort_expr)

        # pagination
        if skip:
            q = q.offset(skip)
        if limit:
            q = q.limit(limit)

        for rec in q.all():
            rows.append({
                "variant_id": rec.variant_id,
                "group_id": rec.group_id,
                "product_title": rec.product_title,
                "variant_title": rec.variant_title,
                "sku": rec.sku,
                "barcode": rec.barcode,
                "on_hand": int(rec.on_hand or 0),
                "available": int(rec.available or 0),
                "cost_per_item": float(rec.cost_per_item or 0.0),
                "price": float(rec.price or 0.0),
                "inventory_value": float(rec.inventory_value or 0.0),
                "retail_value": float(rec.retail_value or 0.0),
            })

    return rows, totals, int(total_count)
