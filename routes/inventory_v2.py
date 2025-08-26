# routes/inventory_v2.py
from __future__ import annotations

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_, literal, func, cast, Date

# Prefer the unified session helper if both exist
try:
    from session import get_db  # type: ignore
except Exception:
    from database import get_db  # type: ignore

import models
from crud import inventory_report as crud_inventory_report

# Reuse legacy helpers if present
try:
    from crud import inventory_v2 as crud_inventory_v2  # type: ignore
except Exception:  # pragma: no cover
    crud_inventory_v2 = None  # type: ignore

router = APIRouter(prefix="/api/v2/inventory", tags=["inventory_v2"])
pages = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory="templates")


# ---------- helpers ----------
def _parse_int_list_csv(value: Optional[str]) -> Optional[List[int]]:
    if not value:
        return None
    out: List[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return list(sorted(set(out))) or None


def _parse_str_list_csv(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    out: List[str] = []
    for part in value.split(","):
        part = part.strip()
        if part:
            out.append(part)
    return list(sorted(set(out))) or None


def _group_expr(GM, Variant):
    # Same grouping logic the report uses
    return func.coalesce(
        GM.group_id,
        Variant.barcode_normalized,
        Variant.barcode,
        literal("UNKNOWN"),
    )


# ---------- Filters ----------
@router.get("/filters/")
def get_filters(db: Session = Depends(get_db)):
    """
    Returns filter data for the Inventory Report UI.
    If a legacy helper exists in crud.inventory_v2, reuse it; otherwise compute here.
    """
    if crud_inventory_v2 and hasattr(crud_inventory_v2, "get_filters"):
        return crud_inventory_v2.get_filters(db)  # type: ignore

    # Inline fallback
    stores = [
        {"id": s.id, "name": s.name}
        for s in db.query(models.Store).order_by(models.Store.name.asc()).all()
    ]

    product_types = [
        r[0]
        for r in db.query(models.Product.product_type)
        .filter(models.Product.product_type.isnot(None))
        .distinct()
        .order_by(models.Product.product_type.asc())
        .all()
    ]

    statuses = [
        r[0]
        for r in db.query(models.Product.status)
        .filter(models.Product.status.isnot(None))
        .distinct()
        .order_by(models.Product.status.asc())
        .all()
    ]

    return {"stores": stores, "types": product_types, "statuses": statuses}


# ---------- Report ----------
@router.get("/report/")
def get_report(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=0, le=500),
    sort_by: str = Query("on_hand"),
    sort_order: str = Query("desc"),
    view: str = Query("individual"),
    search: Optional[str] = Query(None),
    # accept both singular and plural params for compatibility with the JS
    store: Optional[int] = Query(None, description="Single store id"),
    stores: Optional[str] = Query(None, description="Comma-separated store ids e.g. '1,2,3'"),
    status: Optional[str] = Query(None, description="Single product status"),
    statuses: Optional[str] = Query(None, description="Comma-separated product statuses"),
    type: Optional[str] = Query(None, description="Single product type"),
    types: Optional[str] = Query(None, description="Comma-separated product types"),
    totals_mode: str = Query("grouped"),
):
    """
    Inventory report data source for the Inventory Report UI.
    Totals are always computed on grouped-deduped data.
    """
    v = (view or "").lower()
    if v not in ("individual", "grouped"):
        raise HTTPException(status_code=400, detail="view must be 'individual' or 'grouped'")
    if (totals_mode or "").lower() != "grouped":
        raise HTTPException(status_code=400, detail="totals_mode must be 'grouped'")

    # parse stores (accept 'store' and 'stores')
    store_list = []
    if store is not None:
        try:
            store_list.append(int(store))
        except ValueError:
            pass
    store_list += _parse_int_list_csv(stores) or []
    store_list = list(sorted(set(store_list))) or None

    # parse statuses/types
    status_list = _parse_str_list_csv(statuses) or []
    if status:
        status_list.append(status.strip())
    status_list = list(sorted(set(status_list))) or None

    type_list = _parse_str_list_csv(types) or []
    if type:
        type_list.append(type.strip())
    type_list = list(sorted(set(type_list))) or None

    rows, totals, total_count = crud_inventory_report.get_inventory_report(
        db,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
        view=v,
        search=search,
        store_ids=store_list,
        statuses=status_list,
        product_types=type_list,
        totals_mode="grouped",
    )

    if rows is None:
        rows = []

    return {
        "inventory": rows,
        "total_count": total_count,
        "total_retail_value": totals.get("retail_value", 0.0),
        "total_inventory_value": totals.get("inventory_value", 0.0),
        "total_on_hand": totals.get("on_hand", 0),
    }


# ---------- Product Details (legacy/modal) ----------
@router.get("/product-details/{barcode_or_group}")
def get_product_details(
    barcode_or_group: str,
    db: Session = Depends(get_db),
):
    """
    Returns committed orders, all orders (last 200), and stock movements for the group key,
    which can be: GroupMembership.group_id OR normalized barcode OR barcode (or SKU fallback).
    """
    if not barcode_or_group:
        raise HTTPException(status_code=400, detail="group key is required")

    Variant = models.ProductVariant
    GM = models.GroupMembership
    Order = models.Order
    LineItem = models.LineItem
    Store = models.Store
    StockMovement = getattr(models, "StockMovement", None)

    # Grouping key (same as report)
    group_key = func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode)

    # Variants in this group (across stores)
    variants_q = (
        db.query(
            Variant.id.label("variant_id"),
            Variant.sku.label("sku"),
            Variant.store_id.label("store_id"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .filter(group_key == barcode_or_group)
        .all()
    )
    if not variants_q:
        # Fallback: direct match by barcode/normalized/sku
        variants_q = (
            db.query(
                Variant.id.label("variant_id"),
                Variant.sku.label("sku"),
                Variant.store_id.label("store_id"),
            )
            .filter(
                or_(
                    Variant.barcode == barcode_or_group,
                    Variant.barcode_normalized == barcode_or_group,
                    Variant.sku == barcode_or_group,
                )
            )
            .all()
        )
        if not variants_q:
            return {"committed_orders": [], "all_orders": [], "stock_movements": []}

    variant_ids = [int(v.variant_id) for v in variants_q]
    skus = [v.sku for v in variants_q if v.sku]

    base_ol = (
        db.query(
            Order.id.label("order_id"),
            Order.name.label("name"),
            Order.created_at.label("created_at"),
            Order.financial_status.label("financial_status"),
            Order.fulfillment_status.label("fulfillment_status"),
            LineItem.quantity.label("quantity"),
            Store.shopify_url.label("shopify_url"),
        )
        .join(LineItem, LineItem.order_id == Order.id)
        .join(Store, Store.id == Order.store_id)
        .filter(LineItem.variant_id.in_(variant_ids))
    )

    # committed = open/unfulfilled
    committed_q = (
        base_ol.filter(
            Order.cancelled_at.is_(None),
            or_(Order.fulfillment_status.is_(None), Order.fulfillment_status.in_(["partial", "unfulfilled"])),
        )
        .order_by(Order.created_at.desc())
        .limit(200)
        .all()
    )
    committed_orders = [
        {
            "id": int(r.order_id),
            "name": r.name,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "quantity": int(r.quantity or 0),
            "fulfillment_status": r.fulfillment_status,
            "shopify_url": r.shopify_url,
        }
        for r in committed_q
    ]

    all_orders_q = base_ol.order_by(Order.created_at.desc()).limit(200).all()
    all_orders = [
        {
            "id": int(r.order_id),
            "name": r.name,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "quantity": int(r.quantity or 0),
            "financial_status": r.financial_status,
            "fulfillment_status": r.fulfillment_status,
        }
        for r in all_orders_q
    ]

    # Stock movements (if table exists)
    stock_movements: List[Dict[str, Any]] = []
    if StockMovement and skus:
        sm_q = (
            db.query(
                StockMovement.created_at,
                StockMovement.product_sku,
                StockMovement.change_quantity,
                StockMovement.new_quantity,
                StockMovement.reason,
                StockMovement.source_info,
            )
            .filter(StockMovement.product_sku.in_(skus))
            .order_by(StockMovement.created_at.desc())
            .limit(200)
            .all()
        )
        stock_movements = [
            {
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "product_sku": r.product_sku,
                "change_quantity": int(r.change_quantity or 0),
                "new_quantity": int(r.new_quantity or 0),
                "reason": r.reason,
                "source_info": r.source_info,
            }
            for r in sm_q
        ]

    return {"committed_orders": committed_orders, "all_orders": all_orders, "stock_movements": stock_movements}


# ---------- Product Analytics (for the dedicated page) ----------
@router.get("/product-analytics/{group_key}")
def product_analytics(
    group_key: str,
    db: Session = Depends(get_db),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    stores: Optional[str] = Query(None, description="Comma-separated store ids"),
):
    """
    Compute analytics for a product group identified by the same group key used in the report:
      GroupMembership.group_id -> Variant.barcode_normalized -> Variant.barcode
    """
    if not group_key:
        raise HTTPException(status_code=400, detail="group_key is required")

    Variant = models.ProductVariant
    GM = models.GroupMembership
    Product = models.Product
    Store = models.Store
    InventoryLevel = models.InventoryLevel
    Location = models.Location
    Order = models.Order
    LineItem = models.LineItem
    StockMovement = getattr(models, "StockMovement", None)
    InventorySnapshot = getattr(models, "InventorySnapshot", None)

    # Resolve date range (default last 90 days)
    today = datetime.utcnow().date()
    end_d = end or today
    start_d = start or (end_d - timedelta(days=89))  # inclusive 90 days window
    if start_d > end_d:
        start_d, end_d = end_d, start_d

    store_ids = _parse_int_list_csv(stores)

    # Group expression
    group_expr = func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode)

    # Member variants (+ primary/title/image/store)
    members_q = (
        db.query(
            Variant.id.label("variant_id"),
            Variant.sku.label("sku"),
            Variant.is_primary_variant.label("is_primary"),
            Store.id.label("store_id"),
            Store.name.label("store_name"),
            Product.title.label("product_title"),
            Product.image_url.label("image_url"),
            Product.created_at.label("product_created_at"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Variant.store_id)
        .filter(group_expr == group_key)
    )
    if store_ids:
        members_q = members_q.filter(Store.id.in_(store_ids))
    members = members_q.all()
    if not members:
        # Fallback: try direct barcode/sku
        members = (
            db.query(
                Variant.id.label("variant_id"),
                Variant.sku.label("sku"),
                Variant.is_primary_variant.label("is_primary"),
                Store.id.label("store_id"),
                Store.name.label("store_name"),
                Product.title.label("product_title"),
                Product.image_url.label("image_url"),
                Product.created_at.label("product_created_at"),
            )
            .join(Product, Product.id == Variant.product_id)
            .join(Store, Store.id == Variant.store_id)
            .filter(
                or_(
                    Variant.barcode == group_key,
                    Variant.barcode_normalized == group_key,
                    Variant.sku == group_key,
                )
            )
            .all()
        )

    if not members:
        # Nothing found at all
        payload = {
            "header": {"group_key": group_key, "members": []},
            "inventory_snapshot": {"on_hand": 0, "available": 0, "committed": 0},
            "sales_by_day": [],
            "sales_by_month": [],
            "stock_movements_by_day": [],
            "stock_evolution": [],
            "metrics": {},
        }
        return payload

    # Choose primary
    primary = next((m for m in members if m.is_primary), members[0])

    variant_ids = [int(m.variant_id) for m in members]
    skus = [m.sku for m in members if m.sku]

    # Inventory (dedup across stores): sum per store then MIN across stores
    inv_q = (
        db.query(
            Location.store_id.label("store_id"),
            func.sum(func.coalesce(InventoryLevel.on_hand, 0)).label("on_hand"),
            func.sum(func.coalesce(InventoryLevel.available, 0)).label("available"),
        )
        .join(Variant, Variant.inventory_item_id == InventoryLevel.inventory_item_id)
        .join(Location, Location.id == InventoryLevel.location_id)
        .filter(Variant.id.in_(variant_ids))
    )
    if store_ids:
        inv_q = inv_q.filter(Location.store_id.in_(store_ids))
    inv_per_store = inv_q.group_by(Location.store_id).all()

    if inv_per_store:
        on_hand_min = min(int(r.on_hand or 0) for r in inv_per_store)
        available_min = min(int(r.available or 0) for r in inv_per_store)
    else:
        on_hand_min = 0
        available_min = 0
    committed_min = on_hand_min - available_min

    # Sales by day (exclude cancelled)
    od = cast(func.date_trunc("day", Order.created_at), Date)
    sales_q = (
        db.query(
            od.label("day"),
            func.sum(LineItem.quantity).label("units"),
            func.count(func.distinct(Order.id)).label("orders"),
        )
        .join(LineItem, LineItem.order_id == Order.id)
        .filter(
            LineItem.variant_id.in_(variant_ids),
            Order.cancelled_at.is_(None),
            Order.created_at >= datetime.combine(start_d, datetime.min.time()),
            Order.created_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
    )
    if store_ids:
        sales_q = sales_q.filter(Order.store_id.in_(store_ids))
    sales_q = sales_q.group_by(od).order_by(od.asc()).all()
    sales_by_day = [
        {"day": r.day.isoformat(), "units": int(r.units or 0), "orders": int(r.orders or 0)}
        for r in sales_q
    ]
    total_units_period = sum(x["units"] for x in sales_by_day)
    period_days = (end_d - start_d).days + 1
    avg_daily_sales = (total_units_period / period_days) if period_days > 0 else 0.0
    avg_monthly_sales = avg_daily_sales * 30.0

    # Quick-window velocities (7/30/90 relative to end date)
    def _window_units(days: int) -> float:
        w_start = end_d - timedelta(days=days - 1)
        units = sum(
            x["units"]
            for x in sales_by_day
            if w_start <= datetime.fromisoformat(x["day"]).date() <= end_d
        )
        return units / float(days) if days > 0 else 0.0

    velocity_7 = _window_units(7)
    velocity_30 = _window_units(30)
    velocity_90 = _window_units(90)
    days_of_cover_30 = (available_min / velocity_30) if velocity_30 > 0 else None

    # Month totals (group by month)
    om = cast(func.date_trunc("month", Order.created_at), Date)
    month_q = (
        db.query(
            om.label("month"),
            func.sum(LineItem.quantity).label("units"),
        )
        .join(LineItem, LineItem.order_id == Order.id)
        .filter(
            LineItem.variant_id.in_(variant_ids),
            Order.cancelled_at.is_(None),
            Order.created_at >= datetime.combine(start_d, datetime.min.time()),
            Order.created_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
    )
    if store_ids:
        month_q = month_q.filter(Order.store_id.in_(store_ids))
    month_q = month_q.group_by(om).order_by(om.asc()).all()
    sales_by_month = [{"month": r.month.isoformat(), "units": int(r.units or 0)} for r in month_q]

    # Weekday averages (0=Mon..6=Sun)
    ow = func.extract("dow", Order.created_at)
    wday_q = (
        db.query(
            ow.label("dow"),
            func.sum(LineItem.quantity).label("units"),
        )
        .join(LineItem, LineItem.order_id == Order.id)
        .filter(
            LineItem.variant_id.in_(variant_ids),
            Order.cancelled_at.is_(None),
            Order.created_at >= datetime.combine(start_d, datetime.min.time()),
            Order.created_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
        .group_by(ow)
        .order_by(ow.asc())
        .all()
    )
    total_weeks = max(1, period_days // 7)
    avg_by_weekday = [
        {"dow": int(r.dow), "avg_units": float(r.units or 0) / float(total_weeks)} for r in wday_q
    ]

    # Life on shelf
    first_product_date = (
        db.query(func.min(Product.created_at))
        .join(Variant, Variant.product_id == Product.id)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .filter(group_expr == group_key)
        .scalar()
    )
    first_order_date = (
        db.query(func.min(Order.created_at))
        .join(LineItem, LineItem.order_id == Order.id)
        .filter(LineItem.variant_id.in_(variant_ids))
        .scalar()
    )
    first_inv_date = (
        db.query(func.min(InventoryLevel.updated_at))
        .join(Variant, Variant.inventory_item_id == InventoryLevel.inventory_item_id)
        .filter(Variant.id.in_(variant_ids))
        .scalar()
    )
    first_seen = first_product_date or first_order_date or first_inv_date
    life_on_shelf_days = None
    if first_seen:
        first_seen_d = first_seen.date() if hasattr(first_seen, "date") else first_seen
        life_on_shelf_days = (today - first_seen_d).days

    # Stock movements by day (optional)
    stock_movements_by_day: List[Dict[str, Any]] = []
    if StockMovement and skus:
        smd = cast(func.date_trunc("day", StockMovement.created_at), Date)
        sm_q = (
            db.query(
                smd.label("day"),
                func.sum(StockMovement.change_quantity).label("change"),
            )
            .filter(
                StockMovement.product_sku.in_(skus),
                StockMovement.created_at >= datetime.combine(start_d, datetime.min.time()),
                StockMovement.created_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
            )
            .group_by(smd)
            .order_by(smd.asc())
            .all()
        )
        stock_movements_by_day = [
            {"day": r.day.isoformat(), "change": int(r.change or 0)} for r in sm_q
        ]
    
    # Stock Evolution from Snapshots
    stock_evolution_data = []
    if InventorySnapshot:
        snapshot_query = db.query(
            InventorySnapshot.date,
            func.sum(InventorySnapshot.on_hand).label("total_on_hand")
        ).filter(
            InventorySnapshot.product_variant_id.in_(variant_ids),
            InventorySnapshot.date >= start_d,
            InventorySnapshot.date <= end_d
        )
        if store_ids:
            snapshot_query = snapshot_query.filter(InventorySnapshot.store_id.in_(store_ids))

        daily_snapshots = snapshot_query.group_by(InventorySnapshot.date).order_by(InventorySnapshot.date.asc()).all()

        stock_evolution_data = [
            {"date": row.date.isoformat(), "on_hand": int(row.total_on_hand)}
            for row in daily_snapshots
        ]


    payload = {
        "header": {
            "group_key": group_key,
            "title": getattr(primary, "product_title", None),
            "image_url": getattr(primary, "image_url", None),
            "members": [
                {
                    "variant_id": int(m.variant_id),
                    "sku": m.sku,
                    "store_id": int(m.store_id),
                    "store_name": m.store_name,
                    "is_primary": bool(m.is_primary),
                }
                for m in members
            ],
        },
        "inventory_snapshot": {
            "on_hand": on_hand_min,
            "available": available_min,
            "committed": committed_min,
        },
        "sales_by_day": sales_by_day,
        "sales_by_month": sales_by_month,
        "stock_movements_by_day": stock_movements_by_day,
        "stock_evolution": stock_evolution_data,
        "metrics": {
            "period_days": period_days,
            "total_units_period": int(total_units_period),
            "avg_daily_sales": float(round(avg_daily_sales, 4)),
            "avg_monthly_sales": float(round(avg_monthly_sales, 4)),
            "velocity_7": float(round(velocity_7, 4)),
            "velocity_30": float(round(velocity_30, 4)),
            "velocity_90": float(round(velocity_90, 4)),
            "days_of_cover_30": float(days_of_cover_30) if days_of_cover_30 is not None else None,
            "life_on_shelf_days": int(life_on_shelf_days) if life_on_shelf_days is not None else None,
            "first_seen": first_seen.isoformat() if first_seen else None,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "avg_by_weekday": avg_by_weekday,
        },
    }
    return payload


# ---------- Make Primary ----------
class MakePrimaryPayload(BaseModel):
    barcode: Optional[str] = None
    variant_id: int


@router.post("/set-primary")
@router.post("/make-primary")  # alias for compatibility
def make_primary_variant(payload: MakePrimaryPayload, db: Session = Depends(get_db)):
    """
    Sets `is_primary_variant=True` on the selected variant, and False on all other
    variants in the same barcode group (by GroupMembership/group key).
    Fix: use explicit select_from(ProductVariant) when resolving group to avoid ambiguous joins.
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership

    # Find selected variant
    variant = db.query(Variant).filter(Variant.id == payload.variant_id).first()
    if not variant:
        raise HTTPException(status_code=404, detail="Variant not found")

    # If payload.barcode is empty/UNKNOWN, derive it from the variant using the same coalesce chain
    derive_key = (not payload.barcode) or (payload.barcode.strip().upper() == "UNKNOWN")
    if derive_key:
        group_key = (
            db.query(
                func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode)
            )
            .select_from(Variant)
            .outerjoin(GM, GM.variant_id == Variant.id)
            .filter(Variant.id == variant.id)
            .scalar()
        )
    else:
        group_key = payload.barcode

    if not group_key:
        raise HTTPException(status_code=404, detail="Group not found for selected variant")

    # All variants in this group
    group_variants = (
        db.query(Variant)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .filter(func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode) == group_key)
        .all()
    )
    if not group_variants:
        raise HTTPException(status_code=404, detail="No variants found in the resolved group")

    # Update flags
    for v in group_variants:
        v.is_primary_variant = (v.id == variant.id)

    db.commit()
    return {"ok": True, "group": group_key, "primary_variant_id": variant.id, "count": len(group_variants)}


# ---------- Details (legacy compatibility) ----------
@router.get("/details/")
def get_details(
    barcode: str = Query(..., description="Exact barcode or normalized barcode"),
    db: Session = Depends(get_db),
):
    """
    Backward-compatible endpoint. Delegates to /product-details/{barcode}.
    """
    if not barcode:
        raise HTTPException(status_code=400, detail="barcode is required")
    return get_product_details(barcode_or_group=barcode, db=db)


# ---------- Pages (inventory + product details) ----------
@pages.get("/inventory")
def inventory_page(request: Request):
    return templates.TemplateResponse("inventory.html", {"request": request})


@pages.get("/inventory/product/{group_key}")
def product_details_page(request: Request, group_key: str):
    return templates.TemplateResponse("product_details.html", {"request": request, "group_key": group_key})