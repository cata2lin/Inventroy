# routes/inventory_v2.py
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_, literal, func

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


# ---------- Product Details ----------
@router.get("/product-details/{barcode}")
def get_product_details(barcode: str, db: Session = Depends(get_db)):
    """
    Returns committed orders, all orders (last 100), and stock movements for the group identified by `barcode`.
    """
    if not barcode:
        raise HTTPException(status_code=400, detail="barcode is required")

    Variant = models.ProductVariant
    GM = models.GroupMembership
    Order = models.Order
    LineItem = models.LineItem
    Store = models.Store

    # Group key
    group_key = func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode)

    # Variants in this group (across stores)
    variants_q = (
        db.query(Variant.id.label("variant_id"), Variant.sku.label("sku"), Variant.store_id.label("store_id"))
        .outerjoin(GM, GM.variant_id == Variant.id)
        .filter(group_key == barcode)
        .all()
    )
    if not variants_q:
        variants_q = (
            db.query(Variant.id.label("variant_id"), Variant.sku.label("sku"), Variant.store_id.label("store_id"))
            .filter(or_(Variant.barcode == barcode, Variant.barcode_normalized == barcode))
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
        .limit(100)
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

    all_orders_q = base_ol.order_by(Order.created_at.desc()).limit(100).all()
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

    # Stock movements (optional model)
    stock_movements: List[Dict[str, Any]] = []
    StockMovement = getattr(models, "StockMovement", None)
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
            .limit(100)
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


# ---------- Make Primary ----------
class MakePrimaryPayload(BaseModel):
    barcode: str
    variant_id: int


@router.post("/set-primary")
@router.post("/make-primary")  # alias for compatibility
def make_primary_variant(payload: MakePrimaryPayload, db: Session = Depends(get_db)):
    """
    Sets `is_primary_variant=True` on the selected variant, and False on all other
    variants in the same barcode group (by GroupMembership/group key).
    """
    Variant = models.ProductVariant
    GM = models.GroupMembership

    # Find selected variant
    variant = db.query(Variant).filter(Variant.id == payload.variant_id).first()
    if not variant:
        raise HTTPException(status_code=404, detail="Variant not found")

    # Determine group key from payload.barcode OR existing membership
    group_key = db.query(
        func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode)
    ).outerjoin(GM, GM.variant_id == Variant.id).filter(Variant.id == variant.id).scalar()

    if not group_key:
        # fallback to provided barcode
        group_key = payload.barcode

    # All variants in this group
    group_variants = (
        db.query(Variant)
        .outerjoin(GM, GM.variant_id == Variant.id)
        .filter(func.coalesce(GM.group_id, Variant.barcode_normalized, Variant.barcode) == group_key)
        .all()
    )
    if not group_variants:
        raise HTTPException(status_code=404, detail="Group not found")

    # Update flags
    for v in group_variants:
        v.is_primary_variant = (v.id == variant.id)

    db.commit()
    return {"ok": True, "group": group_key, "primary_variant_id": variant.id}


# ---------- Details (legacy compatibility) ----------
@router.get("/details/")
def get_details(barcode: str = Query(..., description="Exact barcode or normalized barcode"), db: Session = Depends(get_db)):
    """
    Backward-compatible endpoint. Delegates to /product-details/{barcode}.
    """
    if not barcode:
        raise HTTPException(status_code=400, detail="barcode is required")
    return get_product_details(barcode=barcode, db=db)
