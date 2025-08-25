# routes/inventory_v2.py

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import or_, literal  # used in details fallback

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
def get_filters(
    db: Session = Depends(get_db),
):
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

    return {
        "stores": stores,
        "types": product_types,
        "statuses": statuses,
    }


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
    stores: Optional[str] = Query(
        None, description="Comma-separated store ids: e.g., '1,2,3'"
    ),
    statuses: Optional[str] = Query(
        None, description="Comma-separated product statuses"
    ),
    types: Optional[str] = Query(
        None, description="Comma-separated product types"
    ),
    totals_mode: str = Query("grouped"),
):
    """
    Inventory report data source for the Inventory Report UI.

    - view='grouped': one row per barcode group (deduped across stores)
    - view='individual': one row per variant (summing its own per-location levels)
    - totals ALWAYS come from grouped-deduped logic so we don't triple-count
      products that exist in multiple stores.
    """
    # Simple validation for view/totals_mode
    v = (view or "").lower()
    if v not in ("individual", "grouped"):
        raise HTTPException(status_code=400, detail="view must be 'individual' or 'grouped'")

    if (totals_mode or "").lower() != "grouped":
        raise HTTPException(status_code=400, detail="totals_mode must be 'grouped'")

    # parse store ids
    store_list: Optional[List[int]] = None
    if stores:
        tmp: List[int] = []
        for s in (stores or "").split(","):
            s = s.strip()
            if not s:
                continue
            try:
                tmp.append(int(s))
            except ValueError:
                continue
        store_list = list(sorted(set(tmp))) or None

    # parse statuses/types
    status_list: Optional[List[str]] = None
    if statuses:
        status_list = list(sorted(set([s.strip() for s in statuses.split(",") if s.strip()])))

    type_list: Optional[List[str]] = None
    if types:
        type_list = list(sorted(set([t.strip() for t in types.split(",") if t.strip()])))

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

    # Shape response exactly as the frontend expects
    # Ensure arrays are always present (avoids `.map` on undefined)
    if rows is None:
        rows = []

    return {
        "inventory": rows,
        "total_count": total_count,
        "total_retail_value": totals.get("retail_value", 0.0),
        "total_inventory_value": totals.get("inventory_value", 0.0),
        "total_on_hand": totals.get("on_hand", 0),
    }


# ---------- Details (used by the modal in the UI) ----------
@router.get("/details/")
def get_details(
    barcode: str = Query(..., description="Exact barcode or normalized barcode"),
    db: Session = Depends(get_db),
):
    """
    Details for a given barcode/group used by the report's details modal.
    If a legacy helper exists, reuse it. Otherwise, return a minimal, safe payload.
    """
    if not barcode:
        raise HTTPException(status_code=400, detail="barcode is required")

    if crud_inventory_v2 and hasattr(crud_inventory_v2, "get_inventory_details"):
        return crud_inventory_v2.get_inventory_details(db, barcode=barcode)  # type: ignore

    Variant = models.ProductVariant
    GM = models.GroupMembership
    Product = models.Product
    Store = models.Store

    # Find group id (if any) by barcode / normalized barcode
    group_id_q = (
        db.query(GM.group_id)
        .join(Variant, Variant.id == GM.variant_id)
        .filter(
            or_(
                Variant.barcode == barcode,
                Variant.barcode_normalized == barcode,
            )
        )
        .distinct()
        .first()
    )
    group_id = group_id_q[0] if group_id_q else None

    # Member variants (across stores)
    members_q = (
        db.query(
            Variant.id,
            Variant.sku,
            Variant.barcode,
            Variant.barcode_normalized,
            Variant.title,
            Product.title.label("product_title"),
            Store.name.label("store_name"),
            Product.status.label("status"),
            Product.product_type.label("product_type"),
        )
        .outerjoin(GM, GM.variant_id == Variant.id)
        .join(Product, Product.id == Variant.product_id)
        .join(Store, Store.id == Variant.store_id)
        .filter(
            or_(
                Variant.barcode == barcode,
                Variant.barcode_normalized == barcode,
                (GM.group_id == group_id) if group_id is not None else literal(False),
            )
        )
        .all()
    )

    members = [
        {
            "variant_id": v.id,
            "sku": v.sku,
            "barcode": v.barcode,
            "barcode_normalized": v.barcode_normalized,
            "variant_title": v.title,
            "product_title": v.product_title,
            "store_name": v.store_name,
            "status": v.status,
            "product_type": v.product_type,
        }
        for v in members_q
    ]

    return {
        "barcode": barcode,
        "group_id": group_id,
        "members": members,
    }
