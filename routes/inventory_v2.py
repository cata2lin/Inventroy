# routes/inventory_v2.py

from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from database import get_db
from crud.inventory_report import get_inventory_report

router = APIRouter(
    prefix="/api/v2/inventory",
    tags=["Inventory V2"],
    responses={404: {"description": "Not found"}},
)


def _parse_store_ids(store_ids: Optional[str]) -> Optional[List[int]]:
    """
    Accepts comma-separated string (e.g., "1,2,3") -> List[int]
    """
    if not store_ids:
        return None
    parts = [p.strip() for p in store_ids.split(",")]
    out = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue
    return out or None


@router.get("/report/", summary="Inventory report with deduped totals by barcode group")
def inventory_report(
    skip: int = 0,
    limit: int = 50,
    sort_by: str = "on_hand",
    sort_order: str = "desc",
    view: str = "individual",  # 'individual' or 'grouped'
    search: Optional[str] = None,
    store_ids: Optional[str] = Query(None, description="Comma-separated store ids"),
    totals_mode: str = "grouped",  # 'grouped' (deduped) or 'raw' (not recommended)
    db: Session = Depends(get_db),
):
    """
    Returns:
      {
        "rows": [...],
        "totals": {
          "on_hand": int,
          "available": int,
          "inventory_value": float,
          "retail_value": float,
          "mode": "grouped"
        },
        "pagination": {"skip": int, "limit": int, "total": int}
      }

    Notes:
      - Totals are always deduped by barcode group (MAX cost/price per group * group on_hand).
      - Rows:
          view='grouped' -> one row per barcode group
          view='individual' -> one row per variant (summed across locations)
    """
    store_list = _parse_store_ids(store_ids)
    rows, totals, total_count = get_inventory_report(
        db,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
        view=view,
        search=search,
        store_ids=store_list,
        totals_mode=totals_mode,
    )

    return {
        "rows": rows,
        "totals": totals,
        "pagination": {"skip": skip, "limit": limit, "total": total_count},
    }
