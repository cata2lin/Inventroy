# routes/snapshots.py
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, Query, Request, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from schemas import SnapshotWithMetricsResponse
from crud import snapshots as crud_snapshots

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

METRIC_KEYS: List[str] = [
    "average_stock_level","min_stock_level","max_stock_level","stock_range",
    "stock_stddev","days_out_of_stock","stockout_rate","replenishment_days",
    "depletion_days","total_outflow","stock_turnover","avg_days_in_inventory",
    "dead_stock_days","dead_stock_ratio","avg_inventory_value","stock_health_index"
]


def _to_float(v):
    return float(v) if isinstance(v, Decimal) else v


def convert_metrics(d: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {k: _to_float(v) for k, v in (d or {}).items()}


def apply_metric_filters(items: List[Dict[str, Any]], qp: Dict[str, str]) -> List[Dict[str, Any]]:
    def in_range(val, lo, hi):
        if val is None:
            return not (lo is not None or hi is not None)
        try:
            x = float(val)
        except Exception:
            return True
        if lo is not None and x < lo:
            return False
        if hi is not None and x > hi:
            return False
        return True

    out = []
    for s in items:
        m = s.get("metrics") or {}
        ok = True
        for k in METRIC_KEYS:
            lo = qp.get(f"{k}_min")
            hi = qp.get(f"{k}_max")
            lo = float(lo) if lo not in (None, "") else None
            hi = float(hi) if hi not in (None, "") else None
            if lo is None and hi is None:
                continue
            if not in_range(m.get(k), lo, hi):
                ok = False
                break
        if ok:
            out.append(s)
    return out


def sort_items(items: List[Dict[str, Any]], sort_field: str, sort_order: str) -> List[Dict[str, Any]]:
    reverse = sort_order == "desc"

    def key_fn(s: Dict[str, Any]):
        val = s.get(sort_field)
        if val is None:
            val = (s.get("metrics") or {}).get(sort_field)
        if isinstance(val, Decimal):
            val = float(val)
        # push missing to the bottom for both orders
        return (val is None, val)

    return sorted(items, key=key_fn, reverse=reverse)


@router.get("/", response_model=SnapshotWithMetricsResponse, summary="Retrieve snapshots with metrics")
def read_snapshots_with_metrics(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=100),
    store_id: Optional[int] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    sort_field: str = Query("date"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
):
    payload = crud_snapshots.get_snapshots_with_metrics(
        db=db,
        skip=0,               # fetch a large set, paginate after filters
        limit=10_000,
        store_id=store_id,
        start_date=start_date,
        end_date=end_date,
    )
    snapshots = payload.get("snapshots", [])

    # Normalize Decimal inside metrics
    for s in snapshots:
        s["metrics"] = convert_metrics(s.get("metrics"))

    # Metric range filters: *_min and *_max from query params
    qp = dict(request.query_params)
    filtered = apply_metric_filters(snapshots, qp)

    # Sort by requested field or metric
    filtered = sort_items(filtered, sort_field, sort_order)

    total_count = len(filtered)
    window = filtered[skip: skip + limit]

    return {"total_count": total_count, "snapshots": window}


@router.post("/trigger", summary="Trigger inventory snapshot collection for a store")
def trigger_snapshot(store_id: int = Query(...), db: Session = Depends(get_db)):
    if not store_id:
        raise HTTPException(status_code=400, detail="store_id is required")
    crud_snapshots.create_snapshot_for_store(db, store_id)
    return {"status": "ok"}
