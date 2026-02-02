from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from crud.snapshots import (
    get_products_with_velocity,
    create_snapshot_for_store,
    has_snapshot_data,
    get_last_snapshot_date_by_store,
)

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

@router.get("/stores")
def list_stores(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT id, name FROM stores WHERE enabled = TRUE ORDER BY name")).mappings().all()
    return [{"id": int(r["id"]), "name": r["name"]} for r in rows]

@router.post("/trigger")
def trigger_snapshot(store_id: int = Query(..., ge=1), db: Session = Depends(get_db)):
    create_snapshot_for_store(db, store_id)
    return {"ok": True, "store_id": store_id}

@router.get("/")
def list_products_velocity(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=200),
    store_id: Optional[int] = Query(None, description="Leave empty for all stores"),
    q: Optional[str] = Query(None, description="Search by SKU, barcode, or title"),
    sort_field: str = Query("days_left", description="Sort by: days_left, velocity, current_stock, title, sku"),
    sort_order: str = Query("asc", description="asc or desc"),
    velocity_days: int = Query(7, ge=1, le=90, description="Calculate velocity over this many days"),
):
    """
    Get all products with sales velocity and stock days left.
    
    - **velocity**: Units sold per day (based on snapshot history)
    - **days_left**: How many days until stock runs out at current velocity
    - **velocity_days**: Period to calculate velocity over (default 7 days)
    
    Products are sorted by days_left ascending by default (most urgent first).
    """
    result = get_products_with_velocity(
        db=db,
        skip=skip,
        limit=limit,
        store_id=store_id,
        q=q,
        sort_col=sort_field,
        sort_order=sort_order,
        velocity_days=velocity_days,
    )
    return result


@router.get("/status")
def get_snapshot_status(
    db: Session = Depends(get_db),
    store_id: Optional[int] = Query(None),
):
    """Get the status of snapshot data - whether data exists and when the last snapshot was taken."""
    has_data = has_snapshot_data(db, store_id)
    last_snapshot = get_last_snapshot_date_by_store(db, store_id)
    
    return {
        "has_data": has_data,
        "last_snapshot_date": last_snapshot.isoformat() if last_snapshot else None,
        "store_id": store_id,
    }
