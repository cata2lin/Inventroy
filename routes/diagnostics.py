# routes/diagnostics.py
"""Read-only diagnostics & remediation API. Every endpoint is a pure SELECT — nothing here
mutates production data."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from database import get_db
from services import diagnostics
from services import reconciliation_engine
from services import pool_ops, pool_backfill

router = APIRouter(prefix="/api/diagnostics", tags=["Diagnostics"])


@router.get("/pool/dashboard")
def get_pool_dashboard():
    """Phase 3C operational dashboard data: flags, headline metrics, per-canary health, rollback
    events, SLA breaches, live-vs-canonical diffs. Read-only."""
    return pool_ops.dashboard()


@router.get("/pool/health")
def get_pool_health():
    return pool_ops.canary_health()


@router.get("/pool/backfill-plan")
def get_pool_backfill_plan(barcode: str = Query(...), db: Session = Depends(get_db)):
    """READ-ONLY backfill dry-run for one barcode: live per-store quantities, computed Q, spread,
    and the safety verdict. Mutates nothing (the real backfill is an explicit operator action)."""
    return pool_backfill.plan_backfill(db, barcode)


@router.get("/reconcile-plan")
def get_reconcile_plan(min_spread: int = Query(1, ge=0), limit: int = Query(500, le=10000),
                       db: Session = Depends(get_db)):
    """READ-ONLY convergence review report: proposed authoritative target + per-store moves
    for every diverged barcode group. Applies nothing."""
    plans = reconciliation_engine.plan_all_diverged(db, min_spread=min_spread, limit=limit)
    return {"total": len(plans), "plans": plans}


@router.get("/summary")
def get_summary(db: Session = Depends(get_db)):
    return diagnostics.summary(db)


@router.get("/duplicate-barcodes")
def get_duplicate_barcodes(limit: int = Query(500, le=10000), db: Session = Depends(get_db)):
    rows = diagnostics.scan_duplicate_barcode_groups(db, limit=limit)
    return {"total": len(rows), "groups": rows}


@router.get("/divergence")
def get_divergence(min_spread: int = Query(1, ge=0), limit: int = Query(500, le=10000),
                   db: Session = Depends(get_db)):
    rows = diagnostics.detect_divergence(db, min_spread=min_spread, limit=limit)
    return {"total": len(rows), "groups": rows}


@router.get("/negative-inventory")
def get_negative_inventory(floor: int = 0, limit: int = Query(1000, le=10000),
                           db: Session = Depends(get_db)):
    return diagnostics.detect_negative_inventory(db, floor=floor, limit=limit)


@router.get("/historical-storms")
def get_historical_storms(days: int = Query(14, le=90), db: Session = Depends(get_db)):
    rows = diagnostics.detect_historical_storms(db, days=days)
    return {"total": len(rows), "events": rows}


@router.get("/impossible-states")
def get_impossible_states(days: int = Query(14, le=90), db: Session = Depends(get_db)):
    rows = diagnostics.detect_impossible_states(db, days=days)
    return {"total": len(rows), "barcodes": rows}


@router.get("/lock-status")
def get_lock_status():
    return diagnostics.lock_status()


@router.get("/replay/barcode/{barcode}")
def replay_barcode(barcode: str, hours: int = Query(168, le=2160), db: Session = Depends(get_db)):
    from services import forensic
    return forensic.replay_barcode(db, barcode, hours=hours)


@router.get("/replay/operation/{sync_op}")
def replay_operation(sync_op: str, db: Session = Depends(get_db)):
    from services import forensic
    return forensic.replay_operation(db, sync_op)
