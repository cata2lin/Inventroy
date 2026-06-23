# services/pool_canary.py
"""
PHASE 3B — CANARY WRITE PATH + AUTOMATIC ROLLBACK (dormant: requires SYNC_POOL_ENGINE_WRITES=true
AND the barcode in SYNC_POOL_CANARY_BARCODES AND a live-truth BACKFILL).

For a canary barcode the engine becomes authoritative: a genuine webhook is ingested (idempotent),
folded into PoolState (per-source ordering, monotonic version), and every store is driven to the
canonical Q by idempotent compare-and-set — bypassing legacy delta propagation for that barcode only.

Hard safety gates (ALL required, else the barcode stays on legacy):
  • SYNC_POOL_ENGINE_WRITES on
  • barcode in the canary allowlist (or list empty = global, Phase 4)
  • PoolState.backfilled_at IS NOT NULL  (NEVER write from a bootstrapped Q)
  • no active rollback marker for the barcode

Automatic rollback: if a canary barcode shows CAS instability, write amplification, oscillation, or
persistent live mismatch, it is reverted to legacy (rollback marker), CRITICAL-alerted, audit kept.
"""
import os
import time
from typing import Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

import models
from database import SessionLocal
from services import pool_engine, audit_logger, alerting


ROLLBACK_CAS_FAILURES = int(os.getenv("POOL_CANARY_ROLLBACK_CAS_FAILURES", "2"))
ROLLBACK_AMPLIFICATION = int(os.getenv("POOL_CANARY_ROLLBACK_AMPLIFICATION", "20"))  # convergences / 60s
ROLLBACK_OSCILLATION = int(os.getenv("POOL_CANARY_ROLLBACK_OSCILLATION", "6"))       # sign flips in last 10 obs


def is_rolled_back(db: Session, barcode: str) -> bool:
    return db.query(models.PoolCanaryRollback).filter(
        models.PoolCanaryRollback.barcode == barcode).first() is not None


def canary_active_for(db: Session, barcode: str) -> bool:
    """The single authority gate. Defensive: any condition unmet => legacy serves the barcode."""
    if not pool_engine.pool_writes_enabled():
        return False
    allow = pool_engine.canary_barcodes()
    if allow and barcode not in allow:        # non-empty list = explicit canary set
        return False
    state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
    if state is None or state.backfilled_at is None:   # bootstrapped Q is NEVER write-authoritative
        return False
    if is_rolled_back(db, barcode):
        return False
    return True


def trigger_rollback(db: Session, barcode: str, reason: str, details: Optional[Dict[str, Any]] = None):
    """Disable canary for this barcode (revert to legacy), keep audit, CRITICAL-alert. Deterministic
    and idempotent (upsert the marker)."""
    existing = db.query(models.PoolCanaryRollback).filter_by(barcode=barcode).first()
    if existing:
        existing.reason = reason
        existing.details = details
    else:
        db.add(models.PoolCanaryRollback(barcode=barcode, reason=reason, details=details))
    db.commit()
    audit_logger.log(category="RECONCILIATION", action="pool_canary_rollback",
                     message=f"[{barcode}] CANARY ROLLBACK -> legacy mode: {reason}",
                     target=barcode, severity="CRITICAL", details={"reason": reason, **(details or {})})
    alerting.critical("pool_canary.rollback",
                      f"[{barcode}] canary write path rolled back to legacy: {reason}",
                      {"barcode": barcode, "reason": reason, **(details or {})})


def clear_rollback(db: Session, barcode: str) -> bool:
    """Operator action: clear a rollback marker so the barcode can re-enter canary (after backfill)."""
    row = db.query(models.PoolCanaryRollback).filter_by(barcode=barcode).first()
    if not row:
        return False
    db.delete(row); db.commit()
    audit_logger.log(category="RECONCILIATION", action="pool_canary_rollback_cleared",
                     message=f"[{barcode}] canary rollback cleared (operator)",
                     target=barcode, severity="WARN")
    return True


def _recent_convergences(db: Session, barcode: str, seconds: int = 60) -> int:
    return db.execute(text("""
        SELECT count(*) FROM pool_events
        WHERE barcode = :b AND kind = 'convergence' AND created_at >= now() - (:s || ' seconds')::interval
    """), {"b": barcode, "s": seconds}).scalar() or 0


def _oscillation_flips(db: Session, barcode: str, n: int = 10) -> int:
    obs = [r[0] for r in db.execute(text("""
        SELECT observed_quantity FROM pool_events
        WHERE barcode = :b AND kind = 'observation' ORDER BY event_id DESC LIMIT :n
    """), {"b": barcode, "n": n}).fetchall()]
    obs = list(reversed(obs))
    deltas = [obs[i] - obs[i - 1] for i in range(1, len(obs))]
    flips = sum(1 for i in range(1, len(deltas))
                if deltas[i] != 0 and deltas[i - 1] != 0 and (deltas[i] > 0) != (deltas[i - 1] > 0))
    return flips


def evaluate_canary_rollback(db: Session, barcode: str, converge_result: Dict[str, Any]) -> Optional[str]:
    """Inspect the latest convergence + recent history; trip an automatic rollback if unstable.
    Returns the rollback reason (and performs the rollback) or None."""
    reason, details = None, {}
    if converge_result.get("failed", 0) >= ROLLBACK_CAS_FAILURES:
        reason = "repeated_cas_conflict"
        details = {"cas_conflicts": converge_result.get("failed"), "retries": converge_result.get("retries")}
    elif _recent_convergences(db, barcode) >= ROLLBACK_AMPLIFICATION:
        reason = "write_amplification"
        details = {"convergences_60s": _recent_convergences(db, barcode)}
    elif _oscillation_flips(db, barcode) >= ROLLBACK_OSCILLATION:
        reason = "oscillation"
        details = {"sign_flips": _oscillation_flips(db, barcode)}
    if reason:
        trigger_rollback(db, barcode, reason, details)
        return reason
    return None


def canary_handle(*, barcode: str, source_store_id: Optional[int], source_variant_id: Optional[int],
                  inventory_item_id: Optional[int], observed_quantity: int, source_timestamp,
                  webhook_id: Optional[str]) -> Dict[str, Any]:
    """Engine-AUTHORITATIVE handling of one genuine webhook for a canary barcode. Opens its OWN db
    session (isolated from the legacy transaction); the caller (handle_webhook) holds the per-barcode
    advisory lock so this is serialized. Bypasses legacy propagation. Returns a structured result."""
    db = SessionLocal()
    try:
        return _canary_handle_inner(db, barcode=barcode, source_store_id=source_store_id,
                                    source_variant_id=source_variant_id, inventory_item_id=inventory_item_id,
                                    observed_quantity=observed_quantity, source_timestamp=source_timestamp,
                                    webhook_id=webhook_id)
    finally:
        db.close()


def _canary_handle_inner(db: Session, *, barcode, source_store_id, source_variant_id, inventory_item_id,
                         observed_quantity, source_timestamp, webhook_id) -> Dict[str, Any]:
    t0 = time.monotonic()
    ev_id = pool_engine.ingest_event(db, barcode=barcode, source_store_id=source_store_id,
                                     source_variant_id=source_variant_id, inventory_item_id=inventory_item_id,
                                     observed_quantity=observed_quantity, source_timestamp=source_timestamp,
                                     webhook_id=webhook_id)
    if ev_id is None:
        audit_logger.log(category="STOCK", action="pool_canary_dup_suppressed",
                         message=f"[{barcode}] canary: duplicate webhook suppressed (idempotent)",
                         target=barcode, severity="INFO", details={"webhook_id": webhook_id})
        return {"barcode": barcode, "result": "duplicate"}

    res = pool_engine.apply_event(db, ev_id, skip_lock=True)
    if res is None:
        audit_logger.log(category="STOCK", action="pool_canary_stale_reject",
                         message=f"[{barcode}] canary: out-of-order event rejected (per-source)",
                         target=barcode, severity="INFO", details={"webhook_id": webhook_id})
        return {"barcode": barcode, "result": "stale_reject"}

    q, version = res["quantity"], res["version"]
    conv = pool_engine.converge_pool(db, barcode)        # idempotent CAS-to-Q for all stores
    rollback_reason = evaluate_canary_rollback(db, barcode, conv)
    latency_ms = int((time.monotonic() - t0) * 1000)

    audit_logger.log(
        category="STOCK", action="pool_canary_write",
        message=f"[{barcode}] canary Q={q} v{version} set={conv.get('converged')} "
                f"cas_conflict={conv.get('failed')} retries={conv.get('retries')} {latency_ms}ms"
                f"{' ROLLBACK='+rollback_reason if rollback_reason else ''}",
        target=barcode, severity="WARN" if (conv.get("failed") or rollback_reason) else "INFO",
        details={"barcode": barcode, "pool_version": version, "source_store": source_store_id,
                 "canonical_Q": q, "live_quantities": conv.get("live_quantities"),
                 "cas_result": conv.get("per_store"), "retries": conv.get("retries"),
                 "rollback_reason": rollback_reason, "propagation_latency_ms": latency_ms,
                 "webhook_id": webhook_id})
    return {"barcode": barcode, "result": "converged", "canonical_Q": q, "version": version,
            "converged": conv.get("converged"), "failed": conv.get("failed"),
            "rollback_reason": rollback_reason, "latency_ms": latency_ms}
