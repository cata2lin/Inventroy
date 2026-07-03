# services/pool_onboarding.py
"""
POOL ONBOARDING SWEEP — the standing mechanism that moves multi-store pools onto the absolute
convergence engine, so no pool is left permanently on the amplification-prone legacy path.

A multi-store barcode is served by the safe engine ONLY once its PoolState has been backfilled from
CONFIRMED live truth (canary_active_for requires backfilled_at). Pools that were created diverged, or
never observed while stores agreed, sit at backfilled_at=NULL and fall through to legacy propagation.
This sweep finds them and, for each pool whose stores currently AGREE on a non-negative live value,
runs the live-truth backfill (which itself refuses diverged/negative/partial reads), onboarding it.

It NEVER forces a value: the backfill's safety contract (>= 2 readable stores, spread == 0, Q >= 0) is
the only thing that lets a pool through. Diverged / negative / unreadable pools are reported for an
operator (they need an evidence-based decision on which store holds the real stock). Idempotent —
once onboarded, backfilled_at is set and the pool is skipped next run.

Combined with the absolute (non-amplifying) legacy path, pools converge and then onboard automatically,
so "all stores stay in sync" holds even for pools not yet on the engine.
"""
import os
from typing import Dict, Any, List

from sqlalchemy import text
from database import SessionLocal
from services import audit_logger, alerting, pool_backfill, dist_lock, diagnostics


ONBOARDING_MAX_PER_RUN = int(os.getenv("SYNC_ONBOARDING_MAX_PER_RUN", "60"))   # cap live reads / run


def onboarding_enabled() -> bool:
    return os.getenv("SYNC_POOL_ONBOARDING", "true").strip().lower() in ("1", "true", "yes", "on")


def _off_engine_multistore_barcodes(db, limit: int) -> List[str]:
    """Multi-store barcodes NOT yet engine-authoritative (no PoolState, or backfilled_at IS NULL).
    Excludes PLACEHOLDER barcodes (all-zeros/empty) exactly as the propagation + diagnostics paths do —
    those are FALSE sync groups (unrelated products sharing a pseudo-barcode); onboarding one would let
    the engine SET every unrelated product to a single value = cross-product stock corruption."""
    ph = diagnostics._placeholder_sql("pv.barcode")
    return [r[0] for r in db.execute(text(f"""
        WITH ms AS (
            SELECT pv.barcode FROM product_variants pv
            JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
            JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
            WHERE {ph} AND pv.inventory_item_id IS NOT NULL
            GROUP BY pv.barcode HAVING count(DISTINCT pv.store_id) > 1)
        SELECT ms.barcode FROM ms LEFT JOIN pool_states ps ON ps.barcode = ms.barcode
        WHERE ps.barcode IS NULL OR ps.backfilled_at IS NULL
        ORDER BY ms.barcode LIMIT :lim
    """), {"lim": limit}).fetchall()]


def _distinct_skus(db, barcode: str) -> int:
    """How many DISTINCT non-empty SKUs share this barcode across enabled stores. A genuine shared-stock
    pool is ONE product (1 SKU); >1 SKU is a FALSE-group signature (different products, same barcode) —
    we must never auto-onboard/converge those or we contaminate unrelated stock."""
    return db.execute(text("""
        SELECT count(DISTINCT btrim(pv.sku)) FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
          AND pv.sku IS NOT NULL AND btrim(pv.sku) <> ''
    """), {"b": barcode}).scalar() or 0


def run_onboarding_sweep() -> Dict[str, Any]:
    """Scheduled entrypoint. Onboards every off-engine multi-store pool whose stores currently agree on
    a non-negative live value; reports the rest. Writes only PoolState (never inventory) via the
    live-truth backfill, per-barcode-locked so it never races a live webhook."""
    if not onboarding_enabled():
        return {"disabled": True}
    db = SessionLocal()
    try:
        barcodes = _off_engine_multistore_barcodes(db, ONBOARDING_MAX_PER_RUN)
    finally:
        db.close()

    onboarded, onboarded_at_zero, needs_attention = [], [], {}
    for bc in barcodes:
        handle = dist_lock.acquire(f"barcode:{bc}")
        if handle is None:
            needs_attention.setdefault("lock_busy", []).append(bc)
            continue
        db = SessionLocal()
        try:
            # FALSE-GROUP guard: never auto-onboard a barcode shared by >1 distinct product (SKU) —
            # that would let the engine drive unrelated products to one stock value.
            if _distinct_skus(db, bc) > 1:
                needs_attention.setdefault("false_group_multi_sku", []).append(bc)
                continue
            # PRE-SCREEN with the read-only planner (does NOT alert). Only pools the backfill contract
            # deems SAFE (>=2 readable stores, spread==0, Q>=0) get the real write. This keeps the
            # recurring diverged/negative backlog from firing a CRITICAL alert every 30-min run.
            plan = pool_backfill.plan_backfill(db, bc)
            if not plan.get("safe"):
                needs_attention.setdefault(plan.get("action") or "skipped", []).append(bc)
                continue
            res = pool_backfill.backfill_pool_state_from_live_truth(
                [bc], dry_run=False, operator_confirmed=True)
            for r in res.get("results", []):
                if r.get("action") == "backfilled":
                    onboarded.append(bc)
                    if r.get("computed_Q") == 0:
                        onboarded_at_zero.append(bc)   # onboarded at 0 everywhere — invisible to the
                                                       # uniform-collapse detector (Q==live==0); surface it
                else:
                    needs_attention.setdefault(r.get("action") or "skipped", []).append(bc)
        except Exception as e:
            needs_attention.setdefault("error", []).append(f"{bc}: {e}")
        finally:
            db.close()
            dist_lock.release(handle)

    remaining = sum(len(v) for v in needs_attention.values())
    if needs_attention:
        # Diverged / negative / false-group pools can't be auto-onboarded — they need an operator to
        # establish the real stock (or split the false group). WARN once per sweep (a single summary,
        # not per-pool CRITICAL): they stay safely on the legacy relative path (spread-preserved) meanwhile.
        alerting.warning("pool_onboarding.needs_attention",
                         f"{remaining} off-engine multi-store pools could not be auto-onboarded "
                         f"(diverged/negative/false-group) — need evidence-based resolution.",
                         {k: v[:15] for k, v in needs_attention.items()})

    audit_logger.log(category="SYSTEM", action="pool_onboarding_sweep",
                     message=f"Onboarding sweep: {len(onboarded)} pools moved onto the engine "
                             f"({len(onboarded_at_zero)} at Q=0), {remaining} need attention "
                             f"(of {len(barcodes)} off-engine checked)",
                     severity="INFO",
                     details={"onboarded": len(onboarded), "onboarded_examples": onboarded[:20],
                              "onboarded_at_zero": onboarded_at_zero[:20],
                              "needs_attention": {k: len(v) for k, v in needs_attention.items()},
                              "needs_attention_examples": {k: v[:15] for k, v in needs_attention.items()}})
    return {"checked": len(barcodes), "onboarded": len(onboarded),
            "onboarded_at_zero": len(onboarded_at_zero), "onboarded_barcodes": onboarded,
            "needs_attention": {k: len(v) for k, v in needs_attention.items()},
            "needs_attention_barcodes": needs_attention}
