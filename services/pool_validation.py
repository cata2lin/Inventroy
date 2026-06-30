# services/pool_validation.py
"""
PHASE 2 — LIVE-TRUTH VALIDATION of the canonical pool engine (read-only, ALERT-ONLY).

Phase 1 (shadow) proved the engine evolves PoolState sanely vs the legacy *value*. Phase 2 validates
PoolState against the only thing that actually matters — LIVE Shopify — and against the legacy mirror,
so we KNOW the engine would converge correctly before we ever let it write (Phase 3).

For each engine-observed pool it reads live `available` per canonical store and reports:
  • pool_quantity (PoolState.quantity, the engine's canonical Q)
  • per-store LIVE quantities + spread
  • canonical_drift  — stores whose LIVE value != Q (what the engine WOULD correct)
  • mirror_drift     — stores whose LIVE value != local mirror (legacy cache staleness)
  • last_event + unresolved_duration (how long this pool has been diverged on live)

It maintains `pool_states.diverged_since` to drive a PERMANENT-DIVERGENCE detector + convergence-SLA:
a pool diverged on live longer than POOL_SLA_HOURS raises CRITICAL. It NEVER writes inventory and
NEVER auto-heals — that is Phase 3, gated on this phase looking clean.
"""
import os
from datetime import datetime, timezone
from typing import Dict, Any, List

from sqlalchemy import text
from database import SessionLocal
from services import audit_logger, alerting, live_truth
import models


POOL_VALIDATION_MAX_READS = int(os.getenv("POOL_VALIDATION_MAX_READS", "300"))
POOL_VALIDATION_SAMPLE = int(os.getenv("POOL_VALIDATION_SAMPLE", "80"))
POOL_SLA_HOURS = float(os.getenv("POOL_SLA_HOURS", "6"))         # diverged-on-live longer than this => CRITICAL


def _candidate_barcodes(db) -> List[str]:
    """Validate (1) every pool currently flagged diverged (track resolution + unresolved duration),
    then (2) a sample of other engine-observed pools — bounded, rotating via random()."""
    diverged = [r[0] for r in db.execute(text(
        "SELECT barcode FROM pool_states WHERE diverged_since IS NOT NULL ORDER BY diverged_since ASC LIMIT 1000"
    )).fetchall()]
    sample = [r[0] for r in db.execute(text(
        "SELECT barcode FROM pool_states ORDER BY random() LIMIT :k"
    ), {"k": POOL_VALIDATION_SAMPLE}).fetchall()]
    seen, out = set(), []
    for b in diverged + sample:
        if b not in seen:
            seen.add(b); out.append(b)
    return out


def _validate_pool(db, barcode: str) -> Dict[str, Any]:
    """Read live Shopify per canonical store, compare to the engine's Q and the mirror. No writes.

    The SLA clock (`diverged_since`) tracks the CUSTOMER-FACING metric only: stores DISAGREE on live
    (live_spread > 0). engine-Q-vs-live while stores AGREE (canonical_drift only — e.g. a rolled-back
    pool whose Q went stale) is engine bookkeeping; it is REPORTED for observability but never starts
    the SLA clock and never escalates to CRITICAL. A pool with < 2 canonical stores is not a sync pool
    at all (a single store cannot diverge from itself) — it is skipped and any stale flag is cleared."""
    state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
    if state is None:
        return {"barcode": barcode, "skipped": "no pool state", "reads": 0}
    q = int(state.quantity)
    rows = live_truth._canonical_rows(db, barcode)
    if len(rows) < 2:
        # Orphaned / single-store pool: cannot diverge, pool_q is inert bookkeeping. Clear any stale
        # SLA flag (this is the class that otherwise alerts CRITICAL forever) and skip.
        cleared = state.diverged_since is not None
        if cleared:
            state.diverged_since = None
            db.commit()
        return {"barcode": barcode, "skipped": "single_store", "reads": 0,
                "single_store": True, "cleared_stale_flag": cleared}
    per_store, lives, reads = [], [], 0
    canonical_drift, mirror_drift = [], []
    for r in rows:
        live = live_truth._read_live(r["shopify_url"], r["api_token"], r["inventory_item_id"], r["sync_location_id"])
        reads += 1
        per_store.append({"store": r["store"], "live": live, "mirror": r["mirror"]})
        if isinstance(live, int):
            lives.append(live)
            if live != q:
                canonical_drift.append({"store": r["store"], "live": live, "pool_q": q})
            if live != r["mirror"]:
                mirror_drift.append({"store": r["store"], "live": live, "mirror": r["mirror"]})
    readable = len(lives) >= 2
    live_spread = (max(lives) - min(lives)) if readable else 0
    stores_disagree = readable and (live_spread > 0)          # the customer-facing divergence
    engine_q_drift = len(canonical_drift) > 0                 # bookkeeping: engine Q vs live (stores may agree)

    now = datetime.now(timezone.utc)
    # Only mutate the SLA clock when we can actually assess agreement (>= 2 readable stores). On a
    # partial/failed read we leave diverged_since untouched (never assert health on unreadable data).
    if readable:
        if stores_disagree:
            if state.diverged_since is None:
                state.diverged_since = now
        else:
            state.diverged_since = None
        db.commit()

    unresolved_s = int((now - state.diverged_since).total_seconds()) if state.diverged_since else 0
    return {"barcode": barcode, "reads": reads, "pool_quantity": q, "per_store_live": per_store,
            "live_spread": live_spread, "canonical_drift": canonical_drift, "mirror_drift": mirror_drift,
            "diverged": stores_disagree, "engine_q_drift": engine_q_drift, "unresolved_duration_s": unresolved_s,
            "last_event": state.source_timestamp.isoformat() if state.source_timestamp else None,
            "pool_version": state.version}


def run_pool_validation_sweep() -> Dict[str, Any]:
    """Scheduled entrypoint. Validates engine PoolState vs live Shopify; reports + alerts; no writes."""
    db = SessionLocal()
    try:
        checked, reads, diverged, permanent, canon_mismatch = 0, 0, 0, 0, 0
        worst = None
        for bc in _candidate_barcodes(db):
            if reads >= POOL_VALIDATION_MAX_READS:
                break
            res = _validate_pool(db, bc)
            if res.get("skipped"):
                continue
            checked += 1
            reads += res["reads"]
            if res["canonical_drift"]:
                canon_mismatch += 1
            if res["diverged"]:
                diverged += 1
                if worst is None or res["live_spread"] > worst["live_spread"]:
                    worst = res
                is_permanent = res["unresolved_duration_s"] >= POOL_SLA_HOURS * 3600
                if is_permanent:
                    permanent += 1
                audit_logger.log(
                    category="RECONCILIATION", action="pool_validation_diverged",
                    message=f"[{bc}] LIVE spread={res['live_spread']} pool_q={res['pool_quantity']} "
                            f"canon_drift={len(res['canonical_drift'])} unresolved={res['unresolved_duration_s']}s"
                            f"{' — PERMANENT(SLA)' if is_permanent else ''}",
                    target=bc, severity="CRITICAL" if is_permanent else "WARN",
                    details={"barcode": bc, "pool_quantity": res["pool_quantity"],
                             "per_store_live": res["per_store_live"], "spread": res["live_spread"],
                             "last_event": res["last_event"], "unresolved_duration": res["unresolved_duration_s"],
                             "canonical_drift": res["canonical_drift"], "mirror_drift": res["mirror_drift"],
                             "pool_version": res["pool_version"]})
                if is_permanent:
                    alerting.critical("pool_validation.permanent_divergence",
                                      f"[{bc}] diverged on LIVE Shopify for {res['unresolved_duration_s']}s "
                                      f"(> {POOL_SLA_HOURS}h SLA); spread {res['live_spread']}, pool_q {res['pool_quantity']}",
                                      {"barcode": bc, "spread": res["live_spread"],
                                       "unresolved_duration": res["unresolved_duration_s"]})
            elif res.get("engine_q_drift"):
                # Stores AGREE on live but the engine's Q disagrees — bookkeeping staleness (typically a
                # rolled-back pool whose Q drifted). Worth visibility, but NOT customer-facing and NOT an
                # SLA breach, so WARN-only and it never starts the convergence clock.
                audit_logger.log(
                    category="RECONCILIATION", action="pool_validation_engine_q_drift",
                    message=f"[{bc}] engine Q={res['pool_quantity']} disagrees with live "
                            f"(stores agree); canon_drift={len(res['canonical_drift'])}",
                    target=bc, severity="WARN",
                    details={"barcode": bc, "pool_quantity": res["pool_quantity"],
                             "canonical_drift": res["canonical_drift"],
                             "per_store_live": res["per_store_live"], "pool_version": res["pool_version"]})

        if diverged:
            sev_worst = worst["barcode"] if worst else None
            alerting.warning("pool_validation.divergence",
                             f"Phase-2 validation: {diverged}/{checked} pools diverged on LIVE Shopify "
                             f"({permanent} permanent, {canon_mismatch} where engine-Q disagrees with live). "
                             f"Worst [{sev_worst}].",
                             {"diverged": diverged, "permanent": permanent, "canon_mismatch": canon_mismatch})
        audit_logger.log(category="SYSTEM", action="pool_validation_sweep",
                         message=f"Phase-2 validation: {checked} pools, {reads} live reads; "
                                 f"diverged={diverged}, permanent={permanent}, engine-vs-live mismatch={canon_mismatch}",
                         severity="INFO",
                         details={"checked": checked, "reads": reads, "diverged": diverged,
                                  "permanent": permanent, "canonical_mismatch": canon_mismatch})
        return {"checked": checked, "reads": reads, "diverged": diverged, "permanent": permanent,
                "canonical_mismatch": canon_mismatch}
    except Exception as e:
        try:
            alerting.warning("pool_validation.sweep", f"pool validation sweep failed: {e}", {})
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        db.close()
