# services/pool_backfill.py
"""
PHASE 3A — LIVE-TRUTH BACKFILL for PoolState (the gate before any canary write).

Validation proved bootstrapped Q is NOT safe as write authority (e.g. 7865789393544: live 2234,
bootstrap 2235). A pool may only become write-authoritative once its Q is seeded from a CONFIRMED
live-truth read where all stores AGREE. This module does that, safely and reversibly.

Safety contract (a pool is backfilled ONLY if ALL hold):
  • every canonical store returned a live quantity (no stale/missing read, no partial API failure)
  • >= 2 readable stores
  • the stores AGREE: spread <= max_spread (default 0 — must be exactly converged)
Otherwise: NO write, action='skipped_*', CRITICAL alert, operator must resolve first.

Modes:
  • dry_run=True (default)              -> plan only, nothing mutated, action='dry_run'
  • dry_run=False, operator_confirmed=F -> refuses to write (explicit confirmation required)
  • dry_run=False, operator_confirmed=T -> writes Q for SAFE pools only

Every operation (incl. dry-runs and skips) is logged to pool_backfills with the live snapshot and the
PRIOR PoolState (prev_quantity/prev_version), so any backfill is auditable and REVERSIBLE.
"""
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from database import SessionLocal
from services import audit_logger, alerting, live_truth, sync_guards
import models


BACKFILL_MAX_SPREAD = int(os.getenv("POOL_BACKFILL_MAX_SPREAD", "0"))   # 0 = stores must agree exactly


def plan_backfill(db, barcode: str) -> Dict[str, Any]:
    """Read live Shopify per canonical store and return the backfill verdict WITHOUT mutating.
    {barcode, live_quantities, computed_Q, spread, action, reason, safe, source_store}"""
    rows = live_truth._canonical_rows(db, barcode)
    live_quantities, lives, missing = {}, [], []
    source_store = None
    for r in rows:
        live = live_truth._read_live(r["shopify_url"], r["api_token"], r["inventory_item_id"], r["sync_location_id"])
        live_quantities[r["store"]] = live
        if isinstance(live, int):
            lives.append(live)
            if source_store is None:
                source_store = r["store_id"]
        else:
            missing.append(r["store"])

    if len(rows) < 2:
        return {"barcode": barcode, "live_quantities": live_quantities, "computed_Q": None,
                "spread": None, "safe": False, "action": "skipped_single_store",
                "reason": "fewer than 2 canonical stores", "source_store": source_store}
    if missing:
        return {"barcode": barcode, "live_quantities": live_quantities, "computed_Q": None,
                "spread": None, "safe": False, "action": "skipped_stale_read",
                "reason": f"missing/failed live read for: {', '.join(missing)}", "source_store": source_store}
    spread = max(lives) - min(lives)
    if spread > BACKFILL_MAX_SPREAD:
        return {"barcode": barcode, "live_quantities": live_quantities, "computed_Q": None,
                "spread": spread, "safe": False, "action": "skipped_diverged",
                "reason": f"stores disagree (spread {spread} > {BACKFILL_MAX_SPREAD}); operator must converge first",
                "source_store": source_store}
    computed_q = max(lives)
    # Never seed a NEGATIVE pool as write-authoritative: a converged-negative pool is oversold across
    # all stores (a real data problem). Backfilling it + converging would floor it to 0 — a live
    # inventory change, not a no-op. Refuse; operator must correct the oversell first.
    if computed_q < sync_guards.INVENTORY_FLOOR:
        return {"barcode": barcode, "live_quantities": live_quantities, "computed_Q": computed_q,
                "spread": spread, "safe": False, "action": "skipped_negative",
                "reason": f"converged at negative {computed_q} (oversold); resolve before backfill",
                "source_store": source_store}
    # SAFE: all stores agree on a non-negative value -> the confirmed live truth.
    return {"barcode": barcode, "live_quantities": live_quantities, "computed_Q": computed_q,
            "spread": spread, "safe": True, "action": "backfill", "reason": "stores converged on live",
            "source_store": source_store}


def _log_backfill(db, *, barcode, action, reason, plan, prev_q, prev_v, new_q, new_v,
                  operator_confirmed, dry_run):
    db.add(models.PoolBackfill(
        barcode=barcode, action=action, reason=reason,
        prev_quantity=prev_q, prev_version=prev_v, new_quantity=new_q, new_version=new_v,
        spread=plan.get("spread"), source_store=plan.get("source_store"),
        live_snapshot=plan.get("live_quantities"), operator_confirmed=operator_confirmed, dry_run=dry_run))
    db.commit()
    audit_logger.log(category="RECONCILIATION", action=f"pool_backfill_{action}",
                     message=f"[{barcode}] backfill {action}: {reason} "
                             f"(Q {prev_q}->{new_q}, dry_run={dry_run}, confirmed={operator_confirmed})",
                     target=barcode, severity="WARN" if action.startswith("skipped") else "INFO",
                     details={"action": action, "reason": reason, "computed_Q": new_q,
                              "prev_quantity": prev_q, "live": plan.get("live_quantities"),
                              "spread": plan.get("spread"), "dry_run": dry_run,
                              "operator_confirmed": operator_confirmed})


def backfill_pool_state_from_live_truth(barcodes: Optional[List[str]] = None, *, dry_run: bool = True,
                                        operator_confirmed: bool = False) -> Dict[str, Any]:
    """Backfill PoolState.quantity from confirmed live truth. SAFE BY DEFAULT (dry_run=True writes
    nothing). Real writes require dry_run=False AND operator_confirmed=True AND a SAFE (converged) pool."""
    db = SessionLocal()
    try:
        if barcodes is None:
            # default scope: every engine-observed pool not yet backfilled
            barcodes = [r[0] for r in db.execute(text(
                "SELECT barcode FROM pool_states WHERE backfilled_at IS NULL ORDER BY barcode")).fetchall()]
        done, skipped, dry, needs_confirm = 0, 0, 0, 0
        results = []
        for bc in barcodes:
            plan = plan_backfill(db, bc)
            state = db.query(models.PoolState).filter(models.PoolState.barcode == bc).first()
            prev_q = state.quantity if state else None
            prev_v = state.version if state else None

            if not plan["safe"]:
                _log_backfill(db, barcode=bc, action=plan["action"], reason=plan["reason"], plan=plan,
                              prev_q=prev_q, prev_v=prev_v, new_q=None, new_v=None,
                              operator_confirmed=operator_confirmed, dry_run=dry_run)
                alerting.critical("pool_backfill.unsafe",
                                  f"[{bc}] backfill refused: {plan['reason']}",
                                  {"barcode": bc, "live": plan["live_quantities"], "spread": plan["spread"]})
                skipped += 1
                results.append(plan)
                continue

            new_q = plan["computed_Q"]
            new_v = (prev_v + 1) if prev_v else 1
            if dry_run:
                _log_backfill(db, barcode=bc, action="dry_run", reason="safe; dry-run (no write)", plan=plan,
                              prev_q=prev_q, prev_v=prev_v, new_q=new_q, new_v=new_v,
                              operator_confirmed=operator_confirmed, dry_run=True)
                dry += 1
                results.append({**plan, "action": "dry_run", "computed_Q": new_q})
                continue
            if not operator_confirmed:
                _log_backfill(db, barcode=bc, action="skipped_needs_confirmation",
                              reason="real backfill requires operator_confirmed=True", plan=plan,
                              prev_q=prev_q, prev_v=prev_v, new_q=new_q, new_v=new_v,
                              operator_confirmed=False, dry_run=False)
                needs_confirm += 1
                results.append({**plan, "action": "skipped_needs_confirmation"})
                continue

            # APPLY — seed Q from confirmed live truth; bump monotonic version; stamp eligibility.
            now = datetime.now(timezone.utc)
            if state is None:
                state = models.PoolState(barcode=bc, quantity=new_q, version=new_v,
                                         source_store_id=plan["source_store"], source_timestamp=now,
                                         backfilled_at=now, backfill_source_store=plan["source_store"])
                db.add(state)
            else:
                state.quantity = new_q
                state.version = new_v
                state.backfilled_at = now
                state.backfill_source_store = plan["source_store"]
                state.diverged_since = None
            _log_backfill(db, barcode=bc, action="backfilled", reason="seeded Q from confirmed live truth",
                          plan=plan, prev_q=prev_q, prev_v=prev_v, new_q=new_q, new_v=new_v,
                          operator_confirmed=True, dry_run=False)
            done += 1
            results.append({**plan, "action": "backfilled", "computed_Q": new_q, "new_version": new_v})

        return {"backfilled": done, "skipped_unsafe": skipped, "dry_run": dry,
                "needs_confirmation": needs_confirm, "results": results[:200]}
    finally:
        db.close()


def reverse_backfill(backfill_id: int) -> Dict[str, Any]:
    """Reversibility: restore PoolState to the prev_quantity/prev_version captured in a pool_backfills
    row (operator-driven undo). Audited."""
    db = SessionLocal()
    try:
        bf = db.query(models.PoolBackfill).filter(models.PoolBackfill.id == backfill_id).first()
        if bf is None or bf.action != "backfilled":
            return {"reversed": False, "reason": "no applied backfill with that id"}
        state = db.query(models.PoolState).filter(models.PoolState.barcode == bf.barcode).first()
        if state is None:
            return {"reversed": False, "reason": "pool state missing"}
        state.quantity = bf.prev_quantity if bf.prev_quantity is not None else state.quantity
        state.version = (state.version or 0) + 1   # version only ever moves forward
        state.backfilled_at = None                 # reverting removes write-eligibility (safety)
        db.commit()
        audit_logger.log(category="RECONCILIATION", action="pool_backfill_reversed",
                         message=f"[{bf.barcode}] backfill #{backfill_id} reversed -> Q={state.quantity}; "
                                 f"write-eligibility cleared",
                         target=bf.barcode, severity="WARN",
                         details={"backfill_id": backfill_id, "restored_quantity": bf.prev_quantity})
        return {"reversed": True, "barcode": bf.barcode, "restored_quantity": bf.prev_quantity}
    finally:
        db.close()
