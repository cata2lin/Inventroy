"""
Recover pools parked during the 2026-07-14 cascade remediation.

Pools whose stores disagreed were parked on the LEGACY sync path (pool_canary_rollbacks,
reason='await_operator_resolution') so the engine could never converge them onto a wrong value.
As operators set the correct stock in Shopify (or the auto-healer converges small spreads), this
script re-checks each parked pool against live truth and, once ALL its listings agree, re-anchors
PoolState from live (official backfill) and clears the rollback — the pool returns to the engine.

Idempotent and safe to run on a schedule: it only acts on pools whose live listings agree
(plan_backfill safe=True); everything else is left parked. Run via cron, e.g.:
    */30 * * * * flock -n /tmp/recover_parked.lock /root/Inventroy/venv/bin/python \
        /root/Inventroy/recover_parked_pools.py >> /var/log/recover_parked.log 2>&1
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from datetime import datetime, timezone
from sqlalchemy import text

from database import SessionLocal
from services import pool_backfill, pool_canary, alerting


def reanchor_uniform_drift(db) -> int:
    """Self-heal the 'uniform collapse' class: the validation sweep flags engine pools whose Q
    disagrees with a value ALL stores agree on (pool_validation_engine_q_drift). Stores-in-agreement
    is exactly the audited backfill's safety bar, so re-anchor Q from live truth instead of alerting
    forever. Event-driven off recent audit rows — no full-fleet live scan."""
    recent = [r[0] for r in db.execute(text("""
        SELECT DISTINCT target FROM audit_logs
        WHERE action = 'pool_validation_engine_q_drift'
          AND timestamp >= now() - interval '45 minutes'
          AND target IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM pool_canary_rollbacks r WHERE r.barcode = target)
    """)).fetchall()]
    healed = 0
    for bc in recent:
        try:
            plan = pool_backfill.plan_backfill(db, bc)
            if plan["safe"]:
                res = pool_backfill.backfill_pool_state_from_live_truth(
                    [bc], dry_run=False, operator_confirmed=True)
                if res.get("backfilled"):
                    healed += 1
                    print(f"  drift re-anchored: {bc} -> {plan['computed_Q']}")
        except Exception as e:
            print(f"  {bc}: drift re-anchor ERROR {e}")
    if healed:
        alerting.warning("pool_remediation.drift_reanchored",
                         f"{healed} engine pools with uniform Q-drift re-anchored from live truth",
                         {"healed": healed})
    return healed


def main():
    db = SessionLocal()
    try:
        # ALL rollback reasons qualify (await_operator_resolution from the 2026-07-14 remediation,
        # plus the auto-rollbacks tripped during the cascade chaos: repeated_cas_conflict,
        # oscillation, canary_exception, write_amplification). The recovery gate is the same for
        # every reason — and stronger than the reason itself: ALL live listings must AGREE and the
        # live-truth backfill must succeed before the pool returns to the engine. If instability
        # recurs, evaluate_canary_rollback trips the pool right back out.
        drift_healed = reanchor_uniform_drift(db)
        parked = [r[0] for r in db.execute(text(
            "SELECT barcode FROM pool_canary_rollbacks"
        )).fetchall()]
        if not parked:
            print(f"{datetime.now(timezone.utc).isoformat()} nothing parked — done "
                  f"(drift_healed={drift_healed})")
            return
        recovered, still = 0, 0
        for bc in parked:
            try:
                plan = pool_backfill.plan_backfill(db, bc)
                if plan["safe"]:
                    res = pool_backfill.backfill_pool_state_from_live_truth(
                        [bc], dry_run=False, operator_confirmed=True)
                    if res.get("backfilled"):
                        pool_canary.clear_rollback(db, bc)
                        recovered += 1
                        continue
                still += 1
            except Exception as e:
                still += 1
                print(f"  {bc}: ERROR {e}")
        print(f"{datetime.now(timezone.utc).isoformat()} recovered={recovered} still_parked={still}")
        if recovered:
            alerting.warning("pool_remediation.recovered",
                             f"{recovered} parked pools re-anchored from live truth and returned "
                             f"to the engine ({still} still parked)",
                             {"recovered": recovered, "still_parked": still})
    finally:
        db.close()


if __name__ == "__main__":
    main()
