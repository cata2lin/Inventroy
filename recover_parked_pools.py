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


def main():
    db = SessionLocal()
    try:
        # ALL rollback reasons qualify (await_operator_resolution from the 2026-07-14 remediation,
        # plus the auto-rollbacks tripped during the cascade chaos: repeated_cas_conflict,
        # oscillation, canary_exception, write_amplification). The recovery gate is the same for
        # every reason — and stronger than the reason itself: ALL live listings must AGREE and the
        # live-truth backfill must succeed before the pool returns to the engine. If instability
        # recurs, evaluate_canary_rollback trips the pool right back out.
        parked = [r[0] for r in db.execute(text(
            "SELECT barcode FROM pool_canary_rollbacks"
        )).fetchall()]
        if not parked:
            print(f"{datetime.now(timezone.utc).isoformat()} nothing parked — done")
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
