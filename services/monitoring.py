# services/monitoring.py
"""
Scheduled health monitor (P2.3 observability). Runs periodically from the app scheduler and
turns the read-only diagnostics into a time series + alerts, so the system observes ITSELF
continuously — independent of any operator being attached.

It records a SYSTEM audit tick each run (queryable time series) and raises alerts when:
  - negative inventory exists / grows,
  - a barcode group has diverged beyond a threshold,
  - a propagation storm was tripped recently.
Read-only: it never mutates inventory.
"""
import os
from datetime import datetime, timezone, timedelta

from database import SessionLocal
from sqlalchemy import text
from services import diagnostics, alerting, audit_logger

DIVERGENCE_ALERT_SPREAD = int(os.getenv("MON_DIVERGENCE_SPREAD", "50"))
NEGATIVE_ALERT_THRESHOLD = int(os.getenv("MON_NEGATIVE_THRESHOLD", "1"))


def run_health_monitor():
    db = SessionLocal()
    try:
        summ = diagnostics.summary(db)
        neg = summ.get("negative_inventory", {}) or {}
        div = summ.get("divergence", {}) or {}
        dups = summ.get("duplicate_groups", {}) or {}

        # recent storm trips (last 15 min)
        recent_storms = db.execute(text("""
            SELECT count(*) FROM audit_logs
            WHERE action IN ('propagation_storm_tripped','propagation_blocked_oversized_delta')
              AND timestamp >= now() - interval '15 minutes'
        """)).scalar() or 0

        # time-series tick (always)
        audit_logger.log(
            category="SYSTEM", action="health_monitor_tick",
            message=(f"negatives={neg.get('levels', 0)} diverged={div.get('diverged_barcodes', 0)} "
                     f"dup_suspect={dups.get('suspect', 0)} dup_error={dups.get('confirmed_error', 0)} "
                     f"recent_storms={recent_storms}"),
            severity="INFO",
            details={"negative_inventory": neg, "divergence": div,
                     "duplicate_groups": dups, "recent_storms": int(recent_storms)},
        )

        # alerts
        neg_levels = int(neg.get("levels", 0) or 0)
        if neg_levels >= NEGATIVE_ALERT_THRESHOLD:
            alerting.warning("monitoring.negative_inventory",
                             f"{neg_levels} negative inventory levels across {neg.get('barcodes', 0)} barcodes (worst {neg.get('worst')})",
                             neg)

        worst_spread = int(div.get("worst_spread", 0) or 0)
        if worst_spread >= DIVERGENCE_ALERT_SPREAD:
            alerting.warning("monitoring.divergence",
                             f"{div.get('diverged_barcodes', 0)} diverged barcode groups; worst spread {worst_spread}",
                             div)

        if recent_storms:
            alerting.critical("monitoring.storm",
                              f"{recent_storms} storm/blocked-delta events in the last 15 min",
                              {"recent_storms": int(recent_storms)})

        # P2 lock health
        try:
            lk = diagnostics.lock_status()
            m = lk.get("metrics", {})
            if m.get("timeouts", 0) or m.get("errors", 0) or m.get("waits_over_1s", 0):
                alerting.warning("monitoring.lock_contention",
                                 f"distributed-lock contention: timeouts={m.get('timeouts')} "
                                 f"errors={m.get('errors')} waits>1s={m.get('waits_over_1s')}", lk)
        except Exception:
            pass

    except Exception as e:
        try:
            alerting.warning("monitoring.health_monitor", f"health monitor failed: {e}", {})
        except Exception:
            pass
    finally:
        db.close()
