# services/forensic.py
"""
P6 — forensic replay & timeline reconstruction. Read-ONLY: reconstructs the full event lineage
for a barcode or a sync_operation_uuid from the audit trail (webhooks, propagations, suppressed
echoes via lineage, reconciles, classification changes, guard trips). Supports complete forensic
replay of any incident.
"""
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session


def replay_barcode(db: Session, barcode: str, hours: int = 168, limit: int = 1000) -> Dict[str, Any]:
    """Chronological reconstruction of everything that touched a barcode."""
    events = db.execute(text("""
        SELECT to_char(timestamp AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') AS ts,
               category, action, severity, store_id, store_name,
               details->>'delta' AS delta, details->>'quantity' AS qty,
               details->>'last_known' AS last_known,
               left(details->>'sync_operation_uuid',8) AS op,
               details->>'mode' AS mode, substring(message,1,120) AS message
        FROM audit_logs
        WHERE target = :b AND timestamp >= now() - (:hours || ' hours')::interval
        ORDER BY timestamp ASC
        LIMIT :limit
    """), {"b": barcode, "hours": hours, "limit": limit}).mappings().all()

    counts = db.execute(text("""
        SELECT action, count(*) AS n FROM audit_logs
        WHERE target = :b AND timestamp >= now() - (:hours || ' hours')::interval
        GROUP BY action ORDER BY n DESC
    """), {"b": barcode, "hours": hours}).mappings().all()

    return {"barcode": barcode, "hours": hours, "event_count": len(events),
            "action_counts": {r["action"]: r["n"] for r in counts},
            "timeline": [dict(e) for e in events]}


def replay_operation(db: Session, sync_op_prefix: str) -> Dict[str, Any]:
    """All events belonging to one propagation/reconcile operation (by uuid or prefix)."""
    rows = db.execute(text("""
        SELECT to_char(timestamp AT TIME ZONE 'UTC','HH24:MI:SS.MS') AS ts,
               action, target AS barcode, store_name,
               details->>'delta' AS delta, substring(message,1,120) AS message
        FROM audit_logs
        WHERE details->>'sync_operation_uuid' LIKE :p
        ORDER BY timestamp ASC LIMIT 500
    """), {"p": sync_op_prefix + "%"}).mappings().all()
    return {"operation": sync_op_prefix, "event_count": len(rows), "events": [dict(r) for r in rows]}


def storm_window(db: Session, barcode: str, around_utc: str, window_minutes: int = 5) -> Dict[str, Any]:
    """Reconstruct a tight window around a known incident (for storm/cascade analysis)."""
    rows = db.execute(text("""
        SELECT to_char(timestamp AT TIME ZONE 'UTC','HH24:MI:SS.MS') AS ts, action, store_name,
               details->>'delta' AS delta, details->>'quantity' AS qty,
               details->>'variant_count' AS vc, substring(message,1,100) AS message
        FROM audit_logs
        WHERE target = :b
          AND timestamp BETWEEN (CAST(:t AS timestamptz) - (:w || ' minutes')::interval)
                            AND (CAST(:t AS timestamptz) + (:w || ' minutes')::interval)
        ORDER BY timestamp ASC LIMIT 1000
    """), {"b": barcode, "t": around_utc, "w": window_minutes}).mappings().all()
    return {"barcode": barcode, "center": around_utc, "events": [dict(r) for r in rows]}
