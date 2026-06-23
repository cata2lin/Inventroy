# services/pool_ops.py
"""
PHASE 3C — observability / operations data layer for the pool engine.

Read-only aggregations over the audit trail + pool tables that power the operational views:
canary pool health, divergence timeline, rollback events, convergence SLA, live-vs-canonical diff,
and a headline metrics summary. The web dashboard is a thin consumer of these functions (exposed at
/api/diagnostics/pool/* ). No writes.
"""
from typing import Dict, Any, List
from sqlalchemy import text
from database import SessionLocal
from services import pool_engine, pool_canary


def metrics_summary(hours: int = 24) -> Dict[str, Any]:
    """Headline metrics over the window: convergence success rate, CAS retries, divergence rate,
    rollback count, live-truth mismatch count, latency."""
    db = SessionLocal()
    try:
        row = db.execute(text("""
          SELECT
            count(*) FILTER (WHERE action='pool_canary_write')                              canary_writes,
            count(*) FILTER (WHERE action='pool_canary_write' AND (details->>'rollback_reason') IS NOT NULL) rolled_back_writes,
            count(*) FILTER (WHERE action='pool_canary_dup_suppressed')                      dup_suppressed,
            count(*) FILTER (WHERE action='pool_canary_stale_reject')                        stale_rejects,
            count(*) FILTER (WHERE action='pool_canary_rollback')                            rollbacks,
            count(*) FILTER (WHERE action='pool_validation_diverged')                        live_diverged_events,
            coalesce(sum((details->>'retries')::int) FILTER (WHERE action='pool_canary_write'), 0) cas_retries,
            coalesce(avg((details->>'propagation_latency_ms')::numeric) FILTER (WHERE action='pool_canary_write'), 0) avg_latency_ms,
            coalesce(sum((details->>'failed')::int) FILTER (WHERE action='pool_converged'), 0) cas_conflicts
          FROM audit_logs WHERE timestamp >= now() - (:h || ' hours')::interval
        """), {"h": hours}).mappings().first()
        m = dict(row)
        writes = m.get("canary_writes") or 0
        clean = writes - (m.get("rolled_back_writes") or 0)
        m["convergence_success_rate"] = round(clean / writes, 4) if writes else None
        m["window_hours"] = hours
        return m
    finally:
        db.close()


def canary_health() -> List[Dict[str, Any]]:
    """Per-barcode health for everything currently in (or recently exercised by) the canary path."""
    db = SessionLocal()
    try:
        active = sorted(pool_engine.canary_barcodes())
        rolled = {r[0]: r[1] for r in db.execute(text(
            "SELECT barcode, reason FROM pool_canary_rollbacks")).fetchall()}
        out = []
        # union of: configured canary list + any barcode with recent canary activity
        recent = [r[0] for r in db.execute(text("""
            SELECT DISTINCT target FROM audit_logs
            WHERE action IN ('pool_canary_write','pool_canary_rollback') AND target IS NOT NULL
              AND timestamp >= now() - interval '24 hours'""")).fetchall()]
        for bc in sorted(set(active) | set(recent) | set(rolled)):
            st = db.execute(text("SELECT quantity, version, backfilled_at, diverged_since FROM pool_states WHERE barcode=:b"),
                            {"b": bc}).mappings().first()
            stats = db.execute(text("""
                SELECT count(*) writes,
                       coalesce(sum((details->>'retries')::int),0) retries,
                       coalesce(sum((details->>'failed')::int),0) cas_conflicts,
                       coalesce(avg((details->>'propagation_latency_ms')::numeric),0) avg_latency_ms,
                       max(timestamp) last_write
                FROM audit_logs WHERE action='pool_canary_write' AND target=:b
                  AND timestamp >= now() - interval '24 hours'""", ), {"b": bc}).mappings().first()
            writes = stats["writes"] or 0
            score = 100
            if bc in rolled: score = 0
            elif writes:
                score -= min(60, int((stats["cas_conflicts"] or 0) * 20))
                score -= min(20, int((stats["retries"] or 0)))
            out.append({
                "barcode": bc,
                "in_canary_list": bc in active,
                "rolled_back": bc in rolled,
                "rollback_reason": rolled.get(bc),
                "backfilled": bool(st and st["backfilled_at"]),
                "write_eligible": bool(st and st["backfilled_at"]) and bc in active and bc not in rolled
                                  and pool_engine.pool_writes_enabled(),
                "pool_quantity": st["quantity"] if st else None,
                "pool_version": st["version"] if st else None,
                "diverged_since": st["diverged_since"].isoformat() if (st and st["diverged_since"]) else None,
                "writes_24h": writes, "cas_conflicts_24h": stats["cas_conflicts"],
                "retries_24h": stats["retries"],
                "avg_latency_ms": float(stats["avg_latency_ms"] or 0),
                "health_score": max(0, score),
            })
        return out
    finally:
        db.close()


def rollback_events(limit: int = 50) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        return [dict(r) for r in db.execute(text("""
            SELECT to_char(timestamp AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') ts, target barcode,
                   details->>'reason' reason, severity, message
            FROM audit_logs WHERE action IN ('pool_canary_rollback','pool_canary_rollback_cleared')
            ORDER BY timestamp DESC LIMIT :l"""), {"l": limit}).mappings().all()]
    finally:
        db.close()


def convergence_sla() -> List[Dict[str, Any]]:
    """Pools breaching the live-truth convergence SLA (diverged on live beyond POOL_SLA_HOURS)."""
    db = SessionLocal()
    try:
        return [dict(r) for r in db.execute(text("""
            SELECT barcode, quantity pool_quantity, version,
                   to_char(diverged_since AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') diverged_since,
                   extract(epoch FROM (now() - diverged_since))::int unresolved_seconds
            FROM pool_states WHERE diverged_since IS NOT NULL
            ORDER BY diverged_since ASC LIMIT 200"""), {}).mappings().all()]
    finally:
        db.close()


def live_vs_canonical(limit: int = 50) -> List[Dict[str, Any]]:
    """Most recent Phase-2 reports where the engine's Q disagreed with live Shopify."""
    db = SessionLocal()
    try:
        return [dict(r) for r in db.execute(text("""
            SELECT to_char(timestamp AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') ts, target barcode,
                   details->>'pool_quantity' pool_quantity, details->>'spread' spread,
                   details->'canonical_drift' canonical_drift
            FROM audit_logs WHERE action='pool_validation_diverged'
            ORDER BY timestamp DESC LIMIT :l"""), {"l": limit}).mappings().all()]
    finally:
        db.close()


def dashboard() -> Dict[str, Any]:
    """One call powering the operational dashboard."""
    return {
        "flags": {
            "writes_enabled": pool_engine.pool_writes_enabled(),
            "shadow": pool_engine.pool_shadow_enabled(),
            "canary_barcodes": sorted(pool_engine.canary_barcodes()),
        },
        "metrics": metrics_summary(),
        "canary_health": canary_health(),
        "rollback_events": rollback_events(),
        "convergence_sla": convergence_sla(),
        "live_vs_canonical": live_vs_canonical(),
    }
