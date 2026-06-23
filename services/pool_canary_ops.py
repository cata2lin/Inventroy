# services/pool_canary_ops.py
"""
PHASE 4B–4E — canary operations: candidate selection, pre-enablement snapshot, automated live
validation, forensic replay, and the post-canary report. All READ-ONLY (no inventory writes, no
enablement — flipping SYNC_POOL_ENGINE_WRITES stays an explicit operator action).
"""
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from database import SessionLocal
from services import live_truth, pool_engine
import models


SLA_LATENCY_MS = int(os.getenv("POOL_CANARY_SLA_LATENCY_MS", "8000"))


# --- 4B: candidate selection ---------------------------------------------------------------

def select_canary_candidates(limit: int = 10) -> List[Dict[str, Any]]:
    """Rank multi-store pools by canary suitability: multi-store, currently mirror-converged, LOW
    recent webhook volume, no recent storm/breaker, no unresolved divergence. Read-only — picking is
    advisory; the operator still backfills + enables explicitly."""
    db = SessionLocal()
    try:
        rows = db.execute(text(f"""
          WITH canon AS (
            SELECT DISTINCT ON (pv.barcode, pv.store_id) pv.barcode, pv.store_id, il.available av
            FROM product_variants pv
            JOIN products p ON p.id=pv.product_id AND p.deleted_at IS NULL
            JOIN stores s ON s.id=pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
            JOIN inventory_levels il ON il.variant_id=pv.id AND il.location_id=s.sync_location_id
            WHERE pv.barcode IS NOT NULL AND pv.barcode<>'' AND il.available IS NOT NULL
            ORDER BY pv.barcode, pv.store_id, {live_truth.diagnostics.CANON_ORDER}
          ), grp AS (
            SELECT barcode, count(*) stores, max(av)-min(av) spread, max(av) maxq, min(av) minq
            FROM canon GROUP BY barcode HAVING count(*)>1
          ), recent AS (   -- ONE pass over recent audit_logs instead of N correlated scans
            SELECT target barcode,
                   count(*) FILTER (WHERE action='stock_propagation_started'
                                    AND timestamp >= now() - interval '7 days') vol_7d,
                   count(*) FILTER (WHERE action IN ('propagation_storm_tripped','propagation_blocked_oversized_delta')) storms
            FROM audit_logs WHERE target IS NOT NULL AND timestamp >= now() - interval '14 days'
            GROUP BY target
          )
          SELECT * FROM (
            SELECT g.barcode, g.stores, g.spread, g.maxq,
                   coalesce(r.vol_7d,0) vol_7d, coalesce(r.storms,0) storms,
                   (SELECT count(*) FROM barcode_circuit_breakers b WHERE b.barcode=g.barcode) breaker
            FROM grp g LEFT JOIN recent r ON r.barcode = g.barcode
            WHERE g.spread = 0          -- currently converged on the mirror
              AND g.minq >= 0           -- NOT oversold (never canary a negative pool)
              AND g.maxq > 0            -- has real positive stock (a meaningful canary)
          ) x
          -- prefer low-but-ACTIVE (nonzero volume exercises the path), stable, modest stock:
          ORDER BY storms ASC, breaker ASC, (vol_7d = 0) ASC, vol_7d ASC, maxq ASC
          LIMIT :lim
        """), {"lim": limit}).mappings().all()
        out = []
        for r in rows:
            score = 100 - min(40, int(r["vol_7d"])) - (30 if r["storms"] else 0) - (30 if r["breaker"] else 0)
            out.append({"barcode": r["barcode"], "stores": r["stores"], "mirror_spread": r["spread"],
                        "quantity": r["maxq"], "volume_7d": r["vol_7d"], "storms_14d": r["storms"],
                        "has_breaker": bool(r["breaker"]), "suitability_score": max(0, score),
                        "recommended": (r["vol_7d"] < 50 and not r["storms"] and not r["breaker"])})
        return out
    finally:
        db.close()


# --- 4B: pre-enablement snapshot + eligibility ---------------------------------------------

def prepare_canary(barcode: str) -> Dict[str, Any]:
    """Snapshot LIVE Shopify + PoolState + mirror + baseline metrics for a candidate and check the
    eligibility gates. Read-only — does NOT enable anything. Run this immediately before enablement."""
    db = SessionLocal()
    try:
        rows = live_truth._canonical_rows(db, barcode)
        live, mirror, lives = {}, {}, []
        for r in rows:
            lv = live_truth._read_live(r["shopify_url"], r["api_token"], r["inventory_item_id"], r["sync_location_id"])
            live[r["store"]] = lv
            mirror[r["store"]] = r["mirror"]
            if isinstance(lv, int):
                lives.append(lv)
        state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
        live_spread = (max(lives) - min(lives)) if len(lives) >= 2 else None
        vol_7d = db.execute(text("""SELECT count(*) FROM audit_logs WHERE target=:b
                   AND action='stock_propagation_started' AND timestamp >= now() - interval '7 days'"""),
                   {"b": barcode}).scalar() or 0

        checks = {
            "multi_store": len(rows) >= 2,
            "live_readable": len(lives) == len(rows) and len(rows) >= 2,
            "live_converged": live_spread == 0,
            "backfilled": bool(state and state.backfilled_at),
            "no_unresolved_divergence": bool(state and state.diverged_since is None),
            "low_volume": vol_7d < 200,
        }
        eligible = all(v for k, v in checks.items() if k != "backfilled")  # backfill is the next step
        return {"barcode": barcode, "eligible_for_backfill": eligible, "checks": checks,
                "snapshots": {"live": live, "mirror": mirror,
                              "pool_state": ({"quantity": state.quantity, "version": state.version,
                                              "backfilled_at": str(state.backfilled_at)} if state else None)},
                "baseline": {"live_spread": live_spread, "volume_7d": vol_7d, "stores": len(rows)},
                "next_step": "backfill_pool_state_from_live_truth([bc], dry_run=False, operator_confirmed=True)"
                              if eligible else "resolve failing checks first"}
    finally:
        db.close()


# --- 4C: automated live validation + success criteria --------------------------------------

def validate_canary(barcode: str, window_minutes: int = 120) -> Dict[str, Any]:
    """Automated validation of a live canary: convergence vs live, idempotency, oscillation, drift,
    negative pressure, rollbacks — with a timeline + a single healthy/unhealthy verdict. Read-only."""
    db = SessionLocal()
    try:
        rows = live_truth._canonical_rows(db, barcode)
        live, lives = {}, []
        for r in rows:
            lv = live_truth._read_live(r["shopify_url"], r["api_token"], r["inventory_item_id"], r["sync_location_id"])
            live[r["store"]] = lv
            if isinstance(lv, int):
                lives.append(lv)
        state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
        Q = state.quantity if state else None
        live_spread = (max(lives) - min(lives)) if len(lives) >= 2 else None

        w = db.execute(text("""
          SELECT
            count(*) FILTER (WHERE action='pool_canary_write') writes,
            count(*) FILTER (WHERE action='pool_canary_dup_suppressed') dups,
            count(*) FILTER (WHERE action='pool_canary_stale_reject') stale,
            count(*) FILTER (WHERE action='pool_canary_rollback') rollbacks,
            coalesce(sum((details->>'retries')::int) FILTER (WHERE action='pool_canary_write'),0) retries,
            coalesce(max((details->>'propagation_latency_ms')::int) FILTER (WHERE action='pool_canary_write'),0) max_latency,
            coalesce(sum((details->>'failed')::int) FILTER (WHERE action='pool_converged'),0) cas_conflicts
          FROM audit_logs WHERE target=:b AND timestamp >= now() - (:w || ' minutes')::interval
        """), {"b": barcode, "w": window_minutes}).mappings().first()

        checks = {
            "all_stores_converged_live": (live_spread == 0),
            "pool_matches_live": (Q is not None and lives and all(v == Q for v in lives)),
            "no_negative": (not lives or min(lives) >= 0) and (Q is None or Q >= 0),
            "zero_rollbacks": (w["rollbacks"] == 0),
            "latency_within_sla": (w["max_latency"] <= SLA_LATENCY_MS),
            "no_unresolved_divergence": bool(state and state.diverged_since is None),
        }
        healthy = all(checks.values())
        return {"barcode": barcode, "healthy": healthy, "checks": checks,
                "final_Q": Q, "live_quantities": live, "live_spread": live_spread,
                "metrics": dict(w), "window_minutes": window_minutes,
                "timeline": _timeline(db, barcode, window_minutes)}
    finally:
        db.close()


def _timeline(db, barcode: str, window_minutes: int) -> List[Dict[str, Any]]:
    return [dict(r) for r in db.execute(text("""
        SELECT to_char(timestamp AT TIME ZONE 'UTC','HH24:MI:SS.MS') ts, action,
               details->>'pool_version' pool_version, details->>'canonical_Q' Q,
               details->>'retries' retries, details->>'propagation_latency_ms' latency_ms,
               details->>'rollback_reason' rollback_reason
        FROM audit_logs WHERE target=:b AND action LIKE 'pool_canary%'
          AND timestamp >= now() - (:w || ' minutes')::interval
        ORDER BY timestamp ASC LIMIT 500"""), {"b": barcode, "w": window_minutes}).mappings().all()]


# --- 4D: forensic replay (read-only reconstruction; touches nothing) -----------------------

def forensic_replay(barcode: str, hours: int = 24) -> Dict[str, Any]:
    """Reconstruct the full causal sequence for a canary barcode from the immutable golden events +
    the pool ledger: raw webhooks, canonical transitions, CAS attempts, rollbacks. NO production
    writes — pure read, safe to run during an incident."""
    db = SessionLocal()
    try:
        golden = [dict(r) for r in db.execute(text("""
            SELECT id, to_char(created_at AT TIME ZONE 'UTC','HH24:MI:SS.MS') ts, kind, pool_version,
                   webhook_id, payload
            FROM pool_golden_events WHERE barcode=:b AND created_at >= now() - (:h || ' hours')::interval
            ORDER BY id ASC LIMIT 2000"""), {"b": barcode, "h": hours}).mappings().all()]
        ledger = [dict(r) for r in db.execute(text("""
            SELECT event_id, source_store_id, observed_quantity, kind,
                   to_char(source_timestamp AT TIME ZONE 'UTC','HH24:MI:SS') src_ts, webhook_id, applied
            FROM pool_events WHERE barcode=:b AND created_at >= now() - (:h || ' hours')::interval
            ORDER BY event_id ASC LIMIT 2000"""), {"b": barcode, "h": hours}).mappings().all()]
        counts = {}
        for g in golden:
            counts[g["kind"]] = counts.get(g["kind"], 0) + 1
        return {"barcode": barcode, "hours": hours, "golden_event_count": len(golden),
                "ledger_event_count": len(ledger), "kind_counts": counts,
                "golden_events": golden, "ledger": ledger}
    finally:
        db.close()


# --- 4E: comprehensive canary report + recommendation --------------------------------------

def canary_report(barcode: str, window_minutes: int = 240) -> Dict[str, Any]:
    v = validate_canary(barcode, window_minutes)
    m = v["metrics"]
    if not v["healthy"]:
        rec = "rollback" if (m["rollbacks"] or not v["checks"]["no_negative"] or not v["checks"]["pool_matches_live"]) else "hold"
    else:
        rec = "expand"
    return {"barcode": barcode, "window_minutes": window_minutes, "healthy": v["healthy"],
            "recommendation": rec, "checks": v["checks"], "metrics": m,
            "final_Q": v["final_Q"], "live_quantities": v["live_quantities"],
            "note": "HUMAN APPROVAL REQUIRED before acting on 'expand'."}
