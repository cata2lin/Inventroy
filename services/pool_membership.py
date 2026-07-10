# services/pool_membership.py
"""
P2 — POOL MEMBERSHIP MONITOR (read-mostly; the only write is healing a stale SLA flag).

Every other detector watches QUANTITIES. None watches the SHAPE of a pool — which (store, variant)
tuples make it up — yet that shape silently changes and breaks sync in ways quantity sweeps miss:

  • ORPHANED pool      — a multi-store pool shrank to < 2 canonical stores (a variant was soft-deleted,
                         a store disabled, a barcode edited). It can no longer sync (nothing to sync
                         against) and its pool_q becomes inert bookkeeping that otherwise alerts
                         CRITICAL forever via the live-truth SLA. We auto-clear that stale flag.
  • MEMBERSHIP SHRINK  — a store that was actively reporting for this barcode is no longer canonical
                         (dropped out). Its listing stops receiving convergence — a silent divergence.
  • CANONICAL FLIP     — within a store, the canonical variant (CANON_ORDER tie-break, pv.id ASC last)
                         changed to a DIFFERENT variant than the one recently observed. Writes now
                         target a different listing than sales come from.

It NEVER writes inventory. The single mutation is clearing `diverged_since` on an orphaned pool (a
safe heal of a flag no convergence can ever resolve). Everything else is audit + alert.
"""
import os
from typing import Dict, Any, List, Set

from sqlalchemy import text
from database import SessionLocal
from services import audit_logger, alerting, diagnostics


# A store counts as "recently active" for shrink/flip detection if it produced an observation within
# this window. Long enough to span normal restock cadence, short enough to ignore ancient history.
MEMBERSHIP_ACTIVE_DAYS = int(os.getenv("POOL_MEMBERSHIP_ACTIVE_DAYS", "30"))


def _canonical_membership(db) -> Dict[str, Dict[int, int]]:
    """barcode -> {store_id: canonical_variant_id} for every barcode that has a PoolState, using the
    SAME canonical ordering convergence targets (so we monitor exactly what the engine writes to)."""
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (pv.barcode, pv.store_id) pv.barcode, pv.store_id, pv.id AS variant_id
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode IS NOT NULL AND length(pv.barcode) > 0 AND pv.inventory_item_id IS NOT NULL
          AND pv.barcode IN (SELECT barcode FROM pool_states)
        ORDER BY pv.barcode, pv.store_id, {diagnostics.CANON_ORDER}
    """)).mappings().all()
    out: Dict[str, Dict[int, int]] = {}
    for r in rows:
        out.setdefault(r["barcode"], {})[r["store_id"]] = r["variant_id"]
    return out


def _recent_observed_membership(db, days: int) -> Dict[str, Dict[int, int]]:
    """barcode -> {store_id: most-recently-observed variant_id} from real observations in the window."""
    rows = db.execute(text("""
        SELECT DISTINCT ON (barcode, source_store_id) barcode, source_store_id, source_variant_id
        FROM pool_events
        WHERE kind = 'observation' AND source_store_id IS NOT NULL AND source_variant_id IS NOT NULL
          AND created_at >= now() - (:days || ' days')::interval
        ORDER BY barcode, source_store_id, event_id DESC
    """), {"days": days}).mappings().all()
    out: Dict[str, Dict[int, int]] = {}
    for r in rows:
        out.setdefault(r["barcode"], {})[r["source_store_id"]] = r["source_variant_id"]
    return out


def _all_pool_barcodes(db) -> List[str]:
    return [r[0] for r in db.execute(text("SELECT barcode FROM pool_states")).fetchall()]


def _clear_orphan_flags(db, barcodes: List[str]) -> int:
    """Heal: clear diverged_since on orphaned pools (a flag no convergence can ever resolve)."""
    if not barcodes:
        return 0
    res = db.execute(text("""
        UPDATE pool_states SET diverged_since = NULL
        WHERE barcode = ANY(:bcs) AND diverged_since IS NOT NULL
    """), {"bcs": barcodes})
    db.commit()
    return res.rowcount or 0


def _false_group_barcodes(db) -> List[str]:
    """Pool barcodes shared by >1 distinct PRODUCT class (diagnostics.count_sku_classes) — unsyncable
    FALSE groups. One aggregate query, classified in memory."""
    rows = db.execute(text("""
        SELECT pv.barcode, array_agg(DISTINCT btrim(pv.sku) ORDER BY btrim(pv.sku)) skus
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode IN (SELECT barcode FROM pool_states)
          AND pv.inventory_item_id IS NOT NULL AND pv.sku IS NOT NULL AND btrim(pv.sku) <> ''
        GROUP BY pv.barcode
    """)).mappings().all()
    return [r["barcode"] for r in rows if diagnostics.count_sku_classes(r["skus"]) > 1]


def _recently_logged(db, action: str, barcode: str, hours: int = 6) -> bool:
    """Dedup helper: True if this (action, barcode) was already audited within the window — keeps a
    standing condition from spamming one WARN per sweep (the canonical-flip 48-rows/day problem)."""
    return db.execute(text("""
        SELECT 1 FROM audit_logs WHERE action = :a AND target = :b
          AND timestamp >= now() - (:h || ' hours')::interval LIMIT 1
    """), {"a": action, "b": barcode, "h": hours}).first() is not None


def run_membership_sweep() -> Dict[str, Any]:
    """Scheduled entrypoint. Audits pool SHAPE churn; heals stale SLA flags on orphaned pools. The only
    write is clearing diverged_since on orphans — never inventory."""
    db = SessionLocal()
    try:
        canon = _canonical_membership(db)
        recent = _recent_observed_membership(db, MEMBERSHIP_ACTIVE_DAYS)
        pool_barcodes = _all_pool_barcodes(db)

        orphaned: List[Dict[str, Any]] = []
        shrunk: List[Dict[str, Any]] = []
        flipped: List[Dict[str, Any]] = []

        for bc in pool_barcodes:
            members = canon.get(bc, {})            # {store_id: variant_id}; absent => 0 canonical stores
            stores: Set[int] = set(members.keys())
            if len(stores) < 2:
                orphaned.append({"barcode": bc, "stores": sorted(stores)})
                continue
            recent_members = recent.get(bc, {})
            recent_stores = set(recent_members.keys())
            # SHRINK: a store that recently reported is no longer canonical for this barcode.
            dropped = sorted(recent_stores - stores)
            if dropped:
                shrunk.append({"barcode": bc, "dropped_stores": dropped, "current_stores": sorted(stores)})
            # FLIP: a store's canonical variant differs from the variant it was recently observed on.
            flips = [{"store_id": sid, "canonical_variant": members[sid], "observed_variant": recent_members[sid]}
                     for sid in (stores & recent_stores) if members[sid] != recent_members[sid]]
            if flips:
                flipped.append({"barcode": bc, "flips": flips})

        healed = _clear_orphan_flags(db, [o["barcode"] for o in orphaned])

        # MULTI-PRODUCT-SKU pools — REPORT ONLY (2026-07-10 policy: the barcode is the intentional
        # sync key; these pools SYNC like any other via the per-listing engine). The classifier stays
        # as observability: distinct SKU classes on one barcode is usually deliberate pooling, but is
        # occasionally a barcode typo — worth a WARN so an operator can eyeball it. Never gates sync.
        false_groups = _false_group_barcodes(db)
        deauthorized = 0
        for bc in false_groups:
            if not _recently_logged(db, "pool_membership_multi_sku_pool", bc, hours=24):
                audit_logger.log(category="RECONCILIATION", action="pool_membership_multi_sku_pool",
                                 message=f"[{bc}] barcode pools multiple distinct SKU classes "
                                         f"(intentional pooling or a barcode typo?) — syncing normally",
                                 target=bc, severity="INFO", details={"barcode": bc})

        for s in shrunk:
            if _recently_logged(db, "pool_membership_shrink", s["barcode"], hours=6):
                continue
            audit_logger.log(category="RECONCILIATION", action="pool_membership_shrink",
                             message=f"[{s['barcode']}] store(s) {s['dropped_stores']} dropped out of the "
                                     f"pool (recently active, no longer canonical) — silent divergence risk",
                             target=s["barcode"], severity="WARN", details=s)
        for f in flipped:
            if _recently_logged(db, "pool_membership_canonical_flip", f["barcode"], hours=6):
                continue
            audit_logger.log(category="RECONCILIATION", action="pool_membership_canonical_flip",
                             message=f"[{f['barcode']}] canonical variant flipped vs recently-observed "
                                     f"variant — convergence may target a different listing than sales",
                             target=f["barcode"], severity="WARN", details=f)

        if shrunk or flipped:
            alerting.warning("pool_membership.churn",
                             f"Pool membership churn: {len(shrunk)} shrink, {len(flipped)} canonical-flip "
                             f"(multi-SKU pools={len(false_groups)}, orphaned={len(orphaned)}, "
                             f"healed {healed} stale flags).",
                             {"shrink": len(shrunk), "flip": len(flipped), "orphaned": len(orphaned),
                              "multi_sku_pools": false_groups[:15],
                              "examples": [s["barcode"] for s in (shrunk + flipped)][:10]})

        audit_logger.log(category="SYSTEM", action="pool_membership_sweep",
                         message=f"Membership sweep: {len(pool_barcodes)} pools; orphaned={len(orphaned)} "
                                 f"(healed {healed} stale flags), shrink={len(shrunk)}, flip={len(flipped)}, "
                                 f"multi-SKU pools={len(false_groups)} (report-only)",
                         severity="INFO",
                         details={"pools": len(pool_barcodes), "orphaned": len(orphaned),
                                  "healed_flags": healed, "shrink": len(shrunk), "flip": len(flipped),
                                  "multi_sku_pools": false_groups[:30],
                                  "orphaned_examples": [o["barcode"] for o in orphaned[:20]]})
        return {"pools": len(pool_barcodes), "orphaned": len(orphaned), "healed_flags": healed,
                "shrink": len(shrunk), "flip": len(flipped),
                "multi_sku_pools": false_groups,
                "orphaned_barcodes": [o["barcode"] for o in orphaned]}
    except Exception as e:
        try:
            alerting.warning("pool_membership.sweep", f"membership sweep failed: {e}", {})
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        db.close()
