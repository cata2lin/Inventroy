# services/diagnostics.py
"""
Read-ONLY remediation & observability diagnostics.

Every function in this module only SELECTs — it never writes to or mutates production
inventory, never deletes/merges products, never changes barcodes. It classifies and
reports so a human can decide. The persisted classification/quarantine *workflow*
(applying decisions) is a separate, explicitly-confirmed step (P1).

Detectors:
  scan_duplicate_barcode_groups : intra-store duplicate barcodes + heuristic classification
  detect_divergence             : barcode groups whose stores disagree on quantity
  detect_negative_inventory     : inventory levels below the floor
  detect_historical_storms      : barcodes whose audit history shows propagation storms
  detect_impossible_states      : propagations that drove stock negative (corruption signature)
  summary                       : one-call rollup for dashboards/alerts
"""
from typing import Dict, Any, List
from sqlalchemy import text
from sqlalchemy.orm import Session

# Heuristic classification proposals (NOT auto-applied — for human review)
VALID_SHARED = "VALID_SHARED"          # same SKU repeated — plausibly intentional
SUSPECT_DUPLICATE = "SUSPECT_DUPLICATE"  # different SKUs share one barcode in a store — review
CONFIRMED_ERROR = "CONFIRMED_ERROR"    # different SKUs + the group has diverged/gone negative

_PLACEHOLDERS = ("0", "00", "000", "0000", "00000", "000000", "0000000", "00000000",
                 "000000000", "0000000000", "00000000000", "000000000000", "0000000000000")


def _placeholder_sql(col: str) -> str:
    items = ",".join(f"'{p}'" for p in _PLACEHOLDERS)
    return f"({col} IS NOT NULL AND btrim({col}) <> '' AND {col} NOT IN ({items}))"


# --------------------------------------------------------------------------------------------------
# FALSE-GROUP CLASSIFIER — same barcode, genuinely DIFFERENT products (2026-07-10 pijama incident)
# --------------------------------------------------------------------------------------------------
# A barcode pool is only syncable if every member is the SAME physical product. SKU evidence decides:
#   • identical SKUs                       -> same product
#   • store-prefixed SKUs (`zn-127`/`127`) -> same product (one is '-'-suffix of the other); this
#     fleet has ~120 legit pools named that way, so naive "different SKU = different product" is WRONG
#   • anything else (`negru-4XL`/`negru-5XL`, `HA-0061-1`/`HA-0061M`, `oglinda`/`oglinda-acrilica`)
#     -> DIFFERENT products = a FALSE group. One pool number cannot represent two physical stocks:
#     the engine folds both variants' webhooks into one per-store baseline and manufactures phantom
#     deltas (a 5XL sale 3->2 against a 4XL baseline of 202 folded as -200 and clobbered the restock).
# Empty SKUs carry no evidence and are ignored.

def sku_equivalent(a: str, b: str) -> bool:
    """True if two non-empty SKUs plausibly denote the SAME product: identical, or one is the other
    with a store prefix (equal after stripping a leading '<prefix>-'), i.e. '-'-suffix match.
    Case-insensitive: 'ZN-127' and 'zn-127' are the same SKU, never a reason to quarantine."""
    a, b = a.strip().casefold(), b.strip().casefold()
    if a == b:
        return True
    return a.endswith("-" + b) or b.endswith("-" + a)


def count_sku_classes(skus) -> int:
    """Number of distinct PRODUCT classes among the given SKUs under sku_equivalent (transitive via
    shared members: {'127','zn-127','gt-127'} is ONE class). Empty/None SKUs are ignored."""
    vals = [s.strip() for s in (skus or []) if s and s.strip()]
    classes: List[List[str]] = []
    for s in vals:
        merged = None
        for cl in classes:
            if any(sku_equivalent(s, m) for m in cl):
                cl.append(s)
                merged = cl
                break
        if merged is None:
            classes.append([s])
    return len(classes)


def group_skus(db: Session, barcode: str) -> List[str]:
    """Distinct non-empty SKUs on this barcode across enabled, non-deleted, synced-store variants."""
    return [r[0] for r in db.execute(text("""
        SELECT DISTINCT btrim(pv.sku) FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
          AND pv.sku IS NOT NULL AND btrim(pv.sku) <> ''
    """), {"b": barcode}).fetchall()]


def is_false_barcode_group(db: Session, barcode: str) -> bool:
    """True if this barcode is shared by more than one distinct PRODUCT (SKU class) — an unsyncable
    FALSE group. Such a pool must never be folded, converged, or propagated: quarantine + report."""
    return count_sku_classes(group_skus(db, barcode)) > 1


def scan_duplicate_barcode_groups(db: Session, limit: int = 500) -> List[Dict[str, Any]]:
    """Intra-store duplicate barcodes (same barcode on >1 live variant in one store), with a
    PROPOSED classification + evidence. Different SKUs sharing a barcode is the cascade-prone
    pattern (the Șezlong HA-0901/0902/0903 case)."""
    rows = db.execute(text(f"""
        SELECT pv.store_id, s.name AS store, pv.barcode,
               count(*) AS variant_count,
               count(DISTINCT NULLIF(pv.sku,'')) AS distinct_skus,
               array_agg(DISTINCT NULLIF(pv.sku,'')) AS skus,
               array_agg(pv.id) AS variant_ids,
               min(COALESCE(il.available, 0)) AS min_avail,
               max(COALESCE(il.available, 0)) AS max_avail
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE {_placeholder_sql('pv.barcode')}
        GROUP BY pv.store_id, s.name, pv.barcode
        HAVING count(*) > 1
        ORDER BY count(*) DESC, count(DISTINCT NULLIF(pv.sku,'')) DESC
        LIMIT :limit
    """), {"limit": limit}).mappings().all()

    out = []
    for r in rows:
        diverged_or_negative = (r["min_avail"] != r["max_avail"]) or (r["min_avail"] < 0)
        if r["distinct_skus"] and r["distinct_skus"] > 1:
            proposed = CONFIRMED_ERROR if diverged_or_negative else SUSPECT_DUPLICATE
        else:
            proposed = VALID_SHARED
        out.append({
            "store_id": r["store_id"], "store": r["store"], "barcode": r["barcode"],
            "variant_count": r["variant_count"], "distinct_skus": r["distinct_skus"],
            "skus": [x for x in (r["skus"] or []) if x], "variant_ids": r["variant_ids"],
            "min_available": r["min_avail"], "max_available": r["max_avail"],
            "proposed_classification": proposed,
        })
    return out


# Canonical-variant ordering — MUST match services.sync_guards._canonical_rank so the
# detectors measure the SAME variant propagation actually targets.
CANON_ORDER = ("pv.is_barcode_primary DESC, pv.is_primary_variant DESC, "
               "(CASE WHEN NULLIF(pv.sku,'') IS NOT NULL THEN 0 ELSE 1 END), pv.id ASC")


def detect_divergence(db: Session, min_spread: int = 1, limit: int = 500) -> List[Dict[str, Any]]:
    """Barcode groups whose CANONICAL per-store quantities disagree by more than min_spread.
    Uses the canonical variant per (barcode,store) — not max() — so SKU-less orphan duplicates
    don't inflate the spread (they are a separate, classification problem)."""
    rows = db.execute(text(f"""
        WITH per_store AS (
            SELECT DISTINCT ON (pv.barcode, pv.store_id) pv.barcode, pv.store_id, il.available AS avail
            FROM product_variants pv
            JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
            JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
            JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
            WHERE {_placeholder_sql('pv.barcode')} AND il.available IS NOT NULL
            ORDER BY pv.barcode, pv.store_id, {CANON_ORDER}
        )
        SELECT barcode, count(*) AS stores, min(avail) AS lo, max(avail) AS hi,
               max(avail) - min(avail) AS spread
        FROM per_store
        GROUP BY barcode
        HAVING count(*) > 1 AND (max(avail) - min(avail)) >= :min_spread
        ORDER BY (max(avail) - min(avail)) DESC
        LIMIT :limit
    """), {"min_spread": min_spread, "limit": limit}).mappings().all()
    return [dict(r) for r in rows]


def detect_negative_inventory(db: Session, floor: int = 0, limit: int = 1000) -> Dict[str, Any]:
    rows = db.execute(text("""
        SELECT s.name AS store, pv.barcode, pv.sku, il.available,
               to_char(il.updated_at, 'YYYY-MM-DD HH24:MI') AS updated
        FROM inventory_levels il
        JOIN product_variants pv ON pv.id = il.variant_id
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id
        WHERE il.available < :floor
        ORDER BY il.available ASC
        LIMIT :limit
    """), {"floor": floor, "limit": limit}).mappings().all()
    agg = db.execute(text("""
        SELECT count(*) AS levels, count(DISTINCT pv.barcode) AS barcodes, min(il.available) AS worst
        FROM inventory_levels il
        JOIN product_variants pv ON pv.id = il.variant_id
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        WHERE il.available < :floor
    """), {"floor": floor}).mappings().first()
    return {"summary": dict(agg) if agg else {}, "rows": [dict(r) for r in rows]}


def detect_historical_storms(db: Session, window_minutes: int = 2, min_events: int = 8,
                             days: int = 14, limit: int = 200) -> List[Dict[str, Any]]:
    """Barcodes that had >= min_events propagation_started within any window_minutes bucket —
    the runaway-cascade signature (read from the audit trail)."""
    rows = db.execute(text("""
        SELECT target AS barcode,
               date_trunc('minute', timestamp) AS minute_bucket,
               count(*) AS events
        FROM audit_logs
        WHERE category='STOCK' AND action='stock_propagation_started'
          AND timestamp >= now() - (:days || ' days')::interval
        GROUP BY target, date_trunc('minute', timestamp)
        HAVING count(*) >= :min_events
        ORDER BY count(*) DESC
        LIMIT :limit
    """), {"days": days, "min_events": min_events, "limit": limit}).mappings().all()
    return [{"barcode": r["barcode"], "minute": str(r["minute_bucket"]), "events": r["events"]} for r in rows]


def detect_impossible_states(db: Session, days: int = 14, limit: int = 200) -> List[Dict[str, Any]]:
    """Propagation events that drove (or recorded) negative quantity — a corruption signature."""
    rows = db.execute(text("""
        SELECT target AS barcode, count(*) AS negative_propagations,
               min((details->>'quantity')::int) AS worst_qty,
               max(timestamp) AS last_seen
        FROM audit_logs
        WHERE category='STOCK' AND action IN ('stock_propagation_started','stock_propagated')
          AND timestamp >= now() - (:days || ' days')::interval
          AND (details->>'quantity') ~ '^-?[0-9]+$'
          AND (details->>'quantity')::int < 0
        GROUP BY target
        ORDER BY min((details->>'quantity')::int) ASC
        LIMIT :limit
    """), {"days": days, "limit": limit}).mappings().all()
    return [{"barcode": r["barcode"], "negative_propagations": r["negative_propagations"],
             "worst_qty": r["worst_qty"], "last_seen": str(r["last_seen"])} for r in rows]


def lock_status() -> Dict[str, Any]:
    """P2 lock observability: in-process contention metrics + advisory locks currently held."""
    from services import dist_lock
    return {"metrics": dist_lock.metrics(), "advisory_locks_held": dist_lock.held_count(),
            "enabled": dist_lock.DIST_LOCK_ENABLED}


def summary(db: Session) -> Dict[str, Any]:
    """One-call rollup for the monitoring job / dashboard."""
    dups = scan_duplicate_barcode_groups(db, limit=10000)
    div = detect_divergence(db, min_spread=1, limit=10000)
    neg = detect_negative_inventory(db, limit=1)
    return {
        "duplicate_groups": {
            "total": len(dups),
            "suspect": sum(1 for d in dups if d["proposed_classification"] == SUSPECT_DUPLICATE),
            "confirmed_error": sum(1 for d in dups if d["proposed_classification"] == CONFIRMED_ERROR),
            "valid_shared": sum(1 for d in dups if d["proposed_classification"] == VALID_SHARED),
        },
        "divergence": {
            "diverged_barcodes": len(div),
            "worst_spread": max((d["spread"] for d in div), default=0),
        },
        "negative_inventory": neg["summary"],
    }
