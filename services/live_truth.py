# services/live_truth.py
"""
Stage 0 — LIVE-TRUTH SWEEP (read-only, alert-only).

Every other divergence detector in this system reads the LOCAL MIRROR (inventory_levels), which is
written only AFTER a successful Shopify write. A dropped/missed/blocked write therefore never moves
the mirror, so mirror-based detection reports spread 0 while Shopify reality diverges — the failure
class that produces PERMANENT divergence is invisible (audit CRITICAL-1).

This sweep reads the ACTUAL `available` quantity from Shopify per canonical variant and compares the
LIVE values across stores. It NEVER writes inventory — it only audits + alerts, so it is safe to run
during live traffic. It is the ground-truth observability layer that Stage 2's reconciler will later
consume to drive absolute convergence.

Coverage per run is BOUNDED (API cost): it always checks every pool the mirror already flags as
diverged, plus a random sample of mirror-"converged" pools (to surface mirror-blind divergence).
Over repeated runs the random sample covers the catalog. Coverage is reported honestly each run.
"""
import os
import hashlib
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from database import SessionLocal
from shopify_service import ShopifyService
from services import audit_logger, alerting, diagnostics
import models


LIVE_SWEEP_MAX_READS = int(os.getenv("LIVE_SWEEP_MAX_READS", "300"))     # cap Shopify reads per run
LIVE_SWEEP_SAMPLE = int(os.getenv("LIVE_SWEEP_SAMPLE", "60"))            # mirror-converged pools sampled/run
LIVE_SWEEP_ALERT_SPREAD = int(os.getenv("LIVE_SWEEP_ALERT_SPREAD", "1")) # live spread to alert on


def _canonical_rows(db, barcode: str) -> List[Dict[str, Any]]:
    """The canonical variant per enabled store for a barcode (same canonical ordering the propagation
    + reconcile paths use), with the inventory_item_id + sync_location needed for a live Shopify read."""
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (pv.barcode, pv.store_id)
               pv.store_id, s.name AS store, s.shopify_url, s.api_token, s.sync_location_id,
               pv.inventory_item_id, il.available AS mirror
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.barcode, pv.store_id, {diagnostics.CANON_ORDER}
    """), {"b": barcode}).mappings().all()
    return [dict(r) for r in rows]


def _read_live(store_url: str, token: str, inventory_item_id: int, sync_location_id: int) -> Optional[int]:
    try:
        svc = ShopifyService(store_url=store_url, token=token)
        return svc.get_available_single(
            f"gid://shopify/InventoryItem/{inventory_item_id}",
            f"gid://shopify/Location/{sync_location_id}",
        )
    except Exception:
        return None


def _check_pool(db, barcode: str) -> Optional[Dict[str, Any]]:
    """Read every canonical variant of a pool LIVE from Shopify and return the live divergence picture.
    Returns None if fewer than 2 readable stores. Does NOT write anything."""
    rows = _canonical_rows(db, barcode)
    if len(rows) < 2:
        return None
    per_store, lives, reads = [], [], 0
    for r in rows:
        live = _read_live(r["shopify_url"], r["api_token"], r["inventory_item_id"], r["sync_location_id"])
        reads += 1
        per_store.append({"store": r["store"], "live": live, "mirror": r["mirror"],
                          "mirror_drift": (live != r["mirror"]) if live is not None else None})
        if isinstance(live, int):
            lives.append(live)
    if len(lives) < 2:
        return {"barcode": barcode, "reads": reads, "live_spread": None, "per_store": per_store, "unreadable": True}
    live_spread = max(lives) - min(lives)
    mirror_blind = (live_spread > 0) and len({d["mirror"] for d in per_store if d["mirror"] is not None}) <= 1
    return {"barcode": barcode, "reads": reads, "live_spread": live_spread,
            "live_min": min(lives), "live_max": max(lives), "per_store": per_store,
            "mirror_blind": mirror_blind}


def run_live_truth_sweep() -> Dict[str, Any]:
    """Scheduled entrypoint. Reads live Shopify quantities for diverged + sampled pools, audits the
    live divergence, and alerts on real (and especially mirror-BLIND) divergence. Read-only."""
    db = SessionLocal()
    try:
        # 1) Pools the mirror already flags as diverged — always confirm these against live.
        mirror_diverged = [d["barcode"] for d in diagnostics.detect_divergence(db, min_spread=1, limit=1000)]
        # 2) A random sample of OTHER multi-store pools — to surface mirror-blind divergence.
        sampled = [r[0] for r in db.execute(text("""
            SELECT pv.barcode FROM product_variants pv
            JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
            JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
            WHERE pv.barcode IS NOT NULL AND pv.barcode <> ''
            GROUP BY pv.barcode HAVING count(DISTINCT pv.store_id) > 1
            ORDER BY random() LIMIT :k
        """), {"k": LIVE_SWEEP_SAMPLE}).fetchall()]

        seen, targets = set(), []
        for b in (mirror_diverged + sampled):
            if b not in seen:
                seen.add(b)
                targets.append(b)

        checked, reads, live_diverged, mirror_blind_hits, worst = 0, 0, [], [], None
        from services import diagnostics as _diag
        for bc in targets:
            if reads >= LIVE_SWEEP_MAX_READS:
                break
            # FALSE GROUP: different products sharing a barcode legitimately hold different stocks —
            # "divergence" between them is meaningless and would page mirror-blind CRITICALs forever.
            try:
                if _diag.is_false_barcode_group(db, bc):
                    continue
            except Exception:
                db.rollback()
            res = _check_pool(db, bc)
            if res is None:
                continue
            checked += 1
            reads += res.get("reads", 0)
            sp = res.get("live_spread")
            if sp is not None and sp >= LIVE_SWEEP_ALERT_SPREAD:
                live_diverged.append(res)
                if worst is None or sp > worst.get("live_spread", 0):
                    worst = res
                if res.get("mirror_blind"):
                    mirror_blind_hits.append(res)
                audit_logger.log(
                    category="RECONCILIATION", action="live_divergence_detected",
                    message=f"[{bc}] LIVE spread={sp} (min={res['live_min']} max={res['live_max']}"
                            f"{' — MIRROR-BLIND' if res.get('mirror_blind') else ''})",
                    target=bc, severity="WARN",
                    details={"live_spread": sp, "mirror_blind": res.get("mirror_blind"),
                             "per_store": res["per_store"]})

        coverage = f"{checked} pools checked ({len(mirror_diverged)} mirror-diverged + sample), {reads} live reads"
        if mirror_blind_hits:
            alerting.critical("live_truth.mirror_blind",
                              f"{len(mirror_blind_hits)} pools diverge on LIVE Shopify while the mirror shows them "
                              f"converged (silent divergence). Worst: [{worst['barcode']}] spread {worst['live_spread']}.",
                              {"count": len(mirror_blind_hits),
                               "examples": [h["barcode"] for h in mirror_blind_hits[:10]]})
        elif live_diverged:
            alerting.warning("live_truth.divergence",
                             f"{len(live_diverged)} pools diverged on LIVE Shopify (worst "
                             f"[{worst['barcode']}] spread {worst['live_spread']}). {coverage}.",
                             {"count": len(live_diverged), "worst": worst["barcode"]})

        audit_logger.log(category="SYSTEM", action="live_truth_sweep",
                         message=f"Live-truth sweep: {coverage}; live-diverged={len(live_diverged)}, "
                                 f"mirror-blind={len(mirror_blind_hits)}",
                         severity="INFO",
                         details={"checked": checked, "reads": reads,
                                  "live_diverged": len(live_diverged),
                                  "mirror_blind": len(mirror_blind_hits),
                                  "sampled_only": len(targets) >= LIVE_SWEEP_MAX_READS})
        return {"checked": checked, "reads": reads, "live_diverged": len(live_diverged),
                "mirror_blind": len(mirror_blind_hits),
                "worst": (worst or {}).get("barcode")}
    except Exception as e:
        try:
            alerting.warning("live_truth.sweep", f"live-truth sweep failed: {e}", {})
        except Exception:
            pass
        return {"error": str(e)}
    finally:
        db.close()
