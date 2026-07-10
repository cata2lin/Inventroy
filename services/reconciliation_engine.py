# services/reconciliation_engine.py
"""
P1.3/P1.4 — Convergence reconciliation engine.

Design:
  - PLANNING is read-only and is the review-report deliverable: for each diverged barcode it
    derives a single authoritative target (live-read from the authoritative store, falling back
    to cache/local), and lists the per-store current->target moves. It NEVER writes.
  - APPLY converges a group to its authoritative value using an absolute compare-and-set
    (Shopify compareQuantity), and records value-independent lineage markers so the engine's
    own writes are echo-suppressed by the P0 mechanism (no self-trigger). APPLY only runs when
    explicitly invoked, or by the scheduled job when auto-heal is enabled AND the case is "safe".
  - SCHEDULED auto_reconverge defaults to REPORT/ALERT only (auto-heal off) so deploying it is
    inert until you turn it on.

Safety rules honoured: never blind-trust the local mirror (target comes from a live read),
never auto-modify large/suspicious divergence, always audit, always lineage-tagged + reversible.
"""
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from database import SessionLocal
import models
from shopify_service import ShopifyService
from services import diagnostics, alerting, audit_logger, sync_guards, dist_lock
from services import inventory_sync_service as iss
from services.stock_reconciliation import _determine_authoritative_stock


def _env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


AUTOHEAL_ENABLED = lambda: _env_bool("RECONCILE_AUTOHEAL_ENABLED", False)
AUTOHEAL_MAX_SPREAD = int(os.getenv("RECONCILE_AUTOHEAL_MAX_SPREAD", "100"))


def _per_store_current(db: Session, barcode: str) -> List[Dict[str, Any]]:
    """The CANONICAL variant's current value per enabled store (matches what propagation
    targets — SKU-preferring canonical, not max()), so reconcile plans the real variant."""
    rows = db.execute(text(f"""
        SELECT store_id, store, current FROM (
            SELECT DISTINCT ON (pv.barcode, pv.store_id) pv.store_id, s.name AS store, il.available AS current
            FROM product_variants pv
            JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
            JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
            JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
            WHERE pv.barcode = :b AND il.available IS NOT NULL
            ORDER BY pv.barcode, pv.store_id, {diagnostics.CANON_ORDER}
        ) q ORDER BY store
    """), {"b": barcode}).mappings().all()
    return [dict(r) for r in rows]


def plan_barcode(db: Session, barcode: str) -> Dict[str, Any]:
    """READ-ONLY. Returns the convergence plan for one barcode (no writes)."""
    target, source = _determine_authoritative_stock(db, barcode)
    per_store = _per_store_current(db, barcode)
    currents = [r["current"] for r in per_store if r["current"] is not None]
    spread = (max(currents) - min(currents)) if currents else 0

    moves = []
    for r in per_store:
        if target is not None and r["current"] != target:
            moves.append({"store_id": r["store_id"], "store": r["store"],
                          "current": r["current"], "target": target, "delta": target - (r["current"] or 0)})

    is_suspect = _is_suspect_duplicate(db, barcode)
    safe = (
        target is not None
        and spread <= AUTOHEAL_MAX_SPREAD
        and not iss._is_barcode_broken(db, barcode)
        and not is_suspect
        and isinstance(source, str) and source.startswith("live")
    )
    reason = []
    if target is None: reason.append("no authoritative value")
    if spread > AUTOHEAL_MAX_SPREAD: reason.append(f"spread {spread} > {AUTOHEAL_MAX_SPREAD}")
    if is_suspect: reason.append("suspect intra-store duplicate")
    if isinstance(source, str) and not source.startswith("live"): reason.append(f"non-live source ({source})")

    return {"barcode": barcode, "authoritative_target": target, "source": source,
            "spread": spread, "per_store": per_store, "moves": moves,
            "safe_to_autoheal": safe, "blockers": reason}


def _is_suspect_duplicate(db: Session, barcode: str) -> bool:
    """True if this barcode is on >1 variant with DIFFERENT SKUs within a single store."""
    row = db.execute(text("""
        SELECT 1 FROM (
            SELECT pv.store_id
            FROM product_variants pv
            JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
            WHERE pv.barcode = :b
            GROUP BY pv.store_id
            HAVING count(*) > 1 AND count(DISTINCT NULLIF(pv.sku,'')) > 1
            LIMIT 1
        ) x
    """), {"b": barcode}).first()
    return row is not None


def plan_all_diverged(db: Session, min_spread: int = 1, limit: int = 500) -> List[Dict[str, Any]]:
    """READ-ONLY review report: convergence plans for every diverged barcode group."""
    diverged = diagnostics.detect_divergence(db, min_spread=min_spread, limit=limit)
    return [plan_barcode(db, d["barcode"]) for d in diverged]


def apply_plan(db: Session, plan: Dict[str, Any]) -> Dict[str, Any]:
    """WRITES. Converge a barcode group to its authoritative target via absolute compare-and-set,
    recording lineage markers so the engine's echoes are suppressed. Call only on confirmation."""
    barcode = plan["barcode"]
    target = plan["authoritative_target"]
    if target is None or not plan["moves"]:
        return {"barcode": barcode, "applied": 0, "skipped": "nothing to do"}

    # P2: hold the SAME distributed lock as propagation so reconcile never races a webhook write.
    _h = dist_lock.acquire(f"barcode:{barcode}")
    if _h is None:
        return {"barcode": barcode, "applied": 0, "skipped": "distributed lock busy"}
    try:
        return _apply_plan_locked(db, plan, barcode, target)
    finally:
        dist_lock.release(_h)


def _apply_plan_locked(db: Session, plan: Dict[str, Any], barcode: str, target: int) -> Dict[str, Any]:
    sync_op = f"reconcile-{uuid.uuid4()}"
    # NEGATIVE PROTECTION (Stage 0): never SET a store below the floor, even if the authoritative
    # value was computed as negative (stale/orphan source). An absolute reconcile write is exactly
    # where a negative value would be propagated pool-wide, so clamp here unconditionally.
    if target is not None and target < sync_guards.INVENTORY_FLOOR:
        audit_logger.log(category="RECONCILIATION", action="reconcile_floor_clamp",
                         message=f"[{barcode}] authoritative target {target} < floor; clamped to {sync_guards.INVENTORY_FLOOR}",
                         target=barcode, severity="WARN",
                         details={"raw_target": target, "floor": sync_guards.INVENTORY_FLOOR})
        target = sync_guards.INVENTORY_FLOOR
    applied = 0
    for mv in plan["moves"]:
        store = db.query(models.Store).filter(models.Store.id == mv["store_id"]).first()
        if not store or not store.enabled or not store.sync_location_id:
            continue
        variants = (
            db.query(models.ProductVariant)
            .join(models.Product, models.Product.id == models.ProductVariant.product_id)
            .filter(models.ProductVariant.barcode == barcode,
                    models.ProductVariant.store_id == store.id,
                    models.Product.deleted_at.is_(None),
                    models.ProductVariant.inventory_item_id.isnot(None))
            .all()
        )
        # canonical only (one per store) to mirror propagation semantics
        canon = sync_guards.select_canonical_targets(variants, origin_store_id=-1)
        loc_gid = f"gid://shopify/Location/{store.sync_location_id}"
        payload = []
        for v in canon:
            iss._create_echo_marker(db, barcode, store.id, v.inventory_item_id, target,
                                    sync_op, origin_store_id=-1, origin_item_id=None, depth=1)
            payload.append({"inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                            "locationId": loc_gid, "quantity": target,
                            "compareQuantity": mv["current"]})
        if not payload:
            continue
        db.commit()
        try:
            svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            # compare-and-set: Shopify rejects if its real value != our assumed current
            result = svc.set_inventory_quantities(payload, reference_uri=f"inventory-sync://op/{sync_op}",
                                                  ignore_compare=False)
            ue = result.get("inventorySetQuantities", {}).get("userErrors", [])
            if ue:
                alerting.warning("reconcile.compare_mismatch",
                                 f"compare-and-set rejected for {barcode}@{store.name} (real value drifted)",
                                 {"barcode": barcode, "store": store.name, "errors": str(ue)})
                continue
            for v in canon:
                import crud.product as crud_product
                crud_product.update_inventory_levels_for_variants(
                    db, variant_ids=[v.id], location_id=store.sync_location_id, new_quantity=target)
            applied += len(canon)
        except Exception as e:
            alerting.warning("reconcile.apply", f"reconcile write failed for {barcode}@{store.name}: {e}",
                             {"barcode": barcode, "store": store.name})

    audit_logger.log(category="RECONCILIATION", action="reconcile_applied",
                     message=f"Converged [{barcode}] to {target} ({applied} variants)",
                     target=barcode, details={"target": target, "applied": applied,
                                              "sync_operation_uuid": sync_op, "source": plan["source"]})
    return {"barcode": barcode, "target": target, "applied": applied, "sync_operation_uuid": sync_op}


def auto_reconverge(auto_heal: Optional[bool] = None, min_spread: int = 1) -> Dict[str, Any]:
    """Scheduled entrypoint. Detects divergence and REPORTS/ALERTS. Applies a fix only for
    'safe' cases AND only when auto-heal is enabled (default OFF → inert)."""
    if auto_heal is None:
        auto_heal = AUTOHEAL_ENABLED()
    db = SessionLocal()
    healed, reported = 0, 0
    try:
        plans = plan_all_diverged(db, min_spread=min_spread, limit=500)
        for p in plans:
            reported += 1
            if auto_heal and p["safe_to_autoheal"]:
                res = apply_plan(db, p)
                healed += 1 if res.get("applied") else 0
            else:
                audit_logger.log(category="RECONCILIATION", action="divergence_detected",
                                 message=f"[{p['barcode']}] spread={p['spread']} target={p['authoritative_target']} "
                                         f"safe={p['safe_to_autoheal']} {('blockers='+';'.join(p['blockers'])) if p['blockers'] else ''}",
                                 target=p["barcode"], severity="WARN",
                                 details={"spread": p["spread"], "target": p["authoritative_target"],
                                          "moves": len(p["moves"]), "safe": p["safe_to_autoheal"],
                                          "blockers": p["blockers"]})
        if reported:
            alerting.warning("reconcile.divergence_scan",
                             f"divergence scan: {reported} diverged groups, {healed} auto-healed (auto_heal={auto_heal})",
                             {"reported": reported, "healed": healed, "auto_heal": auto_heal})
    except Exception as e:
        alerting.warning("reconcile.auto_reconverge", f"auto_reconverge failed: {e}", {})
    finally:
        db.close()
    return {"reported": reported, "healed": healed, "auto_heal": auto_heal}
