# services/pool_engine.py
"""
STAGE 2 — canonical absolute-convergence pool engine (flag-gated: SYNC_POOL_ENGINE, default OFF).

This is the replacement for relative-delta propagation. It models each barcode as ONE shared pool
(PoolState) fed by an append-only ledger (PoolEvent), and converges every store to the pool quantity
by IDEMPOTENT ABSOLUTE compare-and-set — never by relative `adjust`.

Why this converges (the property delta-propagation lacks):
  • Absolute SET-to-Q is idempotent: a missed SET is fixed by the next SET; a duplicate SET is a
    no-op (compareQuantity already matches); an out-of-order SET is rejected by the monotonic version.
  • Pool quantity is computed by a CONSERVATION FOLD, not last-writer-wins, so two stores selling
    concurrently both reduce the pool (no lost sale):
        Q_new = max(Q_old + (observed_source - source_prev_observed), floor)
    Each source contributes only its OWN signed change; independent concurrent changes compose.
  • Idempotency is structural: pool_events.webhook_id is UNIQUE (INSERT … ON CONFLICT DO NOTHING),
    so a Shopify retry/replay — even days later — records nothing and applies nothing twice.
  • Ordering is PER-SOURCE: a stale redelivery from the SAME (barcode, source_store) is rejected;
    a genuine concurrent event from a DIFFERENT store is NEVER cross-rejected (audit HIGH-5 fix).

NOT yet wired into handle_webhook — the Stage-2 cutover (review-gated) flips the write path to this.
The pure fold below is the mathematically-verified core and is unit-tested independently of any DB.
"""
import os
import uuid
from typing import Optional, Dict, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

import models
from services import sync_guards, audit_logger, alerting, dist_lock


def pool_engine_enabled() -> bool:
    return os.getenv("SYNC_POOL_ENGINE", "false").strip().lower() in ("1", "true", "yes", "on")


# --------------------------------------------------------------------------------------------------
# PURE CONVERGENCE CORE (no I/O — this is the part whose correctness is proven by tests)
# --------------------------------------------------------------------------------------------------

def fold_observation(q_old: Optional[int], source_prev_observed: Optional[int],
                     observed: int, floor: int = 0) -> int:
    """Fold one source's new observation into the canonical pool quantity.

      • Pool not yet initialised (q_old is None)      -> bootstrap to `observed`.
      • First observation from this source (prev None) -> a replica joining; do NOT move the pool
        (it will be CONVERGED to Q instead). Returns q_old unchanged.
      • Otherwise apply this source's OWN signed change, clamped to the floor:
            Q_new = max(q_old + (observed - source_prev_observed), floor)

    This is conservation, not last-writer-wins: concurrent independent changes on different stores
    each apply their own delta, so no sale is lost and no oversell is amplified."""
    if q_old is None:
        return max(observed, floor)
    if source_prev_observed is None:
        return q_old
    return max(q_old + (observed - source_prev_observed), floor)


def is_stale_for_source(prev_source_timestamp, new_source_timestamp) -> bool:
    """Per-source monotonicity: reject only an out-of-order redelivery of the SAME emitter. Never
    used to compare two DIFFERENT stores (that would drop a concurrent sale — audit HIGH-5)."""
    if prev_source_timestamp is None or new_source_timestamp is None:
        return False
    return new_source_timestamp < prev_source_timestamp


# --------------------------------------------------------------------------------------------------
# LEDGER INGEST (idempotent) + POOL FOLD (DB)
# --------------------------------------------------------------------------------------------------

def ingest_event(db: Session, *, barcode: str, source_store_id: Optional[int],
                 source_variant_id: Optional[int], inventory_item_id: Optional[int],
                 observed_quantity: int, source_timestamp, webhook_id: Optional[str],
                 kind: str = "observation") -> Optional[int]:
    """Append one observation to the ledger, IDEMPOTENTLY. Returns the new event_id, or None if this
    webhook_id was already recorded (duplicate/replay → no-op). Uses INSERT … ON CONFLICT DO NOTHING
    on the unique webhook_id, so a Shopify retry can never create a second ledger row."""
    row = db.execute(text("""
        INSERT INTO pool_events
            (barcode, source_store_id, source_variant_id, inventory_item_id,
             observed_quantity, source_timestamp, webhook_id, kind)
        VALUES (:barcode, :ssid, :svid, :iid, :obs, :ts, :wid, :kind)
        ON CONFLICT (webhook_id) WHERE webhook_id IS NOT NULL DO NOTHING
        RETURNING event_id
    """), {"barcode": barcode, "ssid": source_store_id, "svid": source_variant_id,
           "iid": inventory_item_id, "obs": observed_quantity, "ts": source_timestamp,
           "wid": webhook_id, "kind": kind}).first()
    db.commit()
    return row[0] if row else None


def _source_prev(db: Session, barcode: str, source_store_id: Optional[int], before_event_id: int):
    """The most recent prior observation from the SAME (barcode, source_store), used for the
    per-source signed delta and the per-source staleness check."""
    return db.execute(text("""
        SELECT observed_quantity, source_timestamp
        FROM pool_events
        WHERE barcode = :b AND source_store_id IS NOT DISTINCT FROM :s AND event_id < :e
        ORDER BY event_id DESC LIMIT 1
    """), {"b": barcode, "s": source_store_id, "e": before_event_id}).mappings().first()


def apply_event(db: Session, event_id: int) -> Optional[Dict[str, Any]]:
    """Fold a ledger event into PoolState under the per-barcode advisory lock (the same lock domain
    webhook propagation and reconcile use, so this never races them). Advances the monotonic version.
    Returns {barcode, quantity, version, delta} or None if rejected (stale-for-source) / locked out.
    Does NOT write to Shopify — converge_pool() does that."""
    ev = db.query(models.PoolEvent).filter(models.PoolEvent.event_id == event_id).first()
    if ev is None:
        return None
    handle = dist_lock.acquire(f"barcode:{ev.barcode}")
    if handle is None:
        return None
    try:
        prev = _source_prev(db, ev.barcode, ev.source_store_id, ev.event_id)
        prev_obs = prev["observed_quantity"] if prev else None
        prev_ts = prev["source_timestamp"] if prev else None

        if is_stale_for_source(prev_ts, ev.source_timestamp):
            audit_logger.log(category="RECONCILIATION", action="pool_event_stale",
                             message=f"[{ev.barcode}] dropped out-of-order event from store {ev.source_store_id}",
                             target=ev.barcode, severity="INFO",
                             details={"event_id": event_id, "observed": ev.observed_quantity})
            return None

        state = db.query(models.PoolState).filter(models.PoolState.barcode == ev.barcode).first()
        q_old = state.quantity if state else None
        q_new = fold_observation(q_old, prev_obs, ev.observed_quantity, floor=sync_guards.INVENTORY_FLOOR)
        delta = q_new - (q_old if q_old is not None else q_new)

        if state is None:
            state = models.PoolState(barcode=ev.barcode, quantity=q_new, version=1,
                                     source_event_id=event_id, source_store_id=ev.source_store_id,
                                     source_timestamp=ev.source_timestamp)
            db.add(state)
        else:
            state.quantity = q_new
            state.version = (state.version or 0) + 1
            state.source_event_id = event_id
            state.source_store_id = ev.source_store_id
            state.source_timestamp = ev.source_timestamp
        ev.applied = True
        db.commit()
        return {"barcode": ev.barcode, "quantity": q_new, "version": state.version, "delta": delta}
    except Exception as e:
        db.rollback()
        alerting.warning("pool_engine.apply_event", f"apply_event failed for {ev.barcode}: {e}",
                         {"event_id": event_id})
        return None
    finally:
        dist_lock.release(handle)


# --------------------------------------------------------------------------------------------------
# CONVERGENCE WRITER (idempotent absolute compare-and-set to Q)  — the Stage-2 cutover wires this in
# --------------------------------------------------------------------------------------------------

def converge_pool(db: Session, barcode: str, exclude_store_id: Optional[int] = None) -> Dict[str, Any]:
    """Drive every canonical store replica to the pool's quantity Q via idempotent absolute
    compare-and-set (floored). Reads each store's live value as the compareQuantity so a concurrent
    change is detected (retry), never clobbered; tags the write with referenceDocumentUri = the pool
    op so the echo is attributable by reference (no time-window guessing). Idempotent: re-running is
    a no-op once converged. Flag-gated; only runs when SYNC_POOL_ENGINE is on."""
    from shopify_service import ShopifyService
    state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
    if state is None:
        return {"barcode": barcode, "skipped": "no pool state"}
    target = max(int(state.quantity), sync_guards.INVENTORY_FLOOR)
    op = f"pool-{uuid.uuid4()}"
    ref_uri = f"inventory-sync://pool/{op}"

    rows = db.execute(text(f"""
        SELECT DISTINCT ON (pv.barcode, pv.store_id)
               pv.store_id, s.name store, s.shopify_url, s.api_token, s.sync_location_id, pv.inventory_item_id
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.barcode, pv.store_id, {__import__('services.diagnostics', fromlist=['CANON_ORDER']).CANON_ORDER}
    """), {"b": barcode}).mappings().all()

    converged, skipped, failed = 0, 0, 0
    for r in rows:
        if exclude_store_id is not None and r["store_id"] == exclude_store_id:
            continue
        item_gid = f"gid://shopify/InventoryItem/{r['inventory_item_id']}"
        loc_gid = f"gid://shopify/Location/{r['sync_location_id']}"
        try:
            svc = ShopifyService(store_url=r["shopify_url"], token=r["api_token"])
            live = svc.get_available_single(item_gid, loc_gid)
            if live == target:
                skipped += 1
                continue
            _raw, ue = svc.set_inventory_quantities_single(item_gid, loc_gid, target,
                                                           reference_uri=ref_uri, compare_quantity=live)
            if ue:
                # stale compare → one bounded re-read+retry (a concurrent sale moved it)
                live2 = svc.get_available_single(item_gid, loc_gid)
                if live2 is not None and live2 != target:
                    _raw2, ue2 = svc.set_inventory_quantities_single(item_gid, loc_gid, target,
                                                                     reference_uri=ref_uri, compare_quantity=live2)
                    if ue2:
                        failed += 1
                        continue
                elif live2 == target:
                    skipped += 1
                    continue
            converged += 1
        except Exception:
            failed += 1
    audit_logger.log(category="STOCK", action="pool_converged",
                     message=f"[{barcode}] converged stores to Q={target} v{state.version} "
                             f"(set={converged}, already={skipped}, failed={failed})",
                     target=barcode, severity="INFO" if not failed else "WARN",
                     details={"target": target, "version": state.version, "op": op,
                              "set": converged, "already": skipped, "failed": failed})
    return {"barcode": barcode, "target": target, "version": state.version,
            "converged": converged, "already": skipped, "failed": failed}
