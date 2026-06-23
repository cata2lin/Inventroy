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
from database import SessionLocal
from services import sync_guards, audit_logger, alerting, dist_lock, diagnostics


def pool_engine_enabled() -> bool:
    """Master switch: when on, the pool engine is AUTHORITATIVE and performs real Shopify CAS writes.
    Default OFF. Phase 3+ (canary) turns this on per-barcode; Phase 4 globally."""
    return os.getenv("SYNC_POOL_ENGINE", "false").strip().lower() in ("1", "true", "yes", "on")


def pool_shadow_enabled() -> bool:
    """Phase 1 SHADOW switch (independent of pool_engine_enabled). When on, the engine runs in
    parallel on every genuine webhook — ingesting events, evolving PoolState, and SIMULATING the
    convergence plan — but performs NO Shopify writes. The legacy delta path stays authoritative.
    Kill switch: SYNC_POOL_SHADOW=false."""
    return os.getenv("SYNC_POOL_SHADOW", "false").strip().lower() in ("1", "true", "yes", "on")


SHADOW_ALERT_DELTA = int(os.getenv("SYNC_POOL_SHADOW_ALERT_DELTA", "5"))  # |Q - observed| to alert on


def pool_writes_enabled() -> bool:
    """Phase 3 master WRITE switch. Even when on, only canary-listed + backfilled barcodes write."""
    return os.getenv("SYNC_POOL_ENGINE_WRITES", "false").strip().lower() in ("1", "true", "yes", "on")


def canary_barcodes() -> set:
    """Phase 3 canary allowlist (SYNC_POOL_CANARY_BARCODES, comma-separated). Empty + writes-on =
    GLOBAL (Phase 4); empty + writes-off = nothing writes."""
    raw = os.getenv("SYNC_POOL_CANARY_BARCODES", "").strip()
    return {b.strip() for b in raw.split(",") if b.strip()}


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


def apply_event(db: Session, event_id: int, skip_lock: bool = False) -> Optional[Dict[str, Any]]:
    """Fold a ledger event into PoolState under the per-barcode advisory lock (the same lock domain
    webhook propagation and reconcile use, so this never races them). Advances the monotonic version.
    Returns {barcode, quantity, version, delta} or None if rejected (stale-for-source) / locked out.
    Does NOT write to Shopify — converge_pool() does that.

    skip_lock=True: the CALLER already holds the per-barcode advisory lock (e.g. shadow mode running
    inside handle_webhook's locked section). Re-acquiring the same key on a second connection would
    fail/deadlock, so we trust the caller's lock for serialization."""
    ev = db.query(models.PoolEvent).filter(models.PoolEvent.event_id == event_id).first()
    if ev is None:
        return None
    handle = None
    if not skip_lock:
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
        if handle is not None:
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

    version = state.version
    ref_uri = f"inventory-sync://pool/{op}?v={version}"   # ref carries the pool version (attribution)
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (pv.barcode, pv.store_id)
               pv.id AS variant_id, pv.store_id, s.name store, s.shopify_url, s.api_token,
               s.sync_location_id, pv.inventory_item_id
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.barcode, pv.store_id, {diagnostics.CANON_ORDER}
    """), {"b": barcode}).mappings().all()

    converged, skipped, failed, retries = 0, 0, 0, 0
    per_store, live_quantities = [], {}
    for r in rows:
        if exclude_store_id is not None and r["store_id"] == exclude_store_id:
            continue
        item_gid = f"gid://shopify/InventoryItem/{r['inventory_item_id']}"
        loc_gid = f"gid://shopify/Location/{r['sync_location_id']}"
        cas_result, n_try = "skip", 0
        try:
            svc = ShopifyService(store_url=r["shopify_url"], token=r["api_token"])
            live = svc.get_available_single(item_gid, loc_gid)
            live_quantities[r["store"]] = live
            if live == target:
                skipped += 1
                cas_result = "already"
            else:
                _raw, ue = svc.set_inventory_quantities_single(item_gid, loc_gid, target,
                                                               reference_uri=ref_uri, compare_quantity=live)
                n_try += 1
                if ue:
                    retries += 1
                    live2 = svc.get_available_single(item_gid, loc_gid)
                    if live2 == target:
                        skipped += 1; cas_result = "already_after_retry"
                    elif live2 is not None:
                        _r2, ue2 = svc.set_inventory_quantities_single(item_gid, loc_gid, target,
                                                                       reference_uri=ref_uri, compare_quantity=live2)
                        n_try += 1
                        if ue2:
                            failed += 1; cas_result = "cas_conflict"
                        else:
                            converged += 1; cas_result = "set_after_retry"
                    else:
                        failed += 1; cas_result = "unreadable"
                else:
                    converged += 1; cas_result = "set"
            # On a landed write (set/already), keep mirror + ledger baseline at Q so the store's
            # resulting echo webhook folds to delta 0 (no self-amplification).
            if cas_result in ("set", "set_after_retry", "already", "already_after_retry"):
                db.execute(text("""UPDATE inventory_levels SET available=:q, updated_at=now()
                                   WHERE variant_id=:vid AND location_id=:loc"""),
                           {"q": target, "vid": r["variant_id"], "loc": r["sync_location_id"]})
                db.execute(text("""INSERT INTO pool_events
                                   (barcode, source_store_id, source_variant_id, inventory_item_id,
                                    observed_quantity, source_timestamp, kind)
                                   VALUES (:b,:s,:v,:i,:q, now(), 'convergence')"""),
                           {"b": barcode, "s": r["store_id"], "v": r["variant_id"],
                            "i": r["inventory_item_id"], "q": target})
        except Exception:
            failed += 1; cas_result = "error"
        per_store.append({"store": r["store"], "cas_result": cas_result, "retries": n_try,
                          "live_before": live_quantities.get(r["store"]), "target": target})
    try:
        db.commit()
    except Exception:
        db.rollback()

    audit_logger.log(category="STOCK", action="pool_converged",
                     message=f"[{barcode}] converged stores to Q={target} v{version} "
                             f"(set={converged}, already={skipped}, cas_conflict={failed}, retries={retries})",
                     target=barcode, severity="INFO" if not failed else "WARN",
                     details={"barcode": barcode, "canonical_Q": target, "pool_version": version, "op": op,
                              "set": converged, "already": skipped, "failed": failed, "retries": retries,
                              "live_quantities": live_quantities, "per_store": per_store})
    return {"barcode": barcode, "target": target, "version": version,
            "converged": converged, "already": skipped, "failed": failed, "retries": retries,
            "per_store": per_store, "live_quantities": live_quantities}


# --------------------------------------------------------------------------------------------------
# PHASE 1 — SHADOW MODE (no Shopify writes): simulate convergence + compare against legacy
# --------------------------------------------------------------------------------------------------

def simulate_convergence(db: Session, barcode: str) -> Dict[str, Any]:
    """Compute the convergence plan the engine WOULD execute, WITHOUT any Shopify write. Reads the
    local MIRROR per canonical store (cheap; Phase 2 adds the live-Shopify comparison) and the pool
    target Q. Returns the intended absolute-SET writes (stores whose mirror != Q)."""
    state = db.query(models.PoolState).filter(models.PoolState.barcode == barcode).first()
    if state is None:
        return {"target": None, "version": None, "intended_writes": [], "already": 0}
    target = max(int(state.quantity), sync_guards.INVENTORY_FLOOR)
    rows = db.execute(text(f"""
        SELECT DISTINCT ON (pv.barcode, pv.store_id) s.name store, il.available AS mirror
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.barcode, pv.store_id, {diagnostics.CANON_ORDER}
    """), {"b": barcode}).mappings().all()
    intended, already = [], 0
    for r in rows:
        cur = r["mirror"]
        if cur == target:
            already += 1
        else:
            intended.append({"store": r["store"], "current": cur, "target": target})
    return {"target": target, "version": state.version, "intended_writes": intended, "already": already}


def shadow_observe(*, barcode: str, source_store_id: Optional[int], source_variant_id: Optional[int],
                   inventory_item_id: Optional[int], observed_quantity: int, source_timestamp,
                   webhook_id: Optional[str], legacy_quantity: int,
                   caller_holds_lock: bool = True) -> Optional[Dict[str, Any]]:
    """PHASE 1 SHADOW. Runs the engine on ONE genuine webhook in parallel with the legacy path —
    ingest (idempotent) → apply (fold + monotonic version) → SIMULATE convergence → compare vs legacy
    → structured log + metrics + alerts. PERFORMS NO SHOPIFY WRITES. Best-effort: opens its OWN DB
    session (isolated from the legacy transaction) and never raises into the caller. Returns a summary
    dict or None. caller_holds_lock=True means handle_webhook already holds barcode:{bc} (skip re-lock)."""
    if not pool_shadow_enabled():
        return None
    db = SessionLocal()
    try:
        ev_id = ingest_event(db, barcode=barcode, source_store_id=source_store_id,
                             source_variant_id=source_variant_id, inventory_item_id=inventory_item_id,
                             observed_quantity=observed_quantity, source_timestamp=source_timestamp,
                             webhook_id=webhook_id)
        if ev_id is None:
            # webhook_id already in the ledger -> idempotent duplicate suppression (the engine would
            # NOT double-apply, even past the legacy 120s dedup window).
            audit_logger.log(category="RECONCILIATION", action="pool_shadow_dup_suppressed",
                             message=f"[{barcode}] shadow: duplicate webhook_id suppressed (idempotent)",
                             target=barcode, severity="INFO", details={"webhook_id": webhook_id})
            return {"duplicate": True}

        res = apply_event(db, ev_id, skip_lock=caller_holds_lock)
        if res is None:
            # rejected as stale-for-source (per-source ordering) — NOT cross-store.
            audit_logger.log(category="RECONCILIATION", action="pool_shadow_stale_reject",
                             message=f"[{barcode}] shadow: event rejected (out-of-order for source {source_store_id})",
                             target=barcode, severity="INFO",
                             details={"webhook_id": webhook_id, "observed": observed_quantity})
            return {"stale_reject": True}

        q = res["quantity"]
        version = res["version"]
        pool_delta = res["delta"]
        sim = simulate_convergence(db, barcode)
        delta_difference = q - legacy_quantity
        mismatch = (delta_difference != 0)

        audit_logger.log(
            category="RECONCILIATION", action="pool_shadow_compare",
            message=f"[{barcode}] shadow Q={q} v{version} vs legacy={legacy_quantity} "
                    f"(Δ={delta_difference}); would-write {len(sim['intended_writes'])} stores",
            target=barcode, severity="WARN" if mismatch else "INFO",
            details={
                "barcode": barcode,
                "legacy_quantity": legacy_quantity,
                "poolengine_quantity": q,
                "delta_difference": delta_difference,
                "source_store": source_store_id,
                "webhook_id": webhook_id,
                "pool_version": version,
                "pool_delta": pool_delta,
                "intended_writes": sim["intended_writes"],
                "stores_needing_write": len(sim["intended_writes"]),
                "stores_already_converged": sim["already"],
            })

        # Alerts (Phase 1 requirements)
        if q < 0:  # impossible: fold is floored. If it ever fires, the engine has a bug.
            alerting.critical("pool_shadow.negative",
                              f"[{barcode}] shadow computed NEGATIVE pool quantity {q}",
                              {"barcode": barcode, "webhook_id": webhook_id})
        if abs(delta_difference) >= SHADOW_ALERT_DELTA:
            alerting.warning("pool_shadow.disagreement",
                             f"[{barcode}] shadow Q={q} disagrees with legacy={legacy_quantity} "
                             f"by {delta_difference} (>= {SHADOW_ALERT_DELTA})",
                             {"barcode": barcode, "delta_difference": delta_difference,
                              "source_store": source_store_id, "webhook_id": webhook_id})
        return {"poolengine_quantity": q, "legacy_quantity": legacy_quantity,
                "delta_difference": delta_difference, "version": version,
                "intended_writes": len(sim["intended_writes"])}
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        try:
            alerting.warning("pool_shadow.observe", f"shadow_observe failed for {barcode}: {e}",
                             {"barcode": barcode, "webhook_id": webhook_id})
        except Exception:
            pass
        return None
    finally:
        db.close()
