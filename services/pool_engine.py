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
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

import models
from database import SessionLocal
from services import sync_guards, audit_logger, alerting, dist_lock


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


def spike_corroboration_enabled() -> bool:
    """P0 transient-spike guard. When on, an UP jump is corroborated against the SOURCE store's LIVE
    Shopify value BEFORE it folds into the pool; a phantom webhook (a claimed increase Shopify never
    actually had — the 2026-06-26 990->2050 spike) is corrected to live truth so it can neither
    propagate nor poison the per-source baseline (the zeroing-on-revert mechanism). Magnitude-agnostic:
    it checks PERSISTENCE in live truth, never jump size, so real 6k-12k restocks pass untouched.
    Kill switch: SYNC_POOL_SPIKE_CORROBORATION=false."""
    return os.getenv("SYNC_POOL_SPIKE_CORROBORATION", "true").strip().lower() in ("1", "true", "yes", "on")


# How far live truth may sit BELOW a claimed UP jump and still corroborate it — absorbs sales that
# happen between the webhook firing and our live re-read, and ignores tiny jumps entirely.
SPIKE_CORROBORATION_TOLERANCE = int(os.getenv("POOL_SPIKE_CORROBORATION_TOLERANCE", "10"))


def drop_corroboration_enabled() -> bool:
    """P0 catastrophic-drop guard (2026-07-14), the DOWN-direction twin of spike corroboration: a
    big claimed drop (>= sync_guards.BIG_DROP_VERIFY_UNITS) is verified against the SOURCE store's
    LIVE Shopify value BEFORE it folds. live==observed folds; live!=observed folds LIVE truth
    instead; live unreadable FAILS CLOSED (the event is skipped — a phantom drop that folds converges
    every store toward 0, the 2026-07-13 self-zeroing class). Kill switch: SYNC_POOL_DROP_CORROBORATION=false."""
    return os.getenv("SYNC_POOL_DROP_CORROBORATION", "true").strip().lower() in ("1", "true", "yes", "on")


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


def corroboration_verdict(prev: Optional[int], observed: int, live: Optional[int], tol: int) -> str:
    """PURE decision for the transient-spike guard (P0). Returns the value the pool should fold:
        'fold'    -> use `observed` as-is (no corroboration needed or live confirms it).
        'correct' -> use `live` instead of `observed` (a phantom UP jump live truth refutes).

    'correct' fires ONLY when ALL hold:
        • there is a prior source value to compare against (prev is not None),
        • the webhook claims a real increase           : observed >  prev + tol,
        • live truth shows NO real increase            : live    <= prev + tol  (live readable).
    Every other case folds `observed`: not an up jump, live unreadable (FAIL-OPEN — never block real
    inventory), or live corroborates the jump. This is magnitude-agnostic — it compares live against
    the PRE-JUMP baseline, never the size of the jump — so a genuine restock (live reflects the new
    high, even partly sold down) always folds, while a 990->2050 phantom whose live is still ~990 is
    corrected to 990."""
    if prev is None:
        return "fold"
    if observed <= prev + tol:
        return "fold"
    if live is None:
        return "fold"
    if live <= prev + tol:
        return "correct"
    return "fold"


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


def _source_prev(db: Session, barcode: str, source_variant_id: Optional[int], before_event_id: int):
    """The most recent prior observation from the SAME (barcode, source LISTING/variant), used for the
    per-source signed delta and the per-source staleness check.

    PER-LISTING, not per-store (2026-07-10 fix): a barcode may be listed on SEVERAL variants within
    ONE store (the barcode is the intentional sync key; SKUs/sizes may differ). A per-store baseline
    interleaves those listings' observations into one stream — a 5XL listing reporting 2 folded
    against the 4XL listing's baseline of 202 as a phantom -200, clobbering a fresh restock on every
    store. Each listing is its own replica: its observations fold only against ITS OWN history."""
    return db.execute(text("""
        SELECT observed_quantity, source_timestamp
        FROM pool_events
        WHERE barcode = :b AND source_variant_id IS NOT DISTINCT FROM :v AND event_id < :e
          AND kind <> 'rejected_negative_fold'
        ORDER BY event_id DESC LIMIT 1
    """), {"b": barcode, "v": source_variant_id, "e": before_event_id}).mappings().first()


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
        prev = _source_prev(db, ev.barcode, ev.source_variant_id, ev.event_id)
        prev_obs = prev["observed_quantity"] if prev else None
        prev_ts = prev["source_timestamp"] if prev else None

        if is_stale_for_source(prev_ts, ev.source_timestamp):
            audit_logger.log(category="RECONCILIATION", action="pool_event_stale",
                             message=f"[{ev.barcode}] dropped out-of-order event from variant "
                                     f"{ev.source_variant_id} (store {ev.source_store_id})",
                             target=ev.barcode, severity="INFO",
                             details={"event_id": event_id, "observed": ev.observed_quantity})
            return None

        state = db.query(models.PoolState).filter(models.PoolState.barcode == ev.barcode).first()
        q_old = state.quantity if state else None
        # 2026-07-14: the fold no longer SILENTLY clamps a deep-negative result to 0 (that clamp is
        # what turned a poisoned baseline into a pool-wide zeroing on convergence). A small deficit
        # (stockout race) clamps VISIBLY; a deep one REJECTS the event — the pool does not move, the
        # event is excluded from future baselines, and the caller quarantines/alerts.
        verdict, q_new, deficit = sync_guards.classify_fold(
            q_old, prev_obs, ev.observed_quantity, floor=sync_guards.INVENTORY_FLOOR)
        if verdict == "reject":
            # Live-verify WHICH side is poisoned before excluding the event from baselines:
            #   live == observed -> the OBSERVATION is truth, the BASELINE is poisoned. Keep the pool
            #       unmoved but let this event become the new per-source baseline (baseline_reseed) so
            #       the NEXT fold is sane — otherwise every later genuine observation re-rejects
            #       against the same poisoned baseline forever (an unrecoverable CRITICAL-alert loop
            #       in shadow mode).
            #   live unreadable or disagrees -> the OBSERVATION is phantom: exclude it from baselines.
            live = _read_source_live(db, ev.source_store_id, ev.inventory_item_id)
            if live is not None and live == ev.observed_quantity:
                ev.kind = "baseline_reseed"
                resolution = "baseline_reseeded"
            else:
                ev.kind = "rejected_negative_fold"   # never a future per-source baseline
                resolution = "event_rejected"
            db.commit()
            alerting.critical("pool_engine.negative_fold_rejected",
                              f"[{ev.barcode}] fold rejected: observation {ev.observed_quantity} vs "
                              f"baseline {prev_obs} would take pool Q={q_old} {deficit} below floor "
                              f"— pool NOT moved ({resolution}, live={live}, store {ev.source_store_id})",
                              {"barcode": ev.barcode, "observed": ev.observed_quantity,
                               "source_prev": prev_obs, "q_old": q_old, "deficit": deficit,
                               "source_store_id": ev.source_store_id, "event_id": event_id,
                               "resolution": resolution, "live": live})
            audit_logger.log(category="STOCK", action="pool_negative_fold_rejected",
                             message=f"[{ev.barcode}] negative fold rejected (deficit {deficit}, "
                                     f"obs {ev.observed_quantity} vs baseline {prev_obs}, Q={q_old}, "
                                     f"{resolution})",
                             target=ev.barcode, severity="CRITICAL",
                             details={"event_id": event_id, "observed": ev.observed_quantity,
                                      "source_prev": prev_obs, "q_old": q_old, "deficit": deficit,
                                      "source_store_id": ev.source_store_id,
                                      "resolution": resolution, "live": live})
            return {"barcode": ev.barcode, "rejected": "negative_fold", "deficit": deficit,
                    "observed": ev.observed_quantity, "source_prev": prev_obs, "q_old": q_old,
                    "source_store_id": ev.source_store_id, "resolution": resolution, "live": live}
        if verdict == "clamp":
            alerting.warning("pool_engine.fold_clamped",
                             f"[{ev.barcode}] fold clamped to floor (deficit {deficit}: obs "
                             f"{ev.observed_quantity} vs baseline {prev_obs}, Q={q_old}) — "
                             f"plausible stockout race",
                             {"barcode": ev.barcode, "observed": ev.observed_quantity,
                              "source_prev": prev_obs, "q_old": q_old, "deficit": deficit,
                              "source_store_id": ev.source_store_id, "event_id": event_id})
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
# TRANSIENT-SPIKE CORROBORATION (P0) — refute a phantom UP jump against the source's LIVE truth
# BEFORE it folds, so it can neither propagate nor poison the per-source baseline.
# --------------------------------------------------------------------------------------------------

def latest_source_observed(db: Session, barcode: str, source_variant_id: Optional[int]) -> Optional[int]:
    """The most recent recorded quantity for this (barcode, source LISTING) across the ledger — the
    same per-listing baseline apply_event will fold the next observation against (observations AND
    convergence/baseline anchors, newest wins). None if this listing has never been recorded."""
    row = db.execute(text("""
        SELECT observed_quantity FROM pool_events
        WHERE barcode = :b AND source_variant_id IS NOT DISTINCT FROM :v
          AND kind <> 'rejected_negative_fold'
        ORDER BY event_id DESC LIMIT 1
    """), {"b": barcode, "v": source_variant_id}).first()
    return row[0] if row else None


def _read_source_live(db: Session, source_store_id: Optional[int], inventory_item_id: Optional[int]) -> Optional[int]:
    """Read the SOURCE store's live Shopify `available` for this item. None on any failure (fail-open)."""
    if source_store_id is None or inventory_item_id is None:
        return None
    from services import live_truth
    row = db.execute(text("""
        SELECT shopify_url, api_token, sync_location_id FROM stores
        WHERE id = :sid AND enabled AND sync_location_id IS NOT NULL
    """), {"sid": source_store_id}).mappings().first()
    if not row:
        return None
    return live_truth._read_live(row["shopify_url"], row["api_token"], inventory_item_id,
                                 row["sync_location_id"])


def corroborate_up_jump(db: Session, *, barcode: str, source_store_id: Optional[int],
                        source_variant_id: Optional[int], inventory_item_id: Optional[int],
                        observed: int, tol: Optional[int] = None) -> tuple:
    """Returns (observed_to_use, correction_info). For a claimed UP jump, re-reads the source's LIVE
    Shopify value and applies `corroboration_verdict`. On 'correct' it substitutes the live value (so
    the pool folds truth, never the phantom) and returns the forensic detail; otherwise it returns the
    original observation and None. Fail-open: a failed/missing live read folds `observed` unchanged.
    The pre-jump baseline is PER-LISTING (matching the fold), so a second listing's different value in
    the same store can never make a normal report look like a jump."""
    if tol is None:
        tol = SPIKE_CORROBORATION_TOLERANCE
    prev = latest_source_observed(db, barcode, source_variant_id)
    # Cheap pre-check: only an UP jump beyond tolerance is worth a live read.
    if prev is None or observed <= prev + tol:
        return observed, None
    live = _read_source_live(db, source_store_id, inventory_item_id)
    if corroboration_verdict(prev, observed, live, tol) == "correct":
        return live, {"prev": prev, "claimed": observed, "live": live, "tol": tol}
    return observed, None


def corroborate_big_drop(db: Session, *, barcode: str, source_store_id: Optional[int],
                         source_variant_id: Optional[int], inventory_item_id: Optional[int],
                         observed: int, threshold: Optional[int] = None) -> tuple:
    """DOWN-direction twin of corroborate_up_jump (2026-07-14). Returns (observed_to_use, info):

      • small drop (< threshold vs the per-listing baseline) -> (observed, None): folds unchecked.
      • big drop, live == observed                           -> (observed, None): genuine, folds.
      • big drop, live readable but != observed              -> (live, info): fold LIVE truth (the
        webhook is stale/out-of-order/echo — e.g. our own floored 0 read back late).
      • big drop, live UNREADABLE                            -> (None, info): FAIL-CLOSED — caller
        must SKIP the event entirely. Unlike up-jumps (fail-open: blocking a restock only delays
        sales), folding an unverifiable catastrophic drop converges every store toward 0.
    """
    if threshold is None:
        threshold = sync_guards.BIG_DROP_VERIFY_UNITS
    prev = latest_source_observed(db, barcode, source_variant_id)
    if prev is None or (prev - observed) < threshold:
        return observed, None
    live = _read_source_live(db, source_store_id, inventory_item_id)
    if live is None:
        return None, {"prev": prev, "claimed": observed, "live": None, "threshold": threshold}
    if live != observed:
        return live, {"prev": prev, "claimed": observed, "live": live, "threshold": threshold}
    return observed, None


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
    # EVERY listing of the barcode is a replica of the pool — including MULTIPLE listings within one
    # store (the barcode is the intentional sync key; SKUs may differ). The previous canonical-per-
    # store SELECT wrote only one listing per store, leaving sibling listings stale forever — their
    # divergent values then poisoned the (then store-keyed) fold baseline (the 2026-07-10 -200
    # incident). Converge ALL of them to Q.
    rows = db.execute(text("""
        SELECT pv.id AS variant_id, pv.store_id, s.name store, s.shopify_url, s.api_token,
               s.sync_location_id, pv.inventory_item_id
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.store_id, pv.id
    """), {"b": barcode}).mappings().all()

    converged, skipped, failed, retries = 0, 0, 0, 0
    per_store, live_quantities = [], {}
    for r in rows:
        if exclude_store_id is not None and r["store_id"] == exclude_store_id:
            continue
        item_gid = f"gid://shopify/InventoryItem/{r['inventory_item_id']}"
        loc_gid = f"gid://shopify/Location/{r['sync_location_id']}"
        cas_result, n_try, live = "skip", 0, None
        try:
            svc = ShopifyService(store_url=r["shopify_url"], token=r["api_token"])
            live = svc.get_available_single(item_gid, loc_gid)
            # key by store, disambiguating extra listings within the same store
            lq_key = r["store"] if r["store"] not in live_quantities else f"{r['store']}#{r['variant_id']}"
            live_quantities[lq_key] = live
            if live is None:
                # UNREADABLE: never blind-write (compare_quantity=None would SET unconditionally on
                # exactly the listing where evidence is weakest). Count as failed; the anchor below
                # still reseeds the baseline to Q so a stale spike can't poison the next fold.
                failed += 1
                cas_result = "unreadable"
            elif live == target:
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
            # ALWAYS reseed THIS store's ledger baseline to Q via a 'convergence' anchor — even when
            # the CAS write did NOT land (mirror-blind store / cas_conflict / unreadable). Otherwise
            # that store keeps a STALE source_prev (e.g. a transient spike value), and its next real
            # observation folds against it catastrophically. This is the 2026-06-26 spike-zeroing bug:
            # Esteban spiked 990->2050, an intervening converge pulled Q to 992 but Esteban's CAS was
            # mirror-blind so its baseline stayed ~2049; the next real Esteban obs 991 folded as
            # 992 + (991 - 2049) = -66 -> floored to 0, wiping a 990-unit store. Reseeding every
            # store's baseline to Q makes the revert fold cleanly (991 - 2050 against Q=2050 -> 991).
            # The MIRROR is updated ONLY on a landed write, so the mirror never lies about Shopify.
            db.execute(text("""INSERT INTO pool_events
                               (barcode, source_store_id, source_variant_id, inventory_item_id,
                                observed_quantity, source_timestamp, kind)
                               VALUES (:b,:s,:v,:i,:q, now(), 'convergence')"""),
                       {"b": barcode, "s": r["store_id"], "v": r["variant_id"],
                        "i": r["inventory_item_id"], "q": target})
            if cas_result in ("set", "set_after_retry", "already", "already_after_retry"):
                db.execute(text("""UPDATE inventory_levels SET available=:q, updated_at=now()
                                   WHERE variant_id=:vid AND location_id=:loc"""),
                           {"q": target, "vid": r["variant_id"], "loc": r["sync_location_id"]})
            if cas_result in ("set", "set_after_retry"):
                # P0 ECHO SUPPRESSION: the engine's OWN CAS write bounces back as a fresh inbound
                # webhook (new webhook_id -> the ledger's webhook_id dedup misses it). Record a
                # value-anchored WriteIntent so handle_webhook's _is_echo gate — which runs BEFORE the
                # canary block — recognises and DROPS that echo instead of re-ingesting it as an
                # observation. Without this the engine's own writes feed the oscillation detector and
                # trip FALSE rollbacks to legacy (the 4-day soak quarantined real restocks this way).
                # Deliberately value-based (quantity=target) with NO sync_operation_uuid: a REAL change
                # that rides in on the window (observed != target) is therefore NOT suppressed — it
                # flows to the canary path and folds correctly. inventory_item_id keeps it per-listing
                # precise so multi-listing within a store cannot cross-suppress.
                db.add(models.WriteIntent(
                    barcode=barcode, target_store_id=r["store_id"],
                    inventory_item_id=r["inventory_item_id"], quantity=target, barcode_version=0,
                    expires_at=datetime.now(timezone.utc) + timedelta(seconds=sync_guards.ECHO_TTL_SECONDS)))
        except Exception:
            failed += 1; cas_result = "error"
        # Commit PER LISTING: a mid-loop DB failure must never orphan an already-landed Shopify CAS
        # write without its anchor + echo marker (the un-anchored echo would later fold as a
        # corroborated genuine up-jump and double Q — the 2026-06-26 outage class).
        try:
            db.commit()
        except Exception:
            db.rollback()
        per_store.append({"store": r["store"], "variant_id": r["variant_id"], "cas_result": cas_result,
                          "retries": n_try, "live_before": live, "target": target})

    # P3: a fully-successful convergence drives every processed store to Q, so they now AGREE and any
    # customer-facing divergence is resolved. Clear the SLA clock immediately instead of waiting for
    # the next validation sweep (removes stale 'permanent divergence' CRITICAL noise). Only clears —
    # if the stores secretly still disagree, the next live-truth sweep re-arms diverged_since.
    if failed == 0 and (converged + skipped) > 0 and state.diverged_since is not None:
        try:
            state.diverged_since = None
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
    # ALL listings of the barcode are replicas (incl. several within one store) — mirror the
    # per-listing convergence writer.
    rows = db.execute(text("""
        SELECT pv.id AS variant_id, s.name store, il.available AS mirror
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE pv.barcode = :b AND pv.inventory_item_id IS NOT NULL
        ORDER BY pv.store_id, pv.id
    """), {"b": barcode}).mappings().all()
    intended, already = [], 0
    for r in rows:
        cur = r["mirror"]
        if cur == target:
            already += 1
        else:
            intended.append({"store": r["store"], "variant_id": r["variant_id"],
                             "current": cur, "target": target})
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
        if res.get("rejected"):
            # deep-negative fold rejected (apply_event already alerted CRITICAL) — pool unmoved.
            audit_logger.log(category="RECONCILIATION", action="pool_shadow_fold_rejected",
                             message=f"[{barcode}] shadow: negative fold rejected (deficit {res.get('deficit')})",
                             target=barcode, severity="WARN",
                             details={"webhook_id": webhook_id, **{k: v for k, v in res.items() if k != 'barcode'}})
            return {"fold_rejected": True}

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
