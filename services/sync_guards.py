# services/sync_guards.py
"""
P0 cascade-prevention guards.

Pure, dependency-light functions (easy to unit-test without a DB) plus an in-process
storm circuit-breaker. These are the structural safeguards that make the barcode-5901230000000
class of cascade impossible:

  - select_canonical_targets : at most ONE variant per store, origin store fully excluded (P0.1)
  - check_delta              : abnormal single-delta ceiling (P0.3)
  - apply_floor              : per-variant negative-inventory floor (P0.4)
  - storm breaker            : trips when a barcode is propagated too often in a window (P0.2 backstop)
  - propagation_enabled      : global kill switch (supports staged rollout / emergency stop)

Tuning is via environment variables so production can adjust without a code change.
"""
import os
import threading
import time
from collections import deque, defaultdict
from typing import Dict, List, Optional, Tuple, Any


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# --- Tunables (env-overridable) ---
MAX_ABS_DELTA = _env_int("SYNC_MAX_ABS_DELTA", 1000)          # block single deltas bigger than this
INVENTORY_FLOOR = _env_int("SYNC_INVENTORY_FLOOR", 0)         # never propagate a target below this
# Echo markers now LIVE for 15 minutes (Shopify webhook delivery routinely exceeds 45s under load —
# a late echo of our own write re-ingested as an external observation was the self-zeroing loop of
# 2026-07-13). Suppression is VALUE-INDEPENDENT only within the short window below; an older marker
# suppresses only an exact value match, so a real change can never be swallowed by the long TTL.
ECHO_TTL_SECONDS = _env_int("SYNC_ECHO_TTL_SECONDS", 900)
ECHO_VALUE_INDEPENDENT_SECONDS = _env_int("SYNC_ECHO_VALUE_INDEPENDENT_SECONDS", 45)
STORM_MAX_PROPAGATIONS = _env_int("SYNC_STORM_MAX_PROPAGATIONS", 6)   # per barcode per window
STORM_WINDOW_SECONDS = _env_int("SYNC_STORM_WINDOW_SECONDS", 60)
STORM_QUARANTINE_SECONDS = _env_int("SYNC_STORM_QUARANTINE_SECONDS", 600)
MAX_PROPAGATION_DEPTH = _env_int("SYNC_MAX_PROPAGATION_DEPTH", 3)
# Floor-breach policy (2026-07-14): a negative delta LARGER than a target's current stock is evidence
# the delta is corrupt (a pool cannot lose more than it has) — the write is REJECTED and the barcode
# quarantined instead of flooring the store to 0 (which destroyed 415/496 real units on 2026-07-13).
# Tolerance 0 = every breach rejects; raise only to absorb tiny stockout races (last-unit oversells).
FLOOR_BREACH_TOLERANCE = _env_int("SYNC_FLOOR_BREACH_TOLERANCE", 0)
FLOOR_BREACH_QUARANTINE_SECONDS = _env_int("SYNC_FLOOR_BREACH_QUARANTINE_SECONDS", 86400)
# A drop of at least this many units in one observation is verified against the source store's LIVE
# Shopify value before it is believed (fail-closed: unverifiable big drops do not propagate/fold).
BIG_DROP_VERIFY_UNITS = _env_int("SYNC_BIG_DROP_VERIFY_UNITS", 50)
# Engine fold: a fold that would take the pool this far BELOW the floor is corrupt input (poisoned
# baseline), not sold stock — reject + quarantine instead of silently clamping the pool to 0.
FOLD_NEGATIVE_TOLERANCE = _env_int("SYNC_FOLD_NEGATIVE_TOLERANCE", 3)


def propagation_enabled() -> bool:
    """Global kill switch. Set SYNC_PROPAGATION_ENABLED=false to stop ALL propagation
    while still ingesting webhooks and updating the local mirror + audit trail."""
    return _env_bool("SYNC_PROPAGATION_ENABLED", True)


def use_sync_groups() -> bool:
    """When true, propagation targets are resolved via explicit sync_group membership
    (P3) instead of raw barcode equality. Default false until the backfill is verified."""
    return _env_bool("SYNC_USE_GROUPS", False)


def echo_authoritative_enabled() -> bool:
    """When true, delta propagation writes each item via its OWN single-item Shopify mutation and
    stamps the Shopify-authoritative post-write `available` quantity onto the echo marker. An inbound
    webhook then computes residual = observed - authoritative_qty: residual 0 => pure echo (suppress);
    residual != 0 => a real change rode in on the same echo window (propagate exactly that residual,
    anchored to Shopify truth, never the drifted local mirror). This closes the propagation/sale race
    that drops a real delta. Default OFF => behaviour is byte-identical to the value-independent path,
    and any marker without a captured authoritative_qty always falls back to that safe path, so the
    stale-mirror phantom-delta cascade can never be re-opened."""
    return _env_bool("SYNC_ECHO_AUTHORITATIVE", False)


def echo_authoritative_barcodes() -> set:
    """Optional canary allowlist (comma-separated barcodes). When non-empty, the authoritative-anchored
    path applies ONLY to these barcodes (so it can be validated on one barcode before going broad).
    Empty => applies to all barcodes once the master flag is on."""
    raw = os.getenv("SYNC_ECHO_AUTHORITATIVE_BARCODES", "").strip()
    return {b.strip() for b in raw.split(",") if b.strip()}


def echo_authoritative_for(barcode: str) -> bool:
    """True if the authoritative-anchored echo path should be used for THIS barcode: master flag on
    AND (no canary allowlist set OR this barcode is on it)."""
    if not echo_authoritative_enabled():
        return False
    allow = echo_authoritative_barcodes()
    return (not allow) or (barcode in allow)


# --- P0.1: canonical target selection ---

def _canonical_rank(v: Any) -> tuple:
    """Lower tuple sorts first => more canonical.
    Order: is_barcode_primary, then is_primary_variant, then HAS-SKU (prefer a properly
    cataloged variant over a SKU-less orphan duplicate), then lowest id (stable).

    The SKU tiebreak was added after forensic analysis found 209 (barcode,store) sites where
    the lowest-id variant was a SKU-less orphan carrying a corrupt value (e.g. 74,272 vs the
    real 23,962), so propagation was syncing the orphan and ignoring the real listing."""
    sku = getattr(v, "sku", None)
    has_sku = bool(sku and str(sku).strip())
    return (
        0 if bool(getattr(v, "is_barcode_primary", False)) else 1,
        0 if bool(getattr(v, "is_primary_variant", False)) else 1,
        0 if has_sku else 1,
        getattr(v, "id", 0) or 0,
    )


def select_canonical_targets(variants: List[Any], origin_store_id: int) -> List[Any]:
    """Collapse propagation targets to AT MOST ONE canonical variant per store and
    EXCLUDE the origin store entirely.

    This is the structural fix for same-store amplification: a single inventory event
    can never fan out to >1 inventory item per store, and can never write back into the
    store it originated from (no sibling self-amplification, no write-to-origin).
    """
    best: Dict[int, Any] = {}
    for v in variants:
        store_id = getattr(v, "store_id", None)
        if store_id is None or store_id == origin_store_id:
            continue  # never target the originating store
        if not getattr(v, "inventory_item_id", None):
            continue
        cur = best.get(store_id)
        if cur is None or _canonical_rank(v) < _canonical_rank(cur):
            best[store_id] = v
    return list(best.values())


# --- P0.3: abnormal delta protection ---

def check_delta(delta: Optional[int]) -> Tuple[bool, Optional[str]]:
    """Return (allowed, reason). A delta whose magnitude exceeds MAX_ABS_DELTA is NOT
    auto-propagated (it is routed to reconciliation / review instead of a blind delta)."""
    if delta is None:
        return True, None
    if abs(delta) > MAX_ABS_DELTA:
        return False, f"abs(delta)={abs(delta)} exceeds MAX_ABS_DELTA={MAX_ABS_DELTA}"
    return True, None


# --- P0.4: inventory floor ---

def apply_floor(current_available: Optional[int], delta: int,
                floor: int = INVENTORY_FLOOR) -> Tuple[str, int, bool]:
    """Decide how to write one target so it never drops below the floor.

    Returns (op, value, clamped):
      - ("adjust", delta, False) when current+delta >= floor (normal relative write)
      - ("set", floor, True)     when it would breach the floor (absolute clamp)
    When current is unknown, we cannot project safely → adjust but flag for the caller
    to floor-check post-write (op="adjust", clamped=False)."""
    if current_available is None:
        return "adjust", delta, False
    projected = current_available + delta
    if projected < floor:
        return "set", floor, True
    return "adjust", delta, False


def floor_breach_magnitude(current_available: Optional[int], delta: int,
                           floor: int = INVENTORY_FLOOR) -> int:
    """How many units BELOW the floor current+delta would land (0 = no breach / unknown current)."""
    if current_available is None:
        return 0
    projected = current_available + delta
    return max(floor - projected, 0)


def floor_breach_rejects(current_available: Optional[int], delta: int,
                         floor: int = INVENTORY_FLOOR,
                         tolerance: Optional[int] = None) -> Tuple[bool, int]:
    """Floor-breach policy: (reject, breach_magnitude).

    A breach beyond FLOOR_BREACH_TOLERANCE means the DELTA IS CORRUPT (you cannot sell more than a
    store holds) — the caller must reject the whole propagation and quarantine the barcode, never
    write the floor. A breach within tolerance is a plausible last-units stockout race and may be
    clamped (visibly) instead."""
    if tolerance is None:
        tolerance = FLOOR_BREACH_TOLERANCE
    breach = floor_breach_magnitude(current_available, delta, floor)
    return breach > tolerance, breach


def should_verify_drop(last_known: Optional[int], observed: int,
                       threshold: Optional[int] = None) -> bool:
    """True when an observed drop is big enough to demand live-Shopify verification before it is
    believed (propagated / folded). Drops below the threshold pass unverified (normal sales)."""
    if threshold is None:
        threshold = BIG_DROP_VERIFY_UNITS
    if last_known is None:
        return False
    return (last_known - observed) >= threshold


def classify_fold(q_old: Optional[int], source_prev_observed: Optional[int], observed: int,
                  floor: int = INVENTORY_FLOOR,
                  tolerance: Optional[int] = None) -> Tuple[str, int, int]:
    """PURE verdict for one pool fold: (verdict, quantity, deficit).

      ("apply",  q_new, 0)        -> normal fold (bootstrap / replica-join / non-negative result)
      ("clamp",  floor, deficit)  -> small negative result (<= tolerance): plausible stockout race,
                                     clamp to the floor but the caller must ALERT (never silent)
      ("reject", q_old, deficit)  -> deep negative result: the per-source baseline is poisoned or the
                                     observation is phantom — do NOT move the pool; quarantine + alert.
    """
    if tolerance is None:
        tolerance = FOLD_NEGATIVE_TOLERANCE
    if q_old is None:
        return "apply", max(observed, floor), 0
    if source_prev_observed is None:
        return "apply", q_old, 0
    raw = q_old + (observed - source_prev_observed)
    if raw >= floor:
        return "apply", raw, 0
    deficit = floor - raw
    if deficit <= tolerance:
        return "clamp", floor, deficit
    return "reject", q_old, deficit


# --- P0.2 backstop: per-barcode storm circuit breaker (in-process) ---
# NOTE: in-process state is correct for the current single-uvicorn-worker deployment.
# P2.1 replaces/augments this with a DB/Redis-backed breaker for multi-worker safety.

_storm_lock = threading.Lock()
_propagation_history: Dict[str, deque] = defaultdict(deque)
_quarantined_until: Dict[str, float] = {}


def record_propagation(barcode: str, now: Optional[float] = None) -> None:
    now = now if now is not None else time.monotonic()
    with _storm_lock:
        dq = _propagation_history[barcode]
        dq.append(now)
        cutoff = now - STORM_WINDOW_SECONDS
        while dq and dq[0] < cutoff:
            dq.popleft()


def is_storming(barcode: str, now: Optional[float] = None) -> bool:
    """True if this barcode has been propagated more than STORM_MAX_PROPAGATIONS times
    within STORM_WINDOW_SECONDS (the runaway-cascade signature)."""
    now = now if now is not None else time.monotonic()
    with _storm_lock:
        dq = _propagation_history.get(barcode)
        if not dq:
            return False
        cutoff = now - STORM_WINDOW_SECONDS
        while dq and dq[0] < cutoff:
            dq.popleft()
        return len(dq) > STORM_MAX_PROPAGATIONS


def quarantine(barcode: str, now: Optional[float] = None) -> None:
    now = now if now is not None else time.monotonic()
    with _storm_lock:
        _quarantined_until[barcode] = now + STORM_QUARANTINE_SECONDS


def is_quarantined(barcode: str, now: Optional[float] = None) -> bool:
    now = now if now is not None else time.monotonic()
    with _storm_lock:
        until = _quarantined_until.get(barcode)
        if until is None:
            return False
        if now >= until:
            _quarantined_until.pop(barcode, None)
            return False
        return True


def reset_storm_state() -> None:
    """Test helper."""
    with _storm_lock:
        _propagation_history.clear()
        _quarantined_until.clear()
