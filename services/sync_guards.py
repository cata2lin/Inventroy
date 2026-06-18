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
ECHO_TTL_SECONDS = _env_int("SYNC_ECHO_TTL_SECONDS", 45)      # value-independent echo window
STORM_MAX_PROPAGATIONS = _env_int("SYNC_STORM_MAX_PROPAGATIONS", 6)   # per barcode per window
STORM_WINDOW_SECONDS = _env_int("SYNC_STORM_WINDOW_SECONDS", 60)
STORM_QUARANTINE_SECONDS = _env_int("SYNC_STORM_QUARANTINE_SECONDS", 600)
MAX_PROPAGATION_DEPTH = _env_int("SYNC_MAX_PROPAGATION_DEPTH", 3)


def propagation_enabled() -> bool:
    """Global kill switch. Set SYNC_PROPAGATION_ENABLED=false to stop ALL propagation
    while still ingesting webhooks and updating the local mirror + audit trail."""
    return _env_bool("SYNC_PROPAGATION_ENABLED", True)


def use_sync_groups() -> bool:
    """When true, propagation targets are resolved via explicit sync_group membership
    (P3) instead of raw barcode equality. Default false until the backfill is verified."""
    return _env_bool("SYNC_USE_GROUPS", False)


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
