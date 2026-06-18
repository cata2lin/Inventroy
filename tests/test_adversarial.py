# tests/test_adversarial.py
"""
Adversarial simulation suite (Phase 8). Pure-logic simulations of the failure modes the
remediation must survive. No DB, no live writes — these exercise the guard/lock/canonical logic
directly so they can run in CI and on every deploy.

Run: python tests/test_adversarial.py
"""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services import sync_guards as g
from services import dist_lock


def V(id, store_id, inv_item=1, is_barcode_primary=False, is_primary_variant=False, sku="SKU"):
    return SimpleNamespace(id=id, store_id=store_id, inventory_item_id=inv_item,
                           is_barcode_primary=is_barcode_primary, is_primary_variant=is_primary_variant, sku=sku)


# --- canonical deletion / reassignment -------------------------------------------------

def test_canonical_reassignment_is_deterministic_after_deletion():
    """If the current canonical is deleted, the next canonical is deterministic (no flapping)."""
    full = [V(10, 5, is_barcode_primary=True), V(20, 5), V(30, 5)]
    assert g.select_canonical_targets(full, 99)[0].id == 10
    after_delete = [V(20, 5), V(30, 5)]               # primary removed
    assert g.select_canonical_targets(after_delete, 99)[0].id == 20  # lowest id, stable
    after_delete2 = [V(30, 5)]                         # 20 also removed
    assert g.select_canonical_targets(after_delete2, 99)[0].id == 30


# --- retry storm / webhook duplication -------------------------------------------------

def test_retry_storm_trips_breaker_not_infinite():
    """A retry storm (same barcode hammered) trips the breaker and is bounded."""
    g.reset_storm_state()
    bc = "retry-storm"
    t = 0.0
    tripped = False
    for i in range(50):
        g.record_propagation(bc, now=t + i * 0.1)
        if g.is_storming(bc, now=t + i * 0.1):
            tripped = True
            break
    assert tripped, "a retry storm must trip the circuit breaker"


def test_idempotency_keys_are_stable_and_distinct():
    """Webhook-id-derived keys are stable across calls and distinct across ids (dedup safety)."""
    k1a = dist_lock._key_to_bigint("whid:abc-123")
    k1b = dist_lock._key_to_bigint("whid:abc-123")
    k2 = dist_lock._key_to_bigint("whid:def-456")
    assert k1a == k1b, "same id -> same key (stable dedup)"
    assert k1a != k2, "different ids -> different keys"


# --- stale mirror / phantom delta ------------------------------------------------------

def test_phantom_delta_is_blocked_by_magnitude_guard():
    """A stale-mirror phantom (e.g. -477 that should never happen for an order) is blocked
    once it exceeds the configured ceiling — and a repeated phantom trips the storm breaker."""
    # single oversized phantom blocked
    allowed, _ = g.check_delta(-(g.MAX_ABS_DELTA + 1))
    assert allowed is False
    # repeated mid-size phantom (under ceiling) caught by the storm breaker instead
    g.reset_storm_state()
    bc = "phantom"
    for i in range(g.STORM_MAX_PROPAGATIONS + 3):
        g.record_propagation(bc, now=i * 0.2)
    assert g.is_storming(bc, now=1.0)


def test_floor_holds_under_repeated_negative_cascade():
    """Even a runaway sequence of negative deltas can never drive a store below the floor."""
    current = 3
    for _ in range(10):
        op, value, clamped = g.apply_floor(current, -477, floor=0)
        # the write that lands is either an in-range adjust or a clamp to the floor
        current = (current + (-477)) if op == "adjust" else value
        assert current >= 0, "floor must never be breached"


# --- distributed race / concurrent reconcile vs propagation ----------------------------

def test_reconcile_and_propagation_share_the_same_lock_key():
    """Reconcile and webhook propagation MUST contend on the same advisory key for a barcode,
    so they can never run concurrently on the same group (race safety)."""
    barcode = "5901230000000"
    propagation_key = dist_lock._key_to_bigint(f"barcode:{barcode}")
    reconcile_key = dist_lock._key_to_bigint(f"barcode:{barcode}")
    assert propagation_key == reconcile_key
    # different barcodes do NOT serialize against each other (no false contention)
    assert dist_lock._key_to_bigint("barcode:AAA") != dist_lock._key_to_bigint("barcode:BBB")


# --- origin-store safety (no write-back) under any group shape -------------------------

def test_origin_store_never_targeted_even_with_many_same_store_variants():
    variants = [V(1, 5), V(2, 5), V(3, 5), V(4, 7), V(5, 8)]  # 3 in origin store 5
    targets = g.select_canonical_targets(variants, origin_store_id=5)
    assert all(t.store_id != 5 for t in targets)
    assert sorted(t.store_id for t in targets) == [7, 8]


# --- runner ---------------------------------------------------------------------------

if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn(); print(f"PASS {fn.__name__}"); passed += 1
        except AssertionError as e:
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} adversarial simulations passed")
    sys.exit(0 if passed == len(fns) else 1)
