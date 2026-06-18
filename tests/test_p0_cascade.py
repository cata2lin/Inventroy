# tests/test_p0_cascade.py
"""
Regression tests for the P0 cascade-prevention guards, modelled directly on the
barcode 5901230000000 incident.

The pure guards (services/sync_guards.py) are where the cascade-prevention invariants
live, so they are tested here without a database. DB-integration tests (end-to-end stale
mirror / recursive propagation) are scaffolded at the bottom behind DATABASE_URL_TEST so
they never run against production.

Run: python tests/test_p0_cascade.py     (or: pytest tests/test_p0_cascade.py)
"""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services import sync_guards as g


def V(id, store_id, inv_item, is_barcode_primary=False, is_primary_variant=False, sku="SKU"):
    """A lightweight stand-in for a ProductVariant (the guards are duck-typed)."""
    return SimpleNamespace(id=id, store_id=store_id, inventory_item_id=inv_item,
                           is_barcode_primary=is_barcode_primary, is_primary_variant=is_primary_variant,
                           sku=sku)


def test_canonical_prefers_skud_variant_over_orphan():
    """Forensic regression (barcode 7865789069743): a SKU-less orphan with a LOWER id must NOT
    be chosen over the real SKU'd variant. Prevents propagation syncing the corrupt orphan."""
    orphan = V(100, 7, 1, sku="")        # lower id, no SKU (the 74,272 orphan)
    real = V(200, 7, 2, sku="HA-1234")   # higher id, real SKU (the 23,962 variant)
    t = g.select_canonical_targets([orphan, real], origin_store_id=99)[0]
    assert t.id == 200, "must prefer the SKU'd variant over the SKU-less orphan"


# --- P0.1: canonical targets — the structural cascade fix ------------------------------

def test_canonical_excludes_origin_store_and_collapses_duplicates():
    """The EXACT 5901230000000 shape: 3 variants in the origin store (CasaOfertelor) +
    1 each in 3 other stores. A single event must fan out to ONE item per OTHER store and
    ZERO items in the origin store. Old code would have written 5 items (incl 2 same-store)."""
    origin = 12
    variants = [
        # origin store duplicates (Roșu triggers; Albastru/Portocaliu are siblings)
        V(101, 12, 514694, is_barcode_primary=True),   # Roșu (the trigger's sibling set)
        V(102, 12, 547462),                             # Albastru
        V(103, 12, 580230),                             # Portocaliu
        # legit cross-store group
        V(201, 19, 559362),                             # MagDeal
        V(301, 18, 673686),                             # Ofertele
        V(401, 9, 141769),                              # Reduceri
    ]
    targets = g.select_canonical_targets(variants, origin_store_id=origin)
    store_ids = sorted(t.store_id for t in targets)
    assert store_ids == [9, 18, 19], f"expected one target per OTHER store, got {store_ids}"
    assert all(t.store_id != origin for t in targets), "origin store must NEVER be a target"
    assert len(targets) == 3, f"single event must fan out to 3 items, not amplify; got {len(targets)}"


def test_canonical_one_per_store_even_with_many_duplicates():
    variants = [V(1, 7, 1), V(2, 7, 2), V(3, 7, 3), V(4, 7, 4)]  # 4 dupes in store 7
    targets = g.select_canonical_targets(variants, origin_store_id=99)
    assert len(targets) == 1, "≤1 canonical variant per store"
    assert targets[0].store_id == 7


def test_canonical_selection_is_deterministic_and_prefers_primary():
    # is_barcode_primary wins regardless of id order
    variants = [V(50, 7, 1), V(10, 7, 2, is_barcode_primary=True), V(30, 7, 3, is_primary_variant=True)]
    t = g.select_canonical_targets(variants, origin_store_id=99)[0]
    assert t.id == 10, "is_barcode_primary must be chosen"
    # without primaries, lowest id wins (stable)
    variants2 = [V(50, 7, 1), V(10, 7, 2), V(30, 7, 3)]
    t2 = g.select_canonical_targets(variants2, origin_store_id=99)[0]
    assert t2.id == 10, "lowest id is the stable tiebreak"


def test_canonical_skips_variants_without_inventory_item():
    variants = [V(1, 7, None), V(2, 7, 222)]
    t = g.select_canonical_targets(variants, origin_store_id=99)
    assert len(t) == 1 and t[0].inventory_item_id == 222


# --- P0.3: abnormal delta guard --------------------------------------------------------

def test_delta_guard_blocks_oversized():
    assert g.check_delta(g.MAX_ABS_DELTA + 1)[0] is False
    assert g.check_delta(-(g.MAX_ABS_DELTA + 1))[0] is False
    assert g.check_delta(5)[0] is True
    assert g.check_delta(-5)[0] is True
    assert g.check_delta(None)[0] is True


# --- P0.4: inventory floor -------------------------------------------------------------

def test_floor_clamps_negative_projection():
    op, value, clamped = g.apply_floor(2, -10, floor=0)
    assert (op, value, clamped) == ("set", 0, True), "must clamp to floor, not go to -8"
    op, value, clamped = g.apply_floor(100, -5, floor=0)
    assert (op, value, clamped) == ("adjust", -5, False)
    op, value, clamped = g.apply_floor(None, -5, floor=0)
    assert op == "adjust" and clamped is False, "unknown current → adjust, don't fabricate"


# --- P0.2: storm circuit breaker (the cascade signature) -------------------------------

def test_storm_breaker_trips_on_repeated_propagation():
    g.reset_storm_state()
    bc = "5901230000000"
    t = 1000.0
    # one legit propagation: not a storm
    g.record_propagation(bc, now=t)
    assert g.is_storming(bc, now=t) is False
    # the cascade: many propagations inside the window → storm
    for i in range(g.STORM_MAX_PROPAGATIONS + 2):
        g.record_propagation(bc, now=t + i * 0.5)
    assert g.is_storming(bc, now=t + 1.0) is True, "repeated -477 cascade must trip the breaker"


def test_storm_window_expiry():
    g.reset_storm_state()
    bc = "x"
    t = 0.0
    for i in range(g.STORM_MAX_PROPAGATIONS + 2):
        g.record_propagation(bc, now=t + i)
    # far in the future, the window has slid past all of them
    assert g.is_storming(bc, now=t + g.STORM_WINDOW_SECONDS + 100) is False


def test_quarantine_lifecycle():
    g.reset_storm_state()
    bc = "q"
    assert g.is_quarantined(bc, now=0.0) is False
    g.quarantine(bc, now=0.0)
    assert g.is_quarantined(bc, now=1.0) is True
    assert g.is_quarantined(bc, now=g.STORM_QUARANTINE_SECONDS + 1) is False


# --- kill switch -----------------------------------------------------------------------

def test_kill_switch_reads_env_live():
    old = os.environ.get("SYNC_PROPAGATION_ENABLED")
    try:
        os.environ["SYNC_PROPAGATION_ENABLED"] = "false"
        assert g.propagation_enabled() is False
        os.environ["SYNC_PROPAGATION_ENABLED"] = "true"
        assert g.propagation_enabled() is True
    finally:
        if old is None:
            os.environ.pop("SYNC_PROPAGATION_ENABLED", None)
        else:
            os.environ["SYNC_PROPAGATION_ENABLED"] = old


# --- end-to-end cascade simulation (pure, no DB) ---------------------------------------

def test_full_cascade_is_structurally_impossible():
    """Simulate the incident end-to-end at the guard level:
    origin store has 3 same-barcode variants; 3 other stores have 1 each. Fire the event
    repeatedly (as the old echoes would) and assert (a) each propagation targets exactly
    3 items (one per other store, never the origin), and (b) the storm breaker trips before
    the run can snowball."""
    g.reset_storm_state()
    bc = "5901230000000"
    origin = 12
    group = [V(101, 12, 1, is_barcode_primary=True), V(102, 12, 2), V(103, 12, 3),
             V(201, 19, 4), V(301, 18, 5), V(401, 9, 6)]

    total_items_written = 0
    tripped_at = None
    t = 500.0
    for hop in range(20):  # an old cascade would run away here
        if g.is_quarantined(bc, now=t):
            tripped_at = hop
            break
        targets = g.select_canonical_targets(group, origin_store_id=origin)
        assert len(targets) == 3, "no hop may ever write more than one item per other store"
        assert all(x.store_id != origin for x in targets)
        total_items_written += len(targets)
        g.record_propagation(bc, now=t)
        if g.is_storming(bc, now=t):
            g.quarantine(bc, now=t)
        t += 0.4

    assert tripped_at is not None, "the storm breaker must stop a runaway before 20 hops"
    assert total_items_written <= 3 * (g.STORM_MAX_PROPAGATIONS + 1), "bounded fan-out, no explosion"


# --- DB-integration tests (scaffold; only run when DATABASE_URL_TEST is set) -----------

import unittest

@unittest.skipUnless(os.getenv("DATABASE_URL_TEST"),
                     "set DATABASE_URL_TEST to a throwaway Postgres to run DB integration tests")
class TestHandlerIntegration(unittest.TestCase):
    """End-to-end handler tests (stale mirror, recursive propagation, delayed delivery,
    concurrency) require a real Postgres test DB because the models use JSONB/Computed/
    server_default. Wire a disposable DB via DATABASE_URL_TEST and a Shopify stub to run.
    Scaffolded here so the harness exists; implementation follows in the P1 test pass."""

    def test_placeholder(self):
        self.skipTest("integration harness pending P1 test DB fixture")


# --- runner ---------------------------------------------------------------------------

if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} pure-guard tests passed")
    sys.exit(0 if passed == len(fns) else 1)
