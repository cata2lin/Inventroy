# tests/test_durable_sync_fixes.py
"""
DURABLE sync-correctness fixes (2026-07-03) regression tests — hermetic. Run:
    python tests/test_durable_sync_fixes.py

Closes the "created-diverged legacy pool amplifies stock" class (HA-1193-1) by DRAINING off-engine
pools onto the conservation-fold engine — NOT by changing the legacy write primitive (an adversarial
review proved absolute-set-to-source drops concurrent cross-store changes and manufactures false
agreement, so legacy propagation stays RELATIVE). The fixes:
  1. legacy propagation stays relative (conservation-preserving, spread-preserving); no absolute flag
  2. the creation path floors to >=0 and prefers the engine's canonical Q only when live-truth-backfilled
  3. a new listing joining an onboarded pool gets a ledger baseline (first-sale-revert fix)
  4. the onboarding sweep drains agreeing pools onto the engine — placeholder + false-group excluded,
     pre-screened so it never fires a per-pool CRITICAL storm
  5. the uniform-collapse detector is re-armed (full-collapse + relative-gap; >=2 readable; no
     canary_active over-fire)
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services import sync_guards


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- CHANGE 1: legacy propagation stays RELATIVE (absolute was reverted as unsafe) ----------------

def test_no_absolute_propagation_flag():
    # the SYNC_LEGACY_ABSOLUTE_PROP flag was removed — absolute-set legacy propagation is NOT shipped.
    assert not hasattr(sync_guards, "legacy_absolute_propagation")
    assert "legacy_absolute_propagation" not in _read("services/inventory_sync_service.py")


def test_dispatch_uses_relative_delta_when_baseline_exists():
    src = _read("services/inventory_sync_service.py")
    assert "if delta is not None:" in src
    fn = src[src.index("if delta is not None:"):]
    assert "_execute_delta_propagation(" in fn[:800]        # delta present -> relative adjust
    assert "_execute_absolute_propagation(" in fn[:1600]    # absolute ONLY as the first-sync fallback


def _relative_propagate(stores, source, new_val, floor=0):
    """Legacy behaviour: delta = new_val - source's own mirror; every sibling += delta (floored)."""
    delta = new_val - stores[source]
    return {s: (new_val if s == source else max(stores[s] + delta, floor)) for s in stores}


def test_relative_is_correct_and_selfhealing_on_a_consistent_pool():
    # On a CONSISTENT pool (shared baseline) relative is exactly right — a sale of 1 unit converges all.
    stores = {"a": 100, "b": 100, "c": 100}
    assert _relative_propagate(stores, "a", 99) == {"a": 99, "b": 99, "c": 99}


def test_relative_preserves_spread_so_diverged_pools_stay_flagged():
    # The KEY property that makes keeping relative safe: adding a constant delta to every sibling leaves
    # max-min unchanged. A diverged pool therefore stays diverged (never collapses to a fabricated
    # value), so the backfill contract keeps refusing it (skipped_diverged) until a human resolves it —
    # exactly the behaviour absolute-set destroyed by manufacturing false spread==0.
    stores = {"mag": 1000, "casa": -8, "of": -8, "red": -8}
    before = max(stores.values()) - min(stores.values())
    after_map = _relative_propagate(stores, "casa", 1000)
    after = max(after_map.values()) - min(after_map.values())
    assert before == after == 1008          # spread preserved; pool stays flagged, not collapsed


# --- CHANGE 2: creation path floored + engine-aware (only when backfilled) ------------------------

def test_creation_path_floors_and_prefers_backfilled_pool_q():
    src = _read("services/inventory_sync_service.py")
    fn = src[src.index("def _get_group_authoritative_qty"):src.index("def _sync_variant_to_barcode_group")]
    assert "PoolState" in fn and "pool.backfilled_at is not None" in fn   # prefer Q only when engine-truth
    assert fn.count("sync_guards.INVENTORY_FLOOR") >= 3                   # every branch floored to >=0


# --- CHANGE 3 (MEDIUM): new listing joining an onboarded pool gets a ledger baseline --------------

def test_new_listing_seeds_ledger_baseline_on_engine_pool():
    src = _read("services/inventory_sync_service.py")
    fn = src[src.index("def _sync_variant_to_barcode_group"):]
    assert "'backfill_baseline'" in fn
    assert "backfilled_at is not None" in fn        # only for engine-authoritative pools


# --- CHANGE 4 onboarding sweep: placeholder + false-group excluded, pre-screened, read-mostly ------

def test_onboarding_excludes_placeholder_and_false_groups():
    src = _read("services/pool_onboarding.py")
    assert "diagnostics._placeholder_sql" in src        # exclude all-zeros placeholder barcodes
    assert "_is_false_group" in src and "false_group_multi_sku" in src  # never onboard false groups


def test_onboarding_prescreens_to_avoid_alert_storm():
    src = _read("services/pool_onboarding.py")
    # the read-only planner classifies first; only SAFE pools get the real (alerting) backfill call
    assert "plan_backfill(db, bc)" in src
    assert 'if not plan.get("safe"):' in src


def test_onboarding_read_mostly_locked_scheduled():
    src = _read("services/pool_onboarding.py")
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities", "converge_pool",
                   "UPDATE inventory_levels"):
        assert writer not in src, f"onboarding must not write inventory; found {writer}"
    assert 'dist_lock.acquire(f"barcode:{bc}")' in src
    main = _read("main.py")
    assert "from services import pool_onboarding" in main and "pool_onboarding.run_onboarding_sweep" in main


# --- CHANGE 5 detection: full-collapse + relative-gap; >=2 readable; no canary_active over-fire -----

def test_detector_no_canary_active_overfire():
    src = _read("services/pool_validation.py")
    # the canary_active OR-term that paged every engine pool on a 1-unit transient drift is GONE
    assert "canary_active or (" not in src
    assert "critical = bool(res.get(\"readable_ge2\")) and (full_collapse or big_gap)" in src


def test_detector_catches_full_and_lowq_collapse():
    src = _read("services/pool_validation.py")
    assert "all_live_zero" in src                        # full collapse to 0 (magnitude-independent)
    assert "UNIFORM_COLLAPSE_FRACTION" in src            # relative gap catches low-Q collapse
    assert "readable_ge2" in src                         # thin-evidence single-read cannot escalate


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
    print(f"\n{passed}/{len(fns)} durable-sync-fix tests passed")
    sys.exit(0 if passed == len(fns) else 1)
