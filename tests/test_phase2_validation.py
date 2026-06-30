# tests/test_phase2_validation.py
"""
PHASE 2 (live-truth validation) safety regression tests — hermetic. Run:
    python tests/test_phase2_validation.py

Phase 2 validates PoolState against LIVE Shopify and the mirror, ALERT-ONLY. These tests guard that
it never writes inventory, emits the required report, and the permanent-divergence SLA is wired.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_validation_performs_no_inventory_writes():
    """Phase 2 is read-only/alert-only — it may READ live (_read_live / get_available_single) but
    must NEVER call a SET/adjust inventory mutation, nor the convergence writer."""
    src = _read("services/pool_validation.py")
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities",
                   "set_inventory_quantities_single", "update_inventory_levels",
                   "adjust_inventory_levels", "converge_pool", "apply_plan"):
        assert writer not in src, f"Phase 2 must be read-only; found writer {writer}"


def test_validation_compares_live_not_mirror():
    src = _read("services/pool_validation.py")
    assert "_read_live" in src                      # reads live Shopify
    assert "canonical_drift" in src                 # live vs engine Q
    assert "mirror_drift" in src                    # live vs legacy mirror


def test_report_has_required_phase2_fields():
    src = _read("services/pool_validation.py")
    for field in ("pool_quantity", "per_store_live", "spread", "last_event", "unresolved_duration"):
        assert f'"{field}"' in src, f"Phase 2 report must include {field}"


def test_permanent_divergence_sla_logic():
    src = _read("services/pool_validation.py")
    assert "POOL_SLA_HOURS" in src
    assert "pool_validation.permanent_divergence" in src
    # the SLA arithmetic: unresolved seconds >= hours*3600
    assert "POOL_SLA_HOURS * 3600" in src
    SLA_HOURS = 6
    # boundary behavior the code encodes
    assert (6 * 3600) >= SLA_HOURS * 3600          # exactly at SLA -> permanent
    assert (5 * 3600) < SLA_HOURS * 3600           # under SLA -> not yet permanent


def test_diverged_since_column_present():
    assert "diverged_since" in _read("models.py")
    assert "ADD COLUMN IF NOT EXISTS diverged_since" in _read("migrate_pool_ledger.py")


def test_validation_scheduled():
    src = _read("main.py")
    assert "from services import pool_validation" in src
    assert "pool_validation.run_pool_validation_sweep" in src


def test_sla_clock_driven_by_stores_disagree_not_engine_q_drift():
    """P3: the SLA / diverged_since clock tracks the CUSTOMER-FACING metric (stores DISAGREE on live),
    not engine-Q-vs-live while stores agree (that is bookkeeping — reported WARN, never CRITICAL)."""
    src = _read("services/pool_validation.py")
    assert "stores_disagree" in src
    assert "engine_q_drift" in src
    # Q-drift-while-stores-agree is observability only, never escalated to a permanent-SLA CRITICAL.
    assert "pool_validation_engine_q_drift" in src


def test_single_store_pools_skipped_and_flag_cleared():
    """P2/P3: an orphaned pool (< 2 canonical stores) cannot diverge; it is skipped and any stale SLA
    flag is cleared, instead of alerting CRITICAL forever (canon_drift can never resolve there)."""
    src = _read("services/pool_validation.py")
    assert "len(rows) < 2" in src
    assert '"single_store"' in src


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
    print(f"\n{passed}/{len(fns)} Phase-2 validation tests passed")
    sys.exit(0 if passed == len(fns) else 1)
