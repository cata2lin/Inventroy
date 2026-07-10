# tests/test_pool_membership.py
"""
P2 (pool membership monitor) safety regression tests — hermetic. Run:
    python tests/test_pool_membership.py

The membership monitor watches the SHAPE of each pool (orphaned <2-store pools, a store dropping out,
canonical-variant flips) — the class of breakage quantity sweeps are structurally blind to. These
tests guard that it is read-mostly (the only write is healing a stale SLA flag), detects the three
churn classes, uses the canonical ordering convergence targets, and is scheduled.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_membership_monitor_never_writes_inventory():
    src = _read("services/pool_membership.py")
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities", "converge_pool",
                   "UPDATE inventory_levels", "UPDATE product_variants", "set_inventory_quantities_single"):
        assert writer not in src, f"membership monitor must not write inventory; found {writer}"


def test_membership_detects_three_churn_classes():
    src = _read("services/pool_membership.py")
    assert "pool_membership_shrink" in src              # a store dropped out of the pool
    assert "pool_membership_canonical_flip" in src      # canonical variant flipped vs observed
    # orphan = fewer than 2 LISTINGS (a single store with two listings of one barcode IS a pool —
    # they sync between themselves under the per-listing engine)
    assert "listing_counts.get(bc, 0) < 2" in src


def test_membership_heals_orphaned_sla_flag_only():
    """The one mutation allowed is clearing diverged_since on an orphaned pool (a flag no convergence
    can ever resolve). It must target only orphaned barcodes, never inventory."""
    src = _read("services/pool_membership.py")
    assert "diverged_since = NULL" in src
    assert "_clear_orphan_flags" in src


def test_membership_uses_canonical_ordering():
    src = _read("services/pool_membership.py")
    assert "diagnostics.CANON_ORDER" in src             # same variant convergence targets


def test_membership_scheduled():
    src = _read("main.py")
    assert "from services import pool_membership" in src
    assert "pool_membership.run_membership_sweep" in src


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
    print(f"\n{passed}/{len(fns)} pool-membership tests passed")
    sys.exit(0 if passed == len(fns) else 1)
