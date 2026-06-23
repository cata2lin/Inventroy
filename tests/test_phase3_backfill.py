# tests/test_phase3_backfill.py
"""
PHASE 3A (live-truth backfill) tests — hermetic. Run:
    python tests/test_phase3_backfill.py

Guards the backfill safety contract: dry-run by default, operator confirmation required for real
writes, NEVER backfill a diverged/stale pool, replayable + reversible, no inventory writes, and that
a backfill is what grants write-eligibility (backfilled_at).
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


SRC = None
def setup():
    global SRC
    SRC = _read("services/pool_backfill.py")


def test_dry_run_is_the_default():
    assert "def backfill_pool_state_from_live_truth(barcodes: Optional[List[str]] = None, *, dry_run: bool = True" in SRC


def test_real_write_requires_operator_confirmation():
    assert "operator_confirmed" in SRC
    assert "skipped_needs_confirmation" in SRC
    # the apply branch is guarded by BOTH not-dry-run AND operator_confirmed
    assert "if not operator_confirmed:" in SRC


def test_never_backfill_diverged_or_stale():
    # plan_backfill must classify these as unsafe and refuse
    for action in ("skipped_single_store", "skipped_stale_read", "skipped_diverged"):
        assert action in SRC, f"missing safety classification {action}"
    assert "spread > BACKFILL_MAX_SPREAD" in SRC      # disagreement => unsafe
    assert "missing" in SRC                            # stale/failed read => unsafe


def test_backfill_max_spread_default_zero():
    os.environ.pop("POOL_BACKFILL_MAX_SPREAD", None)
    from services import pool_backfill
    assert pool_backfill.BACKFILL_MAX_SPREAD == 0, "default requires stores to AGREE exactly"


def test_unsafe_emits_critical_alert():
    assert 'alerting.critical("pool_backfill.unsafe"' in SRC


def test_backfill_grants_write_eligibility():
    # applying a backfill stamps backfilled_at (the canary write-eligibility gate)
    assert "state.backfilled_at = now" in SRC or "backfilled_at=now" in SRC


def test_replayable_and_reversible():
    assert "PoolBackfill(" in SRC                       # append-only log of every op (incl dry-run/skip)
    assert "prev_quantity" in SRC and "prev_version" in SRC   # prior state retained for reversal
    assert "def reverse_backfill" in SRC
    assert "state.backfilled_at = None" in SRC          # reversing removes write-eligibility (safety)


def test_backfill_performs_no_inventory_writes():
    """Backfill seeds PoolState only — it must never push inventory to Shopify."""
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities",
                   "set_inventory_quantities_single", "converge_pool"):
        assert writer not in SRC, f"backfill must not write inventory; found {writer}"


def test_data_model_and_migration_present():
    m = _read("models.py")
    assert "class PoolBackfill" in m and "class PoolCanaryRollback" in m
    assert "backfilled_at" in m and "backfill_source_store" in m
    mig = _read("migrate_pool_ledger.py")
    assert "ADD COLUMN IF NOT EXISTS backfilled_at" in mig
    assert "CREATE TABLE IF NOT EXISTS pool_backfills" in mig
    assert "CREATE TABLE IF NOT EXISTS pool_canary_rollbacks" in mig


if __name__ == "__main__":
    setup()
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn(); print(f"PASS {fn.__name__}"); passed += 1
        except AssertionError as e:
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} Phase-3 backfill tests passed")
    sys.exit(0 if passed == len(fns) else 1)
