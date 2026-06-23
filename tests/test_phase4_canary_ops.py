# tests/test_phase4_canary_ops.py
"""
PHASE 4 (pre-canary hardening + canary ops) tests — hermetic. Run:
    python tests/test_phase4_canary_ops.py

Guards: write-rate circuit breaker, immutable golden capture, and that ALL canary-ops tooling is
read-only (selection/prepare/validate/replay/report never write inventory or enable anything).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- 4A.1 write-rate circuit breaker -------------------------------------------------------

def test_write_rate_breaker_thresholds_and_wiring():
    from services import pool_canary
    src = _read("services/pool_canary.py")
    assert hasattr(pool_canary, "pre_write_guard")
    g = src[src.index("def pre_write_guard"):src.index("def golden_capture") if "def golden_capture" in src[src.index("def pre_write_guard"):] else len(src)]
    # both rate classes
    assert "write_rate_exceeded" in src
    assert "cas_conflict_spike" in src
    # pre_write_guard trips a rollback (revert to legacy) and returns a reason so the caller does NOT write
    pg = src[src.index("def pre_write_guard"):]
    assert "trigger_rollback(db, barcode" in pg
    assert pool_canary.WRITE_RATE_PER_MIN > 0 and pool_canary.CAS_CONFLICT_SPIKE > 0


def test_canary_runs_pre_write_guard_before_converge():
    src = _read("services/pool_canary.py")
    inner = src[src.index("def _canary_handle_inner"):]
    gpos = inner.index("pre_write_guard(db, barcode)")
    cpos = inner.index("converge_pool(db, barcode)")
    assert gpos < cpos, "pre-write guard must run BEFORE the convergence write"
    # if the guard trips, we must NOT converge (return early)
    assert "result\": \"pre_write_blocked\"" in inner


# --- 4A.2 immutable golden capture ---------------------------------------------------------

def test_golden_event_model_and_migration():
    m = _read("models.py")
    assert "class PoolGoldenEvent" in m
    assert "pool_golden_events" in m
    mig = _read("migrate_pool_ledger.py")
    assert "CREATE TABLE IF NOT EXISTS pool_golden_events" in mig


def test_golden_capture_records_full_causal_set():
    src = _read("services/pool_canary.py")
    inner = src[src.index("def _canary_handle_inner"):]
    for kind in ('"webhook"', '"transition"', '"cas"', '"rollback"'):
        assert f'golden_capture(db, barcode, {kind}' in inner, f"missing golden capture of {kind}"
    # raw webhook payload is captured (forensic completeness)
    assert "raw_payload=payload" in _read("services/inventory_sync_service.py")


def test_golden_events_never_cleaned():
    # the immutable forensic log must not be touched by the periodic cleanup
    cleanup = _read("services/inventory_sync_service.py")
    cstart = cleanup.index("def cleanup_expired_records")
    cbody = cleanup[cstart:]
    assert "pool_golden_events" not in cbody and "PoolGoldenEvent" not in cbody, \
        "cleanup must never delete immutable golden events"


# --- 4B/C/D/E canary-ops are READ-ONLY -----------------------------------------------------

def test_canary_ops_are_read_only():
    src = _read("services/pool_canary_ops.py")
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities", "converge_pool(",
                   "apply_plan", "SYNC_POOL_ENGINE_WRITES = ", "os.environ["):
        assert writer not in src, f"canary ops must be read-only / non-enabling; found {writer}"
    # it must not IMPORT or CALL the backfill applier (referencing the name in a guidance string is ok)
    assert "import pool_backfill" not in src and "pool_backfill." not in src


def test_canary_ops_functions_present():
    from services import pool_canary_ops
    for fn in ("select_canary_candidates", "prepare_canary", "validate_canary",
               "forensic_replay", "canary_report"):
        assert hasattr(pool_canary_ops, fn), f"missing {fn}"


def test_validate_success_criteria():
    src = _read("services/pool_canary_ops.py")
    v = src[src.index("def validate_canary"):src.index("def _timeline")]
    for chk in ("all_stores_converged_live", "pool_matches_live", "no_negative",
                "zero_rollbacks", "latency_within_sla", "no_unresolved_divergence"):
        assert chk in v, f"validate_canary missing success check {chk}"
    assert "healthy = all(checks.values())" in v


def test_report_recommendation_requires_human():
    src = _read("services/pool_canary_ops.py")
    r = src[src.index("def canary_report"):]
    assert '"expand"' in r and '"hold"' in r and '"rollback"' in r
    assert "HUMAN APPROVAL REQUIRED" in r


def test_forensic_replay_read_only():
    src = _read("services/pool_canary_ops.py")
    fr = src[src.index("def forensic_replay"):src.index("def canary_report")]
    assert "pool_golden_events" in fr and "pool_events" in fr   # reconstruct from immutable sources
    for w in ("INSERT", "UPDATE", "DELETE", "set_inventory"):
        assert w not in fr, f"forensic replay must not mutate; found {w}"


def test_ops_endpoints_registered():
    src = _read("routes/diagnostics.py")
    for ep in ("/pool/candidates", "/pool/prepare", "/pool/validate", "/pool/replay", "/pool/report"):
        assert ep in src, f"missing endpoint {ep}"


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
    print(f"\n{passed}/{len(fns)} Phase-4 canary-ops tests passed")
    sys.exit(0 if passed == len(fns) else 1)
