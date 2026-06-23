# tests/test_phase1_shadow.py
"""
PHASE 1 (shadow mode) safety regression tests — hermetic (no DB / no Shopify). Run:
    python tests/test_phase1_shadow.py

The whole point of shadow mode is that it runs the new engine in parallel WITHOUT touching
production inventory. These tests guard that contract so it cannot silently regress:
  • shadow is flag-gated by SYNC_POOL_SHADOW (default OFF) — independent of SYNC_POOL_ENGINE
  • shadow_observe / simulate_convergence perform NO Shopify inventory writes
  • shadow is wired into handle_webhook BEFORE the legacy version gate, best-effort, isolated session
  • apply_event supports skip_lock (runs inside the caller's advisory lock)
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_shadow_flag_default_off_and_independent():
    from services import pool_engine
    os.environ.pop("SYNC_POOL_SHADOW", None)
    assert pool_engine.pool_shadow_enabled() is False, "shadow must default OFF"
    os.environ["SYNC_POOL_SHADOW"] = "true"
    assert pool_engine.pool_shadow_enabled() is True
    os.environ["SYNC_POOL_SHADOW"] = "false"
    assert pool_engine.pool_shadow_enabled() is False
    os.environ.pop("SYNC_POOL_SHADOW", None)
    # independent of the real-write master switch
    assert "SYNC_POOL_SHADOW" in _read("services/pool_engine.py")
    assert "SYNC_POOL_ENGINE" in _read("services/pool_engine.py")


def test_shadow_observe_noop_when_disabled():
    from services import pool_engine
    os.environ.pop("SYNC_POOL_SHADOW", None)
    # disabled => returns None immediately, opens no session, does nothing
    out = pool_engine.shadow_observe(
        barcode="x", source_store_id=1, source_variant_id=1, inventory_item_id=1,
        observed_quantity=5, source_timestamp=None, webhook_id="w1", legacy_quantity=5)
    assert out is None


def test_shadow_functions_perform_no_shopify_writes():
    """Phase 1 MUST NOT mutate live inventory. The shadow path may only READ (get_available_single);
    it must never call a SET/adjust inventory mutation."""
    src = _read("services/pool_engine.py")
    # isolate the shadow + simulate functions (everything from simulate_convergence onward)
    shadow_src = src[src.index("def simulate_convergence"):]
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities",
                   "set_inventory_quantities_single", "adjust_inventory_quantities_single",
                   "update_inventory_levels", "adjust_inventory_levels"):
        assert writer not in shadow_src, f"shadow path must not write inventory; found {writer}"


def test_converge_pool_is_the_only_writer_and_is_not_in_shadow_path():
    """converge_pool (the real writer) exists, but shadow_observe must call simulate_convergence,
    NOT converge_pool."""
    src = _read("services/pool_engine.py")
    assert "def converge_pool" in src
    shadow_fn = src[src.index("def shadow_observe"):]
    assert "converge_pool(" not in shadow_fn, "shadow_observe must simulate, never converge (write)"
    assert "simulate_convergence(" in shadow_fn


def test_apply_event_supports_skip_lock():
    src = _read("services/pool_engine.py")
    assert "def apply_event(db: Session, event_id: int, skip_lock: bool = False)" in src
    # when skip_lock, it must NOT acquire the dist lock
    body = src[src.index("def apply_event"):src.index("def converge_pool")]
    assert "if not skip_lock:" in body
    assert "if handle is not None:" in body  # release only if we actually acquired


def test_shadow_wired_before_legacy_version_gate_and_best_effort():
    src = _read("services/inventory_sync_service.py")
    assert "from services import pool_engine" in src
    assert "pool_engine.pool_shadow_enabled()" in src
    assert "pool_engine.shadow_observe(" in src
    # ordering: the shadow call must appear BEFORE the legacy version gate so it sees events the
    # cross-store timestamp gate would drop.
    shadow_pos = src.index("pool_engine.shadow_observe(")
    gate_pos = src.index("is_authoritative = _is_new_authoritative_version(")
    assert shadow_pos < gate_pos, "shadow must run before the legacy version gate"
    # best-effort: the call is wrapped in try/except
    window = src[src.index("PHASE 1 SHADOW MODE"):gate_pos]
    assert "try:" in window and "except Exception" in window
    # caller holds the lock -> shadow must skip re-acquire
    assert "caller_holds_lock=True" in window


def test_structured_compare_log_has_required_fields():
    src = _read("services/pool_engine.py")
    for field in ("legacy_quantity", "poolengine_quantity", "delta_difference",
                  "source_store", "webhook_id", "pool_version", "intended_writes"):
        assert f'"{field}"' in src, f"shadow compare log must include {field}"
    for action in ("pool_shadow_compare", "pool_shadow_dup_suppressed", "pool_shadow_stale_reject"):
        assert action in src, f"shadow must emit metric action {action}"


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
    print(f"\n{passed}/{len(fns)} Phase-1 shadow tests passed")
    sys.exit(0 if passed == len(fns) else 1)
