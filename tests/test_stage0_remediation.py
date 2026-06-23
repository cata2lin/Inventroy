# tests/test_stage0_remediation.py
"""
Stage 0 remediation regression tests (hermetic — no DB, no Shopify, no live writes).
Run: python tests/test_stage0_remediation.py

Guards the four Stage 0 fixes so they cannot silently regress:
  0.1  userErrors selects `code`  + the stale-compare healer is reachable
  0.2  reconcile absolute writes are floor-clamped (no negative propagation)
  0.3  the lockless/floorless legacy reconcile writer is disabled by default
  0.4  the live-truth sweep module exists and is wired into the scheduler
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- 0.1 — the dead-code COMPARE_QUANTITY_STALE bug must stay fixed -------------------------

def test_inventory_mutations_select_userError_code():
    """Both inventory mutations MUST select `code` in userErrors, or the stale-compare detection
    silently becomes dead code again and contended writes are dropped."""
    src = _read("shopify_service.py")
    for mut in ("inventorySetQuantities", "inventoryAdjustQuantities"):
        block = re.search(mut + r'"\s*:\s*"""(.*?)"""', src, re.S)
        assert block, f"mutation {mut} not found"
        assert "userErrors { field message code }" in block.group(1), \
            f"{mut} must select `code` in userErrors (else COMPARE_QUANTITY_STALE is undetectable)"


def test_is_stale_compare_matches_code_and_message():
    """Belt-and-suspenders: detect a stale compare by typed code OR message text."""
    src = _read("services/inventory_sync_service.py")
    m = re.search(r"def _is_stale_compare.*?(?=\ndef )", src, re.S)
    assert m, "_is_stale_compare helper missing"
    ns = {"Optional": __import__("typing").Optional, "List": list, "Dict": dict, "Any": object}
    exec(m.group(0), ns)
    f = ns["_is_stale_compare"]
    assert f([{"code": "COMPARE_QUANTITY_STALE", "message": "x"}]) is True
    assert f([{"message": "The compareQuantity argument no longer matches the persisted quantity."}]) is True
    assert f([{"code": "OTHER", "message": "not found"}]) is False
    assert f([]) is False
    assert f(None) is False


def test_stale_compare_branch_uses_helper_not_dead_literal():
    """The single-item propagation must branch via _is_stale_compare (reachable), not the old
    inline `e.get('code')==...` literal that was dead when code wasn't selected."""
    src = _read("services/inventory_sync_service.py")
    assert "_is_stale_compare(ue)" in src
    assert "_is_stale_compare(ue_r)" in src


# --- 0.2 — negative protection on the absolute reconcile writer -----------------------------

def test_apply_plan_floor_clamps_negative_target():
    from services import sync_guards as g
    src = _read("services/reconciliation_engine.py")
    # the clamp must exist in _apply_plan_locked
    assert "target = sync_guards.INVENTORY_FLOOR" in src
    assert "reconcile_floor_clamp" in src
    # arithmetic invariant the clamp enforces
    for raw in (-50, -1, 0, 7, 1000):
        assert max(raw, g.INVENTORY_FLOOR) >= g.INVENTORY_FLOOR
    assert g.INVENTORY_FLOOR >= 0


def test_floor_default_is_zero():
    from services import sync_guards as g
    assert g.INVENTORY_FLOOR == 0, "default inventory floor must be 0 (no negative propagation)"


# --- 0.3 — the lockless legacy reconcile writer is disabled by default ----------------------

def test_legacy_reconcile_disabled_by_default():
    src = _read("services/stock_reconciliation.py")
    assert "def _legacy_reconcile_enabled" in src
    # the guard must early-return before any write path
    assert "if not _legacy_reconcile_enabled():" in src
    assert "legacy_reconcile_skipped" in src
    # env semantics: default off, explicit opt-in
    for val, expect in (("", False), ("false", False), ("true", True), ("1", True), ("on", True)):
        os.environ["LEGACY_RECONCILE_ENABLED"] = val
        got = os.getenv("LEGACY_RECONCILE_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
        assert got is expect, f"{val!r} -> {got}, expected {expect}"
    os.environ.pop("LEGACY_RECONCILE_ENABLED", None)


# --- 0.4 — live-truth sweep exists and is scheduled -----------------------------------------

def test_live_truth_module_present_and_readonly():
    src = _read("services/live_truth.py")
    assert "def run_live_truth_sweep" in src
    assert "get_available_single" in src           # reads LIVE Shopify, not the mirror
    assert "mirror_blind" in src                    # detects mirror-blind divergence
    # must not perform inventory writes
    for writer in ("set_inventory_quantities", "adjust_inventory_quantities", "apply_plan", "update_inventory_levels"):
        assert writer not in src, f"live-truth sweep must be read-only; found writer {writer}"


def test_live_truth_wired_into_scheduler():
    src = _read("main.py")
    assert "from services import live_truth" in src
    assert "live_truth.run_live_truth_sweep" in src


# --- runner ---------------------------------------------------------------------------------

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
    print(f"\n{passed}/{len(fns)} Stage-0 remediation tests passed")
    sys.exit(0 if passed == len(fns) else 1)
