# tests/test_false_groups.py
"""
FALSE-GROUP quarantine (2026-07-10 pijama incident) regression tests — hermetic. Run:
    python tests/test_false_groups.py

Incident: negru-4XL and negru-5XL (two SIZES = two physical stocks) share barcode 0692041036409
across 3 Nocturna stores. The engine keys its per-source baseline by (barcode, store) — variant-blind —
so a 5XL sale (3->2) folded against the 4XL baseline (202) as delta -200 and clobbered a fresh +200
restock on every store ("Inventar" -200 in the Shopify adjustment history).

The fix: a suffix-equivalence SKU classifier decides whether a barcode's members are the SAME product
(identical SKUs, or store-prefixed like zn-127/127 — ~120 legit pools) or DIFFERENT products (a FALSE
group). False groups are quarantined from ALL sync (webhook gate, canary gate, converge refuse,
creation-align guard), de-authorized by the membership sweep, and excluded from onboarding — until the
barcodes are fixed, at which point the gates reopen automatically (classification-driven, no markers).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services.diagnostics import sku_equivalent, count_sku_classes


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- the PURE classifier ---------------------------------------------------------------------------

def test_identical_skus_are_same_product():
    assert sku_equivalent("HA-1193-1", "HA-1193-1")
    assert count_sku_classes(["HA-1193-1", "HA-1193-1"]) == 1


def test_store_prefixed_skus_are_same_product():
    # the fleet's dominant legit pattern: one store bare, the other store-prefixed
    assert sku_equivalent("zn-127", "127")
    assert sku_equivalent("127", "zn-127")
    assert sku_equivalent("gt-100", "100")
    assert count_sku_classes(["127", "zn-127"]) == 1
    assert count_sku_classes(["150", "zn-150"]) == 1          # placeholder barcode, same product


def test_prefix_equivalence_is_transitive_via_shared_member():
    # {'127','zn-127','gt-127'} — zn-127 and gt-127 unify through the bare '127'
    assert count_sku_classes(["127", "zn-127", "gt-127"]) == 1


def test_sizes_are_different_products_the_pijama_incident():
    # THE incident: 4XL and 5XL are different physical stocks
    assert not sku_equivalent("negru-4XL", "negru-5XL")
    assert count_sku_classes(["negru-4XL", "negru-5XL"]) == 2
    for color in ("roz", "rosu", "champagne", "gri", "albastru"):
        assert count_sku_classes([f"{color}-4XL", f"{color}-5XL"]) == 2


def test_known_false_groups_classified_correctly():
    assert count_sku_classes(["HA-0061-1", "HA-0061M", "HA-0062"]) == 3     # 3 different products
    assert count_sku_classes(["oglinda", "oglinda-acrilica"]) == 2          # NOT a prefix pair
    assert count_sku_classes(["112", "zn-112"]) == 1                        # legit (was a false positive)


def test_empty_skus_carry_no_evidence():
    assert count_sku_classes(["", None, "  ", "HA-1"]) == 1
    assert count_sku_classes(["", None]) == 0
    assert count_sku_classes([]) == 0
    assert count_sku_classes(None) == 0


def test_compound_sku_prefix_still_matches():
    # a store prefix on an already-hyphenated SKU: x-a-b endswith '-a-b'
    assert sku_equivalent("zn-HA-1193-1", "HA-1193-1")
    assert count_sku_classes(["zn-HA-1193-1", "HA-1193-1"]) == 1


def test_case_differences_never_quarantine():
    # 'ZN-127' vs 'zn-127' is a data-entry case difference, not a different product
    assert sku_equivalent("ZN-127", "zn-127")
    assert count_sku_classes(["ZN-127", "zn-127", "127"]) == 1
    assert count_sku_classes(["NEGRU-4XL", "negru-5XL"]) == 2   # sizes still differ, case aside


# --- the gates -------------------------------------------------------------------------------------

def test_webhook_gate_quarantines_false_groups_before_canary():
    src = _read("services/inventory_sync_service.py")
    gate = src.index("is_false_barcode_group(db, barcode)")
    canary = src.index("pool_canary.canary_active_for(db, barcode)")
    shadow = src.index("pool_engine.shadow_observe(")
    assert gate < canary < shadow, "false-group gate must run BEFORE canary and shadow"
    win = src[gate:gate + 900]
    assert "_resync_local_baseline" in win     # mirror stays exact while quarantined
    assert "return" in win                     # no fold / converge / propagation


def test_canary_gate_blocks_false_groups():
    src = _read("services/pool_canary.py")
    fn = src[src.index("def canary_active_for"):src.index("def trigger_rollback")]
    assert "is_false_barcode_group" in fn


def test_converge_refuses_false_groups():
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):src.index("def simulate_convergence")]
    assert "is_false_barcode_group" in conv
    assert "pool_converge_refused_false_group" in conv


def test_creation_align_guard():
    src = _read("services/inventory_sync_service.py")
    fn = src[src.index("def _sync_variant_to_barcode_group"):]
    assert "count_sku_classes" in fn
    assert "auto_sync_refused_false_group" in fn


def test_membership_sweep_deauthorizes_false_groups():
    src = _read("services/pool_membership.py")
    assert "_false_group_barcodes" in src
    assert "backfilled_at = NULL" in src       # de-authorize: stale/poisoned Q never converges again
    assert "pool_membership_false_group" in src


def test_membership_flip_logs_are_deduped():
    # the canonical-flip WARN fired every 30 minutes for 20h on one barcode — must be deduped
    src = _read("services/pool_membership.py")
    assert "_recently_logged" in src
    fl = src.index("pool_membership_canonical_flip")
    assert "_recently_logged" in src[fl - 400:fl]


def test_onboarding_uses_classifier_not_naive_sku_count():
    src = _read("services/pool_onboarding.py")
    assert "count_sku_classes" in src
    assert "count(DISTINCT btrim(pv.sku))" not in src   # the naive counter is gone


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
    print(f"\n{passed}/{len(fns)} false-group tests passed")
    sys.exit(0 if passed == len(fns) else 1)
