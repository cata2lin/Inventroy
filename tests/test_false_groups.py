# tests/test_false_groups.py
"""
PER-LISTING POOL ENGINE (2026-07-10 policy) regression tests — hermetic. Run:
    python tests/test_false_groups.py

POLICY: the BARCODE is the intentional sync key. Every listing (variant) sharing a barcode pools its
stock — regardless of SKU, including SEVERAL listings within ONE store. SKU-based sync gating is
FORBIDDEN (an earlier quarantine experiment was reverted); the SKU classifier survives only as
report-only observability in pool_membership.

THE INCIDENT the engine change fixes: negru-4XL and negru-5XL share barcode 0692041036409 on 3 stores
(two listings per store). The fold baseline was keyed per (barcode, STORE), so the two listings'
observations interleaved into one stream: a 5XL listing reporting 2 folded against the 4XL listing's
202 as a phantom -200, clobbering a fresh +200 restock fleet-wide. And convergence only ever wrote ONE
"canonical" listing per store, so sibling listings sat stale forever (the divergence the operator saw).

THE FIX: each listing is its own replica — fold baselines keyed per VARIANT, and converge/simulate/
backfill/validate operate over ALL listings of the barcode.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services.pool_engine import fold_observation


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- the -200 incident replay: per-LISTING baselines fold correctly --------------------------------

def _observe(Q, last, listing, val):
    """Per-listing fold + converge: fold against THIS listing's own baseline, then convergence
    reseeds EVERY listing's baseline to the new Q (per-variant anchors)."""
    Q = fold_observation(Q, last.get(listing), val)
    for k in last:
        last[k] = Q
    last[listing] = Q
    return Q


def test_incident_replay_5xl_sale_folds_minus_1_not_minus_200():
    # 6 listings (4XL + 5XL on 3 stores), pool backfilled at 3, all baselines 3.
    listings = ["A-4XL", "B-4XL", "C-4XL", "A-5XL", "B-5XL", "C-5XL"]
    Q, last = 3, {l: 3 for l in listings}
    Q = _observe(Q, last, "B-4XL", 203)        # colleague +200 on one listing
    assert Q == 203                            # pool restocked
    Q = _observe(Q, last, "A-4XL", 202)        # real 4XL sale
    assert Q == 202
    # THE INCIDENT MOMENT: a 5XL sale on store A. Its own baseline is 202 (reseeded by convergence),
    # so selling one reports 201 -> folds as -1. Under the old store-keyed baseline the 5XL listing
    # (never written, still at its stale value) reported 2 -> folded as -200 and wiped the restock.
    Q = _observe(Q, last, "A-5XL", 201)
    assert Q == 201, "a 1-unit sale on the second listing must fold as -1, never -200"


def test_stale_second_listing_first_report_is_replica_join_not_wipe():
    # A listing with NO baseline yet (never observed, no anchor) reporting a stale value must NOT
    # move the pool (replica-join semantics) — convergence then pulls it to Q.
    Q, last = 202, {"A-4XL": 202}
    Q = fold_observation(Q, last.get("A-5XL"), 2)   # stale listing reports 2, no baseline -> no move
    assert Q == 202


# --- structural: the engine is per-listing ----------------------------------------------------------

def test_fold_baseline_keyed_per_variant():
    src = _read("services/pool_engine.py")
    sp = src[src.index("def _source_prev"):src.index("def apply_event")]
    assert "source_variant_id IS NOT DISTINCT FROM :v" in sp
    assert "source_store_id IS NOT DISTINCT FROM" not in sp     # store-keyed baseline is GONE
    ae = src[src.index("def apply_event"):src.index("def latest_source_observed")]
    assert "_source_prev(db, ev.barcode, ev.source_variant_id, ev.event_id)" in ae


def test_corroboration_baseline_per_variant():
    src = _read("services/pool_engine.py")
    fn = src[src.index("def latest_source_observed"):src.index("def _read_source_live")]
    assert "source_variant_id IS NOT DISTINCT FROM :v" in fn
    cj = src[src.index("def corroborate_up_jump"):src.index("def converge_pool")]
    assert "latest_source_observed(db, barcode, source_variant_id)" in cj


def test_converge_writes_all_listings_not_canonical_only():
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):src.index("def simulate_convergence")]
    assert "DISTINCT ON" not in conv, "converge must target EVERY listing, not one canonical per store"
    assert "false_group" not in conv                     # no SKU-based refuse
    sim = src[src.index("def simulate_convergence"):src.index("def shadow_observe")]
    assert "DISTINCT ON" not in sim


def test_backfill_covers_and_seeds_every_listing():
    src = _read("services/pool_backfill.py")
    assert "_group_rows" in src                          # plan reads ALL listings
    seed = src[src.index("backfill_baseline"):]
    assert '"v": cr["variant_id"]' in src                # per-VARIANT baseline seed (fold is variant-keyed)


def test_validation_and_livetruth_cover_every_listing():
    assert "_group_rows" in _read("services/pool_validation.py")
    lt = _read("services/live_truth.py")
    assert "def _group_rows" in lt
    assert "_group_rows(db, barcode)" in lt              # _check_pool uses it


# --- structural: NO SKU-based sync gating anywhere --------------------------------------------------

def test_no_sku_gating_in_sync_paths():
    iss = _read("services/inventory_sync_service.py")
    assert "false_group_quarantined" not in iss
    assert "auto_sync_refused_false_group" not in iss
    assert "is_false_barcode_group" not in iss
    pc = _read("services/pool_canary.py")
    assert "is_false_barcode_group" not in pc
    pe = _read("services/pool_engine.py")
    assert "is_false_barcode_group" not in pe
    ob = _read("services/pool_onboarding.py")
    assert "false_group_multi_sku" not in ob
    rc = _read("services/reconciliation_engine.py")
    assert "is_false_barcode_group" not in rc
    pv = _read("services/pool_validation.py")
    assert "is_false_barcode_group" not in pv
    lt = _read("services/live_truth.py")
    assert "is_false_barcode_group" not in lt


def test_multi_sku_classifier_is_report_only():
    # the classifier survives ONLY as observability in the membership sweep (INFO, deduped)
    pm = _read("services/pool_membership.py")
    assert "pool_membership_multi_sku_pool" in pm
    assert "backfilled_at = NULL" not in pm              # de-authorization is GONE
    win = pm[pm.index("pool_membership_multi_sku_pool") - 600:]
    assert 'severity="INFO"' in win[:1200]


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
    print(f"\n{passed}/{len(fns)} per-listing engine tests passed")
    sys.exit(0 if passed == len(fns) else 1)
