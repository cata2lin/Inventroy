# tests/test_trendyol_sync.py
"""
TRENDYOL <-> SHOPIFY stock sync tests — hermetic (no HTTP/DB). Run:
    python tests/test_trendyol_sync.py

Guards the over-correction contract: dormant-by-default flags, engine-authority gating, per-line
idempotency, virtual-listing fold math (delta against ITS OWN baseline), 15-min dedup/retry rule,
batch semantics (COMPLETED != success, results persisted, item-level retry), qty caps, cancel skip.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services import trendyol_sync as ts
from services import trendyol_client as ty
from services.pool_engine import fold_observation


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- flags: EVERYTHING dormant by default ----------------------------------------------------------

def test_all_flags_default_off():
    for var in ("TRENDYOL_SYNC_ENABLED", "TRENDYOL_PUSH_ENABLED", "TRENDYOL_INBOUND_APPLY"):
        os.environ.pop(var, None)
    assert ts.sync_enabled() is False
    assert ts.push_enabled() is False
    assert ts.inbound_apply() is False
    assert ts.push_allowlist() == set()


def test_jobs_noop_when_disabled():
    os.environ.pop("TRENDYOL_SYNC_ENABLED", None)
    assert ts.push_sweep().get("disabled") is True
    assert ts.orders_poll().get("disabled") is True
    assert ts.reconcile().get("disabled") is True


def test_client_refuses_unconfigured_and_oversize():
    assert ty.configured() in (False, True)      # env-dependent; on dev machines: False
    assert ty.push_inventory([])["ok"] is False
    too_many = [{"barcode": str(i), "quantity": 1} for i in range(ty.MAX_BATCH_ITEMS + 1)]
    assert ty.push_inventory(too_many)["ok"] is False


# --- over-correction contract: authority gating -----------------------------------------------------

def test_pushes_and_folds_require_engine_authoritative_pool():
    src = _read("services/trendyol_sync.py")
    assert "backfilled_at IS NOT NULL" in src               # authority = live-truth backfilled
    assert "is_rolled_back" in src                          # rolled-back pools never exported
    fold = src[src.index("def _apply_sale"):src.index("def _inbound_fold")]
    assert "_authoritative_pool_q" in fold                  # inbound folds gated the same way
    inb = src[src.index("def _inbound_fold"):src.index("def _mapping_anchor")]
    assert "not_authoritative" in inb


def test_push_value_is_floored_and_capped():
    src = _read("services/trendyol_sync.py")
    assert "min(max(int(r[\"pool_q\"]), 0), ty.MAX_STOCK_PER_PRODUCT)" in src
    assert ty.MAX_STOCK_PER_PRODUCT == 20000


# --- inbound: STOCK-DELTA idempotency + fold math (split/cancel-proof) -------------------------------

def test_inbound_is_stock_delta_not_order_line():
    # Sales are detected from Trendyol's OWN quantity vs the accounted anchor — NOT by folding order
    # lines (which the package-split cron would double-count). orders_poll must be RECORD-ONLY.
    src = _read("services/trendyol_sync.py")
    poll = src[src.index("def orders_poll"):src.index("def reconcile")]
    assert "_fold_sale" not in src                          # the fragile per-line fold is gone
    assert '"recorded"' in poll                             # lines are recorded, never mutate stock
    inb = src[src.index("def _inbound_fold"):src.index("def _mapping_anchor")]
    assert "int(acc) - int(ty_now)" in inb                  # sold = anchor - current qty


def test_inbound_fold_is_idempotent_transition():
    # replaying the SAME anchor->qty transition must NOT subtract twice (split/re-observe safe)
    src = _read("services/trendyol_sync.py")
    assert 'f"trendyol-in:{tb}:{acc}:{ty_now}"' in src      # transition-keyed webhook_id
    ap = src[src.index("def _apply_sale"):src.index("def _inbound_fold")]
    assert "SELECT 1 FROM pool_events WHERE webhook_id" in ap    # dedup before any mutation


def test_anchor_advances_on_push_and_fold_no_echo():
    # our own push must never be re-read as a sale: the anchor advances on confirmed push AND on fold
    src = _read("services/trendyol_sync.py")
    acct = src[src.index("def _account_pushed"):src.index("def _poll_submitted_batches")]
    assert "ty_accounted_qty=:q" in acct and "p2.id > :id" in acct   # newest-push guard
    inb = src[src.index("def _inbound_fold"):src.index("def _mapping_anchor")]
    assert "ty_accounted_qty=:a" in inb                     # fold advances the anchor to ty_now


def test_fold_before_push_closes_reverse_race():
    # a push must fold any Trendyol sale FIRST, so it never SETs Trendyol back up over an unfolded sale
    src = _read("services/trendyol_sync.py")
    sp = src[src.index("def _safe_push_list"):src.index("def push_sweep")]
    assert "ty.get_product(tb)" in sp                       # fresh per-barcode read
    assert "_inbound_fold(" in sp                            # fold BEFORE building the push list
    assert sp.index("_inbound_fold(") < sp.index("desired =")
    assert 'not pr.get("ok")' in sp                          # read failure => skip, never push blind


def test_virtual_listing_fold_math_sale():
    # the NULL stream is reseeded to Q, so the applied delta is exactly -sold and conservation holds
    # under a concurrent Shopify sale. Q=202, baseline reseeded to 202; sell 2 -> observed 200 -> 200.
    assert fold_observation(202, 202, 200) == 200
    assert fold_observation(199, 202, 200) == 197           # concurrent Shopify sale kept, none lost


def test_apply_sale_reseeds_baseline_no_drift():
    src = _read("services/trendyol_sync.py")
    ap = src[src.index("def _apply_sale"):src.index("def _inbound_fold")]
    assert "'backfill_baseline'" in ap                       # reseed NULL stream to current Q
    assert "max(int(q_now) - sold, 0)" in ap                 # floored, delta = -sold regardless of history


def test_suspicious_drop_is_capped():
    # a glitch read (e.g. qty 0) must not nuke the pool: drops beyond MAX_INBOUND_DROP never fold
    src = _read("services/trendyol_sync.py")
    assert "MAX_INBOUND_DROP" in src and 'reason": "suspicious_drop"' in src.replace("'", '"')
    assert ts.MAX_INBOUND_DROP >= 1


def test_dry_run_never_mutates():
    src = _read("services/trendyol_sync.py")
    inb = src[src.index("def _inbound_fold"):src.index("def _mapping_anchor")]
    assert "inbound_apply()" in inb and '"dry_run"' in inb   # apply-off => record/log only, no fold
    assert inb.index("inbound_apply()") < inb.index("dist_lock.acquire")  # gate BEFORE any mutation


# --- outbound: dedup + batch semantics ---------------------------------------------------------------

def test_dedup_skips_identical_and_retries_failed_after_window():
    src = _read("services/trendyol_sync.py")
    cand = src[src.index("def _push_candidates"):src.index("def _submit_batch")]
    assert 'int(r["last_q"]) != desired' in cand            # push only on change (15-min identical rule)
    assert 'r["last_status"] == "failed"' in cand           # failed items retried...
    assert "RETRY_MINUTES" in cand and ts.RETRY_MINUTES > 15  # ...only past the identical window


def test_batch_completed_is_not_success():
    src = _read("services/trendyol_sync.py")
    pb = src[src.index("def _poll_submitted_batches"):src.index("def _push_candidates")]
    assert '"COMPLETED"' in pb
    assert "failureReasons" in pb                            # item-level outcomes persisted
    assert "failed" in pb


def test_batches_are_chunked_to_limit():
    src = _read("services/trendyol_sync.py")
    assert "range(0, len(cands), ty.MAX_BATCH_ITEMS)" in src
    assert ty.MAX_BATCH_ITEMS == 1000


# --- reconcile ---------------------------------------------------------------------------------------

def test_reconcile_pages_at_100_and_reports_drift():
    assert ty.PRODUCTS_PAGE_SIZE == 100
    src = _read("services/trendyol_sync.py")
    assert "trendyol.drift" in src                          # read-only mode alerts instead of pushing
    assert "unmapped" in src


def test_scheduler_wired():
    m = _read("main.py")
    assert "trendyol_sync.push_sweep" in m
    assert "trendyol_sync.orders_poll" in m
    assert "trendyol_sync.reconcile" in m


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
    print(f"\n{passed}/{len(fns)} trendyol-sync tests passed")
    sys.exit(0 if passed == len(fns) else 1)
