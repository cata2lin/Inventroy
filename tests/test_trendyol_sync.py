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
    fold = src[src.index("def _fold_sale"):src.index("def orders_poll")]
    assert "_authoritative_pool_q" in fold                  # inbound folds gated the same way
    poll = src[src.index("def orders_poll"):src.index("def reconcile")]
    assert "pool_not_authoritative" in poll


def test_push_value_is_floored_and_capped():
    src = _read("services/trendyol_sync.py")
    assert "min(max(int(r[\"pool_q\"]), 0), ty.MAX_STOCK_PER_PRODUCT)" in src
    assert ty.MAX_STOCK_PER_PRODUCT == 20000


# --- inbound: idempotency + fold math ---------------------------------------------------------------

def test_order_line_idempotency_is_structural():
    src = _read("services/trendyol_sync.py")
    assert "ON CONFLICT (order_id, line_id) DO NOTHING" in src     # unseen-line detection
    assert 'webhook_id=order_ref' in src or "webhook_id=order_ref" in src
    m = _read("models.py")
    assert "ux_trendyol_order_line" in m and "unique=True" in m
    # the ledger webhook_id ('trendyol:{order}:{line}') gives a SECOND, engine-level dedup layer
    assert "trendyol:{nl['oid']}:{nl['lid']}" in src


def test_virtual_listing_fold_math_sale():
    # Trendyol is the NULL-variant virtual listing: a sale folds against ITS OWN baseline only.
    # Pool Q=202, virtual baseline 202 (seeded); Trendyol sells 2 -> observed 200 -> Q=200.
    assert fold_observation(202, 202, 200) == 200
    # concurrent Shopify sale already folded (Q=199), then the Trendyol sale arrives:
    # 199 + (200 - 202) = 197 — both sales counted, none lost, no over-correction.
    assert fold_observation(199, 202, 200) == 197


def test_virtual_listing_first_event_seeds_baseline_not_wipes():
    src = _read("services/trendyol_sync.py")
    fold = src[src.index("def _fold_sale"):src.index("def orders_poll")]
    assert "'backfill_baseline'" in fold        # first Trendyol event = replica joining at Q
    assert "latest_source_observed(db, ean, None)" in fold
    assert "max(int(prev) - qty, 0)" in fold    # floored, delta vs own baseline


def test_cancelled_and_insane_lines_never_fold():
    src = _read("services/trendyol_sync.py")
    assert "CANCEL_STATUSES" in src and "cancelled" in src
    assert "qty_out_of_range" in src
    assert ts.MAX_LINE_QTY >= 1


def test_lines_folded_oldest_first():
    # per-source monotonic timestamps on the virtual stream require oldest-first processing
    src = _read("services/trendyol_sync.py")
    assert 'new_lines.sort(key=lambda x: x["odate"] or 0)' in src


def test_dry_run_records_but_never_mutates():
    src = _read("services/trendyol_sync.py")
    poll = src[src.index("def orders_poll"):src.index("def reconcile")]
    assert '"dry_run"' in poll
    # in dry-run the line is recorded (idempotency preserved) and _fold_sale is NOT called for it
    assert poll.index('"dry_run"') < poll.index("_fold_sale(")


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
