# tests/test_cascade_hardening.py
"""
Regression tests for the 2026-07-14 P0 cascade-kill fixes (the "app zeroes its own stores" loop):

  1. FLOOR-BREACH REJECTION — a negative delta larger than a target's stock (e.g. -804 on a store
     holding 415) is corrupt input: floor_breach_rejects says REJECT (quarantine), never "SET to 0".
  2. ECHO TTL — markers/intents now live 15 min; blanket value-independent suppression is confined
     to the first ECHO_VALUE_INDEPENDENT_SECONDS; an AGED marker suppresses only an exact value
     match (our late 0-echo) and never a genuinely different value.
  3. CATASTROPHIC-DROP VERIFICATION — should_verify_drop flags big drops for a live read;
     corroborate_big_drop folds live truth / fails CLOSED when live is unreadable.
  4. ENGINE FOLD — classify_fold rejects a deep-negative fold (the 2026-06-26 "992+(991-2049)=-66
     -> floored to 0" wipe) instead of silently clamping the pool to 0.
  5. FULL-SYNC — the UNIQUE(sku, store_id) constraint (927k dead letters) is gone from the model.

Run: python tests/test_cascade_hardening.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, UniqueConstraint
from sqlalchemy.orm import sessionmaker

import models
from services import sync_guards as g
from services import inventory_sync_service as iss
from services import pool_engine as pe


def _session():
    engine = create_engine("sqlite:///:memory:")
    models.WriteIntent.__table__.create(engine)
    return sessionmaker(bind=engine)()


_ID = [1000]


def _marker(db, store_id, item, barcode, expected_qty, sync_op="op-1", age_seconds=0):
    m = iss._create_echo_marker(db, barcode, store_id, item, expected_qty=expected_qty,
                                sync_op=sync_op, origin_store_id=99, origin_item_id=None)
    _ID[0] += 1
    m.id = _ID[0]
    db.commit()
    if age_seconds:
        m.created_at = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        db.commit()
    return m


# --- 1. floor-breach rejection --------------------------------------------------------------------

def test_incident_delta_rejects_never_floors():
    # The 2026-07-13 incident: delta -804 against a store holding 415 => breach 389 => REJECT.
    reject, breach = g.floor_breach_rejects(415, -804)
    assert reject is True and breach == 389
    reject, breach = g.floor_breach_rejects(496, -513)
    assert reject is True and breach == 17


def test_normal_negative_delta_passes():
    assert g.floor_breach_rejects(5, -3) == (False, 0)
    assert g.floor_breach_rejects(5, -5) == (False, 0)   # exactly to the floor is fine
    assert g.floor_breach_rejects(0, 4) == (False, 0)


def test_default_tolerance_zero_rejects_single_unit_breach():
    reject, breach = g.floor_breach_rejects(1, -2)
    assert reject is True and breach == 1, "default tolerance 0: every breach rejects"


def test_tolerance_absorbs_small_stockout_race():
    assert g.floor_breach_rejects(1, -2, tolerance=1) == (False, 1)
    assert g.floor_breach_rejects(1, -3, tolerance=1) == (True, 2)


def test_unknown_current_never_rejects():
    assert g.floor_breach_rejects(None, -804) == (False, 0), \
        "no baseline => cannot project => relative adjust path, not rejection"


# --- 2. echo TTLs + two-tier suppression ------------------------------------------------------------

def test_ttls_extended_and_hard_window_short():
    assert iss.INTENT_TTL_SECONDS >= 600, "write intents must outlive slow Shopify webhook delivery"
    assert g.ECHO_TTL_SECONDS >= 600, "echo markers must outlive slow Shopify webhook delivery"
    assert g.ECHO_VALUE_INDEPENDENT_SECONDS <= 60, \
        "blanket value-independent suppression must stay a short window"
    from services import stock_reconciliation
    assert stock_reconciliation.INTENT_TTL_SECONDS >= 600


def test_fresh_marker_suppresses_value_independent():
    db = _session()
    _marker(db, store_id=9, item=1, barcode="BC", expected_qty=100)
    res = iss._find_self_echo(db, 9, 1, observed=73, barcode="BC")
    assert res is not None and res[1] is None, \
        "inside the hard window the marker suppresses regardless of value (cascade defence)"


def test_aged_marker_suppresses_only_exact_value_match():
    # Our floored write of 0 echoes back 2 minutes later (past the 45s hard window):
    db = _session()
    _marker(db, store_id=9, item=1, barcode="BC", expected_qty=0, age_seconds=120)
    res = iss._find_self_echo(db, 9, 1, observed=0, barcode="BC")
    assert res is not None, "late echo of our own 0-write MUST still be recognised (the incident loop)"


def test_aged_marker_lets_real_change_through_and_survives():
    db = _session()
    m = _marker(db, store_id=9, item=1, barcode="BC", expected_qty=100, age_seconds=120)
    res = iss._find_self_echo(db, 9, 1, observed=97, barcode="BC")
    assert res is None, "aged marker + different value = REAL change; must not be suppressed"
    still_there = db.query(models.WriteIntent).filter(models.WriteIntent.id == m.id).first()
    assert still_there is not None, "marker must survive to catch the actual echo later"
    # ... and the actual echo arriving afterwards is still caught:
    res2 = iss._find_self_echo(db, 9, 1, observed=100, barcode="BC")
    assert res2 is not None


def test_unknown_expected_marker_gets_short_ttl():
    # A marker whose written value is UNKNOWN stores a 0 sentinel; if it lived the full 15-min TTL
    # its aged tier would falsely suppress a genuine drop-to-zero. It must expire with the
    # value-independent window instead.
    db = _session()
    m = iss._create_echo_marker(db, "BC", 9, 1, expected_qty=None,
                                sync_op="op-u", origin_store_id=99, origin_item_id=None)
    _ID[0] += 1
    m.id = _ID[0]
    db.commit()
    life = (m.expires_at.replace(tzinfo=timezone.utc) if m.expires_at.tzinfo is None else m.expires_at) \
        - datetime.now(timezone.utc)
    assert life.total_seconds() <= g.ECHO_VALUE_INDEPENDENT_SECONDS + 5, \
        "unknown-value markers must not live into the aged exact-match tier"


def test_is_echo_match_caps_intent_lifetime():
    # BUG-34 keeps the intent alive for duplicate deliveries — but after the echo has arrived it
    # must not survive the full 15 min, or a genuine A->B->A revert would be swallowed.
    db = _session()
    intent = models.WriteIntent(barcode="BC", target_store_id=9, inventory_item_id=None,
                                quantity=10, barcode_version=0,
                                expires_at=datetime.now(timezone.utc) + timedelta(seconds=900))
    _ID[0] += 1
    intent.id = _ID[0]
    db.add(intent)
    db.commit()
    assert iss._is_echo(db, 9, "BC", 10) is True
    exp = intent.expires_at
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    remaining = (exp - datetime.now(timezone.utc)).total_seconds()
    assert remaining <= iss.DUPLICATE_TTL_SECONDS + 5, \
        f"matched intent must shrink to the duplicate window, still alive {remaining}s"
    # duplicates within the shrunk window are still suppressed
    assert iss._is_echo(db, 9, "BC", 10) is True


# --- 3. catastrophic-drop verification --------------------------------------------------------------

def test_should_verify_drop_thresholds():
    assert g.should_verify_drop(995, 0) is True
    assert g.should_verify_drop(50, 0) is True          # exactly at the default threshold (50)
    assert g.should_verify_drop(60, 20) is False        # 40-unit drop: normal sales territory
    assert g.should_verify_drop(None, 0) is False       # no baseline, nothing to compare
    assert g.should_verify_drop(10, 60) is False        # a rise is not a drop
    assert g.should_verify_drop(30, 10, threshold=20) is True


def test_corroborate_big_drop_matrix():
    orig_latest, orig_live = pe.latest_source_observed, pe._read_source_live
    kw = dict(barcode="B", source_store_id=1, source_variant_id=2, inventory_item_id=3)
    try:
        pe.latest_source_observed = lambda db, b, v: 995

        pe._read_source_live = lambda db, s, i: 995     # live REFUTES the drop to 0
        use, info = pe.corroborate_big_drop(None, observed=0, **kw)
        assert use == 995 and info is not None, "phantom drop must fold live truth instead"

        pe._read_source_live = lambda db, s, i: 0       # live CONFIRMS the drop
        use, info = pe.corroborate_big_drop(None, observed=0, **kw)
        assert use == 0 and info is None, "a genuine drop folds unchanged"

        pe._read_source_live = lambda db, s, i: None    # live UNREADABLE
        use, info = pe.corroborate_big_drop(None, observed=0, **kw)
        assert use is None and info is not None, "unverifiable big drop must FAIL CLOSED (skip)"

        calls = []
        pe.latest_source_observed = lambda db, b, v: 30
        pe._read_source_live = lambda db, s, i: calls.append(1) or 30
        use, info = pe.corroborate_big_drop(None, observed=10, **kw)
        assert use == 10 and info is None and not calls, \
            "a small drop folds unchecked, without any live read"
    finally:
        pe.latest_source_observed, pe._read_source_live = orig_latest, orig_live


# --- 4. engine fold classification -------------------------------------------------------------------

def test_classify_fold_bootstrap_and_replica_join():
    assert g.classify_fold(None, None, 7) == ("apply", 7, 0)
    assert g.classify_fold(None, None, -3) == ("apply", 0, 0)   # bootstrap floored
    assert g.classify_fold(40, None, 12) == ("apply", 40, 0)    # replica joining: pool unmoved


def test_classify_fold_normal_delta():
    assert g.classify_fold(10, 8, 6) == ("apply", 8, 0)         # own -2 applied
    assert g.classify_fold(10, 8, 20) == ("apply", 22, 0)       # restock applied


def test_classify_fold_small_deficit_clamps_visibly():
    verdict, q, deficit = g.classify_fold(1, 5, 3)              # raw = 1 + (3-5) = -1
    assert verdict == "clamp" and q == 0 and deficit == 1


def test_classify_fold_deep_deficit_rejects_pool_unmoved():
    # The 2026-06-26 Esteban wipe: Q=992, poisoned baseline 2049, real observation 991
    # => raw = 992 + (991 - 2049) = -66. The old fold silently floored this to 0.
    verdict, q, deficit = g.classify_fold(992, 2049, 991)
    assert verdict == "reject", "deep-negative fold is poisoned-baseline evidence, not sold stock"
    assert q == 992, "the pool must NOT move on a rejected fold"
    assert deficit == 66


# --- 6. negative-stock test stores (CONTINUE policy, sales tracked below zero) ----------------------

def test_effective_delta_restock_after_tracked_oversell():
    # THE pool-poisoning mechanism: a test store tracks sales to -300, then the operator sets the
    # real stock (500). Raw math propagated 500-(-300) = +800 — the pool (floored at 0 the whole
    # time) inflated by the oversold backlog on every cycle. Floored endpoints propagate exactly 500.
    assert g.effective_delta(-300, 500) == 500
    assert g.effective_delta(-300, -301) == 0      # selling below zero is pool-irrelevant
    assert g.effective_delta(-300, -100) == 0      # backlog recovery below zero too
    assert g.effective_delta(500, -300) == -500    # pool loses only the 500 it actually held
    assert g.effective_delta(3, 5) == 2            # normal movement unchanged
    assert g.effective_delta(None, 5) is None      # no baseline -> absolute fallback


def test_classify_fold_restock_after_tracked_oversell_not_inflated():
    verdict, q, deficit = g.classify_fold(0, -300, 500)
    assert (verdict, q, deficit) == ("apply", 500, 0), \
        "pool must become the SET value, never set + oversold backlog"


def test_fold_observation_floored_endpoints():
    from services.pool_engine import fold_observation
    assert fold_observation(0, -300, 500) == 500   # was 800 pre-fix: the 21497/38095-class inflation
    assert fold_observation(100, 50, -20) == 50    # store shipped its 50; backlog is not pool stock
    assert fold_observation(100, 100, 99) == 99    # normal path unchanged


# --- 5. full-sync constraint removed -----------------------------------------------------------------

def test_sku_store_unique_constraint_removed():
    for c in models.ProductVariant.__table__.constraints:
        if isinstance(c, UniqueConstraint):
            cols = sorted(col.name for col in c.columns)
            assert cols != ["sku", "store_id"], \
                "UNIQUE(sku, store_id) must stay dead — it killed full sync (927k dead letters)"


def test_rejected_fold_events_excluded_from_baseline_queries():
    import inspect
    assert "rejected_negative_fold" in inspect.getsource(pe._source_prev), \
        "_source_prev must skip rejected events or the phantom becomes the next baseline"
    assert "rejected_negative_fold" in inspect.getsource(pe.latest_source_observed)


# --- runner ---------------------------------------------------------------------------------------

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
    print(f"\n{passed}/{len(fns)} cascade-hardening tests passed")
    sys.exit(0 if passed == len(fns) else 1)
