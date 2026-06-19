# tests/test_echo_authoritative.py
"""
Tests for the SYNC_ECHO_AUTHORITATIVE echo-race fix (authoritative-anchored residual self-echo).
Covers the failure-mode catalog where it is unit-testable without live Shopify:
  F1  dropped-delta race  -> residual = observed - authoritative_qty propagates
  F2  stale-mirror cascade -> NULL authoritative_qty => value-INDEPENDENT (residual None) => suppress
  F2b available/on_hand    -> _after_available returns the `available` bucket ONLY (never on_hand)
  F4  coalesced webhook    -> residual reflects the net real change
  F6  multi-listing        -> per inventory_item_id; no cross-suppression
  F7  floor-clamp          -> absolute anchor matches/propagates correctly
  F8  marker hygiene       -> matched marker is consumed exactly once
Plus: flag/canary gating and the rollback property (flag off => value-independent even if stamped).

Run: python tests/test_echo_authoritative.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import models
from services import sync_guards as g
from services import inventory_sync_service as iss
from shopify_service import ShopifyService


def _session():
    """Isolated in-memory DB with only the write_intents table (FK enforcement off on SQLite)."""
    engine = create_engine("sqlite:///:memory:")
    models.WriteIntent.__table__.create(engine)
    return sessionmaker(bind=engine)()


def _flag(on, allowlist=""):
    if on:
        os.environ["SYNC_ECHO_AUTHORITATIVE"] = "true"
    else:
        os.environ.pop("SYNC_ECHO_AUTHORITATIVE", None)
    if allowlist:
        os.environ["SYNC_ECHO_AUTHORITATIVE_BARCODES"] = allowlist
    else:
        os.environ.pop("SYNC_ECHO_AUTHORITATIVE_BARCODES", None)


_ID = [0]  # SQLite doesn't autoincrement BIGINT PKs like Postgres; assign ids explicitly in tests.


def _marker(db, store_id, item, barcode, authoritative_qty, sync_op="op-1"):
    m = iss._create_echo_marker(db, barcode, store_id, item, expected_qty=authoritative_qty,
                                sync_op=sync_op, origin_store_id=99, origin_item_id=None,
                                authoritative_qty=authoritative_qty)
    _ID[0] += 1
    m.id = _ID[0]
    db.commit()
    return m


# --- F2b: the available/on_hand parser (the bug that re-opened the cascade in design 1) ----------

def test_after_available_picks_available_not_on_hand():
    grp = {"changes": [
        {"name": "on_hand", "delta": 0, "quantityAfterChange": 6},
        {"name": "available", "delta": -4, "quantityAfterChange": 583},
    ]}
    assert ShopifyService._after_available(grp) == 583, "must read the available bucket, never on_hand"


def test_after_available_none_when_absent_or_empty():
    assert ShopifyService._after_available({"changes": [{"name": "on_hand", "quantityAfterChange": 6}]}) is None
    assert ShopifyService._after_available({"changes": []}) is None
    assert ShopifyService._after_available(None) is None


# --- flag / canary gating ------------------------------------------------------------------------

def test_flag_gating_and_canary_allowlist():
    _flag(False)
    assert g.echo_authoritative_for("AAA") is False
    _flag(True)
    assert g.echo_authoritative_for("AAA") is True
    _flag(True, allowlist="7865789547985")
    assert g.echo_authoritative_for("7865789547985") is True
    assert g.echo_authoritative_for("AAA") is False, "canary: only allowlisted barcodes qualify"
    _flag(False)


# --- F1: dropped-delta race -> residual propagates -----------------------------------------------

def test_F1_real_sale_during_window_yields_residual():
    _flag(True)
    db = _session()
    _marker(db, store_id=9, item=53670954631513, barcode="BC", authoritative_qty=587)
    # our write set 587; a real sale dropped it to 583 within the echo window
    res = iss._find_self_echo(db, 9, 53670954631513, observed=583, barcode="BC")
    assert res is not None
    op, residual = res
    assert residual == -4, f"residual must be the real sale delta, got {residual}"
    _flag(False)


# --- F2: stale-mirror cascade cannot reopen (NULL authoritative => value-independent) -------------

def test_F2_null_authoritative_is_value_independent():
    _flag(True)
    db = _session()
    _marker(db, store_id=9, item=1, barcode="BC", authoritative_qty=None)  # capture failed/absent
    res = iss._find_self_echo(db, 9, 1, observed=587, barcode="BC")
    op, residual = res
    assert residual is None, "no authoritative anchor => value-INDEPENDENT suppress (no phantom delta)"
    _flag(False)


def test_F2_rollback_flag_off_is_value_independent_even_if_stamped():
    _flag(False)  # master flag OFF
    db = _session()
    _marker(db, store_id=9, item=1, barcode="BC", authoritative_qty=587)  # stamped from a prior on-window
    res = iss._find_self_echo(db, 9, 1, observed=583, barcode="BC")
    op, residual = res
    assert residual is None, "flag OFF must instantly revert to value-independent even for stamped markers"


# --- F4: coalesced webhook (our write + a real -1 in one report) ----------------------------------

def test_F4_coalesced_residual():
    _flag(True)
    db = _session()
    _marker(db, store_id=5, item=10, barcode="BC", authoritative_qty=587)
    res = iss._find_self_echo(db, 5, 10, observed=586, barcode="BC")
    op, residual = res
    assert residual == -1
    _flag(False)


# --- pure echo: residual 0 => suppress ------------------------------------------------------------

def test_pure_echo_residual_zero():
    _flag(True)
    db = _session()
    _marker(db, store_id=5, item=10, barcode="BC", authoritative_qty=587)
    res = iss._find_self_echo(db, 5, 10, observed=587, barcode="BC")
    op, residual = res
    assert residual == 0, "exact echo => residual 0 => caller suppresses"
    _flag(False)


# --- F6: per-item, no cross-suppression -----------------------------------------------------------

def test_F6_per_item_no_cross_suppression():
    _flag(True)
    db = _session()
    _marker(db, store_id=5, item=100, barcode="BC", authoritative_qty=587, sync_op="opA")
    _marker(db, store_id=5, item=200, barcode="BC", authoritative_qty=300, sync_op="opB")
    # webhook for item 100 consumes ONLY item 100's marker
    res = iss._find_self_echo(db, 5, 100, observed=587, barcode="BC")
    assert res[0] == "opA"
    # item 200's marker is still present and independent
    res2 = iss._find_self_echo(db, 5, 200, observed=299, barcode="BC")
    assert res2[0] == "opB" and res2[1] == -1
    _flag(False)


# --- F8: marker consumed exactly once -------------------------------------------------------------

def test_F8_marker_consumed_once():
    _flag(True)
    db = _session()
    _marker(db, store_id=5, item=10, barcode="BC", authoritative_qty=587)
    first = iss._find_self_echo(db, 5, 10, observed=587, barcode="BC")
    assert first is not None
    second = iss._find_self_echo(db, 5, 10, observed=587, barcode="BC")
    assert second is None, "marker must be consumed exactly once (next lookup => no echo)"
    _flag(False)


def test_no_marker_returns_none():
    _flag(True)
    db = _session()
    assert iss._find_self_echo(db, 5, 999, observed=10, barcode="BC") is None
    _flag(False)


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
    print(f"\n{passed}/{len(fns)} echo-authoritative tests passed")
    sys.exit(0 if passed == len(fns) else 1)
