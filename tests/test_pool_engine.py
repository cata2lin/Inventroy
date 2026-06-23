# tests/test_pool_engine.py
"""
STAGE 2 — mathematical convergence tests for the canonical pool engine's PURE core
(fold_observation + per-source ordering). No DB, no Shopify. Run:
    python tests/test_pool_engine.py

These prove the property delta-propagation lacked: under concurrent sales, duplicate webhooks,
out-of-order delivery and restocks, the canonical pool quantity Q converges to the correct physical
total, and never goes negative. A small deterministic SIMULATOR mirrors the engine's idempotency +
per-source ordering so we can assert the final Q for adversarial event orderings.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services.pool_engine import fold_observation, is_stale_for_source


# --- the pure fold ------------------------------------------------------------------------

def test_bootstrap():
    assert fold_observation(None, None, 100) == 100
    assert fold_observation(None, None, -5) == 0          # bootstrap is floored too

def test_single_sale():
    assert fold_observation(100, 100, 99) == 99           # source sold 1

def test_restock_large():
    assert fold_observation(10, 10, 6010) == 6010         # +6000 restock, no ceiling

def test_new_source_does_not_move_pool():
    # a replica joining with a stale local value must NOT change Q (it gets converged to Q instead)
    assert fold_observation(100, None, 40) == 100

def test_negative_is_clamped():
    assert fold_observation(1, 1, -2) == 0                # oversold observation cannot drive Q negative
    assert fold_observation(0, 3, -10) == 0

def test_per_source_staleness():
    assert is_stale_for_source(prev_source_timestamp=5, new_source_timestamp=4) is True   # redelivery
    assert is_stale_for_source(prev_source_timestamp=5, new_source_timestamp=6) is False  # newer
    assert is_stale_for_source(None, 6) is False                                          # first


# --- deterministic simulator (idempotency + per-source ordering, mirrors apply_event) ------

def simulate(events, floor=0):
    """events: list of dicts {store, observed, ts, wid}. Processes in DELIVERY order with
    webhook_id idempotency and per-source monotonic ordering; folds into Q. Returns final Q."""
    seen = set()
    q = None
    last_obs = {}   # store -> last observed
    last_ts = {}    # store -> last source_timestamp
    for e in events:
        wid = e.get("wid")
        if wid is not None and wid in seen:
            continue            # duplicate / replay -> no-op (idempotent)
        if wid is not None:
            seen.add(wid)
        s = e["store"]
        if is_stale_for_source(last_ts.get(s), e["ts"]):
            continue            # out-of-order redelivery of the SAME source -> dropped
        q = fold_observation(q, last_obs.get(s), e["observed"], floor=floor)
        last_obs[s] = e["observed"]
        last_ts[s] = e["ts"]
    return q


def test_concurrent_sales_two_stores_no_lost_sale():
    # pool starts 100 (A,B seeded). A sells 1, B sells 1 concurrently -> pool MUST be 98.
    ev = [
        {"store": "A", "observed": 100, "ts": 1, "wid": "a0"},
        {"store": "B", "observed": 100, "ts": 1, "wid": "b0"},
        {"store": "A", "observed": 99,  "ts": 2, "wid": "a1"},   # A sells 1
        {"store": "B", "observed": 99,  "ts": 2, "wid": "b1"},   # B sells 1 (concurrent)
    ]
    assert simulate(ev) == 98, "both concurrent sales must reduce the pool (delta-sync lost one)"

def test_duplicate_webhook_is_idempotent():
    ev = [
        {"store": "A", "observed": 100, "ts": 1, "wid": "a0"},
        {"store": "A", "observed": 99,  "ts": 2, "wid": "a1"},   # sale
        {"store": "A", "observed": 99,  "ts": 2, "wid": "a1"},   # REDELIVERY (same wid) -> ignored
    ]
    assert simulate(ev) == 99, "a redelivered webhook must not double-decrement"

def test_out_of_order_same_source_dropped():
    ev = [
        {"store": "A", "observed": 100, "ts": 1, "wid": "a0"},
        {"store": "A", "observed": 98,  "ts": 3, "wid": "a2"},   # newer truth: 98
        {"store": "A", "observed": 99,  "ts": 2, "wid": "a1"},   # OLDER, arrives late -> dropped
    ]
    assert simulate(ev) == 98, "a stale redelivery from the same source must not overwrite newer truth"

def test_concurrent_restock_and_sale():
    # A restocks +50 while B sells 1, from pool 100 -> 149.
    ev = [
        {"store": "A", "observed": 100, "ts": 1, "wid": "a0"},
        {"store": "B", "observed": 100, "ts": 1, "wid": "b0"},
        {"store": "A", "observed": 150, "ts": 2, "wid": "a1"},   # +50 restock on A
        {"store": "B", "observed": 99,  "ts": 2, "wid": "b1"},   # -1 sale on B
    ]
    assert simulate(ev) == 149

def test_adversarial_interleaving_converges():
    # mixed: dup, out-of-order, concurrent, restock; final must equal the conservation sum.
    ev = [
        {"store": "A", "observed": 300, "ts": 1, "wid": "a0"},   # bootstrap 300
        {"store": "B", "observed": 300, "ts": 1, "wid": "b0"},   # B joins, Q stays 300
        {"store": "A", "observed": 299, "ts": 2, "wid": "a1"},   # -1  -> 299
        {"store": "B", "observed": 299, "ts": 2, "wid": "b1"},   # -1  -> 298 (concurrent)
        {"store": "A", "observed": 299, "ts": 2, "wid": "a1"},   # dup -> ignore
        {"store": "B", "observed": 305, "ts": 4, "wid": "b3"},   # +6  -> 304
        {"store": "B", "observed": 299, "ts": 3, "wid": "b2"},   # stale (ts3<4) -> drop
    ]
    # conservation: 300 -1(A) -1(B) +6(B) = 304
    assert simulate(ev) == 304

def test_no_permanent_offset_a_la_300_40():
    # the field bug: A=300, B=40 stuck 260 apart. With absolute convergence the pool has ONE
    # quantity; both stores are SET to it. There is no per-store offset to preserve.
    ev = [
        {"store": "A", "observed": 300, "ts": 1, "wid": "a0"},   # Q=300
        {"store": "B", "observed": 40,  "ts": 1, "wid": "b0"},   # B joins late/stale -> Q stays 300
    ]
    # Q is 300; converge_pool would SET B to 300. No 260 offset can survive.
    assert simulate(ev) == 300


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
    print(f"\n{passed}/{len(fns)} pool-engine convergence tests passed")
    sys.exit(0 if passed == len(fns) else 1)
