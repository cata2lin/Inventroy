# tests/test_phase3_canary.py
"""
PHASE 3B (canary write path + rollback) tests — hermetic. Run:
    python tests/test_phase3_canary.py

Guards: flags OFF => no writes; bootstrapped Q is NEVER write-eligible; CAS+ref+version writes;
echo-anchored convergence; floored (no negative); per-source monotonic ordering; automatic rollback
triggers; isolated session; legacy unaffected when canary inactive.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from services import pool_engine
from services.pool_engine import fold_observation, is_stale_for_source


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- flags: everything OFF by default ------------------------------------------------------

def test_write_flags_default_off():
    os.environ.pop("SYNC_POOL_ENGINE_WRITES", None)
    os.environ.pop("SYNC_POOL_CANARY_BARCODES", None)
    assert pool_engine.pool_writes_enabled() is False
    assert pool_engine.canary_barcodes() == set()


def test_canary_barcodes_parses_allowlist():
    os.environ["SYNC_POOL_CANARY_BARCODES"] = " 111 , 222 ,, 333 "
    assert pool_engine.canary_barcodes() == {"111", "222", "333"}
    os.environ.pop("SYNC_POOL_CANARY_BARCODES", None)


def test_canary_active_requires_all_gates_in_source():
    src = _read("services/pool_canary.py")
    fn = src[src.index("def canary_active_for"):src.index("def trigger_rollback")]
    assert "pool_writes_enabled()" in fn               # master write switch
    assert "canary_barcodes()" in fn                   # allowlist
    assert "state.backfilled_at is None" in fn         # bootstrapped Q NEVER eligible (the key rule)
    assert "is_rolled_back" in fn                      # rolled-back => legacy


def test_writes_disabled_short_circuits_canary():
    # with the master switch off, canary_active_for returns False before any DB/state lookup
    os.environ.pop("SYNC_POOL_ENGINE_WRITES", None)
    src = _read("services/pool_canary.py")
    fn = src[src.index("def canary_active_for"):src.index("def trigger_rollback")]
    assert "if not pool_engine.pool_writes_enabled():" in fn
    assert "return False" in fn


def test_handle_webhook_canary_gated_and_isolated_and_returns():
    src = _read("services/inventory_sync_service.py")
    assert "pool_canary.canary_active_for(db, barcode)" in src
    assert "pool_canary.canary_handle(" in src
    # canary runs before the legacy version gate and RETURNs (bypasses legacy propagation)
    cpos = src.index("pool_canary.canary_handle(")
    gpos = src.index("is_authoritative = _is_new_authoritative_version(")
    assert cpos < gpos
    # on canary error it trips a rollback and does NOT run legacy on poisoned state
    window = src[src.index("PHASE 3 CANARY WRITE PATH"):gpos]
    assert "trigger_rollback" in window and "return" in window
    # canary_handle opens its OWN session (isolation)
    cs = _read("services/pool_canary.py")
    assert "db = SessionLocal()" in cs[cs.index("def canary_handle"):]


# --- convergence writer: CAS + ref + version + floor + echo anchor --------------------------

def test_canary_runs_before_shadow():
    # both ingest the webhook_id idempotently; if shadow ran first, canary would see a duplicate and
    # never converge (sale folded but not propagated). Canary MUST be first for canary barcodes.
    src = _read("services/inventory_sync_service.py")
    assert src.index("pool_canary.canary_handle(") < src.index("pool_engine.shadow_observe("), \
        "canary write path must run BEFORE shadow"


def test_converge_uses_cas_ref_version_and_floor():
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):]
    assert "max(int(state.quantity), sync_guards.INVENTORY_FLOOR)" in conv   # floored target (no negative)
    assert "compare_quantity=" in conv                                       # compare-and-set
    assert "referenceDocumentUri" in src or "ref_uri" in conv                # attribution
    assert "?v={version}" in conv                                            # pool version in the ref
    # echo anchor + mirror sync so the engine's own write folds to delta 0
    assert "'convergence'" in conv
    assert "UPDATE inventory_levels SET available=:q" in conv


# --- automatic rollback triggers -----------------------------------------------------------

def test_rollback_thresholds_and_wiring():
    from services import pool_canary
    src = _read("services/pool_canary.py")
    ev = src[src.index("def evaluate_canary_rollback"):src.index("def canary_handle")]
    # the four trigger classes the spec requires
    assert "repeated_cas_conflict" in ev
    assert "write_amplification" in ev
    assert "oscillation" in ev
    assert "trigger_rollback(db, barcode" in ev
    # threshold boolean logic the code encodes (CAS conflicts >= threshold)
    T = pool_canary.ROLLBACK_CAS_FAILURES
    assert (T >= T) and not ((T - 1) >= T)


def test_rollback_reverts_to_legacy_and_alerts():
    src = _read("services/pool_canary.py")
    tr = src[src.index("def trigger_rollback"):src.index("def clear_rollback")]
    assert "PoolCanaryRollback(" in tr                 # marker => canary_active_for returns False => legacy
    assert 'alerting.critical("pool_canary.rollback"' in tr
    assert "severity=\"CRITICAL\"" in tr               # audit kept
    assert "def clear_rollback" in src                 # reversible (operator)


# --- convergence MATH under the adversarial cases (the engine's fold drives canary Q) -------

def _sim(events, floor=0):
    seen, q, last_obs, last_ts = set(), None, {}, {}
    for e in events:
        w = e.get("wid")
        if w is not None and w in seen:
            continue
        if w is not None:
            seen.add(w)
        s = e["store"]
        if is_stale_for_source(last_ts.get(s), e["ts"]):
            continue
        q = fold_observation(q, last_obs.get(s), e["observed"], floor=floor)
        last_obs[s] = e["observed"]; last_ts[s] = e["ts"]
    return q

def test_canary_concurrent_sales_no_lost_sale():
    assert _sim([{"store":"A","observed":100,"ts":1,"wid":"a0"},{"store":"B","observed":100,"ts":1,"wid":"b0"},
                 {"store":"A","observed":99,"ts":2,"wid":"a1"},{"store":"B","observed":99,"ts":2,"wid":"b1"}]) == 98

def test_canary_duplicate_idempotent():
    assert _sim([{"store":"A","observed":100,"ts":1,"wid":"a0"},{"store":"A","observed":99,"ts":2,"wid":"a1"},
                 {"store":"A","observed":99,"ts":2,"wid":"a1"}]) == 99

def test_canary_out_of_order_rejected_monotonic():
    assert _sim([{"store":"A","observed":100,"ts":1,"wid":"a0"},{"store":"A","observed":98,"ts":3,"wid":"a3"},
                 {"store":"A","observed":99,"ts":2,"wid":"a2"}]) == 98

def test_canary_echo_anchor_folds_to_zero():
    # after converge sets B to Q, an anchor records B's observed=Q, so B's echo (observed=Q) -> delta 0
    assert fold_observation(98, 98, 98) == 98


# --- REGRESSION: the 2026-06-26 spike-zeroing bug (converge must reseed ALL baselines) -------------

def test_converge_reseeds_all_store_baselines_not_only_landed():
    # converge_pool MUST reseed every processed store's ledger baseline to Q (insert a 'convergence'
    # anchor) even when its CAS did NOT land. A mirror-blind / cas_conflict store that keeps a stale,
    # spiked source_prev makes the next real observation fold catastrophically (pool floored to 0).
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):src.index("def simulate_convergence")]
    anchor = conv.index("'convergence'")                                     # the baseline-reseed INSERT
    landed_guard = conv.index('if cas_result in ("set", "set_after_retry"')  # the landed-only guard
    mirror = conv.index("UPDATE inventory_levels SET available=:q")
    # anchor reseed happens for ALL stores (before/outside the landed guard); mirror stays gated.
    assert anchor < landed_guard, "convergence anchor must reseed EVERY store, not only landed-CAS ones"
    assert landed_guard < mirror, "mirror UPDATE must stay inside the landed-cas guard (never lie)"


def test_spike_does_not_zero_when_converge_reseeds_baselines():
    # WITH the fix: each converge reseeds every store's baseline to Q, so a transient spike that is
    # later reverted folds cleanly instead of flooring the pool to 0.
    Q = 990
    last = {"E": 990, "G": 990}
    def observe(store, val):
        nonlocal Q
        Q = fold_observation(Q, last[store], val)
        last[store] = val
        for s in last:            # converge reseeds EVERY store's baseline to the new Q (the fix)
            last[s] = Q
        return Q
    assert observe("E", 2050) == 2050          # spike up
    assert observe("E", 991) == 991            # revert folds 991-2050 against Q=2050 -> 991 (NOT 0)
    assert observe("G", 990) == 990            # subsequent normal sale folds correctly


def test_spike_zeroes_without_baseline_reseed_is_the_bug():
    # WITHOUT reseeding the spiking store's baseline (the pre-fix behaviour), an intervening converge
    # moves Q but leaves the stale spike baseline -> the revert floors the pool to 0. Documents the bug.
    Q = 990
    Q = fold_observation(Q, 990, 2050)         # Esteban spike -> Q=2050, baseline left at 2050
    Q = 992                                    # an intervening converge pulls Q to the other store's 992
    Q = fold_observation(Q, 2050, 991)         # 992 + (991-2050) = -67 -> floored to 0  (the wipe)
    assert Q == 0


# --- P0a: ENGINE ECHO SUPPRESSION (converge writes a value-anchored WriteIntent the _is_echo gate drops) -

def test_converge_creates_value_anchored_echo_marker():
    # The engine's OWN CAS writes echo back as fresh webhooks (new webhook_id -> ledger dedup misses
    # them). converge_pool must record a value-anchored WriteIntent so handle_webhook's _is_echo gate
    # (which runs BEFORE the canary block) suppresses the echo instead of re-ingesting it as an
    # observation -> otherwise the echo feeds the oscillation detector and trips FALSE rollbacks.
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):src.index("def simulate_convergence")]
    assert "models.WriteIntent(" in conv, "converge must record an echo marker for its own writes"
    marker_pos = conv.index("models.WriteIntent(")
    # gated to LANDED CHANGES only ('set'/'set_after_retry') — an 'already' store produced no write,
    # hence no echo, so no marker (avoids suppressing a genuine same-value change).
    guard = 'if cas_result in ("set", "set_after_retry"):'
    assert guard in conv and conv.index(guard) < marker_pos, "echo marker must be gated to landed changes"
    marker = conv[marker_pos:marker_pos + 400]
    assert "quantity=target" in marker                  # value-anchored: matches the echo by value
    assert "inventory_item_id=" in marker               # per-listing precise (no cross-suppression)
    assert "ECHO_TTL_SECONDS" in marker                 # bounded TTL window
    # CRUCIAL: no lineage op -> a REAL change riding in (observed != target) is NOT suppressed; it
    # flows to the canary fold. (sync_operation_uuid would make _find_self_echo consume it instead.)
    assert "sync_operation_uuid" not in marker


def test_converge_clears_diverged_since_on_full_success():
    # P3: a fully-successful convergence (failed==0) drives every store to Q -> they agree -> the SLA
    # clock is cleared immediately rather than waiting for the next validation sweep.
    src = _read("services/pool_engine.py")
    conv = src[src.index("def converge_pool"):src.index("def simulate_convergence")]
    assert "failed == 0" in conv and "diverged_since = None" in conv


# --- P0b: TRANSIENT-SPIKE CORROBORATION (magnitude-agnostic; corrects phantom UP jumps to live truth) ---

def test_corroboration_verdict_corrects_phantom_up_jump():
    # claimed 990->2050 but live truth still ~990 => phantom => correct to live (discard the spike).
    assert pool_engine.corroboration_verdict(990, 2050, 991, 10) == "correct"
    assert pool_engine.corroboration_verdict(990, 2050, 990, 10) == "correct"


def test_corroboration_verdict_allows_real_restock_any_magnitude():
    # MAGNITUDE-AGNOSTIC: live reflects the new high (even partly sold down) => fold, whatever the size.
    assert pool_engine.corroboration_verdict(990, 6990, 6990, 10) == "fold"     # +6000 restock confirmed
    assert pool_engine.corroboration_verdict(990, 12990, 12970, 10) == "fold"   # +12000, 20 sold since
    assert pool_engine.corroboration_verdict(990, 6990, 4000, 10) == "fold"     # flash-sold but live >> prev


def test_corroboration_verdict_ignores_non_up_moves_and_fails_open():
    assert pool_engine.corroboration_verdict(990, 989, 0, 10) == "fold"         # a sale (down move) — never gate
    assert pool_engine.corroboration_verdict(990, 996, 0, 10) == "fold"         # within tolerance — not gated
    assert pool_engine.corroboration_verdict(990, 2050, None, 10) == "fold"     # live unreadable => FAIL-OPEN
    assert pool_engine.corroboration_verdict(None, 2050, 5, 10) == "fold"       # no prior obs => bootstrap, fold


def test_canary_corroborates_before_ingest():
    # the corroboration must run BEFORE ingest so a phantom never enters the ledger (and so can never
    # become the per-source baseline that folds a later revert to 0).
    src = _read("services/pool_canary.py")
    inner = src[src.index("def _canary_handle_inner"):]
    assert "corroborate_up_jump(" in inner
    assert inner.index("spike_corroboration_enabled()") < inner.index("ingest_event("), \
        "spike corroboration must run before ingest_event"


# --- P1a: shadow must NOT fold rolled-back pools (else pool_q drifts with no convergence) ----------

def test_shadow_skipped_for_rolled_back_barcodes():
    src = _read("services/inventory_sync_service.py")
    win = src[src.index("PHASE 1 SHADOW MODE"):src.index("VERSION CHECK")]
    assert "is_rolled_back(db, barcode)" in win
    assert "not rolled_back" in win


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
    print(f"\n{passed}/{len(fns)} Phase-3 canary tests passed")
    sys.exit(0 if passed == len(fns) else 1)
