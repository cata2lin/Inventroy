# Pool Engine Cutover — Rollout Procedure

Migrating inventory sync from **relative-delta propagation** → **absolute-state convergence**
(canonical `PoolState` + append-only `PoolEvent` ledger + idempotent CAS-to-Q). Staged, reversible,
observable. **Never a big-bang.** Rollback is always one flag flip.

## Flags (all default OFF — legacy delta path is authoritative until explicitly changed)

| Flag | Default | Effect |
|---|---|---|
| `SYNC_POOL_SHADOW` | false | **Phase 1.** Run the engine in parallel on every genuine webhook — ingest + evolve `PoolState` + simulate convergence + compare vs legacy. **No Shopify writes.** |
| `SYNC_POOL_ENGINE` | false | **Phase 3/4.** Engine becomes authoritative and performs real CAS-to-Q writes. |
| `SYNC_POOL_CANARY_BARCODES` | (empty) | **Phase 3.** Comma list of barcodes for which the engine writes (bypassing legacy); everything else stays legacy. Empty + `SYNC_POOL_ENGINE=true` = global (Phase 4). |
| `LEGACY_RECONCILE_ENABLED` | false | The lockless legacy reconcile writer (kept off). |
| `SYNC_POOL_SHADOW_ALERT_DELTA` | 5 | `|Q − legacy|` that raises a shadow-disagreement alert. |

Rollback at any phase = set the relevant flag back and `systemctl restart inventoryapp`. The legacy
delta path is never removed until Phase 4 is proven, so reverting is always safe and immediate.

## Phase 1 — Shadow mode (CURRENT) — no Shopify writes

Enable: systemd drop-in `Environment="SYNC_POOL_SHADOW=true"`, restart.

What runs: on every genuine (non-echo) webhook, `pool_engine.shadow_observe` ingests the event
(idempotent), folds it into `PoolState` (per-source ordering, monotonic version), simulates the CAS
plan, and writes a `pool_shadow_compare` audit row. **It cannot affect production** — own DB session,
best-effort, no inventory mutations (locked down by `tests/test_phase1_shadow.py`).

Watch (queries over `audit_logs`):
- `action='pool_shadow_compare'` → `details.delta_difference` distribution (Q vs legacy).
- `action='pool_shadow_dup_suppressed'` → idempotency catching retries.
- `action='pool_shadow_stale_reject'` → per-source out-of-order rejects.
- alerts `pool_shadow.disagreement` / `pool_shadow.negative`.

**Go to Phase 2 when:** shadow runs cleanly for ≥ a few days under real traffic; `delta_difference`
is explainable (it should be ~0 for already-converged pools, and equal to the known offset for
pre-existing diverged pools — never random); zero `pool_shadow.negative`; no `shadow_observe failed`.

## Phase 2 — Live-truth validation — still no auto-heal

Compare `PoolState.quantity` against **live Shopify** (not the mirror) and the legacy mirror, via the
live-truth sweep extended to emit `{barcode, pool_quantity, per-store live, spread, last_event,
unresolved_duration}`. Add a permanent-divergence detector (LIVE spread persists across N sweeps) and
convergence-SLA checks. **Alert-only.**

**Go to Phase 3 when:** for stable pools, `PoolState.quantity` matches live Shopify (or the explainable
offset); the engine would have *correctly* converged the known-diverged pools.

## Phase 3 — Canary writes (BUILT, dormant) — operational runbook

Infrastructure shipped & flag-off: `services/pool_backfill.py` (3A), `services/pool_canary.py` (3B),
`services/pool_ops.py` + `/api/diagnostics/pool/*` (3C), tests `test_phase3_backfill.py` /
`test_phase3_canary.py` (3D). Write flags: `SYNC_POOL_ENGINE_WRITES=false`, `SYNC_POOL_CANARY_BARCODES=[]`.

**Eligibility (enforced in `pool_canary.canary_active_for`, all required):** `SYNC_POOL_ENGINE_WRITES`
on · barcode in `SYNC_POOL_CANARY_BARCODES` · `pool_states.backfilled_at IS NOT NULL` (a confirmed
live-truth backfill — bootstrapped Q is NEVER write-authoritative) · no active rollback marker.

### Pre-canary checklist (all must hold)
- [ ] Phase 1 shadow + Phase 2 validation ran clean for ≥ a few days.
- [ ] No unexplained `delta_difference` / no unexplained `pool_validation_diverged`; 0 `pool_shadow.negative`.
- [ ] `/api/diagnostics/pool/dashboard` healthy; `convergence_sla` empty (no permanent divergence).
- [ ] **Backfill done** for each candidate: `POST`-style operator run
  `pool_backfill.backfill_pool_state_from_live_truth([bc], dry_run=False, operator_confirmed=True)` →
  must return `backfilled` (not `skipped_*`). Dry-run first (`/api/diagnostics/pool/backfill-plan?barcode=`).
- [ ] Candidates are low-volume, historically stable, multi-store, low-risk.

### Canary sequence — **1 → 5 → 25 → 100**
At each step: backfill the new barcodes → add them to `SYNC_POOL_CANARY_BARCODES` (drop-in) → restart.
`SYNC_POOL_ENGINE_WRITES=true` is set once, at step 1.

### Observation between steps (minimum healthy runtime ≥ 24h, then expand)
Watch `/api/diagnostics/pool/dashboard`:
- `metrics.convergence_success_rate` ≈ 1.0; `cas_conflicts` low; `avg_latency_ms` stable.
- `canary_health[*].health_score` high; **zero `rollback_events`**.
- `convergence_sla` empty; `live_vs_canonical` empty (engine Q == live).

**Expand criteria:** all green for the full window. **Escalate/hold:** any rollback, any SLA breach,
any persistent `live_vs_canonical` mismatch.

### Automatic rollback (per barcode, built in `evaluate_canary_rollback`)
Trips on: repeated CAS conflicts (`POOL_CANARY_ROLLBACK_CAS_FAILURES`), write amplification
(`..._AMPLIFICATION`/60s), oscillation (`..._OSCILLATION` sign flips), or a canary exception. Effect:
writes a `pool_canary_rollbacks` marker → `canary_active_for` returns False → **barcode instantly
reverts to the legacy delta path**, audit retained, CRITICAL alert. No global impact.

### Emergency rollback (manual)
- **One barcode:** `pool_canary.trigger_rollback(db, bc, "manual", {...})` (reverts to legacy now).
- **All canary writes, instantly:** set `SYNC_POOL_ENGINE_WRITES=false` + restart — every barcode
  falls back to legacy in one flip. (Legacy code is never removed before Phase 4 is proven.)
- **Undo a bad backfill:** `pool_backfill.reverse_backfill(backfill_id)` restores the prior Q and
  clears write-eligibility.

## Phase 4 — Full absolute convergence

`SYNC_POOL_ENGINE_WRITES=true` with an empty `SYNC_POOL_CANARY_BARCODES` (global; every backfilled
pool writes). Legacy `_execute_delta_propagation` is disabled. Every propagation is CAS-to-Q,
version-protected, idempotent, ref-attributed. Keep the legacy code in place (dormant) for one release
as the rollback path.

## Phase 5 — Hardening

Transactional outbox (one row per target, durable retries, dead-letter + replay), chaos/concurrency
integration tests against staging, convergence proofs, and operational tooling (divergence dashboard,
event-timeline explorer, dead-letter replay).

## Invariants enforced at every phase
no negative propagation (floor on every write) · no cross-store timestamp staleness (per-source only)
· idempotent ingest (webhook_id unique) · no real sale silently suppressed (ref/value-anchored echo)
· every mutation observable + auditable + replayable · rollback capability never removed.
