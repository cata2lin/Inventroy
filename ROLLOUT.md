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

## Phase 3 — Canary writes

1. **Backfill** `PoolState.quantity` for the canary barcodes from a live-truth read (seed Q = the
   operator-confirmed correct value), so the first convergence is to truth, not a folded estimate.
2. Add the barcode to `SYNC_POOL_CANARY_BARCODES` + `SYNC_POOL_ENGINE=true`. The engine becomes
   authoritative for those barcodes only (real CAS-to-Q); all others stay legacy.
3. Ramp: **1 → 5 → 25 → 100** barcodes, only while metrics stay healthy at each step.
4. Canary picks: low-volume, historically stable, multi-store, low-risk.

**Automatic rollback** (per barcode): if live spread exceeds threshold, repeated CAS failures,
oscillation, or negative pressure → trip the `BarcodeCircuitBreaker`, drop the barcode from the canary
list, revert to legacy, emit CRITICAL. (Reuses the existing breaker + live-truth detector.)

## Phase 4 — Full absolute convergence

`SYNC_POOL_ENGINE=true` with an empty canary list (global). Legacy `_execute_delta_propagation` is
disabled. Every propagation is CAS-to-Q, version-protected, idempotent, ref-attributed. Keep the
legacy code in place (dormant) for one release as the rollback path.

## Phase 5 — Hardening

Transactional outbox (one row per target, durable retries, dead-letter + replay), chaos/concurrency
integration tests against staging, convergence proofs, and operational tooling (divergence dashboard,
event-timeline explorer, dead-letter replay).

## Invariants enforced at every phase
no negative propagation (floor on every write) · no cross-store timestamp staleness (per-source only)
· idempotent ingest (webhook_id unique) · no real sale silently suppressed (ref/value-anchored echo)
· every mutation observable + auditable + replayable · rollback capability never removed.
