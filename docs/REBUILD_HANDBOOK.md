# Rebuild Handbook — everything the legacy inventory sync taught us

> **Purpose.** The legacy app is being scrapped and rebuilt from zero (new repo, new database).
> This document is the founding reference for that rebuild: the rules the old system proved with
> real incidents, the mechanisms that worked and must be kept, the failures that must be
> structurally impossible in the new design, and the quality-of-life gaps that made operating the
> old system painful. Every claim here was measured in production, not theorized.
>
> Frozen reference: git tag **`legacy-final-2026-07-16`** / branch **`legacy`**
> (github.com/cata2lin/Inventroy). Full DB dump + code tarball: `/root/backups/` on the VDS.
> Written 2026-07-16, immediately after the July 13–16 remediation.

---

## 1. The system in one page

**What it does:** keeps stock synchronized across ~19 Arona Shopify stores plus Trendyol
(marketplace). The domain model that MUST survive into any rebuild:

- A **barcode identifies one physical inventory pool**. Every product variant sharing that barcode
  — across stores AND multiple listings inside one store — is a *replica view* of the same
  physical stock. ~1,240 multi-listing pools, ~6,700 variants at the time of writing.
- The engine's job is a single sentence: **every replica of a pool shows the same, correct
  quantity, and one unit sold anywhere is one unit subtracted everywhere.**
- Business quirks the model must handle natively (not as edge cases):
  - **SKUs are unreliable and duplicated by design** — same-store duplicate SKUs are intentional;
    barcodes are the only pool key (POLICY decision 2026-07-10, after a SKU-based quarantine
    misfired and was reverted the same day).
  - **Negative-stock test stores**: some stores deliberately sell past zero (policy CONTINUE) to
    measure demand; real stock is set later. Sales below zero never belonged to the pool.
  - **New-container intake**: stock is set on ONE store (convention: MagDeal), everything else
    receives barcode + tracking only (see `anne:adaugare-stoc-produse` in the Second Brain).

**The incident timeline that motivates every rule below:**

| Date | Incident | Root cause | Damage |
|---|---|---|---|
| 2026-06-21 | Restock blocked | MAX_ABS_DELTA guard too low → raised to 1,000,000 (guard OFF) | guard neutralized |
| 2026-06-26 | Esteban zeroing | DB outage + spike-poisoned fold (992+(991−2049)=−66→floored 0) | ~990 units |
| 2026-07-03→10 | Slow-burn floor clamps | corrupt deltas floored to 0 per-store | ~9,350 units (measured from audit) |
| 2026-07-13/14 | **Self-zeroing cascade** | floor-clamp WRITES 0 + 45s echo TTL → app re-ingested its own 0-writes; pools inflated by negative-test-store restocks (raw endpoint math) | 2,847 units in 24h; all 19 stores disabled by a watchdog |
| 2026-07-14 | **Out-of-band DB writes** | a helper session "restored" 161 pools by direct SQL (quantity + version+10), bypassing the ledger | phantom stock pushed live; 3-day forensic hunt |
| 2026-07-15 | **Trendyol phantom sales** | qty-drop treated as sales; Trendyol glitch 0-reads folded whole anchors | 238 units off Grandia, zero real orders |

---

## 2. The invariants (the constitution)

Any new design must satisfy **all** of these. Each one is written in blood — the incident that
proved it is cited. If a proposed architecture cannot guarantee one of them *by construction*,
that is a design smell, not a TODO.

1. **Barcode is the pool key. SKU never gates sync.** Listings sharing a barcode are replicas —
   including several listings within one store (per-LISTING, not per-store: a per-store baseline
   interleaved two listings' observations and clobbered a fresh restock as a phantom −200,
   2026-07-10).

2. **Stock truth flows ONE way: Shopify → engine.** A human (or agent) changes stock by writing
   the value to **exactly one store via the Shopify Admin API**; the engine propagates and
   re-anchors. Never write the engine's own database (July 14 incident), never set the same
   barcode on several stores at once (proven amplification: a C41 SKU jumped 500 → 3,030).

3. **Pools are floored at zero and all pool arithmetic uses floored endpoints:**
   `delta = max(observed, 0) − max(previous, 0)`. Movement below zero never held pool units, so a
   restock-set after tracked oversell propagates exactly the set value — raw endpoint math turned
   `−300 → 500` into `+800` and inflated pools by the oversold backlog on every cycle (the origin
   of the 21,497 / 38,095-class pool quantities).

4. **Absolute convergence only.** Replicas are driven to a canonical value with compare-and-set
   writes whose post-state is known. **Relative deltas are forbidden as a propagation mechanism**
   — they echo, they double-count restocks, they amplify (the entire pre-engine bug class).

5. **A stock decrement is only a sale if orders prove it.** Quantity movement alone is a *trigger*
   for investigation, never an amount to subtract. Trendyol glitch 0-reads removed 238 units with
   zero real orders behind them; the fix (fold only order-line-verified amounts, consumed
   exactly-once) is the template for every channel. Corollary worth designing for: **prefer
   orders/refunds/cancellations as the primary sale signal everywhere**, inventory-level events
   only for manual adjustments — your own writes never generate orders, which kills the echo
   problem at the root.

6. **Every state write is attributable and the ledger is append-only.** The July 14 out-of-band
   writes took a multi-day forensic hunt because `updated_at` was the only trace. The new system
   ships day one with: dedicated DB roles (app writes; humans/tools read-only), an attribution
   trigger on canonical state (user, application_name, client IP — see
   `migrate_pool_state_audit.py`), and a firewall that doesn't expose Postgres to `0.0.0.0/0`.

7. **Fail closed, loudly.** An unverifiable destructive input (big drop with live truth
   unreachable, delta bigger than a store's stock, fold deep below zero) is REJECTED and the pool
   QUARANTINED with a CRITICAL alert — never written, never "clamped to something reasonable".
   The old floor-clamp *wrote 0* and destroyed 415/496-unit stores. The new worst case must be
   "this product stopped syncing and someone was emailed", never "stock changed wrongly".

8. **Impossible values are evidence of corruption, not facts to accommodate.** A negative delta
   larger than the target's stock means the *input* is wrong (a pool cannot lose more than it
   has). Same for a fold driving a pool deep below zero. Reject + quarantine + alert.

9. **Big changes get verified against live truth before they are believed.** A drop ≥ N units is
   confirmed by a live read of the source before it anchors or propagates (stale/echo webhooks
   are discarded against live truth; unverifiable ones fail closed). Symmetrically, claimed
   up-jumps are corroborated before folding (2026-06-26 spike).

10. **"Disabled" means inert in BOTH directions.** The legacy `enabled=false` only excluded a
    store as a write *target*; its webhooks still drove deltas into every enabled store — which
    almost turned the overnight manual corrections of July 13 into a fresh cascade. Disabled =
    no reads honored, no writes issued; at most mirror-keeping.

11. **Self-writes must be recognizable for as long as the channel can delay them.** Shopify
    webhook delivery routinely exceeds a minute; 45-second echo windows guaranteed the app would
    eventually meet its own writes as "external facts". Whatever the mechanism (value-anchored
    intents, reference attribution, orders-as-signal), it must tolerate 15+ minute delays and
    never suppress a *genuinely different* value.

12. **One writer.** The legacy system accreted four write paths (legacy delta propagation, pool
    engine convergence, auto-reconciler, legacy lockless reconciler) with different semantics and
    guards. Every incident got worse because paths disagreed. The new system has exactly one
    component that writes stock, and everything else asks it.

---

## 3. What worked — keep these (with legacy references)

Proven mechanisms, worth porting in concept (and sometimes in code — see §8):

- **Append-only event ledger + canonical pool state with a monotonic version**
  (`models.PoolEvent`, `models.PoolState`). Made every incident reconstructable; the version clock
  + `source_event_id` is how the out-of-band writes were even detectable.
- **The conservation fold** (each source folds only its OWN signed change — no last-writer-wins,
  no lost concurrent sales) — with floored endpoints (§2.3) and deep-negative rejection
  (`sync_guards.classify_fold`).
- **Idempotent ingest**: `webhook_id UNIQUE` + `INSERT … ON CONFLICT DO NOTHING`. Shopify retries
  and replays became structurally harmless.
- **Per-barcode advisory locks** (Postgres) for cross-process serialization
  (`services/dist_lock.py`).
- **Durable quarantine with evidence-gated auto-recovery** (`pool_canary_rollbacks` +
  `recover_parked_pools.py`): a suspect pool drops to a safe mode; it returns to the engine
  automatically only when **all its live listings agree** and the audited backfill succeeds. This
  turned "35 broken pools" into a self-draining queue as the operator fixed values. The
  recovery gate (lives-agree) proved stronger than any per-reason logic.
- **Live-truth backfill as THE re-anchor primitive** (`services/pool_backfill.py`): plan
  (read-only, safety verdict) → audited write → reversible log (`pool_backfills`). Refuses
  diverged/negative pools. Every remediation this week went through it.
- **Canary → global rollout with automatic rollback triggers** (cas-conflict rate, write
  amplification, oscillation — `pool_canary.evaluate_canary_rollback`) and **shadow mode** (run
  the new engine on live events with zero writes, compare decisions). This is how the engine
  itself was safely introduced, and it is exactly how v2 must be cut over.
- **Golden event capture** (`pool_golden_events`): immutable raw inbound + decisions per pool.
  Forensics without it would have been guesswork.
- **Daily inventory snapshots** (`inventory_snapshots`, 23:55, upsert-safe): they were the ground
  truth for BOTH damage assessments (July 12 snapshot for the cascade; July 9 snapshot for the
  Trendyol repair). Non-negotiable in v2 — plus heartbeat monitoring (the one night it silently
  didn't run was the night we needed it most).
- **Layered alerting**: audit trail (everything) → system events (WARN/CRITICAL views) → email
  with **per-(source, barcode) throttling** (repeats wait 6h; new problems alert immediately;
  suppressed counts reported in the next mail). One diverged pool sent 24 identical emails in a
  day before the per-key throttle existed.
- **Kill switches and rollout flags read live from env** (`SYNC_PROPAGATION_ENABLED`, engine
  writes, canary allowlists) — being able to freeze propagation-only (ingest continues) saved
  multiple nights. But see §4 on guard-neutralization-by-env.
- **Idempotent `migrate_*.py` scripts** run before restart (no Alembic needed at this scale) —
  every schema change this week was one safe re-runnable file.
- **The test style**: pure decision functions (`sync_guards`) exercised by a <2s suite that is
  both pytest-collectable and standalone-runnable, zero prod-DB dependency, incident numbers as
  literal test cases (`test_cascade_hardening.py` uses −804 vs 415). 178 tests at freeze.
- **Operational pattern: evidence CSV → operator fixes value on one store → system absorbs it**
  (the `stock_de_corectat` flow). Productize this in v2 (§7).

---

## 4. What failed — never again

Each entry: the mechanism → what it cost. These are the anti-patterns the new design must make
*impossible*, not merely discouraged.

1. **Relative-delta propagation.** Echoes re-applied as new deltas, restocks double-counted,
   sibling amplification (HA-1193-1: MagDeal 1000 → 2008). The mother of most incidents.
2. **Value-independent echo suppression with short TTLs** (45s/60s). Late webhooks re-ingested
   the app's own floored 0-writes as external truth → the July 13 self-zeroing loop.
3. **Guards that WRITE when violated.** `apply_floor` converted "impossible delta" into an
   absolute SET-to-0 with compare disabled — the single most destructive line in the codebase
   (415 and 496-unit stores wiped in one night).
4. **Quantity deltas trusted as sales** (Trendyol). 238 phantom units; the ratchet repeated on
   every push/0-read cycle; the magnitude guard (≤500) never fired because each bite was small.
5. **`max()` / orphan reads as "authoritative".** The original cascade seed. Authority comes from
   a versioned ledger or an operator — never inferred from a partial read.
6. **A uniqueness constraint that contradicted the business** (`UNIQUE(sku, store_id)`,
   2025-10-03). Cost chain: 929,167 dead letters (full sync dead for 9 months) → a workaround
   (BUG-33 clear-sibling-SKUs) that silently corrupted data on every run → sync runs reporting
   `status='ok'` while losing pages → soft-delete marking live products deleted after lossy runs.
   *Lesson: when the DB fights reality, fix the constraint, not the data; and a job that lost
   anything must never report success.*
7. **Guards neutralized by configuration.** `SYNC_MAX_ABS_DELTA=1,000,000` in a drop-in turned
   the only magnitude guard off for weeks (set during a restock, never reverted). Guard tunables
   need: sane hard ceilings in code, loud logging of non-default values at boot, and expiry on
   emergency overrides.
8. **Aspirational comments.** "Blocked deltas are routed to reconciliation/review" — no such
   routing existed; blocked meant silently dropped. Every claim in a docstring must be true or
   deleted.
9. **Multiple write paths with different semantics** (legacy delta, engine CAS, auto-reconciler,
   a lockless legacy reconciler kept behind an env flag "just in case" — which converged a pool
   to 0 on July 14 at 01:19). Dead code with write access is a loaded gun; delete it.
10. **Disabled-but-not-inert stores** (§2.10) and **canary paths bypassing the circuit breaker**
    (the engine path returned before the breaker check — quarantining a barcode didn't stop it).
    Guards must sit at the single choke point, not per-path.
11. **In-process guard state under a "single worker" assumption** (storm breaker) — one deploy
    flag away from silently losing cascade protection.
12. **Database exposed to the internet with shared write credentials and zero attribution.**
    Enabled the July 14 out-of-band writes; also collected a failed outsider login the same
    minute. V2: read-only role for humans/tools, app-only write role, restricted pg_hba,
    attribution trigger from day one.
13. **Secrets in the wrong places**: production credentials as hardcoded `os.getenv` fallbacks in
    committed code; API keys in world-readable systemd drop-ins (Trendyol keys sat there for
    months and leaked into every `systemctl show`). Secrets live in `.env` (0600) / the team
    secret store, referenced by name.
14. **Best-effort `except` swallowing structural errors.** A missing `text` import made the
    group-join ledger seeding a silent no-op for its entire life (`NameError` caught by a bare
    except). Catch narrowly; let programming errors crash loudly in dev and alert in prod.
15. **Scheduled jobs without heartbeats.** The nightly snapshot silently didn't run on the
    incident night — discovered days later exactly when the missing data was needed. Every cron
    job in v2 reports success/failure to a monitored heartbeat.
16. **Alerts nobody receives.** CRITICAL alerts went only to a dashboard for weeks
    (`ALERT_WEBHOOK_URL` unset). An alerting system without a delivery test is decoration.

---

## 5. Architecture recommendation for v2 (recommendation, not a decision)

*The user owns the structure; this section records what three days inside the failure modes
suggest. Whatever shape is chosen, it must satisfy §2 by construction.*

**Invert the model: reconciliation-first (level-triggered), events as accelerators.**
The legacy system was webhook-driven (edge-triggered) with reconciliation bolted on; every bug was
some form of *mis-interpreting an edge*. A controller-style design — desired state (pool ledger)
vs observed state (live channel reads), continuously converged — is immune to missed/duplicated/
reordered/echoed events by construction: the next loop iteration heals anything.

- **One writer service** owns the pool ledger and ALL channel writes (Shopify CAS, Trendyol push).
- **Sales come from orders** (Shopify `orders/create`/cancel/refund webhooks + Trendyol order
  lines) — order IDs make sales idempotent and echo-proof (§2.5). Inventory-level events only
  hint "a manual adjustment may have happened → schedule a verify-and-adopt loop for that pool."
- **Channel adapters** (Shopify, Trendyol, future marketplaces) expose the same tiny interface:
  read live, CAS write, list orders. Everything channel-specific (rate limits, token rotation,
  batch semantics) stays inside the adapter.
- **Read models** for UI/snapshots/reports are projections of the ledger — never sources of truth.
- **Postgres only** (no new infra): advisory locks, LISTEN/NOTIFY if needed, and boring
  operability on the existing VDS.

## 6. Testing & rollout doctrine

- **Golden replay**: the legacy production history (pool_events, audit_logs, golden events,
  snapshots — all preserved in the July 16 dump) becomes a fixture library. The new engine must
  replay the recorded July incidents and end with correct stock.
- **The incident taxonomy is the regression suite**: every row of the §1 table becomes an
  executable scenario (self-zeroing echo, restock-after-oversell, Trendyol 0-read, out-of-band
  write detection, outage residue). New failure modes join the suite the day they're found.
- **Property tests** on the pure core: conservation (no unit created/destroyed by any event
  ordering), floor invariants, idempotence under replay/duplication/reorder.
- **Adversarial review before deploy** (independent reviewers trying to refute the change) — this
  caught 8 real defects in the July 14 fix before production did.
- **Shadow → canary → global cutover**, exactly like the legacy engine's own rollout: run v2
  read-only against live traffic comparing decisions; then one barcode with writes; then widen on
  clean metrics; auto-rollback triggers armed the whole way. Keep the legacy app runnable behind a
  kill switch until v2 has survived weeks at global.
- **"Is it fixed?" is a metrics sweep** (divergence count, negative count, guard events, echo
  health over a window) — never a single product spot-check.

## 7. Quality-of-life improvements (operator pain → product features)

Everything below was done by hand this week and should be a screen or a command in v2:

1. **Per-product timeline**: one view merging ledger events, audit entries, alerts, snapshots and
   channel writes for a barcode — the forensic query we assembled manually for every incident.
2. **Quarantine workbench**: parked pools listed with their evidence (per-store live values,
   snapshot references, pre-incident values), a "set true stock" action that writes one store via
   the API, and automatic un-parking status.
3. **Restore-point browser**: diff any pool/store against any snapshot date; one-click export of
   the affected-products CSV (the `stock_de_corectat` flow, productized).
4. **Alert digest**: individual CRITICALs by email (throttled per source+barcode) plus a daily
   digest of WARN-level noise; delivery self-test on boot.
5. **Dry-run by default** on every mutating command/endpoint (`--apply` to execute) — the legacy
   scripts that had this pattern never caused an incident.
6. **Structured logging** (JSON, request/pool correlation IDs) instead of `print()`; log-based
   metrics for the health sweep.
7. **Job heartbeats** (cron + scheduler jobs report to a monitored table; a missed nightly
   snapshot pages someone the same night).
8. **Config registry in the DB with an audit trail** for operational toggles (who flipped the
   kill switch, when, why) — env stays only for secrets and bootstrapping.
9. **Attribution surfaced in the UI**: the `pool_state_audit`-equivalent visible per pool, so
   "who changed this" is a click, not an investigation.

## 8. Reusable code inventory (port with review, from tag `legacy-final-2026-07-16`)

| Piece | Where | Why it's worth keeping |
|---|---|---|
| Pure guard/decision functions | `services/sync_guards.py` | floored-endpoint math, floor-breach policy, fold classification, drop-verify thresholds — all incident-tested with unit suites |
| Shopify GraphQL client | `shopify_service.py` | CAS single-item ops (`set_inventory_quantities_single` + compare), rate-limit backoff, `_after_available` parsing |
| Live-truth backfill | `services/pool_backfill.py` | plan/verdict/audited-write/reversible-log pattern |
| Evidence-gated recovery | `recover_parked_pools.py` | the lives-agree gate + drift re-anchor loop |
| Alerting sink | `services/alerting.py` | layered sinks + per-key email throttling |
| Attribution trigger | `migrate_pool_state_audit.py` | drop-in DDL for canonical-state attribution |
| Snapshot writer | `crud/snapshots.py` (`create_snapshot_for_store`) | upsert-safe daily snapshots |
| Order-verified inbound | `services/trendyol_sync.py` (`select_lines_for_drop`, `_inbound_fold`) | the template for channel sale verification |
| Test harness style | `tests/test_cascade_hardening.py`, `test_echo_authoritative.py` | pure-core testing pattern, incident literals as cases |

## 9. Decisions already made / still open

- **Decided**: new repo + new Postgres database on the same VDS; legacy stays live (hardened)
  until v2 passes shadow + canary; legacy preserved at `legacy-final-2026-07-16` + `/root/backups`.
- **Open (user to decide)**: tech stack; v1 feature scope; the program structure (user will
  provide); cutover timing.
- **Prerequisites worth doing regardless of v2** (pending user go-ahead): restrict Postgres
  access (read-only role for humans/tools, pg_hba tightening), rotate the Trendyol API keys that
  were exposed in systemd drop-ins.
