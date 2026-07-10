# Trendyol integration map (discovered 2026-07-10 on Scripturi VPS)

**Goal:** implement Trendyol ↔ Shopify stock sync. Trendyol sends NO webhooks → sync must be
poll-based (orders + stock reconciliation) with pushes via their update API.

## Where everything lives
- **Scripturi VPS:** `root@84.46.242.181` (vmi2650854.contaboserver.net; system ssh, default id_rsa).
- **The app:** `/opt/apps/scripturi-dashboard` — FastAPI (`app.py`) + static JS dashboard.
  Trendyol modules: `api/trendyol_profit.py`, `api/trendyol_split.py`, `api/trendyol_awb.py`,
  cron worker `trendyol_split.py` (every 5 min), calculator `trendyol_profitability.py`,
  probe `_check_trendyol.py`. Engine copies in git: `team-intelligence/.../metrics-cache/engine/
  trendyol_profitability.py` + `trendyol_get_token.py`.
- **State files (JSON, not DB):** `data/trendyol_mapping.json` (product mapping),
  `data/trendyol_results_history.json` (profit runs), `trendyol_split_log.json` (split audit).

## Auth / account
- Basic auth built from `TRENDYOL_API_KEY:TRENDYOL_API_SECRET` (base64) — env vars with in-code
  defaults in `trendyol_split.py:26-33` (also hardcoded in `_check_trendyol.py` — should move to
  the team secret store). `TRENDYOL_SELLER_ID=1215280`, `TRENDYOL_STORE_FRONT_CODE=RO`.
- Base URL: `https://apigw.trendyol.com/integration`.

## What the app ALREADY pulls/does with the Trendyol API (all READ or logistics ops)
| Capability | Endpoint | Where |
|---|---|---|
| Orders (paged, by status Picking/Invoiced, epoch-ms date range, 14-day window) | `GET /order/sellers/{sellerId}/orders` | `_check_trendyol.py`, split/AWB routes |
| Shipment packages (multi-qty detection) | `GET .../shipment-packages` | `trendyol_split.py`, `api/trendyol_split.py` |
| Split multi-item packages (DPD weight fix) | `POST /order/sellers/{id}/shipment-packages/{pkgId}/split-packages` | `trendyol_split.py:160` (cron every 5 min, auto-split by remembered barcodes + rules) |
| AWB labels download/merge + SKU overlay + downloaded-state | AWB endpoints + local DB | `api/trendyol_awb.py` |
| Products list (for mapping UI) | products fetch inside `api/trendyol_profit.py` (`/api/trendyol/fetch-trendyol-products`) | profit module |
| Settlements/financials (profitability per product/period) | finance/settlement endpoints | `trendyol_profitability.py` |
| Tracking link | `https://www.trendyolexpress.com/gonderi-takip/{ctn}` | AWB/UI |

**NOT implemented anywhere: stock or price PUSH to Trendyol** (no `price-and-inventory` calls in the
codebase) and no stock pull-compare loop. Stock sync = greenfield.

## Product identity mapping (CRITICAL for sync design)
`data/trendyol_mapping.json` → `{"mapping": [{trendyol_barcode, trendyol_sku, trendyol_name,
trendyol_price, shopify_store, shopify_sku, shopify_barcode, shopify_name, shopify_cost}, ...]}`
- **Trendyol "barcode" is Trendyol-generated** (e.g. `2328552INJISGOP8Z`) — NOT the EAN our pool
  engine keys on; `shopify_barcode` is often EMPTY in the mapping.
- **Matching is by SKU** (`trendyol_sku` == `shopify_sku`, e.g. `14581`), store `GRAN` (Grandia).
  Mapping built/edited via dashboard routes: `/api/trendyol/fetch-shopify-products`,
  `/fetch-trendyol-products`, `/auto-map`, `/save-mapping`.
- ⇒ For sync, the join is: Trendyol item (its barcode = write key on their side) ↔ mapping row ↔
  Shopify SKU on GRAN ↔ our pool engine's EAN barcode (via product_variants.sku → barcode).

## Trendyol API facts that shape the sync design (no webhooks)
- Orders endpoint is pollable with `startDate/endDate` (epoch ms) + `status` + pagination — the
  app already polls it. Sales detection = poll orders (e.g. every 2-5 min), diff against seen order
  ids (idempotency like our webhook_id dedup).
- Product stock READ: products endpoint returns per-item `quantity` (approved listings) — pollable
  for full-state reconciliation (like our live-truth sweep).
- Stock/price WRITE: `POST /integration/inventory/sellers/{sellerId}/products/price-and-inventory`
  with `{"items":[{"barcode": <trendyol_barcode>, "quantity": N}]}` (batch ≤100; async batchRequestId
  to poll for result). This is the missing PUSH primitive.

## Proposed sync architecture (starting point — to be designed/reviewed)
Treat Trendyol as ONE more replica of the pool, adapted to polling:
1. **Outbound (Shopify→Trendyol), event-driven:** when our engine converges a pool whose barcode maps
   to a Trendyol item (via mapping), enqueue a Trendyol `price-and-inventory` push of Q; batch+retry;
   poll batchRequestId; alert on failures. Echo-safe: Trendyol doesn't call us back, no loop risk.
2. **Inbound (Trendyol sales), poll orders every few minutes:** new order lines → per-listing
   "observation" events (observed = current pool Q − qty) or better: direct fold of −qty deltas keyed
   to a virtual "Trendyol store" listing per barcode; idempotent by orderId+lineId.
3. **Reconcile sweep (hourly):** GET products stock, compare vs pool Q, alert/converge drift
   (Trendyol-side manual edits, cancels/returns land here too).
4. **Mapping hygiene:** extend mapping to carry the EAN (our pool key); auto-map by SKU exists.
Open questions for next session: returns/cancellations handling (order statuses), which stores' pool
(GRAN today; more?), where the sync lives (Inventroy VDS app vs scripturi-dashboard), rate limits.

## Trendyol integration-developer-tool (github.com/Trendyol/trendyol-integration-developer-tool, studied 2026-07-10)
NOT a runnable client/mock — an **AI-assistant plugin** (Claude Code/Codex/Cursor) + hosted **MCP server**
(`https://apigw.trendyol.com/trendyol-developer-tools-mcp-server/sse`, 12 tools: getEndpoint,
searchEndpoints, generateCurl/ExampleRequest/ImplementationGuide/TestFixtures, getBatchPollingStrategy,
validatePayload/Request, validateProductAgainstCategoryAttributes, getIntegrationModules,
getIntegrationPlan). Install: `/plugin marketplace add trendyol/trendyol-integration-developer-tool`.
Phase 1 scope = PRODUCTS only (catalog lookup, onboarding POST /v2/products ≤1000, content/variant/
delivery bulk updates, price-and-inventory, archive/unlock/delete lifecycle, product search, buybox).
Orders/shipments/claims/finance explicitly future phases. Local clone kept at scratchpad/trendyol-tool.

**Facts that lock our sync design:**
- `price-and-inventory` supports **quantity-only partial update** (`{"items":[{"barcode","quantity"}]}`)
  — price stays out of the sync path. Async: returns `batchRequestId`; poll `GET .../products/
  batch-requests/{id}` every 3-5s; `COMPLETED != success` — check `items[].status` + `failureReasons`;
  **results expire in 4h** (persist them); retry FAILED items only. Max 1000 items/request; 20,000
  stock cap/product; works only on APPROVED products (reconcile must flag unapproved/archived).
- **15-minute idempotency window**: an identical request (same barcode, same values) is REJECTED within
  15 min → keep a per-barcode last-sent (timestamp+values) dedup store; skip identical resends;
  coalesce webhook bursts into batches.
- **NO webhooks of any kind** (confirmed by full-repo grep) → orders polling is the only inbound path.
- Products paging: `filterApprovedProducts` **max size=100** (common mistake: 1000), page×size ≤ 10000,
  then `nextPageToken`; status filters archived/blacklisted/locked/onSale.
- **Stage environment `https://stageapigw.trendyol.com`** is first-class → build + test the whole sync
  there before live. The plugin ships a PreToolUse guard that blocks production calls without explicit
  confirmation — adopt the same guard in our dev flow.
- Auth shape: `Basic base64(apiKey:apiSecret)` + `X-Supplier-Id` + storefrontCode (RO).
- Error-code catalog worth encoding: NO_MUTABLE_FIELD, QUANTITY_EXCEEDS_LIMIT, DUPLICATE_BARCODES.

**Build plan impact:** outbound pusher = dedup store → coalesced ≤1000-item quantity-only batches →
persist batchRequestId → poll → item-level retry/alert; reconcile = approved-products pages of 100 +
per-barcode getProductBase spot-checks; dev/test on stage first, with the MCP's validateRequest wired
into development; orders-poll params must come from the existing dashboard code (tool doesn't cover
orders yet).
