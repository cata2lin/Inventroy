# Tables That Contribute to "Stock by Barcode" — Final Stock Value

## The Displayed Stock Number

The `Stock` column and "Total Units" dashboard metric are driven by a single
code path in `routes/stock.py → GET /api/stock/by-barcode`.

The value shown is:

```
representative_stock = primary_variant["stock"]
                     = SUM(inventory_levels.available)
                       for the "primary" variant of that barcode group
```

---

## Tables Involved (in join order)

### 1. `product_variants`
**Role: Root of the query — one row per listing per store**

| Column used         | Purpose                                               |
|---------------------|-------------------------------------------------------|
| `barcode`           | The grouping key — all rows with the same barcode form one group |
| `id`                | Foreign key target for `inventory_levels.variant_id`  |
| `store_id`          | Joins to `stores` for currency and location info      |
| `product_id`        | Joins to `products` for title, image, deleted_at      |
| `sku`               | Displayed in the variant detail modal                 |
| `is_barcode_primary`| Selects which variant's stock is the representative   |
| `price`             | Used for retail value calculation (not the stock qty) |
| `cost_per_item`     | Used for inventory value calculation                  |
| `inventory_item_id` | Used by bulk-update to call the Shopify API           |

---

### 2. `inventory_levels`
**Role: The actual stock numbers — this is where `available` lives**

| Column used    | Purpose                                                        |
|----------------|----------------------------------------------------------------|
| `variant_id`   | FK → `product_variants.id` — links stock to a specific variant |
| `location_id`  | Identifies which warehouse/location holds this quantity        |
| `available`    | **The raw stock quantity.** Summed per variant across all its locations |

**How the sum is computed (Python, not SQL):**
```python
variant_stock = sum(
    level.available
    for level in variant.inventory_levels
    if level.available is not None
)
```
A single variant can have rows in `inventory_levels` for multiple locations.
All are summed to get the variant's total available stock.

---

### 3. `products`
**Role: Filter + display metadata**

| Column used  | Purpose                                                            |
|--------------|--------------------------------------------------------------------|
| `id`         | Join target from `product_variants.product_id`                     |
| `deleted_at` | **Critical filter** — rows where `deleted_at IS NOT NULL` are excluded entirely from the view |
| `title`      | Displayed as the product name in the table                         |
| `image_url`  | Thumbnail shown in the first column                                |
| `store_id`   | Joins to `stores`                                                  |

---

### 4. `stores`
**Role: Currency conversion for value columns (not the stock qty itself)**

| Column used        | Purpose                                                      |
|--------------------|--------------------------------------------------------------|
| `id`               | Join target from `products.store_id`                         |
| `currency`         | Used to look up the exchange rate (e.g. EUR → RON) for retail/cost value |
| `sync_location_id` | NOT used in this query directly — used by the sync engine and reconciliation |

> `stores` does not affect the stock number. It only affects
> `retail_value_ron` and `inventory_value_ron` via currency conversion.

---

## What Is NOT Used for the Stock Number

| Table                | Why excluded                                                    |
|----------------------|-----------------------------------------------------------------|
| `inventory_snapshots`| Historical snapshots — only used in the Velocity/Analytics view |
| `barcode_versions`   | Used by the sync engine to track authoritative source — not read here |
| `write_intents`      | Echo suppression only — not read by the stock view              |
| `locations`          | Not joined in this query — location filtering happens via `inventory_levels` rows already loaded |
| `sync_runs`          | Sync metadata only                                              |

---

## Full Data Flow Summary

```
product_variants  (filter: barcode IS NOT NULL AND barcode != '')
        │
        ├──► products  (filter: deleted_at IS NULL)
        │         └──► stores  (for currency only)
        │
        └──► inventory_levels  (all rows for this variant_id)
                    └── SUM(available) = variant_stock

Group all variants by barcode
Pick the one where is_barcode_primary = TRUE  (fallback: first in list)
Display that variant's SUM(available) as the "Stock" column
```

---

## Why Only One Variant's Stock Is Shown

Because the **sync engine guarantees all stores mirror the same quantity**
for a given barcode. Showing the primary variant's stock is accurate —
all others are identical by design.

---

## Database Connection

Source: `database.py`

| Setting       | Value               |
|---------------|---------------------|
| **Engine**    | PostgreSQL          |
| **Host / IP** | `38.242.226.83`     |
| **Database**  | `InventorySync`     |
| **User**      | `scraper`           |
| **Password**  | `Scraper123#`       |

Connection string:
```
postgresql://scraper:Scraper123%23@38.242.226.83/InventorySync
```

These are the hardcoded defaults in `database.py`. They can be overridden
by setting the corresponding variables in a `.env` file:
```
DB_USER=scraper
DB_PASSWORD=Scraper123#
DB_HOST=38.242.226.83
DB_NAME=InventorySync
```
