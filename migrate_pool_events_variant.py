# migrate_pool_events_variant.py
"""
PER-LISTING ENGINE migration (2026-07-10). Idempotent, reversible-by-no-op. Run BEFORE (or with)
deploying the per-listing engine, while webhook traffic is briefly paused by the restart.

1. Stamp source_variant_id onto legacy ledger rows that carry only inventory_item_id (old
   backfill_baseline rows inserted variant NULL). The per-variant fold baseline (_source_prev,
   `source_variant_id IS NOT DISTINCT FROM :v`) can never match a NULL row — a dormant pool whose
   only history is its baseline would treat its first post-deploy event as a replica-join and
   converge would REVERT a real restock (the June "manual additions reverted" class).
   product_variants.inventory_item_id is UNIQUE, so the join is exact.

2. Hot-path index for the per-variant lookups (prev/staleness/corroboration run per webhook, and
   converge now inserts one anchor per LISTING per event).
"""
from sqlalchemy import text
from database import engine

STATEMENTS = [
    """UPDATE pool_events pe SET source_variant_id = pv.id
       FROM product_variants pv
       WHERE pe.source_variant_id IS NULL
         AND pe.inventory_item_id IS NOT NULL
         AND pe.inventory_item_id = pv.inventory_item_id""",
    """CREATE INDEX IF NOT EXISTS ix_pool_events_barcode_variant_event
       ON pool_events (barcode, source_variant_id, event_id DESC)""",
]

if __name__ == "__main__":
    with engine.begin() as conn:
        for s in STATEMENTS:
            res = conn.execute(text(s))
            print("OK (%s rows): %s..." % (getattr(res, "rowcount", "?"), " ".join(s.split())[:70]))
    print("migration complete")
