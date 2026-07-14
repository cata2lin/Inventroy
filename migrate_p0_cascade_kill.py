"""
Migration script: P0 cascade-kill (2026-07-14) — drop the UNIQUE(sku, store_id) constraint on
product_variants.

The constraint (added 2025-10-03) contradicts reality: Shopify allows duplicate SKUs per store and
this business deliberately runs same-store duplicate listings. Every full product sync collided on
it, dead-lettering ~927k product bundles since October 2025 (full sync effectively dead => no
reconciliation with truth => permanent drift), while the BUG-33 clear-before-upsert workaround
NULLed sibling SKUs on every pass. Variant row identity is the Shopify variant id; the plain
non-unique index on sku stays for search.

Idempotent + reversible (see revert note below). Finds the constraint/index by its COLUMN SET, not
only by name, in case the production DB predates the model's explicit constraint name.
Run once against the live database (as part of update.sh, BEFORE restarting the service).

Revert: CREATE UNIQUE INDEX product_variants_sku_store_id_key ON product_variants (sku, store_id)
(will fail while duplicate rows exist — which is exactly why it was wrong).
"""
from database import engine
from sqlalchemy import text

# Any UNIQUE constraint or unique index on exactly (sku, store_id) — either order.
FIND_CONSTRAINTS = """
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'product_variants'
      AND c.contype = 'u'
      AND (
        SELECT array_agg(a.attname ORDER BY a.attname)
        FROM unnest(c.conkey) AS k
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k
      ) = ARRAY['sku', 'store_id']::name[]
"""

FIND_INDEXES = """
    SELECT i.indexname
    FROM pg_indexes i
    WHERE i.tablename = 'product_variants'
      AND i.indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND i.indexdef ~* '\\(\\s*(sku\\s*,\\s*store_id|store_id\\s*,\\s*sku)\\s*\\)'
"""

VERIFY_QUERY = FIND_CONSTRAINTS


def run_migration():
    print("[MIGRATION] Connecting to database...")
    with engine.connect() as conn:
        constraints = [r[0] for r in conn.execute(text(FIND_CONSTRAINTS)).fetchall()]
        if not constraints:
            print("  No UNIQUE(sku, store_id) constraint found (already dropped?)")
        for name in constraints:
            try:
                conn.execute(text(f'ALTER TABLE product_variants DROP CONSTRAINT IF EXISTS "{name}"'))
                print(f"  Dropped constraint {name}: OK")
            except Exception as e:
                print(f"  Dropped constraint {name}: WARN: {e}")

        # A unique INDEX (not attached to a constraint) enforces the same collision — drop it too.
        indexes = [r[0] for r in conn.execute(text(FIND_INDEXES)).fetchall()]
        for name in indexes:
            try:
                conn.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
                print(f"  Dropped unique index {name}: OK")
            except Exception as e:
                print(f"  Dropped unique index {name}: WARN: {e}")

        conn.commit()
        remaining = conn.execute(text(VERIFY_QUERY)).fetchall()
        print(f"[MIGRATION] UNIQUE(sku, store_id) still present: {bool(remaining)} (must be False)")
    print("[MIGRATION] Done.")
    print("[MIGRATION] Follow-up (manual, after a clean full sync of every store):")
    print("  -- dead letters are stale once a full sync succeeds; archive or truncate:")
    print("  -- SELECT count(*), reason FROM sync_dead_letters GROUP BY reason;")
    print("  -- TRUNCATE sync_dead_letters;  (or batched DELETE if forensics are wanted)")


if __name__ == "__main__":
    run_migration()
