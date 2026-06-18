"""
Migration P3: explicit sync_group architecture.
Additive + idempotent. Creates sync_groups / sync_group_members and BACKFILLS them from the
current barcode groups (so behavior is unchanged — the engine keeps using barcode grouping until
SYNC_USE_GROUPS is enabled in a later step). Writes ONLY to the new tables; reversible by
TRUNCATE-ing them. Does NOT touch inventory.

Run: python migrate_p3_sync_groups.py
"""
from database import engine
from sqlalchemy import text

_PH = ",".join("'%s'" % p for p in
               ("0", "00", "000", "0000", "00000", "000000", "0000000", "00000000",
                "000000000", "0000000000", "00000000000", "000000000000", "0000000000000"))
_VALID = f"(pv.barcode IS NOT NULL AND btrim(pv.barcode) <> '' AND pv.barcode NOT IN ({_PH}))"

DDL = [
    """
    CREATE TABLE IF NOT EXISTS sync_groups (
        id BIGSERIAL PRIMARY KEY,
        barcode_key VARCHAR(255),
        classification VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
        sync_enabled BOOLEAN NOT NULL DEFAULT true,
        authoritative_variant_id BIGINT,
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_groups_barcode_key ON sync_groups (barcode_key) WHERE barcode_key IS NOT NULL",
    """
    CREATE TABLE IF NOT EXISTS sync_group_members (
        variant_id BIGINT PRIMARY KEY,
        sync_group_id BIGINT NOT NULL REFERENCES sync_groups(id),
        store_id INTEGER NOT NULL,
        excluded BOOLEAN NOT NULL DEFAULT false,
        added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_sgm_group ON sync_group_members (sync_group_id)",
    "CREATE INDEX IF NOT EXISTS ix_sgm_store ON sync_group_members (store_id)",
]

BACKFILL = [
    # 1) one group per shared barcode (idempotent via the unique index)
    f"""
    INSERT INTO sync_groups (barcode_key, classification, sync_enabled)
    SELECT q.barcode, 'ACTIVE', true FROM (
        SELECT pv.barcode
        FROM product_variants pv JOIN products p ON p.id=pv.product_id AND p.deleted_at IS NULL
        WHERE {_VALID}
        GROUP BY pv.barcode HAVING count(*) > 1
    ) q
    ON CONFLICT (barcode_key) WHERE barcode_key IS NOT NULL DO NOTHING
    """,
    # 2) members — mark SKU-less orphans (with a SKU'd sibling in the same store) as excluded
    f"""
    INSERT INTO sync_group_members (variant_id, sync_group_id, store_id, excluded)
    SELECT pv.id, g.id, pv.store_id,
           (NULLIF(pv.sku,'') IS NULL AND EXISTS (
              SELECT 1 FROM product_variants pv2 JOIN products p2 ON p2.id=pv2.product_id AND p2.deleted_at IS NULL
              WHERE pv2.barcode=pv.barcode AND pv2.store_id=pv.store_id AND pv2.id<>pv.id AND NULLIF(pv2.sku,'') IS NOT NULL))
    FROM product_variants pv
    JOIN products p ON p.id=pv.product_id AND p.deleted_at IS NULL
    JOIN sync_groups g ON g.barcode_key = pv.barcode
    WHERE {_VALID}
    ON CONFLICT (variant_id) DO UPDATE
      SET sync_group_id=EXCLUDED.sync_group_id, store_id=EXCLUDED.store_id, excluded=EXCLUDED.excluded
    """,
    # 3) classify groups that have intra-store different-SKU duplicates as SUSPECT_DUPLICATE
    f"""
    UPDATE sync_groups g SET classification='SUSPECT_DUPLICATE'
    WHERE g.classification='ACTIVE' AND EXISTS (
        SELECT 1 FROM product_variants pv JOIN products p ON p.id=pv.product_id AND p.deleted_at IS NULL
        WHERE pv.barcode=g.barcode_key
        GROUP BY pv.store_id HAVING count(*)>1 AND count(DISTINCT NULLIF(pv.sku,''))>1
    )
    """,
]


def run():
    print("[P3] connecting...")
    with engine.connect() as conn:
        for i, s in enumerate(DDL, 1):
            conn.execute(text(s)); print(f"  DDL [{i}/{len(DDL)}] OK")
        conn.commit()
        for i, s in enumerate(BACKFILL, 1):
            r = conn.execute(text(s)); print(f"  BACKFILL [{i}/{len(BACKFILL)}] rows={r.rowcount}")
        conn.commit()
        g = conn.execute(text("SELECT count(*) FROM sync_groups")).scalar()
        m = conn.execute(text("SELECT count(*) FROM sync_group_members")).scalar()
        ex = conn.execute(text("SELECT count(*) FROM sync_group_members WHERE excluded")).scalar()
        susp = conn.execute(text("SELECT count(*) FROM sync_groups WHERE classification='SUSPECT_DUPLICATE'")).scalar()
        print(f"[P3] sync_groups={g} members={m} excluded_orphans={ex} suspect_groups={susp}")
    print("[P3] done.")


if __name__ == "__main__":
    run()
