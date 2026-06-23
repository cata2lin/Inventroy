# migrate_pool_ledger.py
"""
STAGE 2 migration — canonical pool state + append-only event ledger.

Idempotent & additive (CREATE TABLE IF NOT EXISTS). Creating these tables is INERT: the pool engine
is flag-gated (SYNC_POOL_ENGINE, default off) and not wired into the write path until the Stage 2
cutover, so running this migration does not change any behavior.

Run:  python migrate_pool_ledger.py
"""
from sqlalchemy import text
from database import engine

DDL = [
    """
    CREATE TABLE IF NOT EXISTS pool_states (
        barcode           VARCHAR(255) PRIMARY KEY,
        quantity          INTEGER NOT NULL,
        version           BIGINT  NOT NULL DEFAULT 1,
        source_event_id   BIGINT,
        source_store_id   INTEGER,
        source_timestamp  TIMESTAMPTZ,
        updated_at        TIMESTAMPTZ DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pool_events (
        event_id          BIGSERIAL PRIMARY KEY,
        barcode           VARCHAR(255) NOT NULL,
        source_store_id   INTEGER,
        source_variant_id BIGINT,
        inventory_item_id BIGINT,
        observed_quantity INTEGER NOT NULL,
        source_timestamp  TIMESTAMPTZ,
        webhook_id        VARCHAR(255),
        kind              VARCHAR(40) NOT NULL DEFAULT 'observation',
        applied           BOOLEAN NOT NULL DEFAULT false,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # webhook_id UNIQUE → idempotent ledger ingest (INSERT ... ON CONFLICT (webhook_id) DO NOTHING).
    # Postgres allows multiple NULLs, so events without a webhook id never collide.
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pool_events_webhook_id ON pool_events (webhook_id) WHERE webhook_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_pool_events_barcode ON pool_events (barcode)",
    "CREATE INDEX IF NOT EXISTS ix_pool_events_barcode_ts ON pool_events (barcode, source_timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_pool_events_created ON pool_events (created_at)",
]


def main():
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    print("pool_ledger migration applied (pool_states, pool_events). Engine remains flag-gated/off.")


if __name__ == "__main__":
    main()
