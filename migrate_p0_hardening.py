"""
Migration: P0 cascade-hardening schema.
Idempotent + additive (no drops, no data loss). Safe to run multiple times.

Adds:
  - write_intents propagation-lineage columns (sync_operation_uuid, origin_store_id,
    origin_inventory_item_id, propagation_depth)
  - barcode_circuit_breakers table
Run once against the live DB BEFORE deploying the P0 code (the new code reads/writes
these columns; running the migration first keeps the old code working unchanged).
"""
from database import engine
from sqlalchemy import text

STATEMENTS = [
    # --- write_intents lineage columns ---
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS sync_operation_uuid VARCHAR(64)",
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS origin_store_id INTEGER",
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS origin_inventory_item_id BIGINT",
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS propagation_depth INTEGER NOT NULL DEFAULT 0",
    "CREATE INDEX IF NOT EXISTS ix_write_intents_sync_op ON write_intents (sync_operation_uuid)",
    "CREATE INDEX IF NOT EXISTS ix_write_intents_item_live ON write_intents (target_store_id, inventory_item_id, expires_at)",
    # --- circuit breaker table ---
    """
    CREATE TABLE IF NOT EXISTS barcode_circuit_breakers (
        barcode VARCHAR(255) PRIMARY KEY,
        reason TEXT NOT NULL,
        tripped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        details JSONB
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_barcode_breaker_expires ON barcode_circuit_breakers (expires_at)",
]

VERIFY = """
    SELECT
      (SELECT count(*) FROM information_schema.columns
        WHERE table_name='write_intents'
          AND column_name IN ('sync_operation_uuid','origin_store_id','origin_inventory_item_id','propagation_depth')) AS write_intent_cols,
      (SELECT count(*) FROM information_schema.tables
        WHERE table_name='barcode_circuit_breakers') AS breaker_table
"""


def run_migration():
    print("[MIGRATION P0] Connecting...")
    with engine.connect() as conn:
        for i, stmt in enumerate(STATEMENTS, 1):
            try:
                conn.execute(text(stmt))
                print(f"  [{i}/{len(STATEMENTS)}] OK")
            except Exception as e:
                print(f"  [{i}/{len(STATEMENTS)}] WARN: {e}")
        conn.commit()
        row = conn.execute(text(VERIFY)).fetchone()
        print(f"[MIGRATION P0] write_intents new cols present: {row[0]}/4 ; barcode_circuit_breakers table: {row[1]}/1")
    print("[MIGRATION P0] Done.")


if __name__ == "__main__":
    run_migration()
