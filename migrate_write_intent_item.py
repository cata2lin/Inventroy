"""
Migration script: add inventory_item_id to write_intents (per-item echo guard).
Idempotent — safe to run multiple times. Run once against the live database.
"""
from database import engine
from sqlalchemy import text

STATEMENTS = [
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS inventory_item_id BIGINT",
    "CREATE INDEX IF NOT EXISTS ix_write_intents_inventory_item_id ON write_intents (inventory_item_id)",
    "CREATE INDEX IF NOT EXISTS ix_write_intents_item ON write_intents (target_store_id, inventory_item_id, expires_at)",
]

VERIFY_QUERY = """
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'write_intents' AND column_name = 'inventory_item_id'
"""


def run_migration():
    print("[MIGRATION] Connecting to database...")
    with engine.connect() as conn:
        for i, stmt in enumerate(STATEMENTS, 1):
            try:
                conn.execute(text(stmt))
                print(f"  [{i}/{len(STATEMENTS)}] OK")
            except Exception as e:
                print(f"  [{i}/{len(STATEMENTS)}] WARN: {e}")
        conn.commit()
        result = conn.execute(text(VERIFY_QUERY)).fetchall()
        print(f"[MIGRATION] write_intents.inventory_item_id present: {bool(result)}")
    print("[MIGRATION] Done.")


if __name__ == "__main__":
    run_migration()
