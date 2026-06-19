"""
Migration script: add authoritative_qty to write_intents (SYNC_ECHO_AUTHORITATIVE echo anchoring).
Additive + nullable + idempotent — safe to run multiple times, harmless when the flag is off.
Run once against the live database (e.g. as part of update.sh, BEFORE restarting the service).
"""
from database import engine
from sqlalchemy import text

STATEMENTS = [
    "ALTER TABLE write_intents ADD COLUMN IF NOT EXISTS authoritative_qty INTEGER",
]

VERIFY_QUERY = """
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'write_intents' AND column_name = 'authoritative_qty'
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
        print(f"[MIGRATION] write_intents.authoritative_qty present: {bool(result)}")
    print("[MIGRATION] Done.")


if __name__ == "__main__":
    run_migration()
