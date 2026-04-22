"""
Migration script: Create audit_logs and system_events tables.
Run once against the live database.
"""
from database import engine
from sqlalchemy import text

STATEMENTS = [
    # --- audit_logs table ---
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id BIGSERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        category VARCHAR(50) NOT NULL,
        action VARCHAR(100) NOT NULL,
        severity VARCHAR(20) NOT NULL DEFAULT 'INFO',
        actor VARCHAR(255),
        store_id INTEGER,
        store_name VARCHAR(255),
        target VARCHAR(255),
        message TEXT NOT NULL,
        details JSONB,
        duration_ms INTEGER,
        error_message TEXT,
        stack_trace TEXT
    )
    """,
    # --- system_events table ---
    """
    CREATE TABLE IF NOT EXISTS system_events (
        id BIGSERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        level VARCHAR(20) NOT NULL,
        source VARCHAR(255) NOT NULL,
        message TEXT NOT NULL,
        details JSONB,
        stack_trace TEXT,
        resolved BOOLEAN NOT NULL DEFAULT FALSE,
        resolved_at TIMESTAMPTZ,
        resolved_by VARCHAR(255)
    )
    """,
    # --- audit_logs indexes ---
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_timestamp ON audit_logs (timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_category ON audit_logs (category)",
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_action ON audit_logs (action)",
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_store_id ON audit_logs (store_id)",
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_category_timestamp ON audit_logs (category, timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_audit_logs_severity_timestamp ON audit_logs (severity, timestamp)",
    # --- system_events indexes ---
    "CREATE INDEX IF NOT EXISTS ix_system_events_timestamp ON system_events (timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_system_events_level ON system_events (level)",
    "CREATE INDEX IF NOT EXISTS ix_system_events_source ON system_events (source)",
    "CREATE INDEX IF NOT EXISTS ix_system_events_level_timestamp ON system_events (level, timestamp)",
    # --- Previous session: deleted_at on products ---
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_products_deleted_at ON products (deleted_at)",
]

VERIFY_QUERY = """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name IN ('audit_logs', 'system_events')
    ORDER BY table_name
"""

COL_COUNT_QUERY = """
    SELECT COUNT(*) FROM information_schema.columns WHERE table_name = :tbl
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
        print("[MIGRATION] All statements executed. Verifying...")

        result = conn.execute(text(VERIFY_QUERY))
        tables = [row[0] for row in result]
        print(f"[MIGRATION] Tables verified: {tables}")

        for t in tables:
            cols = conn.execute(text(COL_COUNT_QUERY), {"tbl": t}).scalar()
            print(f"  {t}: {cols} columns")

    print("[MIGRATION] Done.")


if __name__ == "__main__":
    run_migration()
