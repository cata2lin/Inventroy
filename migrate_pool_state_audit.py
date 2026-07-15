"""
Migration: attribution audit for pool_states (2026-07-15).

On Jul 14 an out-of-band DB client (direct SQL as the app role) hand-wrote pool_states.quantity
for ~161 barcodes, bypassing the ledger, the backfill audit and golden capture — identifying it
took a full forensic investigation because the row's updated_at was the ONLY trace. This trigger
appends every INSERT/UPDATE of pool_states to an append-only pool_state_audit table with the
connection identity (current_user, application_name, client_addr), so any future writer — app or
human — is attributable in one query.

Idempotent — safe to run multiple times. Run once against the live database BEFORE restarting.
Revert: DROP TRIGGER trg_pool_state_audit ON pool_states; DROP FUNCTION fn_pool_state_audit();
        (keep or drop the audit table as desired — it is append-only data.)
"""
from database import engine
from sqlalchemy import text

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pool_state_audit (
        id           BIGSERIAL PRIMARY KEY,
        barcode      VARCHAR(255) NOT NULL,
        old_quantity INTEGER,
        new_quantity INTEGER,
        old_version  BIGINT,
        new_version  BIGINT,
        db_user      TEXT,
        app_name     TEXT,
        client_addr  TEXT,
        changed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_pool_state_audit_barcode ON pool_state_audit (barcode, changed_at)",
    """
    CREATE OR REPLACE FUNCTION fn_pool_state_audit() RETURNS trigger AS $$
    BEGIN
        IF (TG_OP = 'INSERT') OR (NEW.quantity IS DISTINCT FROM OLD.quantity)
                               OR (NEW.version IS DISTINCT FROM OLD.version) THEN
            INSERT INTO pool_state_audit
                (barcode, old_quantity, new_quantity, old_version, new_version,
                 db_user, app_name, client_addr)
            VALUES
                (NEW.barcode,
                 CASE WHEN TG_OP = 'UPDATE' THEN OLD.quantity END, NEW.quantity,
                 CASE WHEN TG_OP = 'UPDATE' THEN OLD.version END, NEW.version,
                 current_user,
                 current_setting('application_name', true),
                 COALESCE(inet_client_addr()::text, 'local'));
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    "DROP TRIGGER IF EXISTS trg_pool_state_audit ON pool_states",
    """
    CREATE TRIGGER trg_pool_state_audit
    AFTER INSERT OR UPDATE ON pool_states
    FOR EACH ROW EXECUTE FUNCTION fn_pool_state_audit()
    """,
]

VERIFY_QUERY = """
    SELECT tgname FROM pg_trigger
    WHERE tgrelid = 'pool_states'::regclass AND tgname = 'trg_pool_state_audit'
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
        print(f"[MIGRATION] trg_pool_state_audit present: {bool(result)}")
    print("[MIGRATION] Done.")


if __name__ == "__main__":
    run_migration()
