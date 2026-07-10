# migrate_trendyol_snapshot.py
"""Add the Trendyol-side snapshot columns to trendyol_mappings (idempotent)."""
from sqlalchemy import text
from database import engine

COLS = [
    "trendyol_title VARCHAR(400)",
    "trendyol_image VARCHAR(2048)",
    "trendyol_price NUMERIC(12,2)",
    "trendyol_list_price NUMERIC(12,2)",
    "trendyol_quantity INTEGER",
    "trendyol_approved BOOLEAN",
    "trendyol_archived BOOLEAN",
    "ty_synced_at TIMESTAMPTZ",
    "ty_accounted_qty INTEGER",
]

if __name__ == "__main__":
    with engine.begin() as conn:
        for c in COLS:
            conn.execute(text(f"ALTER TABLE trendyol_mappings ADD COLUMN IF NOT EXISTS {c}"))
            print("OK:", c)
    print("migration complete")
