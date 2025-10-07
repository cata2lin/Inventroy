# services/snapshot_runner.py
from sqlalchemy.orm import Session
from database import SessionLocal
from crud import store as crud_store, snapshots as crud_snapshots

def run_daily_snapshot():
    """
    The main function to be called by the scheduler.
    It iterates through all enabled stores and creates an inventory snapshot.
    """
    print("[SNAPSHOT-RUNNER] Starting daily inventory snapshot job.")
    db: Session = SessionLocal()
    try:
        enabled_stores = crud_store.get_enabled_stores(db)
        if not enabled_stores:
            print("[SNAPSHOT-RUNNER] No enabled stores found. Exiting.")
            return

        for store in enabled_stores:
            print(f"[SNAPSHOT-RUNNER] Processing store: {store.name} (ID: {store.id})")
            try:
                crud_snapshots.create_snapshot_for_store(db, store.id)
            except Exception as e:
                print(f"[SNAPSHOT-RUNNER] ERROR: Failed to create snapshot for store {store.name}. Reason: {e}")

    finally:
        db.close()
        print("[SNAPSHOT-RUNNER] Daily snapshot job finished.")