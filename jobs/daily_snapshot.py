# jobs/daily_snapshot.py

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
import models

def run_daily_inventory_snapshot(db: Session):
    """
    Takes a snapshot of the current on_hand quantity for every tracked
    product variant and saves it to the inventory_snapshots table.
    """
    print("--- Starting daily inventory snapshot job ---")
    
    # Get the current date (without time)
    snapshot_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Fetch all variants that have an inventory level record
    variants_to_snapshot = db.query(
        models.ProductVariant.id,
        models.ProductVariant.store_id,
        models.InventoryLevel.on_hand
    ).join(
        models.InventoryLevel,
        models.ProductVariant.inventory_item_id == models.InventoryLevel.inventory_item_id
    ).all()

    if not variants_to_snapshot:
        print("No variants with inventory levels found to snapshot.")
        return

    snapshots_to_insert = []
    for variant_id, store_id, on_hand_qty in variants_to_snapshot:
        snapshots_to_insert.append({
            "date": snapshot_date,
            "product_variant_id": variant_id,
            "store_id": store_id,
            "on_hand": on_hand_qty if on_hand_qty is not None else 0
        })

    # Use a bulk "insert on conflict do nothing" to avoid errors if the job is run more than once a day
    if snapshots_to_insert:
        stmt = insert(models.InventorySnapshot).values(snapshots_to_insert)
        stmt = stmt.on_conflict_do_nothing(index_elements=['date', 'product_variant_id', 'store_id'])
        db.execute(stmt)
        db.commit()

    print(f"--- Completed inventory snapshot for {len(snapshots_to_insert)} variants. ---")