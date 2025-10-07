# crud/snapshots.py
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

import models

def create_snapshot_for_store(db: Session, store_id: int):
    """
    Creates a snapshot of all current inventory levels for a given store.
    """
    now = datetime.now(timezone.utc)
    # Get all inventory levels for the store that have a positive quantity
    inventory_levels = (
        db.query(models.InventoryLevel)
        .join(models.ProductVariant)
        .filter(
            models.ProductVariant.store_id == store_id,
            models.InventoryLevel.on_hand > 0
        )
        .all()
    )

    if not inventory_levels:
        print(f"[SNAPSHOT] No inventory levels to snapshot for store {store_id}.")
        return

    snapshot_entries = []
    for level in inventory_levels:
        snapshot_entries.append(
            models.InventorySnapshot(
                date=now.date(),
                product_variant_id=level.variant_id,
                store_id=store_id,
                on_hand=level.on_hand or 0,
            )
        )

    # Use bulk_insert_mappings for efficiency
    db.bulk_insert_mappings(models.InventorySnapshot, [s.__dict__ for s in snapshot_entries])
    db.commit()
    print(f"[SNAPSHOT] Successfully created snapshot for store {store_id} with {len(snapshot_entries)} entries.")


def get_snapshots(
    db: Session, skip: int = 0, limit: int = 100, store_id: Optional[int] = None, date: Optional[datetime.date] = None
) -> Tuple[List[models.InventorySnapshot], int]:
    """
    Retrieves paginated and filterable inventory snapshots.
    """
    query = db.query(models.InventorySnapshot).options(
        joinedload(models.InventorySnapshot.product_variant).joinedload(models.ProductVariant.product)
    )

    if store_id:
        query = query.filter(models.InventorySnapshot.store_id == store_id)
    if date:
        query = query.filter(models.InventorySnapshot.date == date)

    total_count = query.count()
    snapshots = query.order_by(models.InventorySnapshot.date.desc()).offset(skip).limit(limit).all()

    return snapshots, total_count