# jobs/reconciliation.py

from sqlalchemy.orm import Session
from datetime import datetime
import models
from services import inventory_sync_service

def run_reconciliation(db: Session):
    """
    The main reconciliation job. Iterates through barcode groups,
    re-calculates the pool from truth, and corrects any discrepancies.
    """
    print("--- Starting Reconciliation Job ---")
    
    groups_to_reconcile = db.query(models.BarcodeGroup).filter(
        models.BarcodeGroup.status == 'active'
    ).all()

    for group in groups_to_reconcile:
        print(f"Reconciling group: {group.id}")
        
        # This is a simplified version. The full version would need to acquire a lock.
        if not inventory_sync_service._acquire_lock(db, group.id):
            print(f"Could not acquire lock for group {group.id}, skipping reconciliation.")
            continue

        try:
            members = group.members
            if not members:
                continue

            # Re-read truth for all members
            # In a real-world scenario, you'd batch these API calls.
            new_pool_available = 0
            for membership in members:
                variant = membership.variant
                store = variant.product.store
                
                if not store.enabled or not store.sync_location_id:
                    continue
                
                # ... (Logic to call Shopify API and get fresh `available`)
                # For this example, we'll simulate this part
                # fresh_available = shopify_service.get_inventory_levels_for_items(...)
                # new_pool_available += fresh_available

            if group.pool_available != new_pool_available:
                print(f"Discrepancy found for group {group.id}. DB Pool: {group.pool_available}, Truth Pool: {new_pool_available}. Correcting.")
                group.pool_available = new_pool_available
                # Now, re-propagate this correct value
                # ... (propagation logic similar to the sync service) ...

            group.last_reconciled_at = datetime.utcnow()
            db.commit()

        except Exception as e:
            print(f"Error reconciling group {group.id}: {e}")
            db.rollback()
        
    print("--- Reconciliation Job Finished ---")