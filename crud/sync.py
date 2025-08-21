# crud/sync.py

import re
import uuid
from datetime import datetime, timedelta
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import func
from typing import Optional

import models
from shopify_service import ShopifyService, gid_to_id
from product_service import ProductService

def normalize_barcode(barcode: Optional[str]) -> Optional[str]:
    """
    Cleans and normalizes a barcode.
    - Trims whitespace
    - Converts to uppercase
    - Removes spaces and hyphens
    """
    if not barcode:
        return None
    return re.sub(r'[\s-]', '', barcode).upper()

def update_variant_group_membership(db: Session, variant: models.ProductVariant):
    """
    Ensures a variant is in the correct BarcodeGroup based on its normalized barcode.
    This should be called whenever a variant's barcode changes.
    """
    # Remove from old group if it exists
    if variant.group_membership:
        db.delete(variant.group_membership)
        db.flush()

    normalized_code = normalize_barcode(variant.barcode)
    variant.barcode_normalized = normalized_code

    if not normalized_code:
        # If there's no barcode, ensure it's not in any group
        print(f"Variant {variant.id} has no barcode, removing from any groups.")
        return

    # Find or create the new group
    group = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.id == normalized_code).first()
    if not group:
        print(f"Creating new barcode group for '{normalized_code}'")
        group = models.BarcodeGroup(id=normalized_code)
        db.add(group)
    
    # Add to the new group
    new_membership = models.GroupMembership(variant_id=variant.id, group_id=group.id)
    db.add(new_membership)
    print(f"Variant {variant.id} added to group '{normalized_code}'")


def process_inventory_update_event(db: Session, store_id: int, inventory_item_id: int, location_id: int):
    """
    The "Golden Loop" for processing an inventory level update.
    This is triggered by the inventory_levels/update webhook.
    """
    print(f"--- Golden Loop triggered for item {inventory_item_id} at store {store_id} ---")

    # 1. Resolve Variant & Group
    variant = db.query(models.ProductVariant).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.group_membership)
    ).filter(models.ProductVariant.inventory_item_id == inventory_item_id).first()

    if not variant or not variant.group_membership:
        print(f"Variant for item {inventory_item_id} not found or has no barcode group. Exiting sync.")
        # TODO: Still update local snapshot for non-grouped items as per docs
        return

    group_id = variant.group_membership.group_id
    origin_store = variant.product.store

    if not origin_store.sync_location_id:
        print(f"Origin store '{origin_store.name}' has no sync_location_id configured. Exiting sync.")
        return

    # For now, we only sync if the update is from the designated sync location.
    if location_id != origin_store.sync_location_id:
        print(f"Inventory update for item {inventory_item_id} was at location {location_id}, but store's sync location is {origin_store.sync_location_id}. Ignoring.")
        return

    # 2. Read Truth for the triggering variant from Shopify
    print(f"Step 2: Reading truth for triggering variant {variant.id} from Shopify...")
    shopify_service = ShopifyService(store_url=origin_store.shopify_url, token=origin_store.api_token)
    
    # NOTE: The inventory_levels/update webhook payload contains the new `available` quantity.
    # The plan says to always re-read, which is safer. We will use a dedicated API call for this.
    inventory_data = shopify_service.get_inventory_levels_for_items([inventory_item_id])
    
    if not inventory_data:
        print(f"Could not fetch live inventory for item {inventory_item_id}. Aborting.")
        return
        
    current_level = next((item for item in inventory_data if item['id'] == variant.inventory_item_id and item['location_id'] == origin_store.sync_location_id), None)
    
    if not current_level:
        print(f"Live inventory data for item {inventory_item_id} at sync location {origin_store.sync_location_id} not found. Aborting.")
        return
        
    current_available = current_level['available']
    print(f"Truth from Shopify for variant {variant.id}: {current_available} available.")

    # 3. Echo Suppression
    print("Step 3: Checking for echo...")
    recent_push = db.query(models.PushLog).filter(
        models.PushLog.variant_id == variant.id,
        models.PushLog.written_at >= datetime.utcnow() - timedelta(seconds=60)
    ).order_by(models.PushLog.written_at.desc()).first()

    if recent_push and recent_push.target_available == current_available:
        print(f"Echo detected. We recently pushed {current_available} to this variant. Ignoring webhook.")
        return

    # 4. Per-group Lock (Simulated with a database timestamp)
    print("Step 4: Acquiring group lock (simulated)...")
    group = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.id == group_id).first()
    if group.last_synced_at and group.last_synced_at > datetime.utcnow() - timedelta(seconds=5):
        print(f"Group {group_id} was synced very recently. Debouncing this event.")
        return
    group.last_synced_at = datetime.utcnow()
    db.commit()

    # 5. Compute Pool Available
    print(f"Step 5: Computing pool_available for group '{group_id}'...")
    members = db.query(models.ProductVariant).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.inventory_levels)
    ).join(models.GroupMembership).filter(models.GroupMembership.group_id == group_id).all()

    # Get fresh inventory for all members of the group
    all_member_item_ids = [m.inventory_item_id for m in members]
    live_inventory_for_group = shopify_service.get_inventory_levels_for_items(all_member_item_ids)

    pool_available = 0
    for member in members:
        member_store = member.product.store
        if not member_store.sync_location_id:
            print(f"Warning: Member variant {member.id} in store '{member_store.name}' has no sync location configured. Skipping.")
            continue
        
        member_level = next((item for item in live_inventory_for_group if item['id'] == member.inventory_item_id and item['location_id'] == member_store.sync_location_id), None)
        
        if member_level:
            pool_available += member_level['available']
            # Also update our local snapshot
            local_level = next((level for level in member.inventory_levels if level.location_id == member_store.sync_location_id), None)
            if local_level:
                local_level.available = member_level['available']
                local_level.on_hand = member_level['on_hand']
                local_level.last_fetched_at = datetime.utcnow()
        else:
            print(f"Warning: Could not get live inventory for member {member.id}. Using stale local value.")
            local_level = next((level for level in member.inventory_levels if level.location_id == member_store.sync_location_id), None)
            if local_level and local_level.available:
                pool_available += local_level.available
    
    db.commit()
    print(f"Total pool_available for group '{group_id}' is: {pool_available}")

    # 6 & 7. Decide Targets and Write Only If Changed
    print("Step 6 & 7: Deciding targets and writing to Shopify if changed...")
    correlation_id = str(uuid.uuid4())

    for member in members:
        member_store = member.product.store
        if not member_store.sync_location_id:
            continue # Already warned above

        member_level = next((item for item in live_inventory_for_group if item['id'] == member.inventory_item_id and item['location_id'] == member_store.sync_location_id), None)
        
        if not member_level:
            print(f"Cannot process write for member {member.id} as live data is missing.")
            continue

        current_member_available = member_level['available']
        # Mirror mode: the target for everyone is the total pool
        target_available = pool_available

        # Safety clamp: never push a value greater than on_hand
        on_hand = member_level['on_hand']
        target_available = min(target_available, on_hand)
        
        # Safety clamp: never push a negative value unless store allows it (future feature)
        target_available = max(0, target_available)

        if current_member_available != target_available:
            print(f"  - CHANGE for variant {member.id} (Store: {member_store.name}): {current_member_available} -> {target_available}")
            
            # This is the WRITE operation
            delta = target_available - current_member_available
            
            product_service = ProductService(store_url=member_store.shopify_url, token=member_store.api_token)
            inventory_item_gid = f"gid://shopify/InventoryItem/{member.inventory_item_id}"
            location_gid = f"gid://shopify/Location/{member_store.sync_location_id}"
            
            try:
                product_service.adjust_inventory_quantity(inventory_item_gid, location_gid, delta)
                
                # Record in push log ON SUCCESS
                push = models.PushLog(
                    variant_id=member.id,
                    target_available=target_available,
                    correlation_id=correlation_id
                )
                db.add(push)
                print(f"    - Successfully wrote delta of {delta} to Shopify for variant {member.id}.")
            except Exception as e:
                print(f"    - FAILED to write to Shopify for variant {member.id}: {e}")

        else:
            print(f"  - NO CHANGE for variant {member.id} (Store: {member_store.name}): available is already {target_available}")

    db.commit()
    print(f"--- Golden Loop finished for group '{group_id}' ---")