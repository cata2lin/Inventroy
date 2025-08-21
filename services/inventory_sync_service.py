# services/inventory_sync_service.py

import uuid
from datetime import datetime, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, text

import models
from shopify_service import ShopifyService
from product_service import ProductService

def _acquire_lock(db: Session, group_id: str) -> bool:
    """
    Tries to acquire a session-level advisory lock for a given group_id.
    Uses a hashed integer representation of the string for the lock key.
    Returns True if the lock was acquired, False otherwise.
    """
    try:
        # Postgres advisory locks work with integers. We'll use a hashed value.
        lock_id = func.hashtext(group_id)
        result = db.execute(text(f"SELECT pg_try_advisory_xact_lock({lock_id})")).scalar()
        return result
    except Exception as e:
        print(f"Error acquiring advisory lock for group {group_id}: {e}")
        return False

def process_inventory_update_event(db: Session, shop_domain: str, event_id: str, inventory_item_id: int, location_id: int):
    """
    The "Golden Sync Loop" for processing an inventory level update, using a delta-based approach.
    """
    # 1. Idempotency Check
    if db.query(models.DeliveredEvent).filter_by(shop_domain=shop_domain, event_id=event_id).first():
        print(f"Event {event_id} already processed. Skipping.")
        return
    db.add(models.DeliveredEvent(shop_domain=shop_domain, event_id=event_id))
    db.commit()

    print(f"--- Golden Loop triggered for item {inventory_item_id} from {shop_domain} ---")

    # 2. Resolve Variant & Group
    variant = db.query(models.ProductVariant).options(
        joinedload(models.ProductVariant.product).joinedload(models.Product.store),
        joinedload(models.ProductVariant.group_membership).joinedload(models.GroupMembership.group)
    ).filter(models.ProductVariant.inventory_item_id == inventory_item_id).first()

    if not variant or not variant.group_membership or not variant.group_membership.group:
        print(f"Variant for item {inventory_item_id} not found or has no barcode group. Exiting sync.")
        return

    group = variant.group_membership.group
    origin_store = variant.product.store

    if not origin_store.enabled:
        print(f"Store '{origin_store.name}' is disabled. Exiting sync.")
        return

    if not origin_store.sync_location_id or location_id != origin_store.sync_location_id:
        print(f"Inventory update is not for the designated sync location. Ignoring.")
        return
    
    # 3. Read Truth for the triggering variant from Shopify
    shopify_service = ShopifyService(store_url=origin_store.shopify_url, token=origin_store.api_token)
    inventory_data = shopify_service.get_inventory_levels_for_items([inventory_item_id])
    
    if not inventory_data:
        print(f"Could not fetch live inventory for item {inventory_item_id}. Aborting.")
        return
        
    current_level = next((item for item in inventory_data if item['id'] == variant.inventory_item_id and item['location_id'] == origin_store.sync_location_id), None)
    
    if not current_level:
        print(f"Live inventory data for item {inventory_item_id} at sync location not found. Aborting.")
        return
        
    current_available = current_level['available']
    
    # 4. Echo Suppression
    recent_push = db.query(models.PushLog).filter(
        models.PushLog.variant_id == variant.id,
        models.PushLog.written_at >= datetime.utcnow() - timedelta(seconds=60)
    ).order_by(models.PushLog.written_at.desc()).first()

    if recent_push and recent_push.target_available == current_available:
        print(f"Echo detected. We recently pushed {current_available} to this variant. Ignoring.")
        return

    # 5. Per-group Lock & Delta Calculation
    if not _acquire_lock(db, group.id):
        print(f"Could not acquire lock for group {group.id}. Another process is running. Event will be retried or handled by reconciliation.")
        return

    # --- CRITICAL SECTION: LOGIC INSIDE THE LOCK ---
    try:
        # Re-read last snapshot *inside* the lock
        last_snapshot = db.query(models.InventoryLevel).filter_by(
            inventory_item_id=variant.inventory_item_id,
            location_id=origin_store.sync_location_id
        ).first()
        
        last_known_available = last_snapshot.available if last_snapshot else 0
        delta = current_available - last_known_available
        
        if delta == 0:
            print("Delta is 0. No change in inventory. Releasing lock.")
            return

        # Update the snapshot for this variant to the new truth
        if last_snapshot:
            last_snapshot.available = current_available
            last_snapshot.on_hand = current_level['on_hand']
            last_snapshot.last_fetched_at = datetime.utcnow()
        else:
            db.add(models.InventoryLevel(
                inventory_item_id=variant.inventory_item_id,
                location_id=origin_store.sync_location_id,
                available=current_available,
                on_hand=current_level['on_hand']
            ))
        
        # Update the group's total pool
        group.pool_available += delta
        db.commit()

        # 6. Propagate to all members
        members = db.query(models.ProductVariant).options(
            joinedload(models.ProductVariant.product).joinedload(models.Product.store),
            joinedload(models.ProductVariant.inventory_levels)
        ).join(models.GroupMembership).filter(models.GroupMembership.group_id == group.id).all()

        correlation_id = uuid.uuid4()

        for member in members:
            member_store = member.product.store
            if not member_store.enabled or not member_store.sync_location_id:
                continue

            member_snapshot = next((lvl for lvl in member.inventory_levels if lvl.location_id == member_store.sync_location_id), None)
            
            if not member_snapshot:
                print(f"Warning: No local snapshot for member {member.id}. Cannot calculate target.")
                continue

            target_available = max(0, group.pool_available - member_store.safety_buffer)
            target_available = min(target_available, member_snapshot.on_hand)
            
            if member_snapshot.available != target_available:
                delta = target_available - member_snapshot.available
                
                product_service = ProductService(store_url=member_store.shopify_url, token=member_store.api_token)
                inventory_item_gid = f"gid://shopify/InventoryItem/{member.inventory_item_id}"
                location_gid = f"gid://shopify/Location/{member_store.sync_location_id}"
                
                try:
                    product_service.adjust_inventory_quantity(inventory_item_gid, location_gid, delta)
                    db.add(models.PushLog(
                        variant_id=member.id,
                        target_available=target_available,
                        correlation_id=correlation_id,
                        write_source='sync'
                    ))
                    member_snapshot.available = target_available # Update local state post-write
                except Exception as e:
                    print(f"FAILED to write to Shopify for variant {member.id}: {e}")
        
        db.commit()

    finally:
        # The advisory lock is released automatically when the transaction commits/rolls back.
        print(f"Golden Loop finished for group '{group.id}'. Lock released.")