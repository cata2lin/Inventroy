# services/inventory_sync_service.py
import hmac
import hashlib
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import threading

from sqlalchemy.orm import Session
from sqlalchemy import func

import models
from database import SessionLocal
from shopify_service import ShopifyService

# --- Configuration ---
INTENT_TTL_SECONDS = 60  # How long to wait for an echo before considering it a real change
DUPLICATE_TTL_SECONDS = 120 # How long to remember a webhook to prevent duplicates
LOCK_TIMEOUT_SECONDS = 30 # Max time a barcode can be locked for processing

# --- In-memory lock for per-barcode serialization ---
barcode_locks: Dict[str, threading.Lock] = {}
def get_barcode_lock(barcode: str) -> threading.Lock:
    """Gets or creates a lock for a specific barcode."""
    if barcode not in barcode_locks:
        barcode_locks[barcode] = threading.Lock()
    return barcode_locks[barcode]


# --- Main Service Logic ---

def handle_webhook(store_id: int, payload: Dict[str, Any], triggered_at_str: str):
    """
    Main entry point for processing an inventory_levels/update webhook.
    Orchestrates the entire sync logic.
    """
    db: Session = SessionLocal()
    
    # 1. Parse and Validate Payload
    inventory_item_id = payload.get("inventory_item_id")
    source_timestamp = datetime.fromisoformat(triggered_at_str) if triggered_at_str else datetime.now(timezone.utc)
    
    variant = db.query(models.ProductVariant).filter(
        models.ProductVariant.inventory_item_id == inventory_item_id
    ).first()

    if not variant or not variant.barcode:
        print(f"[SYNC] Ignored: No variant or barcode found for inventory_item_id {inventory_item_id}")
        db.close()
        return

    barcode = variant.barcode
    
    # --- Per-barcode processing guard ---
    lock = get_barcode_lock(barcode)
    if not lock.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        print(f"[SYNC-ERROR] Could not acquire lock for barcode {barcode}. Task timed out.")
        db.close()
        return

    try:
        # 2. Aggregate to store total for the barcode
        current_store_total = _get_store_total_for_barcode(db, store_id, barcode)

        # 3. Duplicate Drop
        if _is_duplicate_webhook(db, store_id, barcode, current_store_total, source_timestamp):
            print(f"[SYNC] Ignored: Duplicate webhook for {barcode} at store {store_id}.")
            return

        # 4. Echo Suppression Check
        if _is_echo(db, store_id, barcode, current_store_total):
            print(f"[SYNC] Suppressed echo for {barcode} at store {store_id}.")
            return
            
        # 5. Choose Authority (Last-Write-Wins)
        is_authoritative = _is_new_authoritative_version(db, barcode, source_timestamp)
        if not is_authoritative:
            print(f"[SYNC] Ignored: Stale event for {barcode} from store {store_id}.")
            return
            
        # 6. Update Local State & Set Authoritative Version
        _update_authoritative_version(db, barcode, store_id, current_store_total, source_timestamp)
        
        # 7. Propagation Plan
        target_stores = _get_propagation_targets(db, store_id, barcode)
        if not target_stores:
            print(f"[SYNC] No propagation needed for {barcode}.")
            return
            
        # 8. Enqueue Self-Write Intents
        barcode_version = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).one()
        _create_write_intents(db, barcode, current_store_total, barcode_version.version, target_stores)
        
        print(f"[SYNC] Propagating {barcode} from store {store_id} to {len(target_stores)} other stores.")
        
        # 9. Perform Writes (can be moved to a separate background worker for larger scale)
        _execute_propagation(db, barcode, current_store_total, target_stores)

    finally:
        lock.release()
        db.close()


# --- Helper Functions ---

def _get_store_total_for_barcode(db: Session, store_id: int, barcode: str) -> int:
    """Calculates the total 'available' quantity for a barcode within a single store."""
    total = (
        db.query(func.sum(models.InventoryLevel.available))
        .join(models.ProductVariant)
        .filter(
            models.ProductVariant.store_id == store_id,
            models.ProductVariant.barcode == barcode
        )
        .scalar()
    )
    return total or 0

def _is_duplicate_webhook(db: Session, store_id: int, barcode: str, total: int, timestamp: datetime) -> bool:
    """Checks if this exact event has been processed recently."""
    # Clean up expired records first
    db.query(models.ProcessedWebhook).filter(models.ProcessedWebhook.expires_at < datetime.now(timezone.utc)).delete()
    
    # Create a unique ID for this event
    event_id = hashlib.sha256(f"{store_id}-{barcode}-{total}-{timestamp.isoformat()}".encode()).hexdigest()
    
    if db.query(models.ProcessedWebhook).filter(models.ProcessedWebhook.id == event_id).first():
        return True
    
    # Record this event to prevent future duplicates
    new_record = models.ProcessedWebhook(
        id=event_id,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=DUPLICATE_TTL_SECONDS)
    )
    db.add(new_record)
    db.commit()
    return False

def _is_echo(db: Session, store_id: int, barcode: str, observed_total: int) -> bool:
    """Checks if the webhook matches a recent self-write intent."""
    intent = (
        db.query(models.WriteIntent)
        .filter(
            models.WriteIntent.target_store_id == store_id,
            models.WriteIntent.barcode == barcode,
            models.WriteIntent.quantity == observed_total,
            models.WriteIntent.expires_at > datetime.now(timezone.utc)
        )
        .first()
    )
    if intent:
        # The webhook is a direct result of our own write. Acknowledge and delete the intent.
        db.delete(intent)
        db.commit()
        return True
    return False

def _is_new_authoritative_version(db: Session, barcode: str, timestamp: datetime) -> bool:
    """Determines if this event is newer than the current authoritative version."""
    current_version = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).first()
    if not current_version or timestamp > current_version.source_timestamp:
        return True
    return False

def _update_authoritative_version(db: Session, barcode: str, store_id: int, quantity: int, timestamp: datetime):
    """Updates or creates the authoritative record for a barcode."""
    current_version = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).first()
    if current_version:
        current_version.authoritative_store_id = store_id
        current_version.quantity = quantity
        current_version.source_timestamp = timestamp
        current_version.version += 1
    else:
        new_version = models.BarcodeVersion(
            barcode=barcode,
            authoritative_store_id=store_id,
            quantity=quantity,
            source_timestamp=timestamp,
            version=1
        )
        db.add(new_version)
    db.commit()

def _get_propagation_targets(db: Session, source_store_id: int, barcode: str) -> List[models.Store]:
    """Finds all other stores that sell this barcode."""
    member_store_ids = (
        db.query(models.ProductVariant.store_id)
        .filter(models.ProductVariant.barcode == barcode)
        .distinct()
        .all()
    )
    target_ids = [sid[0] for sid in member_store_ids if sid[0] != source_store_id]
    
    if not target_ids:
        return []
        
    return db.query(models.Store).filter(models.Store.id.in_(target_ids), models.Store.enabled == True).all()

def _create_write_intents(db: Session, barcode: str, quantity: int, version: int, target_stores: List[models.Store]):
    """Creates records of our intent to write, for echo suppression."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)
    
    for store in target_stores:
        intent = models.WriteIntent(
            barcode=barcode,
            target_store_id=store.id,
            quantity=quantity,
            barcode_version=version,
            expires_at=expires
        )
        db.add(intent)
    db.commit()

def _execute_propagation(db: Session, barcode: str, desired_total: int, target_stores: List[models.Store]):
    """Performs the Shopify API calls to update stock in other stores."""
    for store in target_stores:
        if not store.sync_location_id:
            print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue
            
        # Get all variants in the target store with the same barcode
        variants_to_update = db.query(models.ProductVariant).filter(
            models.ProductVariant.store_id == store.id,
            models.ProductVariant.barcode == barcode
        ).all()

        if not variants_to_update:
            continue
            
        # Use primary location for the write, set others to 0.
        primary_location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        
        quantities_payload = [
            {"inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}", "locationId": primary_location_gid, "quantity": desired_total}
            for v in variants_to_update if v.inventory_item_id
        ]
        
        # Note: This simple policy does not handle setting other locations to zero.
        # A more complex implementation would fetch all locations and set others to 0.

        if not quantities_payload:
            continue
            
        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            variables = {
                "input": {
                    "name": "available",
                    "reason": "correction",
                    "quantities": quantities_payload,
                }
            }
            service.execute_mutation("inventorySetQuantities", variables)
            print(f"[SYNC] Successfully wrote quantity {desired_total} for barcode {barcode} to store '{store.name}'.")
        except Exception as e:
            print(f"[SYNC-ERROR] Failed to write to store '{store.name}': {e}")
            
# --- NEW FUNCTION FOR CATALOG WEBHOOKS ---

def handle_catalog_webhook(store_id: int, topic: str, payload: Dict[str, Any]):
    """
    Handles webhooks related to product catalog changes (create, update, delete).
    """
    db: Session = SessionLocal()
    try:
        if topic == "products/create" or topic == "products/update":
            # The payload is the full product object. We can reuse our existing robust upsert logic.
            # We pass a dummy run_id of 0 as this is not part of a manual sync run.
            crud.product.create_or_update_product_from_webhook(db, store_id, payload)
        
        elif topic == "products/delete":
            crud.product.delete_product_from_webhook(db, payload)
            
        elif topic == "inventory_items/update":
            # This is primarily for barcode mapping changes.
            crud.product.update_variant_from_webhook(db, payload)
            
        elif topic == "inventory_items/delete":
            # This can act as a fallback for variant deletions.
            crud.product.delete_inventory_item_from_webhook(db, payload)
            
    except Exception as e:
        print(f"[SYNC-ERROR] Failed to process catalog webhook '{topic}': {e}")
    finally:
        db.close()