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
from crud import product as crud_product

# --- Configuration ---
INTENT_TTL_SECONDS = 60
DUPLICATE_TTL_SECONDS = 120
LOCK_TIMEOUT_SECONDS = 30

# --- In-memory lock for per-barcode serialization ---
barcode_locks: Dict[str, threading.Lock] = {}
def get_barcode_lock(barcode: str) -> threading.Lock:
    if barcode not in barcode_locks:
        barcode_locks[barcode] = threading.Lock()
    return barcode_locks[barcode]

# --- Main Service Logic ---

def handle_webhook(store_id: int, payload: Dict[str, Any], triggered_at_str: str):
    db: Session = SessionLocal()
    
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
    
    lock = get_barcode_lock(barcode)
    if not lock.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        print(f"[SYNC-ERROR] Could not acquire lock for barcode {barcode}. Task timed out.")
        db.close()
        return

    try:
        current_store_total = _get_store_total_for_barcode(db, store_id, barcode)

        if _is_duplicate_webhook(db, store_id, barcode, current_store_total, source_timestamp):
            print(f"[SYNC] Ignored: Duplicate webhook for {barcode} at store {store_id}.")
            return

        if _is_echo(db, store_id, barcode, current_store_total):
            print(f"[SYNC] Suppressed echo for {barcode} at store {store_id}.")
            return
            
        is_authoritative = _is_new_authoritative_version(db, barcode, source_timestamp)
        if not is_authoritative:
            print(f"[SYNC] Ignored: Stale event for {barcode} from store {store_id}.")
            return
            
        _update_authoritative_version(db, barcode, store_id, current_store_total, source_timestamp)
        
        target_stores = _get_propagation_targets(db, barcode)
        if not target_stores or len(target_stores) < 2:
            print(f"[SYNC] No propagation needed for {barcode}.")
            return
            
        barcode_version_obj = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).one()
        _create_write_intents(db, barcode, current_store_total, barcode_version_obj.version, target_stores)
        
        print(f"[SYNC] Propagating '{barcode}' to all {len(target_stores)} member stores.")
        
        _execute_propagation(db, barcode, current_store_total, target_stores)

    finally:
        lock.release()
        db.close()

def handle_catalog_webhook(store_id: int, topic: str, payload: Dict[str, Any]):
    db: Session = SessionLocal()
    try:
        if topic == "products/create" or topic == "products/update":
            crud_product.create_or_update_product_from_webhook(db, store_id, payload)
        elif topic == "products/delete":
            crud_product.delete_product_from_webhook(db, payload)
        elif topic == "inventory_items/update":
            crud_product.update_variant_from_webhook(db, payload)
        elif topic == "inventory_items/delete":
            crud_product.delete_inventory_item_from_webhook(db, payload)
    except Exception as e:
        print(f"[SYNC-ERROR] Failed to process catalog webhook '{topic}': {e}")
    finally:
        db.close()

# --- Helper Functions ---
def _get_store_total_for_barcode(db: Session, store_id: int, barcode: str) -> int:
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
    db.query(models.ProcessedWebhook).filter(models.ProcessedWebhook.expires_at < datetime.now(timezone.utc)).delete()
    event_id = hashlib.sha256(f"{store_id}-{barcode}-{total}-{timestamp.isoformat()}".encode()).hexdigest()
    if db.query(models.ProcessedWebhook).filter(models.ProcessedWebhook.id == event_id).first():
        return True
    new_record = models.ProcessedWebhook(id=event_id, expires_at=datetime.now(timezone.utc) + timedelta(seconds=DUPLICATE_TTL_SECONDS))
    db.add(new_record)
    db.commit()
    return False

def _is_echo(db: Session, store_id: int, barcode: str, observed_total: int) -> bool:
    intent = db.query(models.WriteIntent).filter(
        models.WriteIntent.target_store_id == store_id,
        models.WriteIntent.barcode == barcode,
        models.WriteIntent.quantity == observed_total,
        models.WriteIntent.expires_at > datetime.now(timezone.utc)
    ).first()
    if intent:
        db.delete(intent)
        db.commit()
        return True
    return False

def _is_new_authoritative_version(db: Session, barcode: str, timestamp: datetime) -> bool:
    current_version = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).first()
    if not current_version or timestamp > current_version.source_timestamp:
        return True
    return False

def _update_authoritative_version(db: Session, barcode: str, store_id: int, quantity: int, timestamp: datetime):
    current_version = db.query(models.BarcodeVersion).filter(models.BarcodeVersion.barcode == barcode).first()
    if current_version:
        current_version.authoritative_store_id = store_id
        current_version.quantity = quantity
        current_version.source_timestamp = timestamp
        current_version.version += 1
    else:
        new_version = models.BarcodeVersion(barcode=barcode, authoritative_store_id=store_id, quantity=quantity, source_timestamp=timestamp, version=1)
        db.add(new_version)
    db.commit()

def _get_propagation_targets(db: Session, barcode: str) -> List[models.Store]:
    member_store_ids = (
        db.query(models.ProductVariant.store_id)
        .filter(models.ProductVariant.barcode == barcode)
        .distinct()
        .all()
    )
    target_ids = [sid[0] for sid in member_store_ids]
    
    if not target_ids:
        return []
        
    return db.query(models.Store).filter(models.Store.id.in_(target_ids), models.Store.enabled == True).all()

def _create_write_intents(db: Session, barcode: str, quantity: int, version: int, target_stores: List[models.Store]):
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)
    
    for store in target_stores:
        intent = models.WriteIntent(barcode=barcode, target_store_id=store.id, quantity=quantity, barcode_version=version, expires_at=expires)
        db.add(intent)
    db.commit()

def _execute_propagation(db: Session, barcode: str, desired_total: int, target_stores: List[models.Store]):
    for store in target_stores:
        if not store.sync_location_id:
            print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue
            
        variants_to_update = db.query(models.ProductVariant).filter(
            models.ProductVariant.store_id == store.id,
            models.ProductVariant.barcode == barcode
        ).all()

        if not variants_to_update:
            continue
            
        primary_location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        
        quantities_payload = [
            {"inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}", "locationId": primary_location_gid, "quantity": desired_total}
            for v in variants_to_update if v.inventory_item_id
        ]
        
        if not quantities_payload:
            continue
            
        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            variables = {
                "input": {
                    "name": "available", "reason": "correction", "ignoreCompareQuantity": True,
                    "quantities": quantities_payload,
                }
            }
            service.execute_mutation("inventorySetQuantities", variables)
            print(f"[SYNC] Successfully wrote quantity {desired_total} for barcode {barcode} to store '{store.name}'.")

            # --- THIS IS THE CRITICAL FIX ---
            # After a successful write to Shopify, update our own database to reflect the change.
            variant_ids = [v.id for v in variants_to_update]
            crud_product.update_inventory_levels_for_variants(
                db, 
                variant_ids=variant_ids, 
                location_id=store.sync_location_id, 
                new_quantity=desired_total
            )
            print(f"[DB-UPDATE] Synced local DB for barcode {barcode} in store '{store.name}'.")

        except Exception as e:
            print(f"[SYNC-ERROR] Failed to write to store '{store.name}': {e}")