# services/inventory_sync_service.py
"""
Core inventory synchronization engine.
Handles webhook-driven stock propagation by barcode across stores.

Key behaviors:
- Same barcode on different products within the SAME store: all are synced.
- Products with any Shopify status (ACTIVE/DRAFT/ARCHIVED) participate in sync.
- Only products with deleted_at set (soft-deleted by sync runner) are excluded.
- WriteIntents prevent echo cascades from Shopify webhooks.
"""
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
from services import audit_logger

# --- Configuration ---
INTENT_TTL_SECONDS = 60
DUPLICATE_TTL_SECONDS = 120
LOCK_TIMEOUT_SECONDS = 30

# Barcodes that are Shopify defaults or placeholders — never sync these
PLACEHOLDER_BARCODES = frozenset({'0', '00', '000', '0000', '00000', '000000', '0000000', '00000000', '000000000', '0000000000', '00000000000', '000000000000', '0000000000000'})

# --- BUG-01 FIX: Thread-safe per-barcode locking ---
_meta_lock = threading.Lock()
barcode_locks: Dict[str, threading.Lock] = {}

def get_barcode_lock(barcode: str) -> threading.Lock:
    """Get or create a per-barcode lock in a thread-safe manner."""
    with _meta_lock:
        if barcode not in barcode_locks:
            barcode_locks[barcode] = threading.Lock()
        return barcode_locks[barcode]

# --- BUG-11 FIX: Periodic cleanup of barcode_locks ---
def cleanup_barcode_locks():
    """Remove locks that are not currently held."""
    with _meta_lock:
        stale_keys = [k for k, v in barcode_locks.items() if not v.locked()]
        for k in stale_keys:
            barcode_locks.pop(k, None)
        if stale_keys:
            print(f"[CLEANUP] Removed {len(stale_keys)} unused barcode locks.")

# --- Main Service Logic ---

def handle_webhook(store_id: int, payload: Dict[str, Any], triggered_at_str: str):
    """
    Process an inventory_levels/update webhook.
    Core flow: deduplicate → suppress echoes → version check → propagate.

    Propagation includes:
    - ALL other stores with the same barcode
    - ALL other variants on the SAME store with the same barcode (multi-listing support)
    """
    db: Session = SessionLocal()

    inventory_item_id = payload.get("inventory_item_id")
    authoritative_quantity = payload.get("available")

    if authoritative_quantity is None:
        print(f"[SYNC-ERROR] Webhook is missing 'available' quantity for inventory_item_id {inventory_item_id}")
        audit_logger.log_error("inventory_sync_service.handle_webhook",
                               f"Missing 'available' quantity for inventory_item_id {inventory_item_id}")
        db.close()
        return

    try:
        source_timestamp = datetime.fromisoformat(triggered_at_str.strip()) if triggered_at_str and triggered_at_str.strip() else datetime.now(timezone.utc)
    except (ValueError, AttributeError):
        source_timestamp = datetime.now(timezone.utc)

    # Lightweight barcode lookup for lock acquisition
    barcode_row = db.query(
        models.ProductVariant.barcode
    ).filter(
        models.ProductVariant.inventory_item_id == inventory_item_id
    ).first()

    if not barcode_row or not barcode_row.barcode:
        print(f"[SYNC] Ignored: No variant or barcode found for inventory_item_id {inventory_item_id}")
        db.close()
        return

    # Sanity: skip placeholder/default barcodes that shouldn't trigger sync
    if barcode_row.barcode.strip() in PLACEHOLDER_BARCODES or not barcode_row.barcode.strip():
        print(f"[SYNC] Ignored: Placeholder/empty barcode '{barcode_row.barcode}' for inventory_item_id {inventory_item_id}")
        db.close()
        return

    barcode = barcode_row.barcode

    lock = get_barcode_lock(barcode)
    if not lock.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        print(f"[SYNC-ERROR] Could not acquire lock for barcode {barcode}. Task timed out.")
        db.close()
        return

    try:
        # Re-query inside the lock for fresh data
        variant = db.query(models.ProductVariant).filter(
            models.ProductVariant.inventory_item_id == inventory_item_id
        ).first()

        if not variant or not variant.barcode:
            print(f"[SYNC] Ignored (inside lock): variant or barcode disappeared for inventory_item_id {inventory_item_id}")
            return

        if variant.barcode != barcode:
            print(f"[SYNC-WARN] Barcode changed from {barcode} to {variant.barcode} between lock acquisition.")
            barcode = variant.barcode

        # Skip variants belonging to soft-deleted products (deleted_at IS NOT NULL)
        product = db.query(models.Product).filter(models.Product.id == variant.product_id).first()
        if product and product.deleted_at is not None:
            print(f"[SYNC] Ignored: Variant belongs to a soft-deleted product (barcode={barcode}, product_id={variant.product_id})")
            return

        # BUG-17 FIX: Dedup with rollback safety
        try:
            if _is_duplicate_webhook(db, store_id, barcode, authoritative_quantity, source_timestamp):
                print(f"[SYNC] Ignored: Duplicate webhook for {barcode} at store {store_id}.")
                return
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Dedup check failed, proceeding anyway: {e}")

        if _is_echo(db, store_id, barcode, authoritative_quantity):
            print(f"[SYNC] Suppressed echo for {barcode} at store {store_id}.")
            return

        is_authoritative = _is_new_authoritative_version(db, barcode, source_timestamp)
        if not is_authoritative:
            print(f"[SYNC] Ignored: Stale event for {barcode} from store {store_id}.")
            return

        # 1. Update the authoritative version
        _update_authoritative_version(db, barcode, store_id, authoritative_quantity, source_timestamp)

        # 2. Update the local database for the triggering variant
        source_location_id = payload.get("location_id")
        if source_location_id:
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=[variant.id], location_id=source_location_id,
                new_quantity=authoritative_quantity
            )

        # 3. Find ALL variants with this barcode to propagate to.
        #    This includes:
        #    - Other variants on the SAME store (multi-listing: same barcode on different products)
        #    - All variants on OTHER stores
        #    The triggering variant is excluded.
        # FIX: Use .first() instead of .one() to prevent NoResultFound crash
        # when a barcode is first seen via webhook before any full sync.
        barcode_version_obj = db.query(models.BarcodeVersion).filter(
            models.BarcodeVersion.barcode == barcode
        ).first()

        current_version = barcode_version_obj.version if barcode_version_obj else 0

        propagation_targets = _get_all_propagation_variants(
            db, barcode, exclude_variant_id=variant.id
        )

        if propagation_targets:
            # Group by store for batched API calls
            store_map: Dict[int, List[models.ProductVariant]] = {}
            for pv in propagation_targets:
                if pv.store_id not in store_map:
                    store_map[pv.store_id] = []
                store_map[pv.store_id].append(pv)

            # Create WriteIntents for all target stores (echo suppression)
            target_store_ids = list(store_map.keys())
            target_stores = db.query(models.Store).filter(
                models.Store.id.in_(target_store_ids),
                models.Store.enabled == True
            ).all()

            try:
                _create_write_intents(db, barcode, authoritative_quantity, current_version, target_stores)
            except Exception as e:
                db.rollback()
                print(f"[SYNC-WARN] Failed to create write intents for {barcode}: {e}")

            total_variants = sum(len(vs) for vs in store_map.values())
            print(f"[SYNC] Propagating '{barcode}' qty={authoritative_quantity} to {total_variants} variants across {len(store_map)} stores.")

            # Audit log the propagation event
            audit_logger.log(
                category="STOCK",
                action="stock_propagation_started",
                message=f"Propagating [{barcode}] qty={authoritative_quantity} to {total_variants} variants across {len(store_map)} stores",
                store_id=store_id,
                target=barcode,
                details={
                    "quantity": authoritative_quantity,
                    "target_stores": {str(k): len(v) for k, v in store_map.items()},
                    "total_variants": total_variants,
                    "inventory_item_id": inventory_item_id,
                },
            )

            try:
                _execute_propagation(db, barcode, authoritative_quantity, target_stores, store_map)
            except Exception as e:
                audit_logger.log_error("inventory_sync_service.handle_webhook",
                                       f"Propagation failed for barcode {barcode}",
                                       details={"barcode": barcode, "quantity": authoritative_quantity},
                                       exc=e)
        else:
            print(f"[SYNC] No other variants to propagate to for barcode {barcode}.")

    finally:
        lock.release()
        db.close()

def handle_catalog_webhook(store_id: int, topic: str, payload: Dict[str, Any]):
    db: Session = SessionLocal()
    try:
        if topic == "products/create":
            crud_product.create_or_update_product_from_webhook(db, store_id, payload)
        elif topic == "products/update":
            crud_product.patch_product_from_webhook(db, store_id, payload)
        elif topic == "products/delete":
            crud_product.delete_product_from_webhook(db, payload)
        elif topic == "inventory_items/update":
            crud_product.update_variant_from_webhook(db, payload)
        elif topic == "inventory_items/delete":
            crud_product.delete_inventory_item_from_webhook(db, payload)
    except Exception as e:
        print(f"[SYNC-ERROR] Failed to process catalog webhook '{topic}': {e}")
        audit_logger.log_error("inventory_sync_service.handle_catalog_webhook",
                               f"Failed to process catalog webhook '{topic}' for store {store_id}",
                               details={"topic": topic}, exc=e)
    finally:
        db.close()

# --- Helper Functions ---

def _is_duplicate_webhook(db: Session, store_id: int, barcode: str, total: int, timestamp: datetime) -> bool:
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
    """Update or create the authoritative version for a barcode. Includes commit safety."""
    try:
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
    except Exception as e:
        db.rollback()
        print(f"[SYNC-ERROR] Failed to update authoritative version for {barcode}: {e}")
        raise

def _get_all_propagation_variants(db: Session, barcode: str, exclude_variant_id: int) -> List[models.ProductVariant]:
    """
    Find ALL variants with the same barcode across ALL stores (including the source store),
    excluding the triggering variant and soft-deleted products.

    This handles the multi-listing scenario: same barcode on different products
    within the same store, or across different stores. All participate in sync
    regardless of Shopify product status (ACTIVE/DRAFT/ARCHIVED).
    """
    return (
        db.query(models.ProductVariant)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.ProductVariant.barcode == barcode,
            models.ProductVariant.id != exclude_variant_id,
            # Exclude soft-deleted products (Option B: deleted_at column)
            models.Product.deleted_at.is_(None),
            # Only include variants that can actually receive inventory updates
            models.ProductVariant.inventory_item_id.isnot(None),
        )
        .all()
    )

def _create_write_intents(db: Session, barcode: str, quantity: int, version: int, target_stores: List[models.Store]):
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)

    for store in target_stores:
        intent = models.WriteIntent(barcode=barcode, target_store_id=store.id, quantity=quantity, barcode_version=version, expires_at=expires)
        db.add(intent)
    db.commit()

def _execute_propagation(
    db: Session,
    barcode: str,
    desired_total: int,
    target_stores: List[models.Store],
    store_variant_map: Dict[int, List[models.ProductVariant]]
):
    """
    Execute propagation to target stores. Uses a pre-built store→variant map
    so the same store can appear as a target (for its OTHER variants with the same barcode).
    """
    store_lookup = {s.id: s for s in target_stores}

    for store_id, variants_to_update in store_variant_map.items():
        store = store_lookup.get(store_id)
        if not store or not store.sync_location_id:
            if store:
                print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue

        primary_location_gid = f"gid://shopify/Location/{store.sync_location_id}"

        quantities_payload = [
            {
                "inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                "locationId": primary_location_gid,
                "quantity": desired_total
            }
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
            result = service.execute_mutation("inventorySetQuantities", variables)
            if result.get("inventorySetQuantities", {}).get("userErrors"):
                 raise Exception(str(result["inventorySetQuantities"]["userErrors"]))

            print(f"[SYNC] Wrote qty {desired_total} for barcode {barcode} to store '{store.name}' ({len(quantities_payload)} variants).")

            audit_logger.log_propagation(
                barcode=barcode,
                source_store="webhook",
                target_store=store.name,
                quantity=desired_total,
                details={"variant_count": len(quantities_payload)},
            )

            # Update local DB
            variant_ids = [v.id for v in variants_to_update]
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=variant_ids, location_id=store.sync_location_id,
                new_quantity=desired_total
            )

        except Exception as e:
            print(f"[SYNC-ERROR] Failed to write to store '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._execute_propagation",
                                   f"Failed to write barcode {barcode} to store '{store.name}'",
                                   details={"barcode": barcode, "quantity": desired_total}, exc=e)


# --- Scheduled Cleanup ---
def cleanup_expired_records():
    """Clean up expired ProcessedWebhook, WriteIntent records, and unused barcode locks."""
    db: Session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)

        expired_webhooks = db.query(models.ProcessedWebhook).filter(
            models.ProcessedWebhook.expires_at < now
        ).delete(synchronize_session=False)

        expired_intents = db.query(models.WriteIntent).filter(
            models.WriteIntent.expires_at < now
        ).delete(synchronize_session=False)

        db.commit()

        if expired_webhooks > 0 or expired_intents > 0:
            print(f"[CLEANUP] Removed {expired_webhooks} expired webhooks and {expired_intents} expired intents.")

        cleanup_barcode_locks()

    except Exception as e:
        db.rollback()
        print(f"[CLEANUP-ERROR] Failed to clean up expired records: {e}")
    finally:
        db.close()