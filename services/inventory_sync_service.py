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
    
    DELTA-BASED PROPAGATION (replaces absolute-value propagation):
    1. Compute delta = new_available - last_known_available
    2. If delta == 0: this is an echo from our own write → skip
    3. If delta != 0: adjust all other stores by delta using inventoryAdjustQuantities
    4. Fallback: if last_known is unavailable, use absolute SET (legacy behavior)
    
    This correctly handles:
    - Concurrent orders on different stores (both deltas are applied)
    - Restocks (positive delta propagated to all stores)
    - Manual corrections (delta propagated to all stores)
    - xConnector fulfillments (delta propagated to all stores)
    """
    db: Session = SessionLocal()

    inventory_item_id = payload.get("inventory_item_id")
    new_available = payload.get("available")

    if new_available is None:
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
            if _is_duplicate_webhook(db, store_id, barcode, new_available, source_timestamp):
                print(f"[SYNC] Ignored: Duplicate webhook for {barcode} at store {store_id}.")
                return
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Dedup check failed, proceeding anyway: {e}")

        # --- DELTA COMPUTATION ---
        # Get the last known stock for this variant at this store's sync location.
        # This is what WE think the stock was before this webhook event.
        store = db.query(models.Store).filter(models.Store.id == store_id).first()
        last_known = None
        if store and store.sync_location_id:
            inv_level = db.query(models.InventoryLevel).filter(
                models.InventoryLevel.variant_id == variant.id,
                models.InventoryLevel.location_id == store.sync_location_id,
            ).first()
            if inv_level and inv_level.available is not None:
                last_known = inv_level.available

        if last_known is not None:
            delta = new_available - last_known
        else:
            delta = None  # First time — no baseline, use absolute fallback

        # --- ECHO DETECTION (delta-based) ---
        # If delta == 0, the stock didn't actually change from our perspective.
        # This happens when our own propagation write bounces back as a webhook.
        if delta is not None and delta == 0:
            print(f"[SYNC] Suppressed echo for {barcode} at store {store_id} (delta=0).")
            return

        # Also check legacy WriteIntent echoes (from reconciliation/absolute-SET operations)
        if _is_echo(db, store_id, barcode, new_available):
            print(f"[SYNC] Suppressed echo for {barcode} at store {store_id} (WriteIntent match).")
            return

        # --- VERSION CHECK ---
        is_authoritative = _is_new_authoritative_version(db, barcode, source_timestamp)
        if not is_authoritative:
            print(f"[SYNC] Ignored: Stale event for {barcode} from store {store_id}.")
            return

        # 1. Update the authoritative version
        _update_authoritative_version(db, barcode, store_id, new_available, source_timestamp)

        # 2. Update the local database for the triggering variant (absolute — this is the source of truth)
        source_location_id = payload.get("location_id")
        if source_location_id:
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=[variant.id], location_id=source_location_id,
                new_quantity=new_available
            )

        # 3. Find ALL variants with this barcode to propagate to.
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

            target_store_ids = list(store_map.keys())
            target_stores = db.query(models.Store).filter(
                models.Store.id.in_(target_store_ids),
                models.Store.enabled == True
            ).all()

            total_variants = sum(len(vs) for vs in store_map.values())
            mode = "delta" if delta is not None else "absolute"
            print(f"[SYNC] Propagating '{barcode}' {mode}={'+'+ str(delta) if delta and delta > 0 else delta} (new_qty={new_available}) to {total_variants} variants across {len(store_map)} stores.")

            # Audit log the propagation event
            audit_logger.log(
                category="STOCK",
                action="stock_propagation_started",
                message=f"Propagating [{barcode}] {mode}={delta} (qty={new_available}) to {total_variants} variants across {len(store_map)} stores",
                store_id=store_id,
                target=barcode,
                details={
                    "mode": mode,
                    "delta": delta,
                    "quantity": new_available,
                    "last_known": last_known,
                    "target_stores": {str(k): len(v) for k, v in store_map.items()},
                    "total_variants": total_variants,
                    "inventory_item_id": inventory_item_id,
                },
            )

            if delta is not None:
                # --- DELTA MODE: adjust all targets by delta ---
                try:
                    _execute_delta_propagation(db, barcode, delta, new_available, target_stores, store_map)
                except Exception as e:
                    audit_logger.log_error("inventory_sync_service.handle_webhook",
                                           f"Delta propagation failed for barcode {barcode}",
                                           details={"barcode": barcode, "delta": delta}, exc=e)
            else:
                # --- ABSOLUTE FALLBACK: first-time sync, no baseline available ---
                # Use legacy SET approach with WriteIntents for echo suppression
                barcode_version_obj = db.query(models.BarcodeVersion).filter(
                    models.BarcodeVersion.barcode == barcode
                ).first()
                current_version = barcode_version_obj.version if barcode_version_obj else 0

                try:
                    _create_write_intents(db, barcode, new_available, current_version, target_stores)
                except Exception as e:
                    db.rollback()
                    print(f"[SYNC-WARN] Failed to create write intents for {barcode}: {e}")

                try:
                    _execute_absolute_propagation(db, barcode, new_available, target_stores, store_map)
                except Exception as e:
                    audit_logger.log_error("inventory_sync_service.handle_webhook",
                                           f"Absolute propagation failed for barcode {barcode}",
                                           details={"barcode": barcode, "quantity": new_available}, exc=e)
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
            # Auto-sync: align new product's variants to existing barcode groups
            _auto_sync_product_barcodes(db, store_id, payload)

        elif topic == "products/update":
            crud_product.patch_product_from_webhook(db, store_id, payload)
            # Auto-sync: if any variant's barcode changed, align to group
            _auto_sync_product_barcodes(db, store_id, payload)

        elif topic == "products/delete":
            crud_product.delete_product_from_webhook(db, payload)

        elif topic == "inventory_items/update":
            # Capture the barcode BEFORE the update to detect changes
            inv_item_id = payload.get("id")
            old_barcode = None
            if inv_item_id:
                old_variant = db.query(models.ProductVariant).filter(
                    models.ProductVariant.inventory_item_id == inv_item_id
                ).first()
                old_barcode = old_variant.barcode if old_variant else None

            crud_product.update_variant_from_webhook(db, payload)

            # If the barcode changed, sync to the new group
            new_barcode = payload.get("barcode")
            if new_barcode and new_barcode != old_barcode and old_variant:
                _sync_variant_to_barcode_group(db, store_id, old_variant.id, new_barcode)

        elif topic == "inventory_items/delete":
            crud_product.delete_inventory_item_from_webhook(db, payload)

    except Exception as e:
        print(f"[SYNC-ERROR] Failed to process catalog webhook '{topic}': {e}")
        audit_logger.log_error("inventory_sync_service.handle_catalog_webhook",
                               f"Failed to process catalog webhook '{topic}' for store {store_id}",
                               details={"topic": topic}, exc=e)
    finally:
        db.close()


def _auto_sync_product_barcodes(db: Session, store_id: int, payload: Dict[str, Any]):
    """
    After a products/create or products/update webhook, check if any variant's
    barcode belongs to an existing barcode group. If so, set the new variant's
    stock to match the group's authoritative level.

    This ensures zero-delay alignment when adding products to stores.
    """
    try:
        # Extract variant barcodes from the webhook payload (REST format)
        variants = payload.get("variants", [])
        if not variants:
            return

        for v_data in variants:
            barcode = v_data.get("barcode")
            variant_id = v_data.get("id")
            if not barcode or not variant_id:
                continue
            _sync_variant_to_barcode_group(db, store_id, variant_id, barcode)

    except Exception as e:
        print(f"[SYNC-AUTO] Error in auto-sync for store {store_id}: {e}")
        audit_logger.log_error("inventory_sync_service._auto_sync_product_barcodes",
                               f"Auto-sync failed for store {store_id}",
                               exc=e)


def _sync_variant_to_barcode_group(db: Session, store_id: int, variant_id: int, barcode: str):
    """
    Sync a single variant's stock to match its barcode group.
    Called when a variant is created or its barcode changes.

    Steps:
    1. Skip placeholder/empty barcodes
    2. Check if the barcode exists on other variants (a group exists)
    3. Get the authoritative stock level from BarcodeVersion or existing inventory
    4. Write that stock to this variant via Shopify API
    5. Update local DB
    """
    if not barcode or barcode.strip() in PLACEHOLDER_BARCODES:
        return

    barcode = barcode.strip()

    # Check: does this barcode already exist on OTHER variants?
    existing_count = (
        db.query(models.ProductVariant.id)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.ProductVariant.barcode == barcode,
            models.ProductVariant.id != variant_id,
            models.Product.deleted_at.is_(None),
            models.ProductVariant.inventory_item_id.isnot(None),
        )
        .count()
    )

    if existing_count == 0:
        return  # No group — this is the only product with this barcode

    # Determine target quantity from BarcodeVersion (most reliable)
    target_quantity = None
    version_obj = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()

    if version_obj and version_obj.quantity is not None:
        target_quantity = version_obj.quantity
    else:
        # Fallback: get stock from any existing variant in the group
        existing_variant = (
            db.query(models.ProductVariant)
            .join(models.Product)
            .filter(
                models.ProductVariant.barcode == barcode,
                models.ProductVariant.id != variant_id,
                models.Product.deleted_at.is_(None),
                models.ProductVariant.inventory_item_id.isnot(None),
            )
            .first()
        )
        if existing_variant and existing_variant.product and existing_variant.product.store:
            ev_store = existing_variant.product.store
            if ev_store.sync_location_id:
                level = db.query(models.InventoryLevel).filter(
                    models.InventoryLevel.variant_id == existing_variant.id,
                    models.InventoryLevel.location_id == ev_store.sync_location_id,
                ).first()
                if level and level.available is not None:
                    target_quantity = level.available

    if target_quantity is None:
        print(f"[SYNC-AUTO] Cannot determine group stock for barcode {barcode}, skipping auto-sync")
        return

    # Get the new variant from DB
    new_variant = db.query(models.ProductVariant).filter(
        models.ProductVariant.id == variant_id
    ).first()
    if not new_variant or not new_variant.inventory_item_id:
        return

    store = db.query(models.Store).filter(models.Store.id == store_id).first()
    if not store or not store.sync_location_id or not store.enabled:
        return

    # Create WriteIntent to suppress the echo webhook
    try:
        _create_write_intents(db, barcode, target_quantity, 
                              version_obj.version if version_obj else 0, [store])
    except Exception:
        pass  # Non-critical, echo suppression is best-effort

    # Set stock on Shopify
    try:
        location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        service = ShopifyService(store_url=store.shopify_url, token=store.api_token)

        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "ignoreCompareQuantity": True,
                "quantities": [{
                    "inventoryItemId": f"gid://shopify/InventoryItem/{new_variant.inventory_item_id}",
                    "locationId": location_gid,
                    "quantity": target_quantity,
                }],
            }
        }

        result = service.execute_mutation("inventorySetQuantities", variables)
        user_errors = result.get("inventorySetQuantities", {}).get("userErrors", [])
        if user_errors:
            print(f"[SYNC-AUTO] Shopify userErrors for barcode {barcode}: {user_errors}")
            return

        # Update local DB
        crud_product.update_inventory_levels_for_variants(
            db, variant_ids=[new_variant.id],
            location_id=store.sync_location_id,
            new_quantity=target_quantity,
        )

        print(f"[SYNC-AUTO] Auto-synced barcode {barcode} on store '{store.name}' to qty {target_quantity}")
        audit_logger.log_propagation(
            barcode=barcode,
            source_store="auto_sync",
            target_store=store.name,
            quantity=target_quantity,
            details={"trigger": "barcode_group_join", "variant_id": variant_id},
        )

    except Exception as e:
        print(f"[SYNC-AUTO-ERROR] Failed to auto-sync barcode {barcode} on store '{store.name}': {e}")
        audit_logger.log_error("inventory_sync_service._sync_variant_to_barcode_group",
                               f"Auto-sync failed for barcode {barcode} on store '{store.name}'",
                               details={"barcode": barcode, "variant_id": variant_id}, exc=e)

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
        # BUG-34 FIX: Do NOT delete the WriteIntent. If Shopify fires duplicate webhooks 
        # (or if multiple workers race), the intent must remain to suppress all echoes 
        # within the TTL window.
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

def _execute_delta_propagation(
    db: Session,
    barcode: str,
    delta: int,
    new_source_qty: int,
    target_stores: List[models.Store],
    store_variant_map: Dict[int, List[models.ProductVariant]]
):
    """
    Execute DELTA-based propagation to target stores.
    Uses inventoryAdjustQuantities (relative delta) instead of inventorySetQuantities (absolute).
    
    This correctly handles concurrent orders: if Store A sells 1 unit (delta=-1),
    all other stores are adjusted by -1 regardless of their current stock.
    """
    store_lookup = {s.id: s for s in target_stores}
    
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)

    for sid, variants_to_update in store_variant_map.items():
        store = store_lookup.get(sid)
        if not store or not store.sync_location_id:
            if store:
                print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue

        # BUG-34 FIX: Calculate the expected absolute quantity and create WriteIntents 
        # BEFORE executing the API call to prevent multi-worker race conditions on echoes.
        try:
            for v in variants_to_update:
                level = db.query(models.InventoryLevel).filter(
                    models.InventoryLevel.variant_id == v.id,
                    models.InventoryLevel.location_id == store.sync_location_id,
                ).first()
                if level and level.available is not None:
                    expected_qty = level.available + delta
                    intent = models.WriteIntent(
                        barcode=barcode, target_store_id=store.id, 
                        quantity=expected_qty, barcode_version=0, expires_at=expires
                    )
                    db.add(intent)
            db.commit()
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Could not create WriteIntent for delta on store {store.name}: {e}")

        primary_location_gid = f"gid://shopify/Location/{store.sync_location_id}"

        changes_payload = [
            {
                "inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                "locationId": primary_location_gid,
                "delta": delta
            }
            for v in variants_to_update if v.inventory_item_id
        ]

        if not changes_payload:
            continue

        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            result = service.adjust_inventory_quantities(changes_payload)

            user_errors = result.get("inventoryAdjustQuantities", {}).get("userErrors", [])
            if user_errors:
                raise Exception(str(user_errors))

            delta_str = f"+{delta}" if delta > 0 else str(delta)
            print(f"[SYNC] Adjusted {delta_str} for barcode {barcode} on store '{store.name}' ({len(changes_payload)} variants).")

            audit_logger.log_propagation(
                barcode=barcode,
                source_store="webhook",
                target_store=store.name,
                quantity=new_source_qty,
                details={"variant_count": len(changes_payload), "delta": delta, "mode": "delta"},
            )

            # Update local DB using delta-based increment (atomic SQL operation)
            variant_ids = [v.id for v in variants_to_update]
            crud_product.adjust_inventory_levels_for_variants(
                db, variant_ids=variant_ids, location_id=store.sync_location_id,
                delta=delta
            )

        except Exception as e:
            print(f"[SYNC-ERROR] Failed to adjust store '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._execute_delta_propagation",
                                   f"Failed to adjust barcode {barcode} on store '{store.name}'",
                                   details={"barcode": barcode, "delta": delta}, exc=e)


def _execute_absolute_propagation(
    db: Session,
    barcode: str,
    desired_total: int,
    target_stores: List[models.Store],
    store_variant_map: Dict[int, List[models.ProductVariant]]
):
    """
    Execute ABSOLUTE propagation to target stores (legacy fallback).
    Used when no baseline is available (first-time sync) or by reconciliation.
    Uses inventorySetQuantities with ignoreCompareQuantity=True.
    """
    store_lookup = {s.id: s for s in target_stores}

    for sid, variants_to_update in store_variant_map.items():
        store = store_lookup.get(sid)
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

            print(f"[SYNC] Set qty {desired_total} for barcode {barcode} on store '{store.name}' ({len(quantities_payload)} variants).")

            audit_logger.log_propagation(
                barcode=barcode,
                source_store="webhook",
                target_store=store.name,
                quantity=desired_total,
                details={"variant_count": len(quantities_payload), "mode": "absolute"},
            )

            # Update local DB with absolute value
            variant_ids = [v.id for v in variants_to_update]
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=variant_ids, location_id=store.sync_location_id,
                new_quantity=desired_total
            )

        except Exception as e:
            print(f"[SYNC-ERROR] Failed to write to store '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._execute_absolute_propagation",
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