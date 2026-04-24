# services/stock_reconciliation.py
"""
Post-sync stock reconciliation service.
Finds products with matching barcodes across stores and aligns them all
to the AUTHORITATIVE stock level (latest known value from BarcodeVersion).

CRITICAL CHANGE: Replaced the destructive min() strategy with latest-value.
The min() approach destroyed legitimate restocks by forcing all stores down
to the lowest value. The latest-value approach uses the most recently updated
store as the source of truth.

BUG-14 FIX: Filter by each store's sync_location_id, not across ALL locations.
BUG-02 FIX: Always provide authoritative_store_id when creating BarcodeVersion.
BUG-03 FIX: Create WriteIntents before calling Shopify to prevent echo cascades.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, literal_column

from database import SessionLocal
import models
from shopify_service import ShopifyService
from . import sync_tracker
from . import audit_logger
import time

# Echo suppression TTL for reconciliation writes (same as inventory_sync_service)
INTENT_TTL_SECONDS = 60


def reconcile_stock_by_barcode(task_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Find all barcodes that exist on multiple variants (across stores or within one store),
    determine the AUTHORITATIVE stock level, and align all variants to that level.

    Authoritative stock is determined by:
    1. BarcodeVersion record (most recent webhook source) — preferred
    2. Live Shopify read from the authoritative store — fallback
    3. Most recently updated InventoryLevel in local DB — last resort

    Returns a summary of what was reconciled.
    """
    db: Session = SessionLocal()
    results = {
        "barcodes_processed": 0,
        "variants_updated": 0,
        "skipped_aligned": 0,
        "errors": [],
        "details": []
    }

    try:
        _start_time = time.monotonic()
        if task_id:
            sync_tracker.step(task_id, 0, note="Finding barcodes across multiple stores...")

        # Step 1: Find all barcodes shared across multiple variants/stores.
        # No min() aggregation — we just discover which barcodes need reconciliation.
        shared_barcodes = (
            db.query(
                models.ProductVariant.barcode,
                func.count(func.distinct(models.ProductVariant.store_id)).label("store_count"),
                func.count(func.distinct(models.ProductVariant.id)).label("variant_count"),
            )
            .join(models.Product, models.Product.id == models.ProductVariant.product_id)
            .join(models.Store, models.Store.id == models.ProductVariant.store_id)
            .join(models.InventoryLevel, and_(
                models.InventoryLevel.variant_id == models.ProductVariant.id,
                models.InventoryLevel.location_id == models.Store.sync_location_id
            ))
            .filter(
                models.ProductVariant.barcode.isnot(None),
                models.ProductVariant.barcode != '',
                func.btrim(models.ProductVariant.barcode) != '',
                ~models.ProductVariant.barcode.in_(['0', '00', '000', '0000', '00000', '000000',
                                                     '0000000', '00000000', '000000000',
                                                     '0000000000', '00000000000',
                                                     '000000000000', '0000000000000']),
                models.Store.enabled == True,
                models.Store.sync_location_id.isnot(None),
                models.Product.deleted_at.is_(None)
            )
            .group_by(models.ProductVariant.barcode)
            .having(func.count(func.distinct(models.ProductVariant.id)) > 1)
            .all()
        )

        total_barcodes = len(shared_barcodes)
        print(f"[RECONCILE] Found {total_barcodes} shared barcodes to check")

        if task_id:
            sync_tracker.step(task_id, 0, note=f"Found {total_barcodes} shared barcodes. Checking alignment...")

        # Step 2: For each barcode, determine authoritative stock and reconcile
        for idx, (barcode, store_count, variant_count) in enumerate(shared_barcodes):
            try:
                target_qty, source_info = _determine_authoritative_stock(db, barcode)

                if target_qty is None:
                    results["errors"].append(f"Could not determine stock for barcode {barcode}")
                    continue

                updated_count, was_aligned = _reconcile_single_barcode(db, barcode, target_qty)

                if was_aligned:
                    results["skipped_aligned"] += 1
                else:
                    results["barcodes_processed"] += 1
                    results["variants_updated"] += updated_count
                    results["details"].append({
                        "barcode": barcode,
                        "target_stock": target_qty,
                        "source": source_info,
                        "store_count": store_count,
                        "variants_updated": updated_count
                    })

                if task_id and (idx + 1) % 10 == 0:
                    sync_tracker.step(task_id, idx + 1, note=f"Processed {idx + 1}/{total_barcodes} barcodes...")

            except Exception as e:
                error_msg = f"Failed to reconcile barcode {barcode}: {str(e)}"
                print(f"[RECONCILE-ERROR] {error_msg}")
                results["errors"].append(error_msg)

        if task_id:
            sync_tracker.finish_task(
                task_id,
                ok=len(results["errors"]) == 0,
                note=f"Reconciled {results['barcodes_processed']} barcodes ({results['skipped_aligned']} already aligned), updated {results['variants_updated']} variants"
            )

        _duration_ms = int((time.monotonic() - _start_time) * 1000)
        print(f"[RECONCILE] Completed: {results['barcodes_processed']} barcodes corrected, {results['skipped_aligned']} already aligned, {results['variants_updated']} variants updated")
        audit_logger.log_reconciliation(
            "reconciliation_completed",
            f"Reconciled {results['barcodes_processed']} barcodes, {results['variants_updated']} variants updated, {results['skipped_aligned']} aligned",
            duration_ms=_duration_ms,
            details={
                "barcodes_processed": results['barcodes_processed'],
                "variants_updated": results['variants_updated'],
                "skipped_aligned": results['skipped_aligned'],
                "error_count": len(results['errors']),
            }
        )

    except Exception as e:
        error_msg = f"Stock reconciliation failed: {str(e)}"
        print(f"[RECONCILE-FATAL] {error_msg}")
        results["errors"].append(error_msg)
        audit_logger.log_reconciliation("reconciliation_failed", error_msg, error=str(e))
        audit_logger.log_error("stock_reconciliation.reconcile_stock_by_barcode",
                               error_msg, exc=e)
        if task_id:
            sync_tracker.finish_task(task_id, ok=False, note=error_msg)
    finally:
        db.close()

    return results


def _determine_authoritative_stock(db: Session, barcode: str) -> tuple:
    """
    Determine the authoritative stock level for a barcode.
    
    Strategy:
    1. BarcodeVersion → read LIVE stock from authoritative store via Shopify API
    2. Fallback: most recently updated InventoryLevel in local DB
    
    Returns:
        (target_quantity: int | None, source_info: str)
    """
    # Strategy 1: Use BarcodeVersion to identify the authoritative store
    version_obj = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()
    
    if version_obj and version_obj.authoritative_store_id:
        auth_store = db.query(models.Store).filter(
            models.Store.id == version_obj.authoritative_store_id
        ).first()
        
        if auth_store and auth_store.enabled and auth_store.sync_location_id:
            # Find a variant with this barcode on the authoritative store
            auth_variant = (
                db.query(models.ProductVariant)
                .join(models.Product)
                .filter(
                    models.ProductVariant.barcode == barcode,
                    models.ProductVariant.store_id == auth_store.id,
                    models.ProductVariant.inventory_item_id.isnot(None),
                    models.Product.deleted_at.is_(None),
                )
                .first()
            )
            
            if auth_variant:
                # Read LIVE stock from Shopify for the authoritative store
                try:
                    svc = ShopifyService(store_url=auth_store.shopify_url, token=auth_store.api_token)
                    location_gid = f"gid://shopify/Location/{auth_store.sync_location_id}"
                    inv_item_gid = f"gid://shopify/InventoryItem/{auth_variant.inventory_item_id}"
                    
                    query = """
                    query GetInventoryLevel($inventoryItemId: ID!) {
                      inventoryItem(id: $inventoryItemId) {
                        inventoryLevels(first: 10) {
                          nodes { location { id } quantities(names: ["available"]) { name quantity } }
                        }
                      }
                    }
                    """
                    result = svc._execute_query(query, {"inventoryItemId": inv_item_gid})
                    inv_item = result.get("data", {}).get("inventoryItem")
                    
                    if inv_item:
                        for level in inv_item.get("inventoryLevels", {}).get("nodes", []):
                            if level.get("location", {}).get("id") == location_gid:
                                for q in level.get("quantities", []):
                                    if q.get("name") == "available":
                                        live_qty = q.get("quantity")
                                        if live_qty is not None:
                                            print(f"[RECONCILE] Authoritative stock for {barcode}: {live_qty} (live from '{auth_store.name}')")
                                            return (live_qty, f"live_shopify:{auth_store.name}")
                except Exception as e:
                    print(f"[RECONCILE-WARN] Failed to read live stock for {barcode} from '{auth_store.name}': {e}")
        
        # If live read failed, use the BarcodeVersion's stored quantity
        if version_obj.quantity is not None:
            print(f"[RECONCILE] Authoritative stock for {barcode}: {version_obj.quantity} (BarcodeVersion cache)")
            return (version_obj.quantity, "barcode_version_cache")
    
    # Strategy 2: Fallback to most recently updated InventoryLevel in local DB
    latest_level = (
        db.query(models.InventoryLevel)
        .join(models.ProductVariant, models.ProductVariant.id == models.InventoryLevel.variant_id)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .join(models.Store, models.Store.id == models.ProductVariant.store_id)
        .filter(
            models.ProductVariant.barcode == barcode,
            models.Product.deleted_at.is_(None),
            models.Store.enabled == True,
            models.Store.sync_location_id.isnot(None),
            models.InventoryLevel.location_id == models.Store.sync_location_id,
        )
        .order_by(models.InventoryLevel.updated_at.desc())
        .first()
    )
    
    if latest_level and latest_level.available is not None:
        print(f"[RECONCILE] Authoritative stock for {barcode}: {latest_level.available} (latest InventoryLevel)")
        return (latest_level.available, "latest_inventory_level")
    
    return (None, "unknown")


def _reconcile_single_barcode(db: Session, barcode: str, target_quantity: int) -> tuple:
    """
    Update all variants with the given barcode to the target quantity.
    Updates both Shopify and local database.
    Creates WriteIntents to suppress echo webhooks (BUG-03 FIX).
    
    Skips stores that already have the correct stock (avoids unnecessary API calls).

    Returns:
        (updated_count: int, was_aligned: bool) — was_aligned is True if all stores
        already had the target quantity and no Shopify writes were needed.
    """
    # Get all variants with this barcode, excluding soft-deleted products
    variants = (
        db.query(models.ProductVariant)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.ProductVariant.barcode == barcode,
            models.Product.deleted_at.is_(None)
        )
        .all()
    )

    if not variants:
        return (0, True)

    # Group variants by store
    store_variants: Dict[int, List[models.ProductVariant]] = {}
    for v in variants:
        if v.store_id not in store_variants:
            store_variants[v.store_id] = []
        store_variants[v.store_id].append(v)

    # Pre-check: are all stores already at the target quantity?
    all_aligned = True
    for store_id_chk, vars_list in store_variants.items():
        store_chk = db.query(models.Store).filter(models.Store.id == store_id_chk).first()
        if not store_chk or not store_chk.enabled or not store_chk.sync_location_id:
            continue
        for v in vars_list:
            if not v.inventory_item_id:
                continue
            level = db.query(models.InventoryLevel).filter(
                models.InventoryLevel.variant_id == v.id,
                models.InventoryLevel.location_id == store_chk.sync_location_id,
            ).first()
            if not level or level.available != target_quantity:
                all_aligned = False
                break
        if not all_aligned:
            break
    
    if all_aligned:
        return (0, True)

    updated_count = 0
    now = datetime.now(timezone.utc)

    # BUG-03 FIX: Create WriteIntents BEFORE calling Shopify API
    _create_reconciliation_write_intents(db, barcode, target_quantity, store_variants.keys())

    for store_id, vars_list in store_variants.items():
        store = db.query(models.Store).filter(models.Store.id == store_id).first()
        if not store or not store.enabled or not store.sync_location_id:
            continue

        location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        quantities_payload = []
        variant_ids_to_update = []

        for v in vars_list:
            if v.inventory_item_id:
                quantities_payload.append({
                    "inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                    "locationId": location_gid,
                    "quantity": target_quantity
                })
                variant_ids_to_update.append(v.id)

        if not quantities_payload:
            continue

        try:
            svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            response = svc.set_inventory_quantities(quantities_payload)

            if response.get("error"):
                print(f"[RECONCILE-ERROR] Shopify API error for store {store.name}: {response['error']}")
                continue

            # Update local database
            db.query(models.InventoryLevel).filter(
                models.InventoryLevel.variant_id.in_(variant_ids_to_update),
                models.InventoryLevel.location_id == store.sync_location_id
            ).update({
                models.InventoryLevel.available: target_quantity,
                models.InventoryLevel.updated_at: now,
                models.InventoryLevel.last_fetched_at: now
            }, synchronize_session=False)

            db.commit()
            updated_count += len(variant_ids_to_update)

            print(f"[RECONCILE] Set {barcode} to {target_quantity} in store '{store.name}' ({len(variant_ids_to_update)} variants)")

        except Exception as e:
            db.rollback()
            print(f"[RECONCILE-ERROR] Failed to update Shopify for store {store.name}: {e}")

    # Update BarcodeVersion to track this reconciliation
    first_store_id = next(iter(store_variants.keys()), None)
    if first_store_id is not None:
        _update_barcode_version(db, barcode, target_quantity, first_store_id)

    return (updated_count, False)


def _create_reconciliation_write_intents(db: Session, barcode: str, quantity: int, store_ids):
    """BUG-03 FIX: Create WriteIntents for all stores before reconciliation writes."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)

    # Get the current barcode version (or use 0 as placeholder)
    version_obj = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()
    version = version_obj.version if version_obj else 0

    for store_id in store_ids:
        intent = models.WriteIntent(
            barcode=barcode,
            target_store_id=store_id,
            quantity=quantity,
            barcode_version=version,
            expires_at=expires
        )
        db.add(intent)

    try:
        db.commit()
    except Exception:
        db.rollback()


def _update_barcode_version(db: Session, barcode: str, quantity: int, authoritative_store_id: int):
    """Update or create the authoritative barcode version after reconciliation.
    BUG-02 FIX: Always include authoritative_store_id (required, non-nullable FK).
    """
    now = datetime.now(timezone.utc)

    version = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()

    if version:
        version.quantity = quantity
        version.source_timestamp = now
        version.authoritative_store_id = authoritative_store_id
        version.version += 1
    else:
        version = models.BarcodeVersion(
            barcode=barcode,
            authoritative_store_id=authoritative_store_id,
            quantity=quantity,
            source_timestamp=now,
            version=1
        )
        db.add(version)

    try:
        db.commit()
    except Exception:
        db.rollback()
