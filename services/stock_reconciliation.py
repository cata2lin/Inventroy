# services/stock_reconciliation.py
"""
Post-sync stock reconciliation service.
Finds products with matching barcodes across stores and sets them all
to the minimum stock level to prevent overselling.

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
    Find all barcodes that exist in multiple stores, get the minimum
    stock at each store's sync_location, and propagate that minimum to all variants.

    Returns a summary of what was reconciled.
    """
    db: Session = SessionLocal()
    results = {
        "barcodes_processed": 0,
        "variants_updated": 0,
        "errors": [],
        "details": []
    }

    try:
        _start_time = time.monotonic()
        if task_id:
            sync_tracker.step(task_id, 0, note="Finding barcodes across multiple stores...")

        # BUG-14 FIX: Join through Store to filter by sync_location_id.
        # Only consider inventory at the designated sync location for each store.
        barcode_stats = (
            db.query(
                models.ProductVariant.barcode,
                func.min(models.InventoryLevel.available).label("min_stock"),
                func.count(func.distinct(models.ProductVariant.store_id)).label("store_count")
            )
            .join(models.Product, models.Product.id == models.ProductVariant.product_id)
            .join(models.Store, models.Store.id == models.ProductVariant.store_id)
            .join(models.InventoryLevel, and_(
                models.InventoryLevel.variant_id == models.ProductVariant.id,
                # Critical: only consider inventory at the store's designated sync location
                models.InventoryLevel.location_id == models.Store.sync_location_id
            ))
            .filter(
                models.ProductVariant.barcode.isnot(None),
                models.ProductVariant.barcode != '',
                # Filter out whitespace-only and placeholder barcodes
                func.btrim(models.ProductVariant.barcode) != '',
                ~models.ProductVariant.barcode.in_(['0', '00', '000', '0000', '00000', '000000',
                                                     '0000000', '00000000', '000000000',
                                                     '0000000000', '00000000000',
                                                     '000000000000', '0000000000000']),
                models.Store.enabled == True,
                models.Store.sync_location_id.isnot(None),
                # BUG-24 FIX: Exclude soft-deleted products
                models.Product.deleted_at.is_(None)
            )
            .group_by(models.ProductVariant.barcode)
            # FIX: Use distinct variant count > 1 so same-store multi-listings are also reconciled.
            # This handles the case where a barcode exists on multiple products within ONE store.
            .having(func.count(func.distinct(models.ProductVariant.id)) > 1)
            .all()
        )

        total_barcodes = len(barcode_stats)
        print(f"[RECONCILE] Found {total_barcodes} barcodes across multiple stores")

        if task_id:
            sync_tracker.step(task_id, 0, note=f"Found {total_barcodes} shared barcodes. Processing...")

        # Step 2: For each barcode, update all variants to the minimum stock
        for idx, (barcode, min_stock, store_count) in enumerate(barcode_stats):
            try:
                # Ensure min_stock is not None
                if min_stock is None:
                    min_stock = 0

                updated_count = _reconcile_single_barcode(db, barcode, min_stock)

                results["barcodes_processed"] += 1
                results["variants_updated"] += updated_count
                results["details"].append({
                    "barcode": barcode,
                    "min_stock": min_stock,
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
                note=f"Reconciled {results['barcodes_processed']} barcodes, updated {results['variants_updated']} variants"
            )

        _duration_ms = int((time.monotonic() - _start_time) * 1000)
        print(f"[RECONCILE] Completed: {results['barcodes_processed']} barcodes, {results['variants_updated']} variants updated")
        audit_logger.log_reconciliation(
            "reconciliation_completed",
            f"Reconciled {results['barcodes_processed']} barcodes, {results['variants_updated']} variants updated",
            duration_ms=_duration_ms,
            details={
                "barcodes_processed": results['barcodes_processed'],
                "variants_updated": results['variants_updated'],
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


def _reconcile_single_barcode(db: Session, barcode: str, target_quantity: int) -> int:
    """
    Update all variants with the given barcode to the target quantity.
    Updates both Shopify and local database.
    Creates WriteIntents to suppress echo webhooks (BUG-03 FIX).

    Returns the number of variants updated.
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
        return 0

    # Group variants by store
    store_variants: Dict[int, List[models.ProductVariant]] = {}
    for v in variants:
        if v.store_id not in store_variants:
            store_variants[v.store_id] = []
        store_variants[v.store_id].append(v)

    updated_count = 0
    now = datetime.now(timezone.utc)

    # BUG-03 FIX: Create WriteIntents BEFORE calling Shopify API
    # This prevents the echo cascade when Shopify fires webhooks back.
    _create_reconciliation_write_intents(db, barcode, target_quantity, store_variants.keys())

    for store_id, vars_list in store_variants.items():
        # Get store info for Shopify API
        store = db.query(models.Store).filter(models.Store.id == store_id).first()
        if not store or not store.enabled or not store.sync_location_id:
            continue

        # Build the Shopify mutation payload
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

        # Call Shopify API to set quantities
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
    # BUG-02 FIX: Pass a valid store_id for authoritative_store_id
    first_store_id = next(iter(store_variants.keys()), None)
    if first_store_id is not None:
        _update_barcode_version(db, barcode, target_quantity, first_store_id)

    return updated_count


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
