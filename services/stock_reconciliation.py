# services/stock_reconciliation.py
"""
Post-sync stock reconciliation service.
Finds products with matching barcodes across stores and sets them all
to the minimum stock level to prevent overselling.
"""
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from database import SessionLocal
import models
from shopify_service import ShopifyService
from . import sync_tracker


def reconcile_stock_by_barcode(task_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Find all barcodes that exist in multiple stores, get the minimum
    stock, and propagate that minimum to all variants with that barcode.
    
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
        if task_id:
            sync_tracker.step(task_id, 0, note="Finding barcodes across multiple stores...")
        
        # Step 1: Find barcodes that exist in multiple stores with their min stock
        # Join product_variants with inventory_levels to get actual stock
        barcode_stats = (
            db.query(
                models.ProductVariant.barcode,
                func.min(models.InventoryLevel.available).label("min_stock"),
                func.count(func.distinct(models.ProductVariant.store_id)).label("store_count")
            )
            .join(models.InventoryLevel, models.InventoryLevel.variant_id == models.ProductVariant.id)
            .filter(
                models.ProductVariant.barcode.isnot(None),
                models.ProductVariant.barcode != ''
            )
            .group_by(models.ProductVariant.barcode)
            .having(func.count(func.distinct(models.ProductVariant.store_id)) > 1)
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
        
        print(f"[RECONCILE] Completed: {results['barcodes_processed']} barcodes, {results['variants_updated']} variants updated")
        
    except Exception as e:
        error_msg = f"Stock reconciliation failed: {str(e)}"
        print(f"[RECONCILE-FATAL] {error_msg}")
        results["errors"].append(error_msg)
        if task_id:
            sync_tracker.finish_task(task_id, ok=False, note=error_msg)
    finally:
        db.close()
    
    return results


def _reconcile_single_barcode(db: Session, barcode: str, target_quantity: int) -> int:
    """
    Update all variants with the given barcode to the target quantity.
    Updates both Shopify and local database.
    
    Returns the number of variants updated.
    """
    # Get all variants with this barcode, grouped by store
    variants = (
        db.query(models.ProductVariant)
        .filter(models.ProductVariant.barcode == barcode)
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
    _update_barcode_version(db, barcode, target_quantity)
    
    return updated_count


def _update_barcode_version(db: Session, barcode: str, quantity: int):
    """Update or create the authoritative barcode version after reconciliation."""
    now = datetime.now(timezone.utc)
    
    version = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()
    
    if version:
        version.quantity = quantity
        version.source_timestamp = now
        version.version += 1
    else:
        version = models.BarcodeVersion(
            barcode=barcode,
            quantity=quantity,
            source_timestamp=now,
            version=1
        )
        db.add(version)
    
    try:
        db.commit()
    except Exception:
        db.rollback()
