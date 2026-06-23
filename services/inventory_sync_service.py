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
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import threading

from sqlalchemy.orm import Session
from sqlalchemy import func, or_

import models
from database import SessionLocal
from shopify_service import ShopifyService
from crud import product as crud_product
from services import audit_logger
from services import sync_guards
from services import alerting
from services import dist_lock

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

def handle_webhook(store_id: int, payload: Dict[str, Any], triggered_at_str: str,
                   webhook_id: Optional[str] = None):
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

    # In-process lock = cheap fast gate (serializes same-process threads for this barcode).
    lock = get_barcode_lock(barcode)
    if not lock.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        print(f"[SYNC-ERROR] Could not acquire lock for barcode {barcode}. Task timed out.")
        db.close()
        return

    # P2 distributed lock = cross-process/instance gate (Postgres advisory lock). Held for the
    # whole critical section on a dedicated connection; auto-released if this worker crashes.
    adv = dist_lock.acquire(f"barcode:{barcode}")
    if adv is None:
        print(f"[SYNC] Skipped {barcode}@{store_id}: distributed lock busy/unavailable.")
        audit_logger.log(category="STOCK", action="dist_lock_contention",
                         message=f"Skipped [{barcode}] — distributed lock busy/unavailable",
                         store_id=store_id, target=barcode, severity="WARN")
        lock.release()
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

        # P0.5: idempotency by Shopify webhook id (stable across retries), with the legacy
        # value-hash as a fallback when the header is absent.
        try:
            if _is_duplicate_webhook(db, store_id, barcode, new_available, source_timestamp, webhook_id=webhook_id):
                print(f"[SYNC] Ignored: Duplicate webhook for {barcode} at store {store_id} (id={webhook_id}).")
                return
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Dedup check failed, proceeding anyway: {e}")

        # P0.5/P0.2: self-echo suppression. If we wrote to THIS exact inventory item within the
        # echo window, this webhook is (at least partly) the echo of our own write.
        #   - VALUE-INDEPENDENT path (default; flag off OR no captured authoritative qty): suppress
        #     regardless of value — the core defence against the stale-mirror phantom-delta cascade
        #     (we never recompute a delta from a drifted baseline for our own echoes).
        #   - AUTHORITATIVE-anchored path (SYNC_ECHO_AUTHORITATIVE on for this barcode + a Shopify
        #     post-write value captured): residual = observed - authoritative_qty. 0 => pure echo
        #     (suppress); != 0 => a real change rode in on the same window, so propagate exactly that
        #     residual — anchored to Shopify truth, never the drifted mirror.
        # Either way we resync the local mirror to the observed (authoritative) value.
        authoritative_residual = False
        echo = _find_self_echo(db, store_id, inventory_item_id, new_available, barcode)
        if echo is not None:
            echo_op, residual = echo
            _resync_local_baseline(db, variant.id, payload.get("location_id"), new_available)
            if residual is None or residual == 0:
                print(f"[SYNC] Suppressed echo (lineage op={echo_op}) for {barcode}@{store_id}.")
                return
            print(f"[SYNC] Authoritative echo for {barcode}@{store_id}: residual={residual} "
                  f"(real change layered on our write op={echo_op}) — propagating residual.")
            delta = residual
            last_known = new_available - residual  # == authoritative_qty; keep baseline consistent
            authoritative_residual = True

        # --- DELTA COMPUTATION (skipped when we already have an authoritative residual) ---
        if not authoritative_residual:
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

            # Per-item WriteIntent echo guard (covers reconciliation/absolute-SET and any case
            # where the local delta baseline drifted). Matching by inventory_item_id + value is
            # precise and never suppresses a genuine different value (no oversell risk).
            if _is_echo(db, store_id, barcode, new_available, inventory_item_id=inventory_item_id):
                print(f"[SYNC] Suppressed echo for {barcode} at store {store_id} (WriteIntent match).")
                # Keep the local baseline exact so future deltas compute correctly.
                source_location_id = payload.get("location_id")
                if source_location_id:
                    try:
                        crud_product.update_inventory_levels_for_variants(
                            db, variant_ids=[variant.id], location_id=source_location_id,
                            new_quantity=new_available
                        )
                    except Exception:
                        db.rollback()
                return

        # --- VERSION CHECK ---
        is_authoritative = _is_new_authoritative_version(db, barcode, source_timestamp)
        if not is_authoritative:
            print(f"[SYNC] Ignored: Stale event for {barcode} from store {store_id}.")
            return

        # 1. Update the authoritative version + keep the source store's local mirror exact.
        _update_authoritative_version(db, barcode, store_id, new_available, source_timestamp)
        _resync_local_baseline(db, variant.id, payload.get("location_id"), new_available)

        # --- P0 PROPAGATION GUARDS (run after the local mirror is updated, so a blocked
        #     propagation still leaves our state consistent and the next delta sane) ---

        # Kill switch: ingest + mirror, but emit nothing to other stores.
        if not sync_guards.propagation_enabled():
            audit_logger.log(category="STOCK", action="propagation_disabled",
                             message=f"Propagation globally disabled; ingested [{barcode}]@{store_id} only",
                             store_id=store_id, target=barcode, severity="WARN",
                             details={"quantity": new_available})
            return

        # P0.2/P0.3 circuit breaker: this barcode tripped the storm/abnormal guard recently.
        if _is_barcode_broken(db, barcode) or sync_guards.is_quarantined(barcode):
            alerting.warning("inventory_sync.breaker",
                             f"Skipped propagation for quarantined barcode {barcode}",
                             {"barcode": barcode, "store_id": store_id, "quantity": new_available})
            return

        # P0.3 abnormal-delta guard: a single delta larger than MAX_ABS_DELTA is NOT
        # propagated as a blind relative move — it is routed to reconciliation/review.
        allowed, reason = sync_guards.check_delta(delta)
        if not allowed:
            alerting.critical("inventory_sync.delta_guard",
                              f"Blocked oversized delta {delta} for barcode {barcode}",
                              {"barcode": barcode, "store_id": store_id, "delta": delta,
                               "last_known": last_known, "new_available": new_available, "reason": reason})
            audit_logger.log(category="STOCK", action="propagation_blocked_oversized_delta",
                             message=f"Blocked delta={delta} for [{barcode}] ({reason})",
                             store_id=store_id, target=barcode, severity="CRITICAL",
                             details={"delta": delta, "last_known": last_known, "quantity": new_available})
            return

        # 3. Resolve candidate variants. P3: prefer explicit sync_group membership (so a shared
        #    barcode can be NON-syncing, orphans are excluded, and quarantined groups never sync).
        #    Falls back to barcode grouping when the variant isn't group-mapped or the flag is off.
        raw_targets = None
        if sync_guards.use_sync_groups():
            raw_targets, blocked = _resolve_group_targets(db, variant)
            if blocked:
                print(f"[SYNC] No propagation for {barcode}@{store_id}: {blocked}")
                audit_logger.log(category="STOCK", action="propagation_group_blocked",
                                 message=f"[{barcode}] blocked: {blocked}", store_id=store_id,
                                 target=barcode, severity="INFO", details={"reason": blocked})
                return
        if raw_targets is None:
            raw_targets = _get_all_propagation_variants(db, barcode, exclude_variant_id=variant.id)

        # Collapse to CANONICAL targets (P0.1): at most one variant per store, origin excluded.
        propagation_targets = sync_guards.select_canonical_targets(
            raw_targets, origin_store_id=variant.store_id
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

            # P0.2 storm breaker: if this barcode has been propagated too many times in the
            # window, trip the circuit breaker, quarantine it, alert, and STOP (the cascade
            # signature). One-off legitimate changes never trip this; a runaway does.
            sync_guards.record_propagation(barcode)
            if sync_guards.is_storming(barcode):
                _trip_breaker(db, barcode, reason="propagation_storm",
                              details={"window_s": sync_guards.STORM_WINDOW_SECONDS,
                                       "max": sync_guards.STORM_MAX_PROPAGATIONS,
                                       "store_id": store_id, "delta": delta})
                alerting.critical("inventory_sync.storm",
                                  f"Propagation storm on barcode {barcode} — quarantined",
                                  {"barcode": barcode, "store_id": store_id, "delta": delta})
                audit_logger.log(category="STOCK", action="propagation_storm_tripped",
                                 message=f"Storm breaker tripped for [{barcode}] — quarantined",
                                 store_id=store_id, target=barcode, severity="CRITICAL",
                                 details={"delta": delta, "quantity": new_available})
                return

            sync_op = str(uuid.uuid4())
            total_variants = sum(len(vs) for vs in store_map.values())
            mode = "delta" if delta is not None else "absolute"
            print(f"[SYNC] Propagating '{barcode}' {mode}={delta} (new_qty={new_available}) op={sync_op} to {total_variants} variants across {len(store_map)} stores.")

            # Audit log the propagation event (with lineage)
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
                    "sync_operation_uuid": sync_op,
                    "origin_store_id": variant.store_id,
                    "origin_inventory_item_id": inventory_item_id,
                    "target_stores": {str(k): len(v) for k, v in store_map.items()},
                    "total_variants": total_variants,
                },
            )

            if delta is not None:
                # --- DELTA MODE: adjust the one canonical target per store by delta ---
                try:
                    _execute_delta_propagation(db, barcode, delta, new_available, target_stores, store_map,
                                               sync_op, variant.store_id, inventory_item_id)
                except Exception as e:
                    audit_logger.log_error("inventory_sync_service.handle_webhook",
                                           f"Delta propagation failed for barcode {barcode}",
                                           details={"barcode": barcode, "delta": delta}, exc=e)
            else:
                # --- ABSOLUTE FALLBACK: first-time sync, no baseline available ---
                try:
                    _execute_absolute_propagation(db, barcode, new_available, target_stores, store_map,
                                                  sync_op, variant.store_id, inventory_item_id)
                except Exception as e:
                    audit_logger.log_error("inventory_sync_service.handle_webhook",
                                           f"Absolute propagation failed for barcode {barcode}",
                                           details={"barcode": barcode, "quantity": new_available}, exc=e)
        else:
            print(f"[SYNC] No other variants to propagate to for barcode {barcode}.")

    finally:
        dist_lock.release(adv)
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

            # If the barcode changed, sync to the new group. force=True because a real
            # barcode change is a genuine group-join — re-aligning is intended here.
            new_barcode = payload.get("barcode")
            if new_barcode and new_barcode != old_barcode and old_variant:
                _sync_variant_to_barcode_group(db, store_id, old_variant.id, new_barcode, force=True)

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


def _get_group_authoritative_qty(db: Session, barcode: str, exclude_variant_id: int) -> Optional[int]:
    """
    Best estimate of a barcode group's CURRENT stock for aligning a newly-joined variant.
    Prefers the most recently updated real InventoryLevel among other group members over
    the BarcodeVersion cache (which can be stale — that stale cache was the source of the
    'reset to 0' clobbering bug). Falls back to the cache only when no live value exists.
    """
    latest = (
        db.query(models.InventoryLevel)
        .join(models.ProductVariant, models.ProductVariant.id == models.InventoryLevel.variant_id)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .join(models.Store, models.Store.id == models.ProductVariant.store_id)
        .filter(
            models.ProductVariant.barcode == barcode,
            models.ProductVariant.id != exclude_variant_id,
            models.Product.deleted_at.is_(None),
            models.Store.enabled == True,
            models.Store.sync_location_id.isnot(None),
            models.InventoryLevel.location_id == models.Store.sync_location_id,
            models.InventoryLevel.available.isnot(None),
        )
        .order_by(models.InventoryLevel.updated_at.desc())
        .first()
    )
    if latest is not None and latest.available is not None:
        return latest.available

    version_obj = db.query(models.BarcodeVersion).filter(
        models.BarcodeVersion.barcode == barcode
    ).first()
    if version_obj and version_obj.quantity is not None:
        return version_obj.quantity
    return None


def _sync_variant_to_barcode_group(db: Session, store_id: int, variant_id: int, barcode: str, force: bool = False):
    """
    Align a single variant's stock to its barcode group. Triggered on products/create,
    products/update, and real barcode changes (inventory_items/update).

    CRITICAL BUG-FIX: this must NEVER overwrite an existing stock value on a routine
    product update. Previously it force-set every matching variant to BarcodeVersion.quantity
    on *every* products/update — and because that cache was frequently stale (0), Shopify's
    constant products/update webhooks repeatedly reset real stock to 0 and reverted manual
    additions (the race the user observed).

    Rules:
      - force=False (create/update): only write when the variant has NO existing stock
        baseline at the store's sync location (genuinely new to the group). Never clobber.
      - force=True (real barcode change): re-align even if a baseline exists, since the
        variant just joined a different group.
    """
    if not barcode or barcode.strip() in PLACEHOLDER_BARCODES:
        return
    barcode = barcode.strip()

    # Does this barcode already exist on OTHER (non-deleted) variants? (a group exists)
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
        return  # No group — nothing to align to.

    new_variant = db.query(models.ProductVariant).filter(
        models.ProductVariant.id == variant_id
    ).first()
    if not new_variant or not new_variant.inventory_item_id:
        return

    store = db.query(models.Store).filter(models.Store.id == store_id).first()
    if not store or not store.sync_location_id or not store.enabled:
        return

    # --- NON-CLOBBER GUARD (the core of the fix) ---
    # If the variant already has a known stock value at the sync location and we are not
    # explicitly forcing a re-align (real barcode change), do NOT touch it. This single
    # guard stops routine products/update webhooks from resetting real stock to a cached value.
    current_level = db.query(models.InventoryLevel).filter(
        models.InventoryLevel.variant_id == new_variant.id,
        models.InventoryLevel.location_id == store.sync_location_id,
    ).first()
    if not force and current_level is not None and current_level.available is not None:
        return  # Already has stock — never overwrite on a routine update.

    # Serialize against the inventory_levels/update handler for this barcode.
    lock = get_barcode_lock(barcode)
    if not lock.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        print(f"[SYNC-AUTO] Could not acquire lock for barcode {barcode}; skipping auto-sync.")
        return

    try:
        target_quantity = _get_group_authoritative_qty(db, barcode, exclude_variant_id=variant_id)
        if target_quantity is None:
            print(f"[SYNC-AUTO] Cannot determine group stock for barcode {barcode}, skipping auto-sync")
            return

        version_obj = db.query(models.BarcodeVersion).filter(
            models.BarcodeVersion.barcode == barcode
        ).first()

        # Per-item echo guard so the resulting webhook echo is suppressed.
        try:
            _create_write_intents(
                db, barcode, target_quantity,
                version_obj.version if version_obj else 0, [store],
                inventory_item_id=new_variant.inventory_item_id,
            )
        except Exception:
            db.rollback()  # best-effort

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

        crud_product.update_inventory_levels_for_variants(
            db, variant_ids=[new_variant.id],
            location_id=store.sync_location_id,
            new_quantity=target_quantity,
        )

        print(f"[SYNC-AUTO] Aligned barcode {barcode} on store '{store.name}' to qty {target_quantity} (force={force})")
        audit_logger.log_propagation(
            barcode=barcode,
            source_store="auto_sync",
            target_store=store.name,
            quantity=target_quantity,
            details={"trigger": "barcode_group_join", "variant_id": variant_id, "force": force},
        )
    except Exception as e:
        print(f"[SYNC-AUTO-ERROR] Failed to auto-sync barcode {barcode} on store '{store.name}': {e}")
        audit_logger.log_error("inventory_sync_service._sync_variant_to_barcode_group",
                               f"Auto-sync failed for barcode {barcode} on store '{store.name}'",
                               details={"barcode": barcode, "variant_id": variant_id}, exc=e)
    finally:
        lock.release()

# --- Helper Functions ---

def _is_duplicate_webhook(db: Session, store_id: int, barcode: str, total: int, timestamp: datetime,
                          webhook_id: Optional[str] = None) -> bool:
    # P0.5: prefer Shopify's X-Shopify-Webhook-Id (stable across retries of the same event)
    # over the weak value+timestamp hash, which collided on distinct cascade steps.
    if webhook_id:
        event_id = f"whid:{webhook_id}"
    else:
        event_id = hashlib.sha256(f"{store_id}-{barcode}-{total}-{timestamp.isoformat()}".encode()).hexdigest()
    if db.query(models.ProcessedWebhook).filter(models.ProcessedWebhook.id == event_id).first():
        return True
    new_record = models.ProcessedWebhook(id=event_id, expires_at=datetime.now(timezone.utc) + timedelta(seconds=DUPLICATE_TTL_SECONDS))
    db.add(new_record)
    db.commit()
    return False


def _resync_local_baseline(db: Session, variant_id: int, location_id, new_available: int):
    """Keep the source store's local mirror exactly equal to the observed (authoritative)
    Shopify value. Never used as a propagation source of truth — only to keep deltas sane."""
    if not location_id:
        return
    try:
        crud_product.update_inventory_levels_for_variants(
            db, variant_ids=[variant_id], location_id=location_id, new_quantity=new_available
        )
    except Exception:
        db.rollback()


def _find_self_echo(db: Session, store_id: int, inventory_item_id: Optional[int],
                    observed: Optional[int] = None, barcode: Optional[str] = None):
    """If WE wrote to this exact (store, inventory_item) within the echo window, this webhook is
    (at least partly) the echo of our own write. CONSUMES the matched marker (one outbound write =>
    exactly one expected echo; exact redeliveries are caught separately by webhook-id dedup).

    Returns:
      None                 -> no marker: not our echo.
      (op, None)           -> matched, VALUE-INDEPENDENT path (flag off or no authoritative_qty):
                              caller suppresses outright (today's behaviour; cascade-safe).
      (op, residual:int)   -> matched WITH Shopify-authoritative anchoring: residual =
                              observed - authoritative_qty. 0 => pure echo (suppress); != 0 => a real
                              change rode in on our write (caller propagates exactly the residual).
    """
    if inventory_item_id is None:
        return None
    marker = (
        db.query(models.WriteIntent)
        .filter(
            models.WriteIntent.target_store_id == store_id,
            models.WriteIntent.inventory_item_id == inventory_item_id,
            models.WriteIntent.sync_operation_uuid.isnot(None),
            models.WriteIntent.expires_at > datetime.now(timezone.utc),
        )
        .order_by(models.WriteIntent.id.asc())  # FIFO: consume oldest first
        .first()
    )
    if marker is None:
        return None
    op = marker.sync_operation_uuid
    auth = marker.authoritative_qty
    # The authoritative-anchored branch is taken ONLY when the master flag is on for this barcode AND
    # we actually captured a Shopify post-write value. Any other case => value-INDEPENDENT suppression,
    # so a drifted mirror can NEVER inject a phantom delta (the stale-mirror cascade stays closed) and
    # flipping the flag off instantly reverts behaviour even for already-stamped markers.
    use_auth = (auth is not None and observed is not None and barcode is not None
                and sync_guards.echo_authoritative_for(barcode))
    residual = (observed - auth) if use_auth else None
    try:
        db.delete(marker)
        db.commit()
    except Exception:
        db.rollback()
    return (op, residual)


def _create_echo_marker(db: Session, barcode: str, target_store_id: int, inventory_item_id: int,
                        expected_qty: Optional[int], sync_op: str, origin_store_id: int,
                        origin_item_id: Optional[int], depth: int = 1,
                        authoritative_qty: Optional[int] = None):
    """Record an outbound-write lineage marker so the resulting webhook echo is recognised as ours.
    expected_qty feeds the legacy value-based fallback. authoritative_qty (when set, from a single-item
    mutation's Shopify-authoritative post-write `available` quantity) enables exact residual detection.
    Returns the created marker so the caller can patch authoritative_qty after the write returns."""
    marker = models.WriteIntent(
        barcode=barcode,
        target_store_id=target_store_id,
        inventory_item_id=inventory_item_id,
        quantity=expected_qty if expected_qty is not None else 0,
        barcode_version=0,
        sync_operation_uuid=sync_op,
        origin_store_id=origin_store_id,
        origin_inventory_item_id=origin_item_id,
        propagation_depth=depth,
        authoritative_qty=authoritative_qty,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=sync_guards.ECHO_TTL_SECONDS),
    )
    db.add(marker)
    return marker


def _is_barcode_broken(db: Session, barcode: str) -> bool:
    """True if a live circuit-breaker row exists for this barcode (DB-persisted quarantine)."""
    try:
        row = (
            db.query(models.BarcodeCircuitBreaker)
            .filter(
                models.BarcodeCircuitBreaker.barcode == barcode,
                models.BarcodeCircuitBreaker.expires_at > datetime.now(timezone.utc),
            )
            .first()
        )
        return row is not None
    except Exception:
        db.rollback()
        return False


def _trip_breaker(db: Session, barcode: str, reason: str, details: Optional[Dict[str, Any]] = None):
    """Persist a circuit-breaker row (and set the in-process quarantine) so this barcode is
    refused for propagation until it expires."""
    expires = datetime.now(timezone.utc) + timedelta(seconds=sync_guards.STORM_QUARANTINE_SECONDS)
    try:
        existing = db.query(models.BarcodeCircuitBreaker).filter_by(barcode=barcode).first()
        if existing:
            existing.reason = reason
            existing.expires_at = expires
            existing.tripped_at = datetime.now(timezone.utc)
            existing.details = details
        else:
            db.add(models.BarcodeCircuitBreaker(barcode=barcode, reason=reason,
                                                expires_at=expires, details=details))
        db.commit()
    except Exception:
        db.rollback()
    sync_guards.quarantine(barcode)

def _is_echo(db: Session, store_id: int, barcode: str, observed_total: int,
             inventory_item_id: Optional[int] = None) -> bool:
    """
    Detect whether an incoming inventory webhook is the echo of one of OUR OWN writes.

    Matches a non-expired WriteIntent for this target store whose recorded quantity equals
    the observed value (per-item when we know which item — so multi-listing within a store
    can't cross-suppress, and a genuine *different* value is never suppressed → no oversell).
    """
    q = db.query(models.WriteIntent).filter(
        models.WriteIntent.target_store_id == store_id,
        models.WriteIntent.barcode == barcode,
        models.WriteIntent.quantity == observed_total,
        models.WriteIntent.expires_at > datetime.now(timezone.utc),
    )
    if inventory_item_id is not None:
        # Prefer an item-specific intent; fall back to store-level (NULL item) intents
        # created by the absolute/reconciliation paths.
        q = q.filter(or_(
            models.WriteIntent.inventory_item_id == inventory_item_id,
            models.WriteIntent.inventory_item_id.is_(None),
        ))
    intent = q.first()
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

def _resolve_group_targets(db: Session, variant: models.ProductVariant):
    """P3 — resolve propagation targets via explicit sync_group membership.

    Returns (targets, blocked_reason):
      - targets: non-excluded sibling variants in the same sync_group (None => fall back to barcode)
      - blocked_reason: a string if this group/variant must NOT propagate (quarantine, excluded
        orphan, sync disabled, confirmed error), else None.
    """
    member = db.query(models.SyncGroupMember).filter(
        models.SyncGroupMember.variant_id == variant.id
    ).first()
    if member is None:
        return None, None  # not group-mapped → caller falls back to barcode grouping

    if member.excluded:
        return [], "trigger is an excluded orphan (not a sync participant)"

    group = db.query(models.SyncGroup).filter(models.SyncGroup.id == member.sync_group_id).first()
    if group is None:
        return None, None
    if not group.sync_enabled or group.classification in ("QUARANTINED", "CONFIRMED_ERROR"):
        return [], f"group {group.id} not syncing (classification={group.classification}, enabled={group.sync_enabled})"

    targets = (
        db.query(models.ProductVariant)
        .join(models.SyncGroupMember, models.SyncGroupMember.variant_id == models.ProductVariant.id)
        .join(models.Product, models.Product.id == models.ProductVariant.product_id)
        .filter(
            models.SyncGroupMember.sync_group_id == group.id,
            models.SyncGroupMember.excluded == False,  # noqa: E712 — exclude orphans
            models.ProductVariant.id != variant.id,
            models.Product.deleted_at.is_(None),
            models.ProductVariant.inventory_item_id.isnot(None),
        )
        .all()
    )
    return targets, None


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

def _create_write_intents(db: Session, barcode: str, quantity: int, version: int,
                          target_stores: List[models.Store], inventory_item_id: Optional[int] = None):
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=INTENT_TTL_SECONDS)

    for store in target_stores:
        intent = models.WriteIntent(barcode=barcode, target_store_id=store.id, quantity=quantity,
                                    barcode_version=version, expires_at=expires,
                                    inventory_item_id=inventory_item_id)
        db.add(intent)
    db.commit()


def _is_stale_compare(ue: Optional[List[Dict[str, Any]]]) -> bool:
    """True if a Shopify userErrors list signals a compare-and-set mismatch. Matches the typed
    `code` (now that the mutations select it) AND, belt-and-suspenders, the message text — so the
    stale-compare healer can never silently regress to dead code if Shopify changes the enum."""
    for e in (ue or []):
        if e.get("code") == "COMPARE_QUANTITY_STALE":
            return True
        msg = (e.get("message") or "").lower()
        if "comparequantity" in msg or "compare quantity" in msg or "no longer matches" in msg:
            return True
    return False


def _propagate_delta_single_item(db: Session, barcode: str, delta: int, new_source_qty: int,
                                 store: models.Store, location_gid: str,
                                 variants_to_update: List[models.ProductVariant], ref_uri: str,
                                 sync_op: str, origin_store_id: int, origin_item_id: Optional[int]):
    """SYNC_ECHO_AUTHORITATIVE path: write each target item via COMPARE-AND-SET so the authoritative
    post-write value is known BY CONSTRUCTION (quantityAfterChange is null on these stores, so it
    can't be read). We SET the item to (mirror M + delta) with compareQuantity=M: if it SUCCEEDS,
    Shopify's current was M, so the result is exactly M+delta — stamp that on the echo marker. If the
    compare FAILS (COMPARE_QUANTITY_STALE: mirror drifted or a concurrent sale moved it), fall back to
    a relative adjust with a value-INDEPENDENT marker (drift-safe = today's behaviour). A floor-clamp
    is an absolute SET to the floor (result known = floor)."""
    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    for v in variants_to_update:
        if not v.inventory_item_id:
            continue
        level = db.query(models.InventoryLevel).filter(
            models.InventoryLevel.variant_id == v.id,
            models.InventoryLevel.location_id == store.sync_location_id,
        ).first()
        current = level.available if (level and level.available is not None) else None
        op, floor_value, clamped = sync_guards.apply_floor(current, delta)
        # The absolute target we intend to land on this item.
        target = floor_value if clamped else ((current + delta) if current is not None else None)
        item_gid = f"gid://shopify/InventoryItem/{v.inventory_item_id}"

        # Marker FIRST (authoritative_qty stamped only once the post-write value is known).
        marker = _create_echo_marker(db, barcode, store.id, v.inventory_item_id, target,
                                     sync_op, origin_store_id, origin_item_id, depth=1,
                                     authoritative_qty=None)
        try:
            db.commit()
        except Exception:
            db.rollback()
            print(f"[SYNC-WARN] Could not stage single-item marker for {store.name}: {barcode}")
            continue

        authoritative = None
        mode = "delta-fallback"
        try:
            if clamped:
                # Absolute SET to the floor; result is known (= floor). Ignore compare so the floor
                # always lands (we are deliberately overriding to prevent going negative).
                raw, ue = service.set_inventory_quantities_single(item_gid, location_gid, target, reference_uri=ref_uri)
                if ue:
                    raise Exception(str(ue))
                crud_product.update_inventory_levels_for_variants(
                    db, variant_ids=[v.id], location_id=store.sync_location_id, new_quantity=target)
                authoritative, mode = target, "delta-clamp"
                alerting.warning("inventory_sync.floor",
                                 f"Floored {barcode} on '{store.name}' to {target} (delta {delta} would breach floor)",
                                 {"barcode": barcode, "store": store.name, "current": current,
                                  "delta": delta, "floored_to": target})
                audit_logger.log(category="STOCK", action="inventory_floor_clamp",
                                 message=f"Floored [{barcode}] on '{store.name}' to {target} (would have been {(current or 0)+delta})",
                                 store_id=store.id, target=barcode, severity="WARN",
                                 details={"current": current, "delta": delta, "floored_to": target,
                                          "sync_operation_uuid": sync_op})
            elif current is not None:
                # COMPARE-AND-SET to current+delta. Success (no userErrors) => Shopify current WAS
                # `current`, so the post-write value is exactly `target` (authoritative, known).
                raw, ue = service.set_inventory_quantities_single(
                    item_gid, location_gid, target, reference_uri=ref_uri, compare_quantity=current)
                if not ue:
                    crud_product.update_inventory_levels_for_variants(
                        db, variant_ids=[v.id], location_id=store.sync_location_id, new_quantity=target)
                    authoritative, mode = target, "delta-cas"
                elif _is_stale_compare(ue):
                    # Mirror drifted / a concurrent change moved it. READ Shopify's true current and
                    # retry the compare-and-set at (C+delta) so we can STILL anchor (and heal the
                    # drift). Bounded retries; only sustained concurrent change exhausts them.
                    healed = False
                    for _attempt in range(2):
                        true_cur = service.get_available_single(item_gid, location_gid)
                        if true_cur is None:
                            break
                        new_target = max(true_cur + delta, sync_guards.INVENTORY_FLOOR)
                        raw_r, ue_r = service.set_inventory_quantities_single(
                            item_gid, location_gid, new_target, reference_uri=ref_uri, compare_quantity=true_cur)
                        if not ue_r:
                            crud_product.update_inventory_levels_for_variants(
                                db, variant_ids=[v.id], location_id=store.sync_location_id, new_quantity=new_target)
                            authoritative, mode = new_target, "delta-cas-retry"
                            healed = True
                            break
                        if not _is_stale_compare(ue_r):
                            raise Exception(str(ue_r))  # a real error, not a stale-compare
                        # else: stale again (another concurrent change) -> loop and re-read
                    if not healed:
                        # Ultimate backstop: relative adjust (drift-safe), value-INDEPENDENT marker.
                        raw2, _a = service.adjust_inventory_quantities_single(item_gid, location_gid, delta, reference_uri=ref_uri)
                        ue2 = (raw2 or {}).get("inventoryAdjustQuantities", {}).get("userErrors", [])
                        if ue2:
                            raise Exception(str(ue2))
                        crud_product.adjust_inventory_levels_for_variants(
                            db, variant_ids=[v.id], location_id=store.sync_location_id, delta=delta)
                        mode = "delta-fallback"
                else:
                    raise Exception(str(ue))
            else:
                # No baseline to compare against -> relative adjust, value-independent marker.
                raw2, _a = service.adjust_inventory_quantities_single(item_gid, location_gid, delta, reference_uri=ref_uri)
                ue2 = (raw2 or {}).get("inventoryAdjustQuantities", {}).get("userErrors", [])
                if ue2:
                    raise Exception(str(ue2))
                crud_product.adjust_inventory_levels_for_variants(
                    db, variant_ids=[v.id], location_id=store.sync_location_id, delta=delta)
                mode = "delta-nobaseline"

            if authoritative is not None:
                marker.authoritative_qty = authoritative
            db.commit()

            audit_logger.log_propagation(
                barcode=barcode, source_store="webhook", target_store=store.name, quantity=new_source_qty,
                details={"variant_count": 1, "delta": delta, "mode": mode,
                         "floored": 1 if clamped else 0, "authoritative_qty": authoritative,
                         "sync_operation_uuid": sync_op, "origin_store_id": origin_store_id})
        except Exception as e:
            db.rollback()
            print(f"[SYNC-ERROR] Failed single-item propagate to '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._propagate_delta_single_item",
                                   f"Failed to propagate barcode {barcode} to store '{store.name}'",
                                   details={"barcode": barcode, "delta": delta}, exc=e)


def _execute_delta_propagation(
    db: Session,
    barcode: str,
    delta: int,
    new_source_qty: int,
    target_stores: List[models.Store],
    store_variant_map: Dict[int, List[models.ProductVariant]],
    sync_op: str,
    origin_store_id: int,
    origin_item_id: Optional[int],
):
    """
    DELTA-based propagation to the canonical target variant of each store.

    P0.4 floor: before each write we project current+delta; if it would breach the floor we
    SET that item to the floor (absolute) instead of applying the raw negative delta, and alert.
    P0.2 lineage: each write records a value-independent echo marker carrying the sync op + origin.
    """
    store_lookup = {s.id: s for s in target_stores}
    ref_uri = f"inventory-sync://op/{sync_op}"

    for sid, variants_to_update in store_variant_map.items():
        store = store_lookup.get(sid)
        if not store or not store.sync_location_id:
            if store:
                print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue

        location_gid = f"gid://shopify/Location/{store.sync_location_id}"

        # SYNC_ECHO_AUTHORITATIVE: write each item via its own single-item mutation so the
        # Shopify-authoritative post-write quantity is attributable and stamped on the marker.
        if sync_guards.echo_authoritative_for(barcode):
            _propagate_delta_single_item(db, barcode, delta, new_source_qty, store, location_gid,
                                         variants_to_update, ref_uri, sync_op, origin_store_id, origin_item_id)
            continue

        adjust_payload, adjust_ids = [], []
        set_payload, set_ids = [], []

        # Build per-variant operations (markers created BEFORE the API call), applying the floor.
        try:
            for v in variants_to_update:
                if not v.inventory_item_id:
                    continue
                level = db.query(models.InventoryLevel).filter(
                    models.InventoryLevel.variant_id == v.id,
                    models.InventoryLevel.location_id == store.sync_location_id,
                ).first()
                current = level.available if (level and level.available is not None) else None
                op, value, clamped = sync_guards.apply_floor(current, delta)
                expected = value if clamped else ((current + delta) if current is not None else None)

                _create_echo_marker(db, barcode, store.id, v.inventory_item_id, expected,
                                    sync_op, origin_store_id, origin_item_id, depth=1)

                item_gid = f"gid://shopify/InventoryItem/{v.inventory_item_id}"
                if clamped:
                    set_payload.append({"inventoryItemId": item_gid, "locationId": location_gid, "quantity": value})
                    set_ids.append(v.id)
                    alerting.warning("inventory_sync.floor",
                                     f"Floored {barcode} on '{store.name}' to {value} (delta {delta} would breach floor)",
                                     {"barcode": barcode, "store": store.name, "current": current,
                                      "delta": delta, "floored_to": value})
                    audit_logger.log(category="STOCK", action="inventory_floor_clamp",
                                     message=f"Floored [{barcode}] on '{store.name}' to {value} (would have been {(current or 0)+delta})",
                                     store_id=store.id, target=barcode, severity="WARN",
                                     details={"current": current, "delta": delta, "floored_to": value,
                                              "sync_operation_uuid": sync_op})
                else:
                    adjust_payload.append({"inventoryItemId": item_gid, "locationId": location_gid, "delta": delta})
                    adjust_ids.append(v.id)
            db.commit()
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Could not stage propagation for store {store.name}: {e}")
            continue

        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)

            if adjust_payload:
                result = service.adjust_inventory_quantities(adjust_payload, reference_uri=ref_uri)
                ue = result.get("inventoryAdjustQuantities", {}).get("userErrors", [])
                if ue:
                    raise Exception(str(ue))
                crud_product.adjust_inventory_levels_for_variants(
                    db, variant_ids=adjust_ids, location_id=store.sync_location_id, delta=delta
                )

            if set_payload:
                result = service.set_inventory_quantities(set_payload, reference_uri=ref_uri, ignore_compare=True)
                ue = result.get("inventorySetQuantities", {}).get("userErrors", [])
                if ue:
                    raise Exception(str(ue))
                # Each clamped item was set to the floor value.
                for vid, item in zip(set_ids, set_payload):
                    crud_product.update_inventory_levels_for_variants(
                        db, variant_ids=[vid], location_id=store.sync_location_id, new_quantity=item["quantity"]
                    )

            print(f"[SYNC] Propagated {barcode} to '{store.name}' (adjust={len(adjust_payload)}, floor-set={len(set_payload)}).")
            audit_logger.log_propagation(
                barcode=barcode, source_store="webhook", target_store=store.name,
                quantity=new_source_qty,
                details={"variant_count": len(adjust_payload) + len(set_payload), "delta": delta,
                         "mode": "delta", "floored": len(set_payload),
                         "sync_operation_uuid": sync_op, "origin_store_id": origin_store_id},
            )
        except Exception as e:
            print(f"[SYNC-ERROR] Failed to propagate to store '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._execute_delta_propagation",
                                   f"Failed to propagate barcode {barcode} to store '{store.name}'",
                                   details={"barcode": barcode, "delta": delta}, exc=e)


def _execute_absolute_propagation(
    db: Session,
    barcode: str,
    desired_total: int,
    target_stores: List[models.Store],
    store_variant_map: Dict[int, List[models.ProductVariant]],
    sync_op: str,
    origin_store_id: int,
    origin_item_id: Optional[int],
):
    """
    ABSOLUTE propagation (first-time sync, no baseline). SETs the canonical target of each
    store to desired_total (floored), recording value-independent echo markers + lineage.
    """
    store_lookup = {s.id: s for s in target_stores}
    ref_uri = f"inventory-sync://op/{sync_op}"
    value = max(desired_total, sync_guards.INVENTORY_FLOOR)

    for sid, variants_to_update in store_variant_map.items():
        store = store_lookup.get(sid)
        if not store or not store.sync_location_id:
            if store:
                print(f"[SYNC-ERROR] Cannot propagate to store '{store.name}': No sync location configured.")
            continue

        location_gid = f"gid://shopify/Location/{store.sync_location_id}"
        quantities_payload, variant_ids = [], []
        try:
            # For an absolute SET the post-write `available` IS `value`, so it is itself the
            # authoritative echo anchor (no single-item capture needed) when the flag is on.
            abs_auth = value if sync_guards.echo_authoritative_for(barcode) else None
            for v in variants_to_update:
                if not v.inventory_item_id:
                    continue
                _create_echo_marker(db, barcode, store.id, v.inventory_item_id, value,
                                    sync_op, origin_store_id, origin_item_id, depth=1,
                                    authoritative_qty=abs_auth)
                quantities_payload.append({
                    "inventoryItemId": f"gid://shopify/InventoryItem/{v.inventory_item_id}",
                    "locationId": location_gid, "quantity": value,
                })
                variant_ids.append(v.id)
            db.commit()
        except Exception as e:
            db.rollback()
            print(f"[SYNC-WARN] Could not stage absolute propagation for store {store.name}: {e}")
            continue

        if not quantities_payload:
            continue

        try:
            service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
            result = service.set_inventory_quantities(quantities_payload, reference_uri=ref_uri, ignore_compare=True)
            if result.get("inventorySetQuantities", {}).get("userErrors"):
                raise Exception(str(result["inventorySetQuantities"]["userErrors"]))

            print(f"[SYNC] Set qty {value} for barcode {barcode} on store '{store.name}' ({len(quantities_payload)} variants).")
            audit_logger.log_propagation(
                barcode=barcode, source_store="webhook", target_store=store.name, quantity=value,
                details={"variant_count": len(quantities_payload), "mode": "absolute",
                         "sync_operation_uuid": sync_op, "origin_store_id": origin_store_id},
            )
            crud_product.update_inventory_levels_for_variants(
                db, variant_ids=variant_ids, location_id=store.sync_location_id, new_quantity=value
            )
        except Exception as e:
            print(f"[SYNC-ERROR] Failed to write to store '{store.name}': {e}")
            audit_logger.log_error("inventory_sync_service._execute_absolute_propagation",
                                   f"Failed to write barcode {barcode} to store '{store.name}'",
                                   details={"barcode": barcode, "quantity": value}, exc=e)


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

        expired_breakers = db.query(models.BarcodeCircuitBreaker).filter(
            models.BarcodeCircuitBreaker.expires_at < now
        ).delete(synchronize_session=False)

        db.commit()

        if expired_webhooks > 0 or expired_intents > 0 or expired_breakers > 0:
            print(f"[CLEANUP] Removed {expired_webhooks} webhooks, {expired_intents} intents, {expired_breakers} breakers.")

        cleanup_barcode_locks()

    except Exception as e:
        db.rollback()
        print(f"[CLEANUP-ERROR] Failed to clean up expired records: {e}")
    finally:
        db.close()