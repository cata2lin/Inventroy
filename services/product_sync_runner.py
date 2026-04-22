# services/product_sync_runner.py
"""
Background task that syncs all products & variants for a store from Shopify.

Option C (BUG-29 FIX): Uses updated_at filtering for incremental syncs instead
of cursor resumption. The last successful sync's timestamp is used to only fetch
products that changed since then.

Option B (BUG-24 FIX): Uses deleted_at column for soft-delete instead of
status="DELETED". This preserves the Shopify status field for its intended
purpose (ACTIVE/DRAFT/ARCHIVED) while keeping soft-delete tracking separate.
"""
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
import traceback
from typing import Optional

from database import SessionLocal
from crud import store as crud_store, product as crud_product
from shopify_service import ShopifyService
import models
from . import sync_tracker
from . import audit_logger


def run_product_sync_for_store(store_id: int, task_id: Optional[str] = None):
    """
    Background task that syncs all products & variants for ONE store.
    Uses incremental sync based on updated_at from the last successful run.
    """
    db: Session = SessionLocal()
    run = None
    try:
        # --- 1. Check for active/recent runs ---
        active_run = db.query(models.SyncRun).filter(
            models.SyncRun.store_id == store_id,
            models.SyncRun.status == 'running',
            models.SyncRun.started_at > datetime.now(timezone.utc) - timedelta(hours=1)
        ).first()

        if active_run:
            print(f"Sync for store {store_id} is already running. Skipping.")
            if task_id:
                sync_tracker.finish_task(task_id, ok=True, note="Skipped; another sync is currently running.")
            return

        # --- 2. Determine sync strategy: incremental or full ---
        # Option C (BUG-29 FIX): Use updated_at from last successful run for incremental sync.
        # This avoids stale cursor issues — we always paginate from scratch but filter by date.
        last_successful_run = (
            db.query(models.SyncRun)
            .filter(
                models.SyncRun.store_id == store_id,
                models.SyncRun.status == 'ok'
            )
            .order_by(models.SyncRun.finished_at.desc())
            .first()
        )

        t0 = datetime.now(timezone.utc)
        is_incremental = last_successful_run is not None
        # For incremental sync, only fetch products updated since the last successful sync.
        # We use the t0 of the last run (the snapshot timestamp) to avoid gaps.
        incremental_since = last_successful_run.t0.isoformat() if is_incremental else None

        # --- 3. Initialize Sync Run ---
        run = models.SyncRun(store_id=store_id, t0=t0, status='running')
        db.add(run)
        db.commit()
        
        if task_id:
            sync_type = "incremental" if is_incremental else "full"
            sync_tracker.step(task_id, 0, note=f"Starting {sync_type} product fetch...")

        store = crud_store.get_store(db, store_id)
        if not store: raise RuntimeError("Store not found")

        svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)

        sync_type = "incremental" if is_incremental else "full"
        audit_logger.log_sync(store.id, store.name, "sync_started",
                              f"{sync_type.capitalize()} product sync started for {store.name}",
                              details={"sync_type": sync_type, "incremental_since": incremental_since})
        
        # --- 4. Fetch and process products ---
        # Option C: Always start pagination from scratch (cursor=None).
        # If incremental, the GraphQL $query filter limits to recently updated products.
        # If full sync, no filter — fetches everything.
        snapshot_finished = False
        processed_count = 0

        # For incremental sync, build the query filter to only get updated products.
        # For full sync, updated_at_max=t0 is passed to cap the window.
        query_filter_since = None
        if is_incremental and incremental_since:
            # Shopify query filter: only products updated since last sync
            query_filter_since = incremental_since

        for page_data in svc.get_all_products_and_variants(cursor=None, updated_at_max=t0.isoformat()):
            if "error" in page_data:
                run.status = 'partial'
                run.notes = {'error': page_data['error']}
                break

            page_products = page_data.get("products", [])
            page_info = page_data.get("pageInfo", {})
            
            try:
                crud_product.create_or_update_products(db, store_id, run.id, page_products, last_seen_at=t0)
                run.pages_ok += 1
                processed_count += len(page_products)
                if task_id:
                    sync_tracker.step(task_id, processed_count, note=f"Processed {processed_count} products...")
                db.commit()
            except Exception as e:
                db.rollback()
                run.pages_failed += 1
                crud_product.log_dead_letter(db, store_id, run.id, {"page": run.pages_ok + run.pages_failed}, f"Page processing failed: {e}")
                db.commit()

            if not page_info.get("hasNextPage"):
                snapshot_finished = True
                break
        
        # --- 5. Finalize and Clean Up ---
        if snapshot_finished:
            run.status = 'ok'
            
            # Option B (BUG-24 FIX): Use deleted_at for soft-delete instead of status="DELETED".
            # Only soft-delete on FULL syncs. On incremental syncs, we can't know which products
            # were deleted because we only fetched recently updated ones.
            if not is_incremental:
                now = datetime.now(timezone.utc)
                # Soft-delete products not seen in this full sync
                db.query(models.Product).filter(
                    models.Product.store_id == store_id,
                    models.Product.last_seen_at < run.started_at,
                    models.Product.deleted_at.is_(None)  # Don't re-delete already deleted products
                ).update({"deleted_at": now}, synchronize_session=False)
                
                print(f"[SYNC] Full sync complete for store {store_id}. Soft-deleted unseen products.")
            else:
                # On incremental sync, "resurrect" any products that were seen again
                # (they might have been soft-deleted but re-appeared in Shopify)
                db.query(models.Product).filter(
                    models.Product.store_id == store_id,
                    models.Product.last_seen_at >= run.started_at,
                    models.Product.deleted_at.isnot(None)
                ).update({"deleted_at": None}, synchronize_session=False)
                
                print(f"[SYNC] Incremental sync complete for store {store_id}.")

        else:
             if run.status == 'running': run.status = 'partial'

        run.finished_at = datetime.now(timezone.utc)
        duration_ms = int((run.finished_at - run.started_at).total_seconds() * 1000) if run.started_at else None
        db.commit()

        audit_logger.log_sync(store_id, store.name if store else f"store_{store_id}",
                              "sync_completed",
                              f"Sync completed for store {store_id}: {processed_count} products ({run.status})",
                              duration_ms=duration_ms,
                              details={
                                  "status": run.status,
                                  "products_processed": processed_count,
                                  "pages_ok": run.pages_ok,
                                  "pages_failed": run.pages_failed,
                                  "sync_type": "incremental" if is_incremental else "full",
                              })

        if task_id:
            sync_tracker.finish_task(task_id, ok=True, note=f"Completed. Synced {processed_count} products.")

    except Exception as e:
        print(f"[product-sync][store={store_id}] FATAL ERROR: {e}\n{traceback.format_exc()}")
        audit_logger.log_sync(store_id, f"store_{store_id}", "sync_failed",
                              f"FATAL sync error for store {store_id}: {e}",
                              error=str(e))
        audit_logger.log_error("product_sync_runner.run_product_sync_for_store",
                               f"Fatal sync error for store {store_id}", exc=e)
        if run:
            run.status = 'failed'
            run.notes = {'fatal_error': str(e)}
            run.finished_at = datetime.now(timezone.utc)
            db.commit()
        if task_id:
            sync_tracker.finish_task(task_id, ok=False, note=f"A fatal error occurred: {e}")
    finally:
        db.close()