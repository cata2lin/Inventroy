# services/product_sync_runner.py
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
import traceback

from database import SessionLocal
from crud import store as crud_store, product as crud_product
from shopify_service import ShopifyService
import models

def run_product_sync_for_store(store_id: int):
    db: Session = SessionLocal()
    run = None
    try:
        # --- 1. Check for an active run for this store ---
        # (Simplified locking, a robust system would use pg_try_advisory_lock)
        active_run = db.query(models.SyncRun).filter(
            models.SyncRun.store_id == store_id,
            models.SyncRun.status.in_(['running', 'partial'])
        ).first()

        if active_run and active_run.started_at > datetime.now(timezone.utc) - timedelta(hours=1):
            print(f"Sync for store {store_id} is already running or recently failed. Skipping.")
            return

        # --- 2. Initialize Sync Run ---
        t0 = datetime.now(timezone.utc)
        run = models.SyncRun(store_id=store_id, t0=t0, status='running')
        db.add(run)
        db.commit()

        store = crud_store.get_store(db, store_id)
        if not store: raise RuntimeError("Store not found")

        svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        
        # --- 3. Run Snapshot Sync ---
        cursor = active_run.last_cursor if active_run else None
        snapshot_finished = False
        for page_data in svc.get_all_products_and_variants(cursor=cursor, updated_at_max=t0.isoformat()):
            if "error" in page_data:
                run.status = 'partial'
                run.notes = {'error': page_data['error']}
                break

            page_products = page_data.get("products", [])
            page_info = page_data.get("pageInfo", {})
            
            try:
                crud_product.create_or_update_products(db, store_id, run.id, page_products, last_seen_at=t0)
                run.pages_ok += 1
                run.last_cursor = page_info.get("endCursor")
                db.commit() # Commit after each successful page
            except Exception as e:
                db.rollback()
                run.pages_failed += 1
                crud_product.log_dead_letter(db, store_id, run.id, {"page_cursor": cursor}, f"Page processing failed: {e}")
                db.commit()

            if not page_info.get("hasNextPage"):
                snapshot_finished = True
                break
        
        # --- 4. Finalize and Clean Up ---
        if snapshot_finished:
            run.status = 'ok'
            run.last_cursor = None # Clear cursor on successful completion

            # Soft-delete products not seen in this run
            db.query(models.Product).filter(
                models.Product.store_id == store_id,
                models.Product.last_seen_at < run.started_at
            ).update({"status": "DELETED"})
            
            db.query(models.ProductVariant).filter(
                models.ProductVariant.store_id == store_id,
                models.ProductVariant.last_seen_at < run.started_at
            ).update({"is_primary_variant": False}) # Or another status flag

        else:
             if run.status == 'running': run.status = 'partial'

        run.finished_at = datetime.now(timezone.utc)
        db.commit()

    except Exception as e:
        print(f"[product-sync][store={store_id}] FATAL ERROR: {e}\n{traceback.format_exc()}")
        if run:
            run.status = 'failed'
            run.notes = {'fatal_error': str(e)}
            run.finished_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()