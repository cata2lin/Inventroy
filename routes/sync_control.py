# routes/sync_control.py

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# Background runner + lightweight in-memory tracker
from services.product_sync_runner import run_product_sync_for_store
from services import sync_tracker


router = APIRouter(prefix="/api", tags=["Sync Control"])


def _get_all_stores(db: Session):
    """
    Try CRUD helpers if present; otherwise fall back to a direct query.
    This avoids AttributeError: crud.store has no attribute 'get_all_stores'.
    """
    try:
        from crud import store as crud_store  # prefer your CRUD module if available
        for fn_name in ("get_all_stores", "get_stores", "list_all", "list_stores"):
            fn = getattr(crud_store, fn_name, None)
            if callable(fn):
                return fn(db)
    except Exception:
        # If the CRUD import or call fails, just use the ORM directly.
        pass

    # Fallback: direct ORM
    return db.query(models.Store).all()


@router.post("/sync-control/products")
def trigger_product_sync(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Enqueue a background product sync for every enabled store.
    Returns 200 with task ids even if one store fails to enqueue.
    """
    stores = _get_all_stores(db)
    task_ids = []

    for store in stores or []:
        # Skip disabled stores
        if not getattr(store, "enabled", True):
            continue

        # Create a small progress handle and enqueue the job
        task_id = sync_tracker.create_task(f"Products for {store.name}")
        task_ids.append(task_id)

        background_tasks.add_task(
            run_product_sync_for_store,
            db_factory=SessionLocal,          # pass a factory; runner owns its own Session
            store_id=store.id,
            shop_url=store.shopify_url,
            api_token=store.api_token,
            task_id=task_id,
        )

    return {"status": "ok", "tasks": task_ids}
