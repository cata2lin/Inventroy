# crud/store.py

from typing import List, Optional
from sqlalchemy.orm import Session
import models


def get_store(db: Session, store_id: int) -> Optional[models.Store]:
    """Fetch a single store by numeric id."""
    return (
        db.query(models.Store)
        .filter(models.Store.id == store_id)
        .first()
    )


def get_store_by_domain(db: Session, shopify_url: str) -> Optional[models.Store]:
    """Fetch a store by its shop domain (e.g., 'yourstore.myshopify.com')."""
    return (
        db.query(models.Store)
        .filter(models.Store.shopify_url == shopify_url)
        .first()
    )


def get_all_stores(db: Session) -> List[models.Store]:
    """Return all stores (enabled or not)."""
    return (
        db.query(models.Store)
        .order_by(models.Store.id.asc())
        .all()
    )


def get_enabled_stores(db: Session) -> List[models.Store]:
    """Return only stores flagged as enabled, if the column exists."""
    q = db.query(models.Store)
    # Be tolerant if older schemas don’t have the column.
    try:
        q = q.filter(models.Store.enabled == True)  # noqa: E712
    except Exception:
        pass
    return q.order_by(models.Store.id.asc()).all()


# Backwards-compat alias some codebases expect.
get_stores = get_all_stores
