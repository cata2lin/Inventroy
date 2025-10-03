from typing import List, Optional
from sqlalchemy.orm import Session
import models

def get_store(db: Session, store_id: int) -> Optional[models.Store]:
    """Fetch a single store by numeric id."""
    return db.query(models.Store).filter(models.Store.id == store_id).first()

def get_all_stores(db: Session) -> List[models.Store]:
    """Return all stores."""
    return db.query(models.Store).order_by(models.Store.id.asc()).all()

def get_enabled_stores(db: Session) -> List[models.Store]:
    """Return only stores flagged as enabled."""
    return db.query(models.Store).filter(models.Store.enabled == True).order_by(models.Store.id.asc()).all()