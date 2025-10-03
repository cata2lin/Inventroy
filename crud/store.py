from typing import List, Optional
from sqlalchemy.orm import Session
import models
import schemas

def get_store(db: Session, store_id: int) -> Optional[models.Store]:
    return db.query(models.Store).filter(models.Store.id == store_id).first()

def get_all_stores(db: Session) -> List[models.Store]:
    return db.query(models.Store).order_by(models.Store.id.asc()).all()

def get_enabled_stores(db: Session) -> List[models.Store]:
    return db.query(models.Store).filter(models.Store.enabled == True).order_by(models.Store.id.asc()).all()

def create_store(db: Session, store: schemas.StoreCreate) -> models.Store:
    db_store = models.Store(**store.dict())
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return db_store