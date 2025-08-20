# crud/store.py

from sqlalchemy.orm import Session
import models
import schemas

def create_store(db: Session, store: schemas.StoreCreate):
    db_store = models.Store(**store.dict())
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return db_store

def get_store(db: Session, store_id: int):
    return db.query(models.Store).filter(models.Store.id == store_id).first()

def get_stores(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Store).order_by(models.Store.name).offset(skip).limit(limit).all()

def update_store(db: Session, store_id: int, store_update: schemas.StoreUpdate):
    db_store = get_store(db, store_id)
    if not db_store:
        return None
    
    update_data = store_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_store, key, value)
        
    db.commit()
    db.refresh(db_store)
    return db_store