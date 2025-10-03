from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import schemas
import models
from database import get_db
from crud import store as crud_store

router = APIRouter(prefix="/api/config", tags=["Configuration"])

@router.get("/stores", response_model=List[schemas.Store])
def get_all_stores(db: Session = Depends(get_db)):
    return crud_store.get_all_stores(db)

@router.post("/stores", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    if db.query(models.Store).filter(models.Store.name == store.name).first():
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    return crud_store.create_store(db=db, store=store)