from fastapi import APIRouter, Depends, HTTPException, Response, Request
from sqlalchemy.orm import Session
from typing import List

import schemas
import models
from database import get_db
from crud import store as crud_store

router = APIRouter(
    prefix="/api/config",
    tags=["Configuration"],
    responses={404: {"description": "Not found"}},
)

@router.get("/stores", response_model=List[schemas.Store])
def get_all_stores(db: Session = Depends(get_db)):
    return crud_store.get_all_stores(db)

@router.post("/stores", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    db_store = db.query(models.Store).filter(models.Store.name == store.name).first()
    if db_store:
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    
    new_store = models.Store(**store.dict())
    db.add(new_store)
    db.commit()
    db.refresh(new_store)
    return new_store