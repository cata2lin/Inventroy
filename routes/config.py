# routes/config.py

from fastapi import APIRouter, Depends, HTTPException
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
    """
    Retrieves a list of all configured stores from the database.
    """
    return crud_store.get_stores(db)

@router.post("/stores", response_model=schemas.Store)
def add_store(store: schemas.StoreCreate, db: Session = Depends(get_db)):
    """
    Adds a new Shopify store to the database.
    """
    db_store = db.query(models.Store).filter(models.Store.name == store.name).first()
    if db_store:
        raise HTTPException(status_code=400, detail="A store with this name already exists.")
    return crud_store.create_store(db=db, store=store)