# routes/mutations.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List

from database import get_db
from product_service import ProductService
from crud import mutations as crud_mutations, store as crud_store

router = APIRouter(
    prefix="/api/mutations",
    tags=["Product Mutations"],
    responses={404: {"description": "Not found"}},
)

# --- Pydantic Models ---
class ProductUpdatePayload(BaseModel):
    title: Optional[str] = None
    bodyHtml: Optional[str] = None
    vendor: Optional[str] = None
    productType: Optional[str] = None
    status: Optional[str] = None
    tags: Optional[str] = None # Trimis ca un șir de caractere separat prin virgulă

class ProductResponse(BaseModel):
    id: int
    shopify_gid: str
    store_id: int
    title: str
    body_html: Optional[str]
    vendor: Optional[str]
    product_type: Optional[str]
    status: Optional[str]
    tags: Optional[str]

    class Config:
        from_attributes = True

# --- API Endpoints ---
@router.get("/product/{product_id}", response_model=ProductResponse)
def get_product_details(product_id: int, db: Session = Depends(get_db)):
    """
    Preia toate detaliile editabile pentru un singur produs din baza de date.
    """
    product = crud_mutations.get_product_by_id(db, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

@router.post("/product/{product_id}", status_code=200)
def update_product_details(product_id: int, payload: ProductUpdatePayload, db: Session = Depends(get_db)):
    """
    Primește actualizări ale produsului, le trimite către ProductService pentru a executa
    mutația Shopify și returnează rezultatul.
    """
    product_db = crud_mutations.get_product_by_id(db, product_id)
    if not product_db:
        raise HTTPException(status_code=404, detail="Product not found in local database.")

    store = crud_store.get_store(db, store_id=product_db.store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Associated store not found.")

    service = ProductService(store_url=store.shopify_url, token=store.api_token)

    # Pregătește datele pentru API-ul Shopify
    update_input = payload.dict(exclude_unset=True)

    # --- CORECTAT: Gestionare îmbunătățită a etichetelor ---
    if 'tags' in update_input:
        # Procesează etichetele doar dacă câmpul nu este gol
        if update_input['tags'] and update_input['tags'].strip():
            # Creează o listă de etichete, eliminând spațiile goale și etichetele goale
            tags_list = [tag.strip() for tag in update_input['tags'].split(',') if tag.strip()]
            if tags_list:
                update_input['tags'] = tags_list
            else:
                # Dacă lista este goală după procesare, șterge cheia pentru a nu trimite o listă goală
                del update_input['tags']
        else:
            # Dacă șirul de etichete este gol sau conține doar spații, șterge cheia
            del update_input['tags']

    try:
        response = service.update_product(product_gid=product_db.shopify_gid, product_input=update_input)
        
        return {"message": "Product updated successfully in Shopify.", "response": response}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")