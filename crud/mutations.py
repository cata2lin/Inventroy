# crud/mutations.py

from sqlalchemy.orm import Session, joinedload
import models

def get_product_by_id(db: Session, product_id: int):
    """
    Fetches a single product by its primary key ID, including its variants.
    """
    return db.query(models.Product)\
        .filter(models.Product.id == product_id)\
        .options(joinedload(models.Product.variants))\
        .first()