# schemas.py

from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from datetime import datetime

class StoreBase(BaseModel):
    name: str
    shopify_url: str
    api_token: str
    api_secret: Optional[str] = None
    webhook_secret: Optional[str] = None

class StoreCreate(StoreBase):
    pass

class Store(StoreBase):
    id: int
    model_config = ConfigDict(from_attributes=True)

class Webhook(BaseModel):
    id: int
    shopify_webhook_id: int
    store_id: int
    topic: str
    address: str
    created_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

# --- NEW SCHEMAS ---
class ProductVariant(BaseModel):
    id: int
    title: str
    sku: Optional[str]
    barcode: Optional[str]
    price: Optional[float]
    inventory_quantity: Optional[int]
    model_config = ConfigDict(from_attributes=True)

class Product(BaseModel):
    id: int
    title: str
    image_url: Optional[str]
    status: Optional[str]
    variants: List[ProductVariant] = []
    model_config = ConfigDict(from_attributes=True)

class ProductResponse(BaseModel):
    total_count: int
    products: List[Product]