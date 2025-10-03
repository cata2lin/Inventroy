# schemas.py

from pydantic import BaseModel, ConfigDict
from typing import Optional
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
    
    # Updated to Pydantic v2 style
    model_config = ConfigDict(from_attributes=True)


# --- ADD THIS NEW CLASS ---
class Webhook(BaseModel):
    id: int
    shopify_webhook_id: int
    store_id: int
    topic: str
    address: str
    created_at: Optional[datetime] = None

    # This allows the model to be created from a database object
    model_config = ConfigDict(from_attributes=True)