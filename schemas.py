from pydantic import BaseModel
from typing import Optional

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

    class Config:
        orm_mode = True