# schemas.py

from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Dict, Any
from datetime import datetime

# --- Schemas for Parsing Shopify API Response ---
class MoneySet(BaseModel):
    amount: float
    currency_code: str = Field(..., alias="currencyCode")

class UnitCost(BaseModel):
    amount: float

class Location(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    name: str

class InventoryLevel(BaseModel):
    quantities: List[Dict[str, Any]]
    location: Location
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

class InventoryItem(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    sku: Optional[str] = None
    unit_cost: Optional[UnitCost] = Field(None, alias="unitCost")
    tracked: Optional[bool] = None
    inventory_levels: List[InventoryLevel] = Field([], alias="inventoryLevels")

class ProductCategory(BaseModel):
    name: str

class FeaturedImage(BaseModel):
    url: HttpUrl

class Product(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    title: str
    body_html: Optional[str] = Field(None, alias="bodyHtml")
    vendor: Optional[str] = None
    product_type: Optional[str] = Field(None, alias="productType")
    category: Optional[ProductCategory] = None
    created_at: datetime = Field(..., alias="createdAt")
    handle: str
    updated_at: datetime = Field(..., alias="updatedAt")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    status: str
    tags: List[str]
    featured_image: Optional[FeaturedImage] = Field(None, alias="featuredImage")

class ProductVariant(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    product: Optional[Product] = None
    title: str
    price: float
    sku: Optional[str] = None
    position: int
    inventory_policy: str = Field(..., alias="inventoryPolicy")
    inventory_management: Optional[str] = Field(None, alias="inventoryManagement")
    compare_at_price: Optional[float] = Field(None, alias="compareAtPrice")
    cost: Optional[float] = None
    barcode: Optional[str] = None
    inventory_item: InventoryItem = Field(..., alias="inventoryItem")
    inventory_quantity: Optional[int] = Field(None, alias="inventoryQuantity")
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")

class LineItem(BaseModel):
    id: str
    title: str
    quantity: int
    sku: Optional[str] = None
    vendor: Optional[str] = None
    taxable: bool
    price: Optional[MoneySet] = Field(None, alias="originalUnitPriceSet")
    total_discount: Optional[MoneySet] = Field(None, alias="totalDiscountSet")
    variant: Optional[ProductVariant] = None

class FulfillmentEvent(BaseModel):
    id: str
    status: str
    happened_at: datetime = Field(..., alias="happenedAt")
    description: Optional[str] = None

class Fulfillment(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    status: str
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    tracking_company: Optional[str] = None
    tracking_number: Optional[str] = None
    tracking_url: Optional[HttpUrl] = None
    events: List[FulfillmentEvent] = []

class ShopifyOrder(BaseModel):
    id: str
    legacy_resource_id: int = Field(..., alias="legacyResourceId")
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    cancelled_at: Optional[datetime] = Field(None, alias="cancelledAt")
    cancel_reason: Optional[str] = Field(None, alias="cancelReason")
    closed_at: Optional[datetime] = Field(None, alias="closedAt")
    processed_at: Optional[datetime] = Field(None, alias="processedAt")
    financial_status: Optional[str] = Field(None, alias="displayFinancialStatus")
    fulfillment_status: str = Field(..., alias="displayFulfillmentStatus")
    currency: str = Field(..., alias="currencyCode")
    paymentGatewayNames: Optional[List[str]] = [] # <-- FIXED: Added the missing field
    total_price: MoneySet = Field(..., alias="totalPriceSet")
    subtotal_price: Optional[MoneySet] = Field(None, alias="subtotalPriceSet")
    total_tax: Optional[MoneySet] = Field(None, alias="totalTaxSet")
    total_discounts: MoneySet = Field(..., alias="totalDiscountsSet")
    total_shipping_price: MoneySet = Field(..., alias="totalShippingPriceSet")
    note: Optional[str] = None
    tags: List[str]
    line_items: List[LineItem] = Field(..., alias="lineItems")
    fulfillments: List[Fulfillment] = []

# --- Schemas for Internal Application API ---
class Order(BaseModel):
    name: str
    created_at: datetime
    email: Optional[str]
    total_price: float
    currency: str
    financial_status: Optional[str]
    fulfillment_status: Optional[str]
    class Config: from_attributes = True

class Fulfillment(BaseModel):
    id: int
    order_name: str
    created_at: datetime
    tracking_company: Optional[str]
    tracking_number: Optional[str]
    status: str
    class Config: from_attributes = True

class Inventory(BaseModel):
    product_title: str
    variant_title: str
    sku: Optional[str]
    inventory_policy: str
    available_quantity: Optional[int]
    location_name: str
    class Config: from_attributes = True

class StoreBase(BaseModel):
    name: str
    shopify_url: str

class StoreCreate(StoreBase):
    api_token: str

class Store(StoreBase):
    id: int
    created_at: datetime
    class Config: from_attributes = True