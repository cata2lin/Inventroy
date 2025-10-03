# schemas.py
from __future__ import annotations

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl, ConfigDict, field_validator

# =========================
# Base model configurations
# =========================

class ORMBase(BaseModel):
    """Base for models mapped to SQLAlchemy objects."""
    model_config = ConfigDict(from_attributes=True)

class APIBase(BaseModel):
    """Base for models mapped to external API payloads."""
    model_config = ConfigDict(populate_by_name=True, extra="allow")

# ======================================================
# App-specific schemas (for API responses)
# ======================================================

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

# --- CORRECTED Schemas for the mutations page and product views ---

class Location(ORMBase):
    id: int
    shopify_gid: Optional[str] = None # GID can be nullable if sync fails
    name: Optional[str] = None

class InventoryLevel(ORMBase):
    location_id: int
    available: Optional[int] = None
    location: Location

class ProductVariant(ORMBase):
    id: int
    shopify_gid: str
    inventory_item_id: Optional[int] = None
    inventory_item_gid: Optional[str] = None # This field will be created by the validator below
    title: Optional[str] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None
    compare_at_price: Optional[float] = None
    cost_per_item: Optional[float] = None
    inventory_quantity: Optional[int] = None
    inventory_levels: List[InventoryLevel] = []

    # This validator is the key fix. It correctly constructs the full GID
    # that the Shopify API needs for inventory mutations.
    @field_validator("inventory_item_gid", mode="before")
    @classmethod
    def assemble_inventory_item_gid(cls, v, values):
        # Access the raw data from the ORM model to get the inventory_item_id
        inventory_item_id = values.data.get('inventory_item_id')
        if inventory_item_id:
            return f"gid://shopify/InventoryItem/{inventory_item_id}"
        return None # Return None if there's no ID

class Product(ORMBase):
    id: int
    shopify_gid: str
    store_id: int
    title: str
    product_type: Optional[str] = None
    image_url: Optional[str] = None
    status: Optional[str] = None
    variants: List[ProductVariant] = []

class ProductResponse(BaseModel):
    total_count: int
    products: List[Product]


# ======================================================
# Shopify GraphQL Ingest Models (from previous working version)
# ======================================================

class Money(APIBase):
    amount: Optional[float] = None
    currency_code: Optional[str] = Field(None, alias="currencyCode")

class LocationModel(APIBase):
    id: Optional[str] = None
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    name: Optional[str] = None

class InventoryLevelModel(APIBase):
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    quantities: Optional[List[Dict[str, Any]]] = None
    location: Optional[LocationModel] = None

class InventoryItemModel(APIBase):
    id: Optional[str] = None
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    unit_cost: Optional[Money] = Field(None, alias="unitCost")
    inventory_levels: Optional[List[InventoryLevelModel]] = Field(None, alias="inventoryLevels")

class ProductModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    title: Optional[str] = None

class VariantModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    id: Optional[str] = None
    title: Optional[str] = None
    price: Optional[float] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    inventory_item: Optional[InventoryItemModel] = Field(None, alias="inventoryItem")
    product: Optional[ProductModel] = None

class LineItemModel(APIBase):
    id: Optional[str] = None
    title: Optional[str] = None
    quantity: Optional[int] = None
    sku: Optional[str] = None
    variant: Optional[VariantModel] = None
    original_unit_price: Optional[Money] = Field(None, alias="originalUnitPriceSet")
    total_discount: Optional[Money] = Field(None, alias="totalDiscountSet")

class TrackingInfo(APIBase):
    company: Optional[str] = None
    number: Optional[str] = None
    url: Optional[HttpUrl] = None

class FulfillmentEventModel(APIBase):
    id: Optional[str] = None
    message: Optional[str] = None
    status: Optional[str] = None
    happened_at: Optional[datetime] = Field(None, alias="happenedAt")

class FulfillmentModel(APIBase):
    id: Optional[str] = None
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    status: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    tracking_info: Optional[List[TrackingInfo]] = Field(default=None, alias="trackingInfo")
    events: Optional[List[FulfillmentEventModel]] = None

class ShopifyOrder(APIBase):
    id: Optional[str] = None
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    name: Optional[str] = None
    email: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    cancelled_at: Optional[datetime] = Field(None, alias="cancelledAt")
    financial_status: Optional[str] = Field(None, alias="displayFinancialStatus")
    fulfillment_status: Optional[str] = Field(None, alias="displayFulfillmentStatus")
    total_price: Optional[Money] = Field(None, alias="totalPriceSet")
    subtotal_price: Optional[Money] = Field(None, alias="subtotalPriceSet")
    total_tax: Optional[Money] = Field(None, alias="totalTaxSet")
    total_discounts: Optional[Money] = Field(None, alias="totalDiscountsSet")
    total_shipping_price: Optional[Money] = Field(None, alias="totalShippingPriceSet")
    tags: Optional[List[str]] = None
    note: Optional[str] = None
    paymentGatewayNames: Optional[List[str]] = Field(None, alias="paymentGatewayNames")
    cancel_reason: Optional[str] = Field(None, alias="cancelReason")
    currency: Optional[str] = Field(None, alias="currencyCode")
    line_items: Optional[List[LineItemModel]] = Field(None, alias="lineItems")
    fulfillments: Optional[List[FulfillmentModel]] = None