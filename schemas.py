# schemas.py
from __future__ import annotations

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, HttpUrl, ConfigDict, model_validator

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
    currency: str = Field("RON", max_length=10)
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

# --- Schemas for the mutations page and product views ---

class Location(ORMBase):
    id: int
    shopify_gid: Optional[str] = None
    name: Optional[str] = None

class InventoryLevel(ORMBase):
    location_id: int
    available: Optional[int] = None
    location: Optional[Location] = None

class ProductVariant(ORMBase):
    id: int
    shopify_gid: str
    inventory_item_id: Optional[int] = None
    inventory_item_gid: Optional[str] = None
    title: Optional[str] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None
    compare_at_price: Optional[float] = None
    cost_per_item: Optional[float] = None
    inventory_quantity: Optional[int] = None
    inventory_levels: List[InventoryLevel] = Field(default_factory=list)

    @model_validator(mode="after")
    def _derive_inventory_item_gid(self):
        if not self.inventory_item_gid and self.inventory_item_id:
            self.inventory_item_gid = f"gid://shopify/InventoryItem/{self.inventory_item_id}"
        return self

class Product(ORMBase):
    id: int
    shopify_gid: str
    store_id: int
    title: str
    product_type: Optional[str] = None
    image_url: Optional[str] = None
    status: Optional[str] = None
    variants: List[ProductVariant] = Field(default_factory=list)

class ProductResponse(BaseModel):
    total_count: int
    products: List[Product]

# --- Schemas for Inventory Snapshots ---

class InventorySnapshot(ORMBase):
    id: int
    date: date
    product_variant_id: int
    store_id: int
    on_hand: int
    product_variant: ProductVariant

class InventorySnapshotResponse(BaseModel):
    total_count: int
    snapshots: List[InventorySnapshot]

# --- NEW SCHEMA FOR SNAPSHOT METRICS ---
class SnapshotMetrics(BaseModel):
    average_stock_level: Optional[float] = None
    min_stock_level: Optional[float] = None
    max_stock_level: Optional[float] = None
    stock_range: Optional[float] = None
    stock_stddev: Optional[float] = None
    days_out_of_stock: Optional[int] = None
    stockout_rate: Optional[float] = None
    replenishment_days: Optional[int] = None
    depletion_days: Optional[int] = None
    total_outflow: Optional[float] = None
    stock_turnover: Optional[float] = None
    avg_days_in_inventory: Optional[float] = None
    dead_stock_days: Optional[int] = None
    dead_stock_ratio: Optional[float] = None
    avg_inventory_value: Optional[float] = None
    avg_sales_value: Optional[float] = None
    avg_gross_margin_value: Optional[float] = None
    stability_index: Optional[float] = None
    stock_health_index: Optional[float] = None

# ======================================================
# Shopify GraphQL Ingest Models
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

# --- FIX FOR FORWARD REFERENCES ---
InventorySnapshot.model_rebuild()
InventorySnapshotResponse.model_rebuild()