# schemas.py  — Pydantic v2 compatible (uses ConfigDict)
from __future__ import annotations

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


# =========================
# Base model configurations
# =========================

class ORMBase(BaseModel):
    """Base that plays nicely with SQLAlchemy rows and unknown fields."""
    model_config = ConfigDict(
        from_attributes=True,     # replaces orm_mode=True
        populate_by_name=True,    # replaces allow_population_by_field_name=True
        extra="allow",            # keep unexpected fields (we're permissive)
    )


class APIBase(BaseModel):
    """Non-ORM payloads (webhooks/GraphQL/etc)."""
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )


# ======================================================
# Dashboard / Config response & request models
# ======================================================

class Store(ORMBase):
    id: int
    name: str
    shopify_url: str
    api_token: Optional[str] = None
    api_secret: Optional[str] = None
    webhook_secret: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StoreCreate(APIBase):
    name: str
    shopify_url: str
    api_token: str
    api_secret: Optional[str] = None
    webhook_secret: Optional[str] = None


class StoreUpdate(APIBase):
    name: Optional[str] = None
    shopify_url: Optional[str] = None
    api_token: Optional[str] = None
    api_secret: Optional[str] = None
    webhook_secret: Optional[str] = None


class Webhook(ORMBase):
    id: int
    shopify_webhook_id: int = Field(..., alias="shopify_webhook_id")
    store_id: int
    topic: str
    address: str
    created_at: Optional[datetime] = None


# ---- Dashboard table schemas (used as response_model) ----

class Order(ORMBase):
    id: int
    store_id: int
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    financial_status: Optional[str] = None
    fulfillment_status: Optional[str] = None
    total_price: Optional[float] = None
    cancelled_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class Fulfillment(ORMBase):
    id: int
    order_id: int
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    tracking_company: Optional[str] = None
    tracking_number: Optional[str] = None
    location_id: Optional[int] = None
    updated_at: Optional[datetime] = None


class Inventory(ORMBase):
    id: Optional[int] = None
    store_id: Optional[int] = None
    location_id: Optional[int] = None
    inventory_item_id: Optional[int] = None
    sku: Optional[str] = None
    variant_id: Optional[int] = None
    product_id: Optional[int] = None
    on_hand: int = 0
    available: int = 0
    updated_at: Optional[datetime] = None


# ======================================================
# Shopify GraphQL ingest models (permissive; camelCase aliases)
# ======================================================

class Money(APIBase):
    amount: Optional[float] = None
    currency_code: Optional[str] = Field(None, alias="currencyCode")


class LocationModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    name: Optional[str] = None


class InventoryLevelModel(APIBase):
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    # ShopifyService flattens into [{'name': 'available'|'on_hand', 'quantity': int}]
    quantities: Optional[List[Dict[str, Any]]] = None
    location: Optional[LocationModel] = None


class InventoryItemModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    tracked: Optional[bool] = None
    unit_cost: Optional[Money] = Field(None, alias="unitCost")
    # ShopifyService flattens inventoryLevels edges -> list
    inventory_levels: Optional[List[InventoryLevelModel]] = Field(None, alias="inventoryLevels")


class ProductModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    title: Optional[str] = None
    body_html: Optional[str] = Field(None, alias="bodyHtml")
    product_type: Optional[str] = Field(None, alias="productType")
    status: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    # FIX: Added published_at with its alias to handle the API response
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    category: Optional[Dict[str, Any]] = None # Shopify returns this as an object


class VariantModel(APIBase):
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    id: Optional[str] = None  # GID
    title: Optional[str] = None
    price: Optional[float] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    inventory_item: Optional[InventoryItemModel] = Field(None, alias="inventoryItem")
    product: Optional[ProductModel] = None


class LineItemModel(APIBase):
    id: Optional[str] = None  # GID
    title: Optional[str] = None
    quantity: Optional[int] = None
    sku: Optional[str] = None
    taxable: Optional[bool] = None
    variant: Optional[VariantModel] = None
    original_unit_price: Optional[Money] = Field(None, alias="originalUnitPriceSet")
    total_discount: Optional[Money] = Field(None, alias="totalDiscountSet")


class TrackingInfo(APIBase):
    company: Optional[str] = None
    number: Optional[str] = None
    url: Optional[HttpUrl] = None


class FulfillmentEventModel(APIBase):
    id: Optional[str] = None  # GID
    message: Optional[str] = None
    status: Optional[str] = None
    happened_at: Optional[datetime] = Field(None, alias="happenedAt")


class FulfillmentModel(APIBase):
    id: Optional[str] = None              # GID
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    status: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    tracking_info: Optional[List[TrackingInfo]] = Field(default=None, alias="trackingInfo")
    events: Optional[List[FulfillmentEventModel]] = None


class ShopifyOrder(APIBase):
    # ids & timing
    id: Optional[str] = None  # GID
    legacy_resource_id: Optional[int] = Field(None, alias="legacyResourceId")
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    cancelled_at: Optional[datetime] = Field(None, alias="cancelledAt")
    closed_at: Optional[datetime] = Field(None, alias="closedAt")
    processed_at: Optional[datetime] = Field(None, alias="processedAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")

    # statuses
    financial_status: Optional[str] = Field(None, alias="financialStatus")
    fulfillment_status: Optional[str] = Field(None, alias="fulfillmentStatus")

    # money (ShopifyService already flattens *PriceSet->shopMoney)
    total_price: Optional[Money] = Field(None, alias="totalPriceSet")
    subtotal_price: Optional[Money] = Field(None, alias="subtotalPriceSet")
    total_tax: Optional[Money] = Field(None, alias="totalTaxSet")
    total_discounts: Optional[Money] = Field(None, alias="totalDiscountsSet")
    total_shipping_price: Optional[Money] = Field(None, alias="totalShippingPriceSet")

    # tags, etc.
    tags: Optional[List[str]] = None

    # lines & fulfillments (ShopifyService flattens edges)
    line_items: Optional[List[LineItemModel]] = Field(None, alias="lineItems")
    fulfillments: Optional[List[FulfillmentModel]] = None


# ======================================================
# Webhook payload schemas (used by routes/services)
# ======================================================

class FulfillmentHold(APIBase):
    reason: Optional[str] = None
    reason_notes: Optional[str] = Field(None, alias="reason_notes")


class FulfillmentOrderWebhook(APIBase):
    fulfillment_order: Dict[str, Any]


class ShopifyOrderWebhook(APIBase):
    # Minimal fields we actually read in services/commited_projector.py etc.
    id: int
    admin_graphql_api_id: Optional[str] = None
    name: Optional[str] = None
    financial_status: Optional[str] = None
    fulfillment_status: Optional[str] = None
    cancelled_at: Optional[datetime] = None


class ShopifyFulfillmentWebhook(APIBase):
    id: int
    order_id: int
    status: Optional[str] = None
    created_at: Optional[datetime] = None


class RefundLineItemWebhook(APIBase):
    id: int
    quantity: int
    subtotal: float
    total_tax: float
    line_item: Dict[str, Any]


class ShopifyRefundWebhook(APIBase):
    id: int
    order_id: int
    created_at: datetime
    note: Optional[str] = None
    transactions: List[Dict[str, Any]] = []
    refund_line_items: List[RefundLineItemWebhook] = []


class DeletePayload(APIBase):
    id: int


# ======================================================
# **NEW**: Product webhook payloads (expected by crud/webhooks.py)
# Permissive models that match Shopify REST product webhooks.
# ======================================================

class ShopifyImageWebhook(APIBase):
    id: Optional[int] = None
    src: Optional[str] = None
    alt: Optional[str] = None
    position: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    variant_ids: Optional[List[int]] = Field(default=None, alias="variant_ids")


class ShopifyOptionWebhook(APIBase):
    id: Optional[int] = None
    name: Optional[str] = None
    position: Optional[int] = None
    values: Optional[List[str]] = None


class ShopifyVariantWebhook(APIBase):
    id: int
    product_id: Optional[int] = None
    title: Optional[str] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None
    compare_at_price: Optional[float] = Field(None, alias="compare_at_price")
    inventory_item_id: Optional[int] = None
    old_inventory_quantity: Optional[int] = None
    option1: Optional[str] = None
    option2: Optional[str] = None
    option3: Optional[str] = None
    image_id: Optional[int] = None
    tax_code: Optional[str] = None
    requires_shipping: Optional[bool] = None
    grams: Optional[int] = None
    weight: Optional[float] = None
    weight_unit: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ShopifyProductWebhook(APIBase):
    id: int
    admin_graphql_api_id: Optional[str] = None
    title: Optional[str] = None
    body_html: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    status: Optional[str] = None
    handle: Optional[str] = None
    tags: Optional[str] = None  # Shopify sends comma-separated string
    # Collections
    variants: Optional[List[ShopifyVariantWebhook]] = None
    options: Optional[List[ShopifyOptionWebhook]] = None
    images: Optional[List[ShopifyImageWebhook]] = None
    image: Optional[ShopifyImageWebhook] = None
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    published_at: Optional[datetime] = None