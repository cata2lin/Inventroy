from sqlalchemy import (
    Column, Integer, String, ForeignKey, DateTime, Text, UniqueConstraint, BOOLEAN,
    func, BIGINT, NUMERIC
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


# ---------------- Stores ----------------

class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(255), unique=True, nullable=False)
    access_token = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    products = relationship("Product", back_populates="store", cascade="all, delete-orphan")
    variants = relationship("ProductVariant", back_populates="store", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="store", cascade="all, delete-orphan")
    locations = relationship("Location", back_populates="store", cascade="all, delete-orphan")
    webhooks = relationship("Webhook", back_populates="store", cascade="all, delete-orphan")


# ---------------- Catalog ----------------

class Product(Base):
    __tablename__ = "products"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False, index=True)

    title = Column(String(255), nullable=False)
    body_html = Column(Text)
    vendor = Column(String(255))
    product_type = Column(String(255))
    product_category = Column(String(255))
    created_at = Column(DateTime(timezone=True))
    handle = Column(String(255), index=True)
    updated_at = Column(DateTime(timezone=True))
    published_at = Column(DateTime(timezone=True))
    status = Column(String(50))
    tags = Column(Text)
    image_url = Column(String(2048))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    store = relationship("Store", back_populates="products")
    variants = relationship("ProductVariant", back_populates="product", cascade="all, delete-orphan")
    line_items = relationship("LineItem", back_populates="product")

    __table_args__ = (
        # for ON CONFLICT (store_id, id)
        UniqueConstraint('store_id', 'id', name='uq_products_store_id_id'),
    )


class ProductVariant(Base):
    __tablename__ = "product_variants"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    product_id = Column(BIGINT, ForeignKey("products.id"), nullable=False, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)

    title = Column(String(255))
    price = Column(NUMERIC(10, 2))
    sku = Column(String(255), index=True)
    position = Column(Integer)
    inventory_policy = Column(String(50))
    compare_at_price = Column(NUMERIC(10, 2))
    cost_per_item = Column(NUMERIC(18, 6), nullable=True)
    tracked = Column(BOOLEAN, nullable=False, default=True)
    fulfillment_service = Column(String(255))
    inventory_management = Column(String(255))
    barcode = Column(String(255), index=True)
    barcode_normalized = Column(String(255), index=True, nullable=True)
    is_primary_variant = Column(BOOLEAN, default=False, nullable=False)
    grams = Column(BIGINT)
    weight = Column(NUMERIC(10, 2))
    weight_unit = Column(String(10))
    inventory_item_id = Column(BIGINT, unique=True, index=True)
    inventory_quantity = Column(Integer)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    cost = Column(NUMERIC(10, 2), nullable=True)

    product = relationship("Product", back_populates="variants")
    store = relationship("Store", back_populates="variants")
    inventory_levels = relationship("InventoryLevel", back_populates="variant", cascade="all, delete-orphan")
    line_items = relationship("LineItem", back_populates="variant")
    group_membership = relationship("GroupMembership", uselist=False, back_populates="variant", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('sku', 'store_id', name='uq_product_variants_sku_store_id'),
        # for ON CONFLICT (store_id, id)
        UniqueConstraint('store_id', 'id', name='uq_product_variants_store_id_id'),
    )


class Location(Base):
    __tablename__ = "locations"
    id = Column(BIGINT, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)

    store = relationship("Store", back_populates="locations")
    inventory_levels = relationship("InventoryLevel", back_populates="location")

    __table_args__ = (
        # for ON CONFLICT (store_id, id)
        UniqueConstraint('store_id', 'id', name='uq_locations_store_id_id'),
    )


class InventoryLevel(Base):
    __tablename__ = "inventory_levels"
    inventory_item_id = Column(BIGINT, ForeignKey("product_variants.inventory_item_id"), primary_key=True)
    location_id = Column(BIGINT, ForeignKey("locations.id"), primary_key=True)
    available = Column(Integer)
    on_hand = Column(Integer)
    updated_at = Column(DateTime(timezone=True))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    variant = relationship("ProductVariant", back_populates="inventory_levels")
    location = relationship("Location", back_populates="inventory_levels")


# ---------------- Orders ----------------

class Order(Base):
    __tablename__ = "orders"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False, index=True)

    name = Column(String(255))
    email = Column(String(255))
    phone = Column(String(50))
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    cancelled_at = Column(DateTime(timezone=True))
    cancel_reason = Column(String(255))
    closed_at = Column(DateTime(timezone=True))
    processed_at = Column(DateTime(timezone=True))
    financial_status = Column(String(50))
    fulfillment_status = Column(String(50))
    currency = Column(String(10))
    payment_gateway_names = Column(Text)
    total_price = Column(NUMERIC(10, 2))
    subtotal_price = Column(NUMERIC(10, 2))
    total_tax = Column(NUMERIC(10, 2))
    total_discounts = Column(NUMERIC(10, 2))
    total_shipping_price = Column(NUMERIC(10, 2))
    note = Column(Text)
    tags = Column(Text)
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    store = relationship("Store", back_populates="orders")
    line_items = relationship("LineItem", back_populates="order", cascade="all, delete-orphan")
    fulfillments = relationship("Fulfillment", back_populates="order", cascade="all, delete-orphan")
    refunds = relationship("Refund", back_populates="order", cascade="all, delete-orphan")

    __table_args__ = (
        # for ON CONFLICT (store_id, id)
        UniqueConstraint('store_id', 'id', name='uq_orders_store_id_id'),
    )


class LineItem(Base):
    __tablename__ = "line_items"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    order_id = Column(BIGINT, ForeignKey("orders.id"), nullable=False, index=True)
    variant_id = Column(BIGINT, ForeignKey("product_variants.id"))
    product_id = Column(BIGINT, ForeignKey("products.id"))
    title = Column(String(255))
    quantity = Column(Integer)
    sku = Column(String(255), index=True)
    vendor = Column(String(255))
    price = Column(NUMERIC(10, 2))
    total_discount = Column(NUMERIC(10, 2))
    taxable = Column(BOOLEAN)
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    order = relationship("Order", back_populates="line_items")
    variant = relationship("ProductVariant", back_populates="line_items")
    product = relationship("Product", back_populates="line_items")


class Fulfillment(Base):
    __tablename__ = "fulfillments"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    order_id = Column(BIGINT, ForeignKey("orders.id"), nullable=False, index=True)
    status = Column(String(50))
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    tracking_company = Column(String(255))
    tracking_number = Column(String(255))
    tracking_url = Column(String(2048))
    shipment_status = Column(String(50))
    location_id = Column(BIGINT)
    hold_status = Column(String(50), nullable=True)
    hold_reason = Column(String(255), nullable=True)
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    order = relationship("Order", back_populates="fulfillments")
    events = relationship("FulfillmentEvent", back_populates="fulfillment", cascade="all, delete-orphan")


class FulfillmentEvent(Base):
    __tablename__ = "fulfillment_events"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    fulfillment_id = Column(BIGINT, ForeignKey("fulfillments.id"), nullable=False, index=True)
    status = Column(String(50))
    happened_at = Column(DateTime(timezone=True))
    description = Column(Text)
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    fulfillment = relationship("Fulfillment", back_populates="events")


class Refund(Base):
    __tablename__ = "refunds"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    order_id = Column(BIGINT, ForeignKey("orders.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True))
    note = Column(Text)
    total_refunded = Column(NUMERIC(10, 2))
    currency = Column(String(10))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    order = relationship("Order", back_populates="refunds")
    refund_line_items = relationship("RefundLineItem", back_populates="refund", cascade="all, delete-orphan")


class RefundLineItem(Base):
    __tablename__ = "refund_line_items"
    id = Column(BIGINT, primary_key=True, index=True)
    refund_id = Column(BIGINT, ForeignKey("refunds.id"), nullable=False, index=True)
    line_item_id = Column(BIGINT, ForeignKey("line_items.id"))
    quantity = Column(Integer)
    subtotal = Column(NUMERIC(10, 2))
    total_tax = Column(NUMERIC(10, 2))

    refund = relationship("Refund", back_populates="refund_line_items")


# ---------------- Barcodes & committed stock ----------------

class BarcodeGroup(Base):
    __tablename__ = "barcode_groups"
    id = Column(String(255), primary_key=True)
    status = Column(String(50), nullable=False, default="active")
    pool_available = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    committed_stock = relationship("CommittedStock", back_populates="group", cascade="all, delete-orphan")


class GroupMembership(Base):
    __tablename__ = "group_memberships"
    variant_id = Column(BIGINT, ForeignKey("product_variants.id"), primary_key=True)
    group_id = Column(String(255), ForeignKey("barcode_groups.id"), nullable=False, index=True)

    variant = relationship("ProductVariant", back_populates="group_membership")
    group = relationship("BarcodeGroup")


class CommittedStock(Base):
    __tablename__ = "committed_stock"
    group_id = Column(String(255), ForeignKey("barcode_groups.id"), primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), primary_key=True)
    committed_units = Column(Integer, default=0, nullable=False)
    open_orders_count = Column(Integer, default=0, nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    group = relationship("BarcodeGroup", back_populates="committed_stock")
    store = relationship("Store")


# ---------------- Webhooks ----------------

class Webhook(Base):
    __tablename__ = "webhooks"
    id = Column(Integer, primary_key=True, index=True)
    shopify_webhook_id = Column(BIGINT, unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False, index=True)
    topic = Column(String(255), nullable=False)
    address = Column(String(2048), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    store = relationship("Store", back_populates="webhooks")
