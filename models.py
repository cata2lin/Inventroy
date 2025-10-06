# models.py

from sqlalchemy import (Column, Integer, String, DateTime, Text,
                        ForeignKey, BIGINT, NUMERIC, BOOLEAN, Index, Computed, UniqueConstraint)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from passlib.context import CryptContext
from database import Base
from sqlalchemy.dialects.postgresql import JSONB


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=True)

    def verify_password(self, password: str) -> bool:
        return pwd_context.verify(password, self.hashed_password)

class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    shopify_url = Column(String(255), unique=True, nullable=False)
    api_token = Column(String(255), nullable=False)
    api_secret = Column(String(255), nullable=True)
    webhook_secret = Column(String(255), nullable=True)
    sync_location_id = Column(BIGINT)
    enabled = Column(BOOLEAN, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_synced_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    inventory_snapshots = relationship("InventorySnapshot", back_populates="store")

class Product(Base):
    __tablename__ = "products"
    id = Column(BIGINT, primary_key=True, index=False)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    title = Column(String(255))
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
    last_seen_at = Column(DateTime(timezone=True))
    variants = relationship("ProductVariant", back_populates="product", cascade="all, delete-orphan")

class ProductVariant(Base):
    __tablename__ = "product_variants"
    id = Column(BIGINT, primary_key=True, index=False)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    product_id = Column(BIGINT, ForeignKey("products.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(String(255))
    price = Column(NUMERIC(10, 2))
    sku = Column(String(255), index=True)
    position = Column(Integer)
    inventory_policy = Column(String(50))
    compare_at_price = Column(NUMERIC(10, 2))
    cost_per_item = Column(NUMERIC(18, 6))
    tracked = Column(BOOLEAN, default=True, nullable=False)
    inventory_management = Column(String(255))
    barcode = Column(String(255), index=True)
    inventory_item_id = Column(BIGINT, unique=True)
    inventory_quantity = Column(Integer)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    is_primary_variant = Column(BOOLEAN, default=False, nullable=False)
    
    # --- THIS COLUMN IS NEW ---
    is_barcode_primary = Column(BOOLEAN, default=False, nullable=False)
    
    sku_normalized = Column(Text, Computed("NULLIF(BTRIM(LOWER(sku)), '')", persisted=True))
    last_seen_at = Column(DateTime(timezone=True))

    product = relationship("Product", back_populates="variants")
    inventory_levels = relationship("InventoryLevel", back_populates="variant", cascade="all, delete-orphan")
    inventory_snapshots = relationship("InventorySnapshot", back_populates="product_variant", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('sku', 'store_id', name='product_variants_sku_store_id_key'),
    )

class Location(Base):
    __tablename__ = "locations"
    id = Column(BIGINT, primary_key=True, index=False)
    shopify_gid = Column(String(255)) # This is the critical field
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(255))
    inventory_levels = relationship("InventoryLevel", back_populates="location")

class InventoryLevel(Base):
    __tablename__ = "inventory_levels"
    variant_id = Column(BIGINT, ForeignKey("product_variants.id", ondelete="CASCADE"), primary_key=True)
    location_id = Column(BIGINT, ForeignKey("locations.id"), primary_key=True)
    inventory_item_id = Column(BIGINT, index=True)
    available = Column(Integer)
    on_hand = Column(Integer)
    updated_at = Column(DateTime(timezone=True))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    variant = relationship("ProductVariant", back_populates="inventory_levels")
    location = relationship("Location", back_populates="inventory_levels")

class InventorySnapshot(Base):
    __tablename__ = "inventory_snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime(timezone=True), nullable=False)
    product_variant_id = Column(BIGINT, ForeignKey("product_variants.id", ondelete="CASCADE"), nullable=False, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    on_hand = Column(Integer, nullable=False)

    product_variant = relationship("ProductVariant", back_populates="inventory_snapshots")
    store = relationship("Store", back_populates="inventory_snapshots")
    
    __table_args__ = (
        UniqueConstraint('date', 'product_variant_id', 'store_id', name='inventory_snapshots_date_product_variant_id_store_id_key'),
    )

class SyncRun(Base):
    __tablename__ = "sync_runs"
    id = Column(BIGINT, primary_key=True)
    store_id = Column(BIGINT, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    t0 = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    status = Column(Text)
    last_cursor = Column(Text)
    pages_ok = Column(Integer, default=0)
    pages_failed = Column(Integer, default=0)
    notes = Column(JSONB, default={})

class SyncDeadLetter(Base):
    __tablename__ = "sync_dead_letters"
    id = Column(BIGINT, primary_key=True)
    store_id = Column(BIGINT, nullable=False)
    run_id = Column(BIGINT, ForeignKey("sync_runs.id"))
    payload = Column(JSONB, nullable=False)
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

class Webhook(Base):
    __tablename__ = 'webhooks'
    id = Column(Integer, primary_key=True)
    shopify_webhook_id = Column(BIGINT, unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey('stores.id'), nullable=False)
    topic = Column(String(255), nullable=False)
    address = Column(String(2048), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())