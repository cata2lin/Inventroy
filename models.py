from sqlalchemy import (Column, Integer, String, DateTime, Text,
                        ForeignKey, BIGINT, NUMERIC, BOOLEAN, UniqueConstraint)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from passlib.context import CryptContext
from database import Base

# Password hashing setup
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
    sync_location_id = Column(BIGINT, nullable=True)
    enabled = Column(BOOLEAN, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_synced_at = Column(DateTime(timezone=True), onupdate=func.now())

class Product(Base):
    __tablename__ = "products"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
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
    variants = relationship("ProductVariant", back_populates="product", cascade="all, delete-orphan")

class ProductVariant(Base):
    __tablename__ = "product_variants"
    id = Column(BIGINT, primary_key=True, index=True)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    product_id = Column(BIGINT, ForeignKey("products.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(String(255))
    price = Column(NUMERIC(10, 2))
    sku = Column(String(255), index=True)
    position = Column(Integer)
    inventory_policy = Column(String(50))
    compare_at_price = Column(NUMERIC(10, 2))
    cost_per_item = Column(NUMERIC(18, 6), nullable=True)
    tracked = Column(BOOLEAN, nullable=False, default=True)
    inventory_management = Column(String(255))
    barcode = Column(String(255), index=True)
    barcode_normalized = Column(String(255), index=True, nullable=True)
    is_primary_variant = Column(BOOLEAN, default=False, nullable=False)
    inventory_item_id = Column(BIGINT, unique=True, index=True)
    inventory_quantity = Column(Integer)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    cost = Column(NUMERIC(10, 2), nullable=True)
    product = relationship("Product", back_populates="variants")
    inventory_levels = relationship("InventoryLevel", back_populates="variant", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint('sku', 'store_id', name='uq_sku_store_id'),)

class Location(Base):
    __tablename__ = "locations"
    id = Column(BIGINT, primary_key=True, index=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(255), nullable=False)
    inventory_levels = relationship("InventoryLevel", back_populates="location")

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

class InventorySnapshot(Base):
    __tablename__ = "inventory_snapshots"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    product_variant_id = Column(BIGINT, ForeignKey("product_variants.id", ondelete="CASCADE"), nullable=False, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    on_hand = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('date', 'product_variant_id', 'store_id', name='uq_snapshot_date_variant_store'),)