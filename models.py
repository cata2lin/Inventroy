# models.py
from sqlalchemy import (Column, Integer, String, DateTime, Text,
                        ForeignKey, BIGINT, NUMERIC, BOOLEAN, Index, Computed, UniqueConstraint, Date)
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
    
    currency = Column(String(10), nullable=False, server_default="RON")
    
    sync_location_id = Column(BIGINT)
    enabled = Column(BOOLEAN, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_synced_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    inventory_snapshots = relationship("InventorySnapshot", back_populates="store")
    products = relationship("Product", back_populates="store")

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
    status = Column(String(50))  # Shopify status: ACTIVE, DRAFT, ARCHIVED — all participate in sync
    tags = Column(Text)
    image_url = Column(String(2048))
    last_fetched_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    last_seen_at = Column(DateTime(timezone=True))
    # Option B: Soft-delete with timestamp. NULL = active, set = deleted.
    # Products with any status (ACTIVE/DRAFT/ARCHIVED) can participate in barcode sync.
    # Only products soft-deleted by the sync runner (disappeared from Shopify) are excluded.
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)
    variants = relationship("ProductVariant", back_populates="product", cascade="all, delete-orphan")
    
    store = relationship("Store", back_populates="products")


class ProductVariant(Base):
    __tablename__ = "product_variants"
    id = Column(BIGINT, primary_key=True, index=False)
    shopify_gid = Column(String(255), unique=True, nullable=False)
    product_id = Column(BIGINT, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
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
    date = Column(Date, nullable=False)
    product_variant_id = Column(BIGINT, ForeignKey("product_variants.id", ondelete="CASCADE"), nullable=False, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False, index=True)
    on_hand = Column(Integer, nullable=False)

    # --- NEW FIELDS FOR METRICS ---
    price = Column(NUMERIC(10, 2), nullable=True)
    cost_per_item = Column(NUMERIC(18, 6), nullable=True)

    product_variant = relationship("ProductVariant", back_populates="inventory_snapshots")
    store = relationship("Store", back_populates="inventory_snapshots")
    
    __table_args__ = (
        UniqueConstraint('date', 'product_variant_id', 'store_id', name='inventory_snapshots_date_product_variant_id_store_id_key'),
    )

class SyncRun(Base):
    __tablename__ = "sync_runs"
    id = Column(BIGINT, primary_key=True, autoincrement=True)
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
    id = Column(BIGINT, primary_key=True, autoincrement=True)
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
    
class BarcodeVersion(Base):
    __tablename__ = "barcode_versions"
    barcode = Column(String(255), primary_key=True)
    authoritative_store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    source_timestamp = Column(DateTime(timezone=True), nullable=False)
    version = Column(BIGINT, nullable=False, default=1)
    last_updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

class WriteIntent(Base):
    __tablename__ = "write_intents"
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    barcode = Column(String(255), nullable=False, index=True)
    target_store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    # The specific inventory item we wrote to. Lets the echo guard match precisely
    # (per-item) instead of by barcode alone, so multi-listing within a store can't
    # cross-suppress a genuine change. NULL = store-level intent (legacy/absolute).
    inventory_item_id = Column(BIGINT, nullable=True, index=True)
    quantity = Column(Integer, nullable=False)
    barcode_version = Column(BIGINT, nullable=False)
    # --- P0.2 propagation lineage: lets an inbound webhook be recognised as OUR OWN
    # echo by (target_store_id, inventory_item_id) within a TTL, independent of value. ---
    sync_operation_uuid = Column(String(64), nullable=True, index=True)
    origin_store_id = Column(Integer, nullable=True)
    origin_inventory_item_id = Column(BIGINT, nullable=True)
    propagation_depth = Column(Integer, nullable=False, server_default="0")
    # SYNC_ECHO_AUTHORITATIVE: the Shopify-authoritative post-write `available` quantity captured
    # from the single-item mutation response. NULL => fall back to value-INDEPENDENT echo suppression
    # (today's behaviour). Non-NULL => the inbound echo's residual is observed - authoritative_qty.
    authoritative_qty = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index('ix_write_intents_lookup', 'barcode', 'target_store_id', 'quantity'),
        Index('ix_write_intents_item', 'target_store_id', 'inventory_item_id', 'expires_at'),
    )


class SyncGroup(Base):
    """P3 — explicit synchronization group. Sync identity is the group, NOT the barcode.
    A group can be quarantined, classified, or given an explicit authoritative variant. This
    allows: same barcode WITHOUT syncing (different groups), syncing WITHOUT barcode equality
    (one group, different barcodes), and manual overrides."""
    __tablename__ = "sync_groups"
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    # The barcode the group was seeded from — a LOOKUP HINT, not the sync identity.
    barcode_key = Column(String(255), nullable=True, index=True)
    # ACTIVE | VALID_SHARED | SUSPECT_DUPLICATE | CONFIRMED_ERROR | QUARANTINED
    classification = Column(String(32), nullable=False, server_default="ACTIVE")
    sync_enabled = Column(BOOLEAN, nullable=False, server_default="true")
    authoritative_variant_id = Column(BIGINT, nullable=True)  # optional explicit canonical
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SyncGroupMember(Base):
    """A variant's membership in a sync group. A variant belongs to at most one group.
    excluded=True keeps a variant in the catalog/group record but OUT of propagation (e.g. a
    SKU-less orphan duplicate) without deleting anything."""
    __tablename__ = "sync_group_members"
    variant_id = Column(BIGINT, primary_key=True)
    sync_group_id = Column(BIGINT, ForeignKey("sync_groups.id"), nullable=False, index=True)
    store_id = Column(Integer, nullable=False, index=True)
    excluded = Column(BOOLEAN, nullable=False, server_default="false")
    added_at = Column(DateTime(timezone=True), server_default=func.now())


class BarcodeCircuitBreaker(Base):
    """P0.2/P0.3 — a barcode that tripped the storm/abnormal-delta breaker. While a row
    exists and has not expired, the sync engine refuses to auto-propagate this barcode."""
    __tablename__ = "barcode_circuit_breakers"
    barcode = Column(String(255), primary_key=True)
    reason = Column(Text, nullable=False)
    tripped_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    details = Column(JSONB, nullable=True)

class ProcessedWebhook(Base):
    __tablename__ = "processed_webhooks"
    id = Column(String(255), primary_key=True) # A unique hash of the payload
    received_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)


class AuditLog(Base):
    """
    Central audit trail for ALL system operations.
    Categories: WEBHOOK, SYNC, STOCK, CONFIG, AUTH, SYSTEM, RECONCILIATION
    """
    __tablename__ = "audit_logs"
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)  # WEBHOOK, SYNC, STOCK, CONFIG, AUTH, SYSTEM
    action = Column(String(100), nullable=False, index=True)   # e.g. webhook_received, sync_started, stock_updated
    severity = Column(String(20), nullable=False, server_default="INFO")  # INFO, WARN, ERROR, CRITICAL
    actor = Column(String(255), nullable=True)  # username or "system"
    store_id = Column(Integer, nullable=True, index=True)
    store_name = Column(String(255), nullable=True)
    target = Column(String(255), nullable=True)  # barcode, product_id, webhook_topic, etc.
    message = Column(Text, nullable=False)
    details = Column(JSONB, nullable=True)  # Flexible payload for any extra context
    duration_ms = Column(Integer, nullable=True)  # How long the operation took
    error_message = Column(Text, nullable=True)
    stack_trace = Column(Text, nullable=True)

    __table_args__ = (
        Index('ix_audit_logs_category_timestamp', 'category', 'timestamp'),
        Index('ix_audit_logs_severity_timestamp', 'severity', 'timestamp'),
    )


class SystemEvent(Base):
    """
    System-level events for monitoring health, errors, and performance.
    Used for the error log view and system dashboard.
    """
    __tablename__ = "system_events"
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    level = Column(String(20), nullable=False, index=True)  # INFO, WARN, ERROR, CRITICAL
    source = Column(String(255), nullable=False, index=True)  # Module/function that generated the event
    message = Column(Text, nullable=False)
    details = Column(JSONB, nullable=True)
    stack_trace = Column(Text, nullable=True)
    resolved = Column(BOOLEAN, default=False, nullable=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by = Column(String(255), nullable=True)

    __table_args__ = (
        Index('ix_system_events_level_timestamp', 'level', 'timestamp'),
    )