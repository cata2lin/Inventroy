# services/inventory_sync_service.py

import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text

import models
from shopify_service import ShopifyService
from product_service import ProductService

# ---------- time helpers (UTC-aware) ----------
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

# Transaction-scoped advisory lock for a barcode group (Postgres)
LOCK_SQL = text("SELECT pg_try_advisory_xact_lock(hashtext(:k))")


def _acquire_lock(db: Session, group_id: str) -> bool:
    """Acquire a TX-level advisory lock for group_id; returns True if lock was obtained."""
    try:
        return bool(db.execute(LOCK_SQL, {"k": group_id}).scalar())
    except Exception as e:
        print(f"[lock] failed group={group_id}: {e}")
        return False


def _is_recent_echo(db: Session, variant_id: int, current_available: int, window_s: int = 60) -> bool:
    """
    Echo suppression: if we just wrote 'current_available' to this variant within the window, drop the event.
    """
    since = _utcnow() - timedelta(seconds=window_s)
    row = (
        db.query(models.PushLog)
        .filter(models.PushLog.variant_id == variant_id, models.PushLog.written_at >= since)
        .order_by(models.PushLog.written_at.desc())
        .first()
    )
    return bool(row and int(row.target_available) == int(current_available))


def _set_available_abs_or_delta(
    db: Session,
    store_url: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
    target_available: int,
):
    """
    Prefer absolute setter (inventorySetQuantities). If it fails, fall back to delta adjust.
    """
    ps = ProductService(store_url=store_url, token=token)

    inv_gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
    loc_gid = f"gid://shopify/Location/{location_id}"

    # Try absolute set first
    try:
        return ps.set_inventory_available(inv_gid, loc_gid, int(target_available))
    except Exception as e:
        print(f"[set-fallback->adjust] absolute set failed: {e}")

    # Fallback: compute a live delta and adjust
    svc = ShopifyService(store_url=store_url, token=token)
    data = svc.get_inventory_levels_for_items([inventory_item_id]) or []
    lvl = next(
        (it for it in data if int(it["id"]) == int(inventory_item_id) and int(it["location_id"]) == int(location_id)),
        None,
    )
    if not lvl:
        raise RuntimeError("Could not fetch current available for delta write fallback")
    current = int(lvl["available"])
    delta = int(target_available) - current
    if delta == 0:
        return None
    return ps.adjust_inventory_quantity(inv_gid, loc_gid, delta)


def _ensure_no_open_tx(db: Session):
    """Commit any implicit (autobegun) transaction before starting an explicit one."""
    try:
        if db.in_transaction():
            db.commit()
    except Exception:
        db.rollback()


def process_inventory_update_event(
    shop_domain: str,
    event_id: str,
    inventory_item_id: int,
    location_id: int,
    db_factory=None,
    db: Optional[Session] = None,
):
    """
    GOLDEN SYNC LOOP (lock-safe, delta-based pool maintenance + absolute writes)
      1) Idempotency
      2) Resolve variant → group → store
      3) Auto-learn store.sync_location_id on first event
      4) Fetch truth for triggering variant
      5) Echo suppression
      6) TX-LOCK: delta vs snapshot → update pool; plan targets
      7) OUTSIDE LOCK: write absolute 'available', record PushLog, refresh snapshot
    Accepts either a Session or a db_factory to create one.
    """
    owns_session = False
    if db is None:
        if db_factory is None:
            from database import SessionLocal  # type: ignore
            db_factory = SessionLocal
        db = db_factory()
        owns_session = True

    try:
        # 1) Idempotency (record early so retries are harmless)
        if db.query(models.DeliveredEvent).filter_by(shop_domain=shop_domain, event_id=event_id).first():
            print(f"[idempotent] {event_id} already processed")
            return
        db.add(models.DeliveredEvent(shop_domain=shop_domain, event_id=event_id))
        db.commit()  # end TX

        # 2) Resolve variant → group → store
        variant = (
            db.query(models.ProductVariant)
            .options(
                joinedload(models.ProductVariant.product).joinedload(models.Product.store),
                joinedload(models.ProductVariant.group_membership).joinedload(models.GroupMembership.group),
                joinedload(models.ProductVariant.inventory_levels),
            )
            .filter(models.ProductVariant.inventory_item_id == inventory_item_id)
            .first()
        )
        if not variant or not variant.group_membership or not variant.group_membership.group:
            print("[skip] unknown variant or no barcode group")
            return

        group = variant.group_membership.group
        store = variant.product.store

        if not store.enabled:
            print(f"[skip] store disabled: {store.name}")
            return

        # 3) ✅ Auto-learn the sync location on the first inventory event for a store
        if not store.sync_location_id:
            store.sync_location_id = int(location_id)
            db.commit()  # persist auto-learn immediately
            print(f"[init] store '{store.name}' sync_location_id set to {store.sync_location_id}")
        elif int(location_id) != int(store.sync_location_id):
            print(f"[skip] event not for store's sync location (event={location_id}, sync={store.sync_location_id})")
            return

        if getattr(variant, "tracked", True) is False:
            print("[skip] variant untracked")
            return
        if getattr(group, "status", "active") == "conflicted":
            print("[skip] group is conflicted")
            return

        # 4) Read truth for the triggering variant (from Shopify)
        s = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        truth_rows = s.get_inventory_levels_for_items([inventory_item_id]) or []
        current_level = next(
            (it for it in truth_rows if int(it["id"]) == int(inventory_item_id) and int(it["location_id"]) == int(store.sync_location_id)),
            None,
        )
        if not current_level:
            print("[abort] live inventory at sync location not found")
            return

        current_available = int(current_level["available"])
        current_on_hand = int(current_level.get("on_hand", current_available))

        # 5) Echo suppression
        if _is_recent_echo(db, variant.id, current_available):
            print("[echo] drop")
            return

        # 6) TX-LOCK — delta vs. snapshot, pool math, plan targets
        planned_writes: List[Tuple[models.ProductVariant, int]] = []

        _ensure_no_open_tx(db)
        tx = db.begin()
        try:
            if not _acquire_lock(db, group.id):
                tx.rollback()
                print(f"[lock-miss] group={group.id}")
                return

            # Reload snapshot for this variant inside the lock
            snap = (
                db.query(models.InventoryLevel)
                .filter(
                    models.InventoryLevel.inventory_item_id == variant.inventory_item_id,
                    models.InventoryLevel.location_id == store.sync_location_id,
                )
                .with_for_update(read=True)
                .first()
            )
            last_available = int(snap.available) if snap and snap.available is not None else 0
            delta = current_available - last_available

            # Update / insert snapshot for the origin variant
            now = _utcnow()
            if snap:
                snap.available = current_available
                snap.on_hand = current_on_hand
                snap.last_fetched_at = now
            else:
                db.add(
                    models.InventoryLevel(
                        inventory_item_id=variant.inventory_item_id,
                        location_id=store.sync_location_id,
                        available=current_available,
                        on_hand=current_on_hand,
                        last_fetched_at=now,
                    )
                )

            if delta == 0:
                tx.commit()
                print("[delta=0] nothing to propagate")
                return

            # Update pool
            group.pool_available = int(group.pool_available) + int(delta)

            # Compute targets for all members (refresh snapshot if missing/stale)
            members = (
                db.query(models.ProductVariant)
                .options(
                    joinedload(models.ProductVariant.product).joinedload(models.Product.store),
                    joinedload(models.ProductVariant.inventory_levels),
                )
                .join(models.GroupMembership)
                .filter(models.GroupMembership.group_id == group.id)
                .all()
            )

            for member in members:
                m_store = member.product.store
                if not m_store.enabled or not m_store.sync_location_id:
                    continue
                if getattr(member, "tracked", True) is False:
                    continue

                m_snap = next(
                    (lvl for lvl in member.inventory_levels if int(lvl.location_id) == int(m_store.sync_location_id)),
                    None,
                )

                # Normalize timestamp (handles legacy naive datetimes)
                last_ts = _ensure_aware(m_snap.last_fetched_at) if m_snap else None
                needs_refresh = (
                    m_snap is None
                    or last_ts is None
                    or (_utcnow() - last_ts).total_seconds() > 10
                )

                if needs_refresh:
                    svc = ShopifyService(store_url=m_store.shopify_url, token=m_store.api_token)
                    data = svc.get_inventory_levels_for_items([member.inventory_item_id]) or []
                    lvl = next(
                        (it for it in data if int(it["id"]) == int(member.inventory_item_id) and int(it["location_id"]) == int(m_store.sync_location_id)),
                        None,
                    )
                    if not lvl:
                        print(f"[warn] cannot fetch member truth store={m_store.name} variant={member.id}")
                        continue
                    now2 = _utcnow()
                    if m_snap:
                        m_snap.available = int(lvl["available"])
                        m_snap.on_hand = int(lvl.get("on_hand", lvl["available"]))
                        m_snap.last_fetched_at = now2
                    else:
                        m_snap = models.InventoryLevel(
                            inventory_item_id=member.inventory_item_id,
                            location_id=m_store.sync_location_id,
                            available=int(lvl["available"]),
                            on_hand=int(lvl.get("on_hand", lvl["available"])),
                            last_fetched_at=now2,
                        )
                        db.add(m_snap)

                # Clamp: 0 ≤ target ≤ on_hand, minus safety buffer
                target = max(0, int(group.pool_available) - int(m_store.safety_buffer))
                target = min(target, int(m_snap.on_hand or 0))

                if int(m_snap.available or 0) != target:
                    planned_writes.append((member, target))

            tx.commit()
        except Exception:
            tx.rollback()
            raise

        # 7) OUTSIDE LOCK — perform writes; record push_log; refresh snapshot
        correlation_id = uuid.uuid4()
        for member, target in planned_writes:
            m_store = member.product.store
            try:
                _set_available_abs_or_delta(
                    db=db,
                    store_url=m_store.shopify_url,
                    token=m_store.api_token,
                    inventory_item_id=member.inventory_item_id,
                    location_id=m_store.sync_location_id,
                    target_available=int(target),
                )
                _ensure_no_open_tx(db)
                tx2 = db.begin()
                try:
                    db.add(
                        models.PushLog(
                            variant_id=member.id,
                            target_available=int(target),
                            correlation_id=str(correlation_id),
                            write_source="sync",
                            written_at=_utcnow(),
                        )
                    )
                    # keep our snapshot in sync right away
                    snap2 = (
                        db.query(models.InventoryLevel)
                        .filter(
                            models.InventoryLevel.inventory_item_id == member.inventory_item_id,
                            models.InventoryLevel.location_id == m_store.sync_location_id,
                        )
                        .first()
                    )
                    if snap2:
                        snap2.available = int(target)
                        snap2.last_fetched_at = _utcnow()
                    tx2.commit()
                except Exception:
                    tx2.rollback()
                    raise
            except Exception as e:
                print(f"[write-fail] store={m_store.name} variant={member.id} target={target}: {e}")

    finally:
        if owns_session:
            db.close()
