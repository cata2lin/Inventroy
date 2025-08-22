# services/inventory_sync_service.py

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict

from sqlalchemy import text
from sqlalchemy.orm import Session, joinedload

import models
from services.shopify_service import ShopifyService
from product_service import ProductService

# ---------- time helpers (UTC-aware) ----------
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

# ---------- policy ----------
STALE_TTL_SECONDS = 10
ECHO_WINDOW_SECONDS = 60

# Bootstrap: if at first read you see many zeros and at least one positive,
# treat zeros as anomalies (take min of positives).
BOOTSTRAP_IGNORE_ZERO_IF_MAJORITY_POSITIVE = True
BYPASS_ON_HAND_CLAMP_FOR_FIRST_BOOTSTRAP = True

# ---------- advisory lock ----------
LOCK_SQL = text("SELECT pg_try_advisory_xact_lock(hashtext(:k))")

def _acquire_lock(db: Session, group_key: str) -> bool:
    try:
        return bool(db.execute(LOCK_SQL, {"k": group_key}).scalar())
    except Exception as e:
        print(f"[lock] failed group={group_key}: {e}")
        return False

# ---------- helpers ----------
def _is_recent_echo(db: Session, variant_id: int, current_available: int, window_s: int = ECHO_WINDOW_SECONDS) -> bool:
    since = _utcnow() - timedelta(seconds=window_s)
    row = (
        db.query(models.PushLog)
        .filter(models.PushLog.variant_id == variant_id, models.PushLog.written_at >= since)
        .order_by(models.PushLog.written_at.desc())
        .first()
    )
    return bool(row and int(row.target_available) == int(current_available))


def _ensure_no_open_tx(db: Session):
    try:
        if db.in_transaction():
            db.commit()
    except Exception:
        db.rollback()


def _refresh_member_snapshot(
    db: Session,
    store_url: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
) -> Optional[models.InventoryLevel]:
    """Fetch live available/on_hand for one member and upsert local snapshot."""
    svc = ShopifyService(store_url=store_url, token=token)
    rows = svc.get_inventory_levels_for_items([inventory_item_id]) or []

    lvl = next(
        (r for r in rows if int(r.get("id", 0)) == int(inventory_item_id) and int(r.get("location_id", 0)) == int(location_id)),
        None,
    )
    if not lvl:
        return None

    now = _utcnow()
    snap = (
        db.query(models.InventoryLevel)
        .filter(
            models.InventoryLevel.inventory_item_id == inventory_item_id,
            models.InventoryLevel.location_id == location_id,
        )
        .first()
    )
    if snap:
        snap.available = int(lvl.get("available", 0))
        snap.on_hand = int(lvl.get("on_hand", snap.available or 0))
        snap.last_fetched_at = now
    else:
        snap = models.InventoryLevel(
            inventory_item_id=inventory_item_id,
            location_id=location_id,
            available=int(lvl.get("available", 0)),
            on_hand=int(lvl.get("on_hand", lvl.get("available", 0))),
            last_fetched_at=now,
        )
        db.add(snap)
    return snap


def _get_fresh_snap(db: Session, member: models.ProductVariant) -> Optional[models.InventoryLevel]:
    """Return a fresh-enough snapshot (refresh if missing/stale)."""
    m_store = member.product.store
    if not m_store.enabled or not m_store.sync_location_id:
        return None

    m_snap = next(
        (lvl for lvl in member.inventory_levels if int(lvl.location_id) == int(m_store.sync_location_id)),
        None,
    )
    last_ts = _aware(m_snap.last_fetched_at) if m_snap else None
    stale = m_snap is None or last_ts is None or (_utcnow() - last_ts).total_seconds() > STALE_TTL_SECONDS
    if stale:
        m_snap = _refresh_member_snapshot(
            db,
            store_url=m_store.shopify_url,
            token=m_store.api_token,
            inventory_item_id=member.inventory_item_id,
            location_id=m_store.sync_location_id,
        )
    return m_snap


def _compute_bootstrap_pool_from_snaps(snaps: List[models.InventoryLevel]) -> int:
    """First-time pool from live truth using min(), with a zero-anomaly guard."""
    vals = [int(s.available or 0) for s in snaps]
    if not vals:
        return 0
    if BOOTSTRAP_IGNORE_ZERO_IF_MAJORITY_POSITIVE:
        positives = [v for v in vals if v > 0]
        zeros = [v for v in vals if v == 0]
        if positives and len(zeros) >= len(vals) // 2:
            return min(positives)
    return min(vals)


def _write_available_delta(
    store_url: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
    target_available: int,
    current_available: int,
):
    """Use delta write (same tested path as bulk editor)."""
    delta = int(target_available) - int(current_available)
    if delta == 0:
        return
    ps = ProductService(store_url=store_url, token=token)
    inv_gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
    loc_gid = f"gid://shopify/Location/{location_id}"
    ps.adjust_inventory_quantity(inv_gid, loc_gid, delta)


def process_inventory_update_event(
    shop_domain: str,
    event_id: str,
    inventory_item_id: int,
    location_id: int,
    db_factory=None,
    db: Optional[Session] = None,
):
    """
    GOLDEN SYNC LOOP (bootstrap once, then delta-based pool)

    • First event per group: refresh all members; pool = min(live) with anomaly guard;
      optionally bypass on_hand clamp to recover.
    • Subsequent events: pool_available += delta_available_at_origin (within lock);
      then propagate to all stores.
    • Always clamp per store: 0 ≤ target ≤ on_hand, and apply safety_buffer.
    • Writes are delta-based to match your tested bulk editor path.
    """
    owns = False
    if db is None:
        if db_factory is None:
            from database import SessionLocal  # type: ignore
            db_factory = SessionLocal
        db = db_factory()
        owns = True

    try:
        # 1) Idempotency
        if db.query(models.DeliveredEvent).filter_by(shop_domain=shop_domain, event_id=event_id).first():
            print(f"[idempotent] {event_id} already processed")
            return
        db.add(models.DeliveredEvent(shop_domain=shop_domain, event_id=event_id))
        db.commit()

        # 2) Resolve variant → group → store
        variant: models.ProductVariant = (
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
        store: models.Store = variant.product.store

        if not store.enabled:
            print(f"[skip] store disabled: {store.name}")
            return

        # Auto-learn sync location (first event)
        if not store.sync_location_id:
            store.sync_location_id = int(location_id)
            db.commit()
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

        # 3) Live truth for origin
        svc_origin = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        origin_rows = svc_origin.get_inventory_levels_for_items([inventory_item_id]) or []
        origin_lvl = next(
            (r for r in origin_rows if int(r.get("id", 0)) == int(inventory_item_id) and int(r.get("location_id", 0)) == int(store.sync_location_id)),
            None,
        )
        if not origin_lvl:
            print("[abort] live inventory at sync location not found")
            return

        current_available = int(origin_lvl.get("available", 0))
        current_on_hand = int(origin_lvl.get("on_hand", current_available))

        # 4) Echo suppression
        if _is_recent_echo(db, variant.id, current_available):
            print("[echo] drop")
            return

        # 5) Transaction + lock
        planned_writes: List[Tuple[models.ProductVariant, int, int, bool]] = []  # (member, target, current_av, bypass_on_hand)
        first_bootstrap = group.last_reconciled_at is None

        _ensure_no_open_tx(db)
        tx = db.begin()
        try:
            if not _acquire_lock(db, group.id):
                tx.rollback()
                print(f"[lock-miss] group={group.id}")
                return

            # Load FOR UPDATE origin snapshot and compute delta BEFORE update
            snap_origin = (
                db.query(models.InventoryLevel)
                .filter(
                    models.InventoryLevel.inventory_item_id == variant.inventory_item_id,
                    models.InventoryLevel.location_id == store.sync_location_id,
                )
                .with_for_update(read=True)
                .first()
            )
            last_available = int(snap_origin.available) if snap_origin and snap_origin.available is not None else 0
            delta_at_origin = current_available - last_available

            # Update/insert origin snapshot
            now = _utcnow()
            if snap_origin:
                snap_origin.available = current_available
                snap_origin.on_hand = current_on_hand
                snap_origin.last_fetched_at = now
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

            # Load all members and ensure EVERY member has a fresh snapshot.
            members: List[models.ProductVariant] = (
                db.query(models.ProductVariant)
                .options(
                    joinedload(models.ProductVariant.product).joinedload(models.Product.store),
                    joinedload(models.ProductVariant.inventory_levels),
                )
                .join(models.GroupMembership)
                .filter(models.GroupMembership.group_id == group.id)
                .all()
            )

            # Build a map of member.id -> snapshot (after refresh if missing/stale)
            snap_map: Dict[int, models.InventoryLevel] = {}
            fresh_snaps: List[models.InventoryLevel] = []
            for m in members:
                m_store = m.product.store
                if not m_store.enabled or not m_store.sync_location_id:
                    continue
                if getattr(m, "tracked", True) is False:
                    continue
                s = _get_fresh_snap(db, m)
                # If still missing (no local row yet), force a refresh to CREATE it:
                if s is None:
                    s = _refresh_member_snapshot(
                        db,
                        store_url=m_store.shopify_url,
                        token=m_store.api_token,
                        inventory_item_id=m.inventory_item_id,
                        location_id=m_store.sync_location_id,
                    )
                if s:
                    snap_map[m.id] = s
                    fresh_snaps.append(s)

            if first_bootstrap:
                # Pool from live truth using safe min with zero-anomaly guard
                new_pool = _compute_bootstrap_pool_from_snaps(fresh_snaps)
                group.pool_available = new_pool
                group.last_reconciled_at = _utcnow()
            else:
                # Delta policy (works for upward propagation on restocks)
                group.pool_available = max(0, int(group.pool_available or 0) + int(delta_at_origin))

            # Plan targets for ALL members using the *snap_map* (guaranteed rows)
            for m in members:
                m_store = m.product.store
                if not m_store.enabled or not m_store.sync_location_id or getattr(m, "tracked", True) is False:
                    continue

                m_snap = snap_map.get(m.id)
                if not m_snap:
                    continue  # should be rare now

                m_current_av = int(m_snap.available or 0)
                # Safety buffer & clamp
                target = max(0, int(group.pool_available or 0) - int(m_store.safety_buffer or 0))

                bypass_on_hand = False
                if first_bootstrap and BYPASS_ON_HAND_CLAMP_FOR_FIRST_BOOTSTRAP:
                    values = [int(s.available or 0) for s in fresh_snaps]
                    zeros = sum(1 for v in values if v == 0)
                    positives = sum(1 for v in values if v > 0)
                    if positives and zeros >= len(values) // 2 and group.pool_available and group.pool_available > 0:
                        bypass_on_hand = True

                if not bypass_on_hand:
                    target = min(target, int(m_snap.on_hand or 0))

                if m_current_av != target:
                    planned_writes.append((m, target, m_current_av, bypass_on_hand))

            tx.commit()
        except Exception:
            tx.rollback()
            raise

        # 6) Writes outside the lock (delta-first), PushLog, update local snapshots
        correlation_id = uuid.uuid4()
        for member, target, current_av, _bypass in planned_writes:
            m_store = member.product.store
            try:
                _write_available_delta(
                    store_url=m_store.shopify_url,
                    token=m_store.api_token,
                    inventory_item_id=member.inventory_item_id,
                    location_id=m_store.sync_location_id,
                    target_available=int(target),
                    current_available=int(current_av),
                )

                # Record push log and set local snapshot to the target (fresh timestamp)
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
        if owns:
            db.close()
