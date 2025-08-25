# services/inventory_sync_service.py

import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text

import models

# Prefer package imports; fallback to root-level service files
try:
    from services.shopify_service import ShopifyService
except Exception:
    from shopify_service import ShopifyService  # type: ignore

# ---------------- config-ish constants ----------------
ECHO_WINDOW_SECONDS = 60
BYPASS_ON_HAND_CLAMP_FOR_FIRST_BOOTSTRAP = True

# ---------------- time helpers ----------------
def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)

def _ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

# ---------------- locking ----------------
def _acquire_lock(db: Session, group_id: str) -> bool:
    # Use pg advisory locks on hashtext(group_id) to serialize group writes
    q = text("SELECT pg_try_advisory_xact_lock(hashtext(:k))")
    row = db.execute(q, {"k": group_id}).scalar()
    return bool(row)

# ---------------- echo suppression ----------------
def _is_recent_echo(db: Session, variant_id: int, current_available: int, window_s: int = ECHO_WINDOW_SECONDS) -> bool:
    since = _utcnow() - timedelta(seconds=window_s)
    row = (
        db.query(models.PushLog)
        .filter(models.PushLog.variant_id == variant_id, models.PushLog.written_at >= since)
        .order_by(models.PushLog.written_at.desc())
        .first()
    )
    return bool(row and int(row.target_available) == int(current_available))

def _pick_best_location(level_rows: List[dict]) -> Optional[int]:
    """
    Choose a good sync location when a store has multiple.
    Strategy: pick the location with the largest (available + on_hand).
    """
    best = None
    best_score = None
    for r in level_rows:
        loc = int(r.get("location_id", 0) or 0)
        av = int(r.get("available", 0) or 0)
        oh = int(r.get("on_hand", av) or 0)
        score = av + oh
        if best is None or score > best_score:
            best = loc
            best_score = score
    return best

def _autolearn_store_location(db: Session, store: models.Store, inventory_item_id: int) -> Optional[int]:
    """
    Auto-detect a store's sync location. IMPORTANT: does NOT commit.
    Caller decides when to commit.
    """
    svc = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    rows = svc.get_inventory_levels_for_items([inventory_item_id]) or []
    if not rows:
        return None
    # uniq by location
    locs = {}
    for r in rows:
        locs[int(r["location_id"])] = r
    chosen = next(iter(locs.keys())) if len(locs) == 1 else _pick_best_location(list(locs.values()))
    if chosen:
        store.sync_location_id = int(chosen)
    return store.sync_location_id

def _refresh_member_snapshot(
    db: Session,
    store_url: str,
    token: str,
    inventory_item_id: int,
    location_id: int,
) -> Optional[models.InventoryLevel]:
    """
    Read live from Shopify and upsert InventoryLevel row. IMPORTANT: does NOT commit.
    """
    svc = ShopifyService(store_url=store_url, token=token)
    rows = svc.get_inventory_levels_for_items([inventory_item_id]) or []
    lvl = next(
        (it for it in rows
         if int(it["id"]) == int(inventory_item_id)
         and int(it["location_id"]) == int(location_id)),
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
    """
    Ensure we have a reasonably fresh snapshot for member at its store's sync_location.
    IMPORTANT: never commits — caller commits after the plan.
    """
    m_store = member.product.store
    if not m_store.enabled:
        return None

    # Auto-learn per-store sync location if missing (no commit here)
    if not m_store.sync_location_id:
        _autolearn_store_location(db, m_store, member.inventory_item_id)
        if not m_store.sync_location_id:
            return None

    m_snap = next(
        (lvl for lvl in member.inventory_levels if int(lvl.location_id) == int(m_store.sync_location_id)),
        None,
    )
    last_ts = _ensure_aware(m_snap.last_fetched_at) if m_snap else None

    # Refresh if missing or stale
    if not m_snap or not last_ts or (_utcnow() - last_ts) > timedelta(minutes=10):
        snap_live = _refresh_member_snapshot(
            db,
            store_url=m_store.shopify_url,
            token=m_store.api_token,
            inventory_item_id=member.inventory_item_id,
            location_id=m_store.sync_location_id,
        )
        return snap_live or m_snap
    return m_snap

def _compute_bootstrap_pool_from_snaps(snaps: List[models.InventoryLevel]) -> int:
    """
    Conservative pool = MIN(available across stores); ignores negatives.
    """
    vals = [max(0, int(s.available or 0)) for s in snaps if s is not None]
    return min(vals) if vals else 0

# ---------------- main entry ----------------
def process_inventory_update_event(
    shop_domain: str,
    event_id: str,
    inventory_item_id: int,
    location_id: int,
    db_factory=None,
    db: Optional[Session] = None,
):
    owns_session = False
    if db is None:
        if db_factory is None:
            from database import SessionLocal  # type: ignore
            db_factory = SessionLocal
        db = db_factory()
        owns_session = True

    try:
        # Idempotency record (commit outside of any group lock)
        if db.query(models.DeliveredEvent).filter_by(shop_domain=shop_domain, event_id=event_id).first():
            print(f"[idempotent] {event_id} already processed")
            return
        db.add(models.DeliveredEvent(shop_domain=shop_domain, event_id=event_id))
        db.commit()

        # Resolve variant → group → store
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

        # Ensure store has a sync location quickly (commit OK here; not under lock yet)
        if not store.sync_location_id:
            store.sync_location_id = int(location_id)
            db.commit()
            print(f"[init] store '{store.name}' sync_location_id set to {store.sync_location_id}")
        else:
            if int(location_id) != int(store.sync_location_id):
                print(f"[info] event at non-sync location (event={location_id}, sync={store.sync_location_id}); processing anyway")

        if getattr(variant, "tracked", True) is False:
            print("[skip] variant untracked")
            return

        if getattr(group, "status", "active") == "conflicted":
            print("[skip] group is conflicted")
            return

        # Live truth at origin location (where webhook fired)
        svc_origin = ShopifyService(store_url=store.shopify_url, token=store.api_token)
        origin_rows = svc_origin.get_inventory_levels_for_items([inventory_item_id]) or []
        origin_lvl = next(
            (it for it in origin_rows
             if int(it["id"]) == int(inventory_item_id)
             and int(it["location_id"]) == int(location_id)),
            None,
        )
        if not origin_lvl:
            print("[abort] live inventory at event location not found")
            return

        current_available = int(origin_lvl.get("available", 0))
        current_on_hand = int(origin_lvl.get("on_hand", current_available))

        # Echo suppression
        if _is_recent_echo(db, variant.id, current_available):
            print("[echo] drop")
            return

        planned_writes: List[Tuple[models.ProductVariant, int, int, bool]] = []
        first_bootstrap = group.last_reconciled_at is None
        stores_changed_sync_loc: List[models.Store] = []

        # -------- LOCKED SECTION (no db.commit inside) --------
        with db.begin():  # opens a transaction; closes on exit
            if not _acquire_lock(db, group.id):
                print(f"[lock-miss] group={group.id}")
                return

            # Compute delta vs last snapshot BEFORE updating it
            snap_origin = (
                db.query(models.InventoryLevel)
                .filter(
                    models.InventoryLevel.inventory_item_id == variant.inventory_item_id,
                    models.InventoryLevel.location_id == location_id,
                )
                .with_for_update(read=True)
                .first()
            )
            last_available = int(snap_origin.available) if snap_origin and snap_origin.available is not None else 0
            delta_at_origin = current_available - last_available

            # Update/insert origin snapshot (no commit here)
            now = _utcnow()
            if snap_origin:
                snap_origin.available = current_available
                snap_origin.on_hand = current_on_hand
                snap_origin.last_fetched_at = now
            else:
                db.add(
                    models.InventoryLevel(
                        inventory_item_id=variant.inventory_item_id,
                        location_id=location_id,
                        available=current_available,
                        on_hand=current_on_hand,
                        last_fetched_at=now,
                    )
                )

            # Load group members (will use snapshots and/or live reads; no commits here)
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

            fresh_snaps: List[models.InventoryLevel] = []
            for member in members:
                m_store = member.product.store
                if not m_store.enabled:
                    continue
                if not m_store.sync_location_id:
                    if _autolearn_store_location(db, m_store, member.inventory_item_id):
                        stores_changed_sync_loc.append(m_store)
                    if not m_store.sync_location_id:
                        continue
                if getattr(member, "tracked", True) is False:
                    continue
                m_snap = _get_fresh_snap(db, member)  # never commits
                if m_snap:
                    fresh_snaps.append(m_snap)

            # Bootstrap or delta advance pool
            if first_bootstrap or (snap_origin is None):
                group.pool_available = _compute_bootstrap_pool_from_snaps(fresh_snaps)
                group.last_reconciled_at = _utcnow()
            else:
                group.pool_available = max(0, int(group.pool_available) + int(delta_at_origin))

            # Plan store targets (clamped by on_hand unless bootstrap bypass or continue_selling)
            for member in members:
                m_store = member.product.store
                if not m_store.enabled or not m_store.sync_location_id or getattr(member, "tracked", True) is False:
                    continue

                m_snap = next(
                    (lvl for lvl in member.inventory_levels if int(lvl.location_id) == int(m_store.sync_location_id)),
                    None,
                )
                if not m_snap:
                    continue

                m_current_av = int(m_snap.available or 0)
                target = max(0, int(group.pool_available) - int(m_store.safety_buffer))

                bypass_on_hand = False
                if first_bootstrap and BYPASS_ON_HAND_CLAMP_FOR_FIRST_BOOTSTRAP:
                    values = [int(s.available or 0) for s in fresh_snaps]
                    zeros = sum(1 for v in values if v == 0)
                    positives = sum(1 for v in values if v > 0)
                    if positives and zeros >= len(values) // 2 and group.pool_available > 0:
                        bypass_on_hand = True

                if not bypass_on_hand and not bool(getattr(m_store, "continue_selling", False)):
                    target = min(target, int(m_snap.on_hand or 0))

                if m_current_av != target:
                    planned_writes.append((member, target, m_current_av, bypass_on_hand))
        # -------- END LOCKED SECTION --------

        # Persist any auto-learned sync_location_id updates (safe to commit now)
        if stores_changed_sync_loc:
            try:
                db.commit()
            except Exception:
                db.rollback()

        # Apply planned writes (each write logged; refresh live snapshot; commit between steps)
        for member, target, current, _bypass_on_hand in planned_writes:
            m_store = member.product.store
            try:
                delta = int(target) - int(current)
                if delta == 0:
                    continue

                svc = ShopifyService(store_url=m_store.shopify_url, token=m_store.api_token)
                # Use inventoryAdjustQuantities (delta) targeting the store's sync_location
                svc.adjust_inventory_quantity(
                    inventory_item_id=member.inventory_item_id,
                    location_id=m_store.sync_location_id,
                    available_delta=delta,
                )

                # Log push for echo suppression
                db.add(
                    models.PushLog(
                        variant_id=member.id,
                        target_available=int(target),
                        correlation_id=str(uuid.uuid4()),
                        written_at=_utcnow(),
                    )
                )

                # Refresh live values and persist both available and on_hand (no nested tx)
                snap_live = _refresh_member_snapshot(
                    db,
                    store_url=m_store.shopify_url,
                    token=m_store.api_token,
                    inventory_item_id=member.inventory_item_id,
                    location_id=m_store.sync_location_id,
                )

                try:
                    db.commit()
                except Exception:
                    db.rollback()

            except Exception as e:
                print(f"[write-fail] store={m_store.name} variant={member.id} target={target}: {e}")

    finally:
        if owns_session and db is not None:
            db.close()
