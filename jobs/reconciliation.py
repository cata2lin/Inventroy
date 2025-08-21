# jobs/reconciliation.py

import uuid
from datetime import datetime
from sqlalchemy.orm import Session, joinedload
import models
from shopify_service import ShopifyService
from product_service import ProductService
from services.inventory_sync_service import _acquire_lock


def run_reconciliation(db_factory):
    """
    Re-read truth for every active group, recompute a SAFE pool (min across members),
    and repair members by setting absolute 'available' targets.
    """
    db: Session = db_factory()
    try:
        print("--- Recon start ---")
        groups = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.status == 'active').all()

        for group in groups:
            planned = []  # [(member_variant, target)]
            with db.begin():
                if not _acquire_lock(db, group.id):
                    continue

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

                # Re-read truth for all members & refresh snapshots
                truths = []  # [(member, store, avail, on_hand)]
                for m in members:
                    s = m.product.store
                    if not s.enabled or not s.sync_location_id or getattr(m, "tracked", True) is False:
                        continue
                    svc = ShopifyService(store_url=s.shopify_url, token=s.api_token)
                    data = svc.get_inventory_levels_for_items([m.inventory_item_id]) or []
                    lvl = next(
                        (it for it in data
                         if int(it["id"]) == int(m.inventory_item_id)
                         and int(it["location_id"]) == int(s.sync_location_id)),
                        None
                    )
                    if not lvl:
                        continue
                    avail = int(lvl["available"])
                    on_hand = int(lvl.get("on_hand", avail))
                    truths.append((m, s, avail, on_hand))

                    # refresh snapshot
                    snap = next((x for x in m.inventory_levels if int(x.location_id) == int(s.sync_location_id)), None)
                    if snap:
                        snap.available = avail
                        snap.on_hand = on_hand
                        snap.last_fetched_at = datetime.utcnow()
                    else:
                        db.add(models.InventoryLevel(
                            inventory_item_id=m.inventory_item_id,
                            location_id=s.sync_location_id,
                            available=avail,
                            on_hand=on_hand,
                            last_fetched_at=datetime.utcnow(),
                        ))

                # --- THIS IS THE CHANGED PART ---
                # OLD (remove/replace if you see it):
                # new_pool = sum(avail for _m, _s, avail, _oh in truths)
                #
                # NEW (safe, mirrors bootstrap policy):
                avail_list = [avail for _m, _s, avail, _oh in truths]
                new_pool = min(avail_list) if avail_list else 0
                # ---------------------------------

                if int(group.pool_available) != int(new_pool):
                    group.pool_available = int(new_pool)
                group.last_reconciled_at = datetime.utcnow()

                # Plan targets using the corrected pool
                for m, s, avail, on_hand in truths:
                    target = max(0, int(group.pool_available) - int(s.safety_buffer))
                    target = min(target, on_hand)
                    if avail != target:
                        planned.append((m, s, target))

            # Outside lock: write & push_log
            corr = uuid.uuid4()
            for m, s, target in planned:
                ps = ProductService(store_url=s.shopify_url, token=s.api_token)
                inv_gid = f"gid://shopify/InventoryItem/{m.inventory_item_id}"
                loc_gid = f"gid://shopify/Location/{s.sync_location_id}"
                try:
                    ps.set_inventory_available(inv_gid, loc_gid, int(target))
                    with db.begin():
                        db.add(models.PushLog(
                            variant_id=m.id,
                            target_available=int(target),
                            correlation_id=str(corr),
                            write_source='recon',
                            written_at=datetime.utcnow(),
                        ))
                except Exception as e:
                    print(f"[recon write-fail] store={s.name} variant={m.id} target={target}: {e}")

        print("--- Recon done ---")
    finally:
        db.close()
