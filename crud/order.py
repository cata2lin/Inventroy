# crud/order.py

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, select, delete
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import models
import schemas
from shopify_service import gid_to_id
from .utils import upsert_batch
from sqlalchemy.dialects.postgresql import insert
# FIX: Import the advisory lock helper from the inventory sync service
from services.inventory_sync_service import _acquire_lock


# ---------------- helpers (dict/attr-safe) ----------------

def _get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _parse_dt(val) -> Optional[datetime]:
    """
    Parse ISO/Shopify timestamps and return TZ-aware UTC datetimes.
    """
    if not val:
        return None
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        s = str(val).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if " " in s and "T" not in s:
            s = s.replace(" ", "T")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ---------------- committed stock projector (IDEMPOTENT + ORDERED) ----------------

def update_committed_stock_for_order(db: Session, order: models.Order):
    """
    Re-project committed stock for ONLY the barcode groups touched by this order,
    based on ALL OPEN orders for the same store. This is idempotent and safe to
    call on create/update/cancel/fulfill.

    Open orders = order.cancelled_at IS NULL AND fulfillment_status NOT IN ('fulfilled','restocked','cancelled')
    """
    # 1) Which groups are impacted by THIS order?
    impacted_group_ids = [
        gid for (gid,) in (
            db.query(models.GroupMembership.group_id)
            .join(models.ProductVariant, models.ProductVariant.id == models.GroupMembership.variant_id)
            .join(models.LineItem, models.LineItem.variant_id == models.ProductVariant.id)
            .filter(models.LineItem.order_id == order.id)
            .distinct()
            .all()
        )
        if gid
    ]
    if not impacted_group_ids:
        return

    # Stable, deterministic order to reduce lock contention between concurrent workers
    impacted_group_ids.sort()

    # 2) Compute totals for those groups across ALL open orders of this store
    OPEN_EXCLUDE = ['fulfilled', 'restocked', 'cancelled']

    # FIX: Correct the query to explicitly join all necessary tables
    totals = (
        db.query(
            models.GroupMembership.group_id.label("group_id"),
            func.coalesce(func.sum(models.LineItem.quantity), 0).label("committed_units"),
            func.count(func.distinct(models.LineItem.order_id)).label("open_orders_count"),
        )
        .select_from(models.Order)  # Start FROM the orders table
        .join(models.LineItem, models.LineItem.order_id == models.Order.id)
        .join(models.ProductVariant, models.ProductVariant.id == models.LineItem.variant_id)
        .join(models.GroupMembership, models.GroupMembership.variant_id == models.ProductVariant.id)
        .filter(
            models.Order.store_id == order.store_id,
            models.Order.cancelled_at.is_(None),
            ~models.Order.fulfillment_status.in_(OPEN_EXCLUDE),
            models.GroupMembership.group_id.in_(impacted_group_ids),
        )
        .group_by(models.GroupMembership.group_id)
        .all()
    )
    
    totals_map: Dict[str, Dict[str, int]] = {
        row.group_id: {
            "committed_units": int(row.committed_units or 0),
            "open_orders_count": int(row.open_orders_count or 0),
        }
        for row in totals
    }

    # 3) UPSERT exact values (replace, not add) in sorted order
    upsert_rows: List[Dict[str, Any]] = []
    for gid in impacted_group_ids:
        vals = totals_map.get(gid, {"committed_units": 0, "open_orders_count": 0})
        upsert_rows.append({
            "group_id": gid,
            "store_id": order.store_id,
            "committed_units": vals["committed_units"],
            "open_orders_count": vals["open_orders_count"],
        })

    if upsert_rows:
        stmt = insert(models.CommittedStock).values(upsert_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=['group_id', 'store_id'],
            set_={
                'committed_units': stmt.excluded.committed_units,
                'open_orders_count': stmt.excluded.open_orders_count,
            },
        )
        db.execute(stmt)


# ---------------- webhook upserts (orders/fulfillments/refunds) ----------------

def create_or_update_order_from_webhook(db: Session, store_id: int, order_data: Any):
    """
    Upserts a single order and its line items from a webhook payload.
    Accepts dict or Pydantic object. Field access is dict/attr-safe.
    """
    oid = _get(order_data, "id")
    if oid is None:
        return
    oid = int(oid)

    financial_status = _get(order_data, "financial_status") or 'pending'
    fulfillment_status = _get(order_data, "fulfillment_status") or 'unfulfilled'

    total_shipping_price = None
    tsp_set = _get(order_data, "total_shipping_price_set")
    if tsp_set:
        shop_money = tsp_set.get("shop_money") if isinstance(tsp_set, dict) else _get(tsp_set, "shop_money")
        if shop_money:
            total_shipping_price = shop_money.get("amount") if isinstance(shop_money, dict) else _get(shop_money, "amount")
    if total_shipping_price is None:
        total_shipping_price = 0.0
        for sl in (_get(order_data, "shipping_lines") or []):
            try:
                p = _get(sl, "price")
                total_shipping_price += float(p) if p is not None else 0.0
            except Exception:
                pass

    payment_gateway_names = _get(order_data, "payment_gateway_names")
    if isinstance(payment_gateway_names, list):
        payment_gateway_names = ", ".join(payment_gateway_names)

    order_dict = {
        "id": oid,
        "shopify_gid": _get(order_data, "admin_graphql_api_id"),
        "store_id": store_id,
        "name": _get(order_data, "name"),
        "email": _get(order_data, "email"),
        "phone": _get(order_data, "phone"),
        "created_at": _parse_dt(_get(order_data, "created_at")),
        "updated_at": _parse_dt(_get(order_data, "updated_at")),
        "cancelled_at": _parse_dt(_get(order_data, "cancelled_at")),
        "cancel_reason": _get(order_data, "cancel_reason"),
        "closed_at": _parse_dt(_get(order_data, "closed_at")),
        "processed_at": _parse_dt(_get(order_data, "processed_at")),
        "financial_status": str(financial_status).lower(),
        "fulfillment_status": str(fulfillment_status).lower(),
        "currency": _get(order_data, "currency") or _get(order_data, "currency_code"),
        "payment_gateway_names": payment_gateway_names,
        "note": _get(order_data, "note"),
        "tags": _get(order_data, "tags"),
        "total_price": _get(order_data, "total_price"),
        "subtotal_price": _get(order_data, "subtotal_price"),
        "total_tax": _get(order_data, "total_tax"),
        "total_discounts": _get(order_data, "total_discounts"),
        "total_shipping_price": total_shipping_price if total_shipping_price is not None else None,
    }
    hold_reason = _get(order_data, "hold_reason")
    if hasattr(models.Order, "hold_reason") and hold_reason is not None:
        order_dict["hold_reason"] = hold_reason

    # STORE-SCOPED UPSERT
    upsert_batch(db, models.Order, [order_dict], ['store_id', 'id'])

    # Ensure referenced Products/Variants exist FOR THIS STORE
    line_items_src = _get(order_data, "line_items") or []
    required_variant_ids = {int(_get(it, "variant_id")) for it in line_items_src if _get(it, "variant_id")}
    required_product_ids = {int(_get(it, "product_id")) for it in line_items_src if _get(it, "product_id")}

    existing_variants = set()
    existing_products = set()
    if required_variant_ids:
        existing_variants = {
            v[0]
            for v in db.query(models.ProductVariant.id)
            .filter(
                models.ProductVariant.store_id == store_id,
                models.ProductVariant.id.in_(required_variant_ids),
            )
            .all()
        }
    if required_product_ids:
        existing_products = {
            p[0]
            for p in db.query(models.Product.id)
            .filter(
                models.Product.store_id == store_id,
                models.Product.id.in_(required_product_ids),
            )
            .all()
        }

    products_to_create: List[Dict[str, Any]] = []
    variants_to_create: List[Dict[str, Any]] = []

    for it in line_items_src:
        pid = _get(it, "product_id")
        vid = _get(it, "variant_id")
        title = _get(it, "title")
        sku = _get(it, "sku")

        if pid and int(pid) not in existing_products:
            products_to_create.append({
                "id": int(pid),
                "store_id": store_id,
                "title": (str(title).split(' - ')[0] if title else "Placeholder Product"),
                "shopify_gid": f"gid://shopify/Product/{int(pid)}",
                "status": "active",
            })
            existing_products.add(int(pid))

        if vid and int(vid) not in existing_variants:
            variants_to_create.append({
                "id": int(vid),
                "product_id": int(pid) if pid else None,
                "store_id": store_id,
                "title": title or "Placeholder Variant",
                "sku": sku or None,
                "shopify_gid": f"gid://shopify/ProductVariant/{int(vid)}",
            })
            existing_variants.add(int(vid))

    if products_to_create:
        upsert_batch(db, models.Product, products_to_create, ['store_id', 'id'])
    if variants_to_create:
        # Before upserting, check for existing SKUs to avoid unique constraint violations
        skus_to_check = {v['sku'] for v in variants_to_create if v['sku']}
        if skus_to_check:
            existing_skus = {
                s[0] for s in db.query(models.ProductVariant.sku).filter(
                    models.ProductVariant.store_id == store_id,
                    models.ProductVariant.sku.in_(skus_to_check)
                ).all()
            }
            variants_to_create = [v for v in variants_to_create if not v['sku'] or v['sku'] not in existing_skus]
        
        if variants_to_create:
            upsert_batch(db, models.ProductVariant, variants_to_create, ['store_id', 'id'])

    # Line items
    line_items_list: List[Dict[str, Any]] = []
    for it in line_items_src:
        li_id = _get(it, "id")
        if not li_id:
            continue
        line_items_list.append({
            "id": int(li_id),
            "order_id": oid,
            "variant_id": int(_get(it, "variant_id")) if _get(it, "variant_id") else None,
            "product_id": int(_get(it, "product_id")) if _get(it, "product_id") else None,
            "title": _get(it, "title"),
            "quantity": _get(it, "quantity"),
            "sku": _get(it, "sku"),
            "vendor": _get(it, "vendor"),
            "price": _get(it, "price"),
            "total_discount": _get(it, "total_discount"),
            "taxable": _get(it, "taxable"),
            "shopify_gid": f"gid://shopify/LineItem/{int(li_id)}",
        })
    if line_items_list:
        upsert_batch(db, models.LineItem, line_items_list, ['id'])

    db.flush()
    order_obj = db.query(models.Order).filter(models.Order.id == oid).one_or_none()
    if order_obj:
        update_committed_stock_for_order(db, order_obj)

    db.commit()


def upsert_order_from_webhook(db: Session, store_id: int, order_obj: Any):
    return create_or_update_order_from_webhook(db, store_id, order_obj)


def create_or_update_fulfillment_from_webhook(db: Session, store_id: int, fulfillment_data: Any):
    order_id = _get(fulfillment_data, "order_id")
    if not order_id:
        return
    order_id = int(order_id)

    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(
            db, models.Order,
            [{"id": order_id, "store_id": store_id, "name": f"#{order_id}", "shopify_gid": f"gid://shopify/Order/{order_id}"}],
            ['store_id', 'id']
        )

    fid = _get(fulfillment_data, "id")
    status = _get(fulfillment_data, "status")

    # Support both REST (top-level fields) and GraphQL-style tracking_info list
    tracking_company = _get(fulfillment_data, "tracking_company")
    tracking_number = _get(fulfillment_data, "tracking_number")
    tracking_url = _get(fulfillment_data, "tracking_url")

    ti = _get(fulfillment_data, "tracking_info") or _get(fulfillment_data, "trackingInfo") or []
    if (not tracking_company or not tracking_number or not tracking_url) and isinstance(ti, list) and ti:
        first = ti[0] or {}
        tracking_company = tracking_company or _get(first, "company")
        tracking_number = tracking_number or _get(first, "number")
        tracking_url = tracking_url or _get(first, "url")

    fulfillment_dict = {
        "id": int(fid) if fid else None,
        "order_id": order_id,
        "status": status.lower() if isinstance(status, str) else status,
        "created_at": _parse_dt(_get(fulfillment_data, "created_at")),
        "updated_at": _parse_dt(_get(fulfillment_data, "updated_at")),
        "tracking_company": tracking_company,
        "tracking_number": tracking_number,
        "tracking_url": str(tracking_url) if tracking_url else None,
        "shopify_gid": f"gid://shopify/Fulfillment/{int(fid)}" if fid else None,
    }
    if fulfillment_dict["id"] is not None:
        upsert_batch(db, models.Fulfillment, [fulfillment_dict], ['id'])

    order_to_update = db.query(models.Order).filter(models.Order.id == order_id).first()
    if order_to_update:
        if status and str(status).lower() == 'success':
            order_to_update.fulfillment_status = 'fulfilled'
        elif status:
            order_to_update.fulfillment_status = str(status).lower()
        db.flush()
        # Re-project committed stock because order fulfillment status changed
        update_committed_stock_for_order(db, order_to_update)
        db.commit()


def create_refund_from_webhook(db: Session, store_id: int, refund_data: Any):
    order_id = _get(refund_data, "order_id")
    if not order_id:
        return
    order_id = int(order_id)

    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(
            db, models.Order,
            [{"id": order_id, "store_id": store_id, "name": f"#{order_id}", "shopify_gid": f"gid://shopify/Order/{order_id}"}],
            ['store_id', 'id']
        )

    refund_line_items = _get(refund_data, "refund_line_items") or []
    required_line_item_ids = { _get(item, "line_item_id") for item in refund_line_items if _get(item, "line_item_id") }
    if required_line_item_ids:
        existing_line_item_ids = {
            lid for (lid,) in db.query(models.LineItem.id).filter(models.LineItem.id.in_(required_line_item_ids)).all()
        }
    else:
        existing_line_item_ids = set()

    line_items_to_create: List[Dict[str, Any]] = []
    for item in refund_line_items:
        li_id = _get(item, "line_item_id")
        if not li_id or int(li_id) in existing_line_item_ids:
            continue
        li = _get(item, "line_item") or {}
        line_items_to_create.append({
            "id": int(_get(li, "id")) if _get(li, "id") else int(li_id),
            "order_id": order_id,
            "variant_id": _get(li, "variant_id"),
            "product_id": _get(li, "product_id"),
            "title": _get(li, "title") or "N/A",
            "quantity": _get(li, "quantity") or 0,
            "sku": _get(li, "sku"),
            "shopify_gid": f"gid://shopify/LineItem/{int(_get(li, 'id') or li_id)}",
        })
    if line_items_to_create:
        upsert_batch(db, models.LineItem, line_items_to_create, ['id'])

    rid = _get(refund_data, "id")
    transactions = _get(refund_data, "transactions") or []
    total_refunded = 0.0
    currency = "USD"
    for t in transactions:
        if _get(t, "kind") == "refund" and _get(t, "status") == "success":
            amt = _get(t, "amount")
            if amt is not None:
                try:
                    total_refunded += float(amt)
                except Exception:
                    pass
            currency = _get(t, "currency") or currency

    refund_dict = {
        "id": int(rid) if rid else None,
        "order_id": order_id,
        "created_at": _parse_dt(_get(refund_data, "created_at")),
        "note": _get(refund_data, "note"),
        "total_refunded": total_refunded,
        "currency": currency,
        "shopify_gid": f"gid://shopify/Refund/{int(rid)}" if rid else None,
    }
    if refund_dict["id"] is not None:
        upsert_batch(db, models.Refund, [refund_dict], ['id'])
        db_refund = db.query(models.Refund).filter(models.Refund.id == refund_dict["id"]).one()

        refund_line_items_list: List[Dict[str, Any]] = []
        for item in refund_line_items:
            refund_line_items_list.append({
                "id": int(_get(item, "id")) if _get(item, "id") else None,
                "refund_id": db_refund.id,
                "line_item_id": int(_get(item, "line_item_id")) if _get(item, "line_item_id") else None,
                "quantity": _get(item, "quantity"),
                "subtotal": _get(item, "subtotal"),
                "total_tax": _get(item, "total_tax"),
            })
        refund_line_items_list = [x for x in refund_line_items_list if x["id"] is not None]
        if refund_line_items_list:
            upsert_batch(db, models.RefundLineItem, refund_line_items_list, ['id'])

    # (Refunds don't change committed stock; skip projector)
    db.commit()


def apply_order_hold_from_webhook(db: Session, store_id: int, payload: Any, on_hold: bool):
    order_id = _get(payload, "order_id")

    if not order_id:
        fo = _get(payload, "fulfillment_order")
        if isinstance(fo, dict):
            order_id = _get(fo, "order_id") or _get(_get(fo, "order"), "id")

    if not order_id:
        print("[holds] Could not resolve order_id from webhook payload.")
        return

    order_id = int(order_id)

    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(
            db, models.Order,
            [{"id": order_id, "store_id": store_id, "name": f"#{order_id}", "shopify_gid": f"gid://shopify/Order/{order_id}"}],
            ['store_id', 'id']
        )

    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not order:
        return

    if on_hold:
        order.fulfillment_status = "on_hold"
        reason = _get(payload, "reason") or _get(payload, "hold_reason")
        if hasattr(order, "hold_reason") and reason:
            order.hold_reason = reason
    else:
        if order.fulfillment_status != "fulfilled":
            order.fulfillment_status = "unfulfilled"
        if hasattr(order, "hold_reason"):
            order.hold_reason = None

    db.flush()
    update_committed_stock_for_order(db, order)
    db.commit()


def create_or_update_orders(db: Session, orders_data: List[schemas.ShopifyOrder], store_id: int):
    if not orders_data:
        return

    all_products, all_variants, all_inventory_levels, all_locations = [], [], [], []
    all_orders, all_line_items, all_fulfillments, all_fulfillment_events = [], [], [], []
    processed_product_ids, processed_variant_ids, processed_location_ids, processed_line_item_ids = set(), set(), set(), set()
    order_ids_to_process: List[int] = []

    for order in orders_data:
        order_ids_to_process.append(order.legacy_resource_id)
        payment_gateway_str = ", ".join(order.paymentGatewayNames) if order.paymentGatewayNames else None

        all_orders.append({
            "id": order.legacy_resource_id, "shopify_gid": order.id, "store_id": store_id, "name": order.name,
            "email": order.email, "phone": order.phone, "created_at": order.created_at, "updated_at": order.updated_at,
            "cancelled_at": order.cancelled_at, "cancel_reason": order.cancel_reason, "closed_at": order.closed_at,
            "processed_at": order.processed_at, "financial_status": (order.financial_status or 'pending').lower(),
            "fulfillment_status": (order.fulfillment_status or 'unfulfilled').lower(), "currency": order.currency,
            "payment_gateway_names": payment_gateway_str, "note": order.note, "tags": ", ".join(order.tags) if order.tags else None,
            "total_price": order.total_price.amount if order.total_price else None,
            "subtotal_price": order.subtotal_price.amount if order.subtotal_price else None,
            "total_tax": order.total_tax.amount if order.total_tax else None,
            "total_discounts": order.total_discounts.amount if order.total_discounts else None,
            "total_shipping_price": order.total_shipping_price.amount if order.total_shipping_price else None,
        })

        for item in order.line_items:
            line_item_id = gid_to_id(item.id)
            if not line_item_id or line_item_id in processed_line_item_ids:
                continue
            processed_line_item_ids.add(line_item_id)

            if item.variant:
                variant = item.variant
                product = variant.product

                if product and product.legacy_resource_id not in processed_product_ids:
                    processed_product_ids.add(product.legacy_resource_id)

                    category_name = None
                    if product.category and isinstance(product.category, dict):
                        category_name = product.category.get('name')
                    elif product.category:
                        category_name = getattr(product.category, 'name', None)

                    image_url = None
                    if product.featured_image and isinstance(product.featured_image, dict):
                        image_url = product.featured_image.get('url')
                    elif product.featured_image:
                        image_url = getattr(product.featured_image, 'url', None)

                    all_products.append({
                        "id": product.legacy_resource_id, "shopify_gid": product.id, "store_id": store_id, "title": product.title,
                        "body_html": product.body_html, "vendor": product.vendor, "product_type": product.product_type,
                        "product_category": category_name, "created_at": product.created_at, "handle": product.handle,
                        "updated_at": product.updated_at, "published_at": product.published_at, "status": product.status,
                        "tags": ", ".join(product.tags) if product.tags else None, "image_url": image_url,
                    })

                if variant.legacy_resource_id not in processed_variant_ids:
                    processed_variant_ids.add(variant.legacy_resource_id)
                    inv_item = variant.inventory_item
                    all_variants.append({
                        "id": variant.legacy_resource_id, "shopify_gid": variant.id, "product_id": product.legacy_resource_id if product else None,
                        "store_id": store_id, "title": variant.title, "price": variant.price, "sku": (variant.sku or None),
                        "position": variant.position, "inventory_policy": variant.inventory_policy,
                        "compare_at_price": variant.compare_at_price, "barcode": variant.barcode,
                        "inventory_item_id": (inv_item.legacy_resource_id if inv_item else None),
                        "inventory_quantity": variant.inventory_quantity, "created_at": variant.created_at,
                        "updated_at": variant.updated_at, "cost": (inv_item.unit_cost.amount if (inv_item and inv_item.unit_cost) else None),
                        "inventory_management": "shopify" if (inv_item and getattr(inv_item, "tracked", False)) else "not_tracked",
                    })

                    if inv_item:
                        for level in inv_item.inventory_levels:
                            loc = level.location
                            if loc.legacy_resource_id not in processed_location_ids:
                                processed_location_ids.add(loc.legacy_resource_id)
                                all_locations.append({"id": loc.legacy_resource_id, "name": loc.name, "store_id": store_id})

                            available_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'available'), None)
                            on_hand_qty = next((q['quantity'] for q in level.quantities if q['name'] == 'on_hand'), None)
                            all_inventory_levels.append({
                                "inventory_item_id": inv_item.legacy_resource_id, "location_id": loc.legacy_resource_id,
                                "available": available_qty, "on_hand": on_hand_qty, "updated_at": level.updated_at,
                            })

            all_line_items.append({
                "id": line_item_id, "shopify_gid": item.id, "order_id": order.legacy_resource_id,
                "variant_id": item.variant.legacy_resource_id if item.variant else None,
                "product_id": item.variant.product.legacy_resource_id if (item.variant and item.variant.product) else None,
                "title": item.title, "quantity": item.quantity, "sku": item.sku, "vendor": item.vendor,
                "price": item.original_unit_price.amount if item.original_unit_price else None,
                "total_discount": item.total_discount.amount if item.total_discount else None, "taxable": item.taxable,
            })

        for fulfillment in order.fulfillments:
            # Extract tracking from tracking_info list first; fallback to top-level fields
            ti = _get(fulfillment, "tracking_info") or _get(fulfillment, "trackingInfo") or []
            company = number = url = None
            if isinstance(ti, list) and ti:
                first = ti[0] or {}
                company = _get(first, "company")
                number = _get(first, "number")
                url = _get(first, "url")
            company = company or _get(fulfillment, "tracking_company")
            number  = number  or _get(fulfillment, "tracking_number")
            url     = url     or _get(fulfillment, "tracking_url")

            all_fulfillments.append({
                "id": fulfillment.legacy_resource_id, "shopify_gid": fulfillment.id, "order_id": order.legacy_resource_id,
                "status": fulfillment.status, "created_at": fulfillment.created_at, "updated_at": fulfillment.updated_at,
                "tracking_company": company, "tracking_number": number,
                "tracking_url": str(url) if url else None,
            })
            for event in fulfillment.events:
                event_id = gid_to_id(event.id)
                if event_id:
                    all_fulfillment_events.append({
                        "id": event_id, "shopify_gid": event.id, "fulfillment_id": fulfillment.legacy_resource_id,
                        "status": event.status, "happened_at": event.happened_at,
                        "description": getattr(event, "message", None) or getattr(event, "description", None),
                    })

    # STORE-SCOPED upserts where possible
    upsert_batch(db, models.Location, all_locations, ['store_id', 'id'])
    upsert_batch(db, models.Product, all_products, ['store_id', 'id'])
    upsert_batch(db, models.ProductVariant, all_variants, ['store_id', 'id'])
    upsert_batch(db, models.InventoryLevel, all_inventory_levels, ['inventory_item_id', 'location_id'])
    upsert_batch(db, models.Order, all_orders, ['store_id', 'id'])
    upsert_batch(db, models.LineItem, all_line_items, ['id'])
    upsert_batch(db, models.Fulfillment, all_fulfillments, ['id'])
    upsert_batch(db, models.FulfillmentEvent, all_fulfillment_events, ['id'])

    db.flush()
    # Project committed stock for affected orders (idempotent)
    print(f"Updating committed stock for {len(order_ids_to_process)} orders...")
    orders_to_update = db.query(models.Order).filter(models.Order.id.in_(order_ids_to_process)).all()
    for order_obj in orders_to_update:
        update_committed_stock_for_order(db, order_obj)
    db.commit()
    print("Database synchronization and committed stock update complete.")


def get_orders_by_store(db: Session, store_id: int):
    return db.query(models.Order).filter(models.Order.store_id == store_id).order_by(models.Order.created_at.desc()).all()


def get_fulfillments_by_store(db: Session, store_id: int):
    return (
        db.query(
            models.Fulfillment.id, models.Fulfillment.created_at, models.Fulfillment.tracking_company,
            models.Fulfillment.tracking_number, models.Fulfillment.status, models.Order.name.label("order_name"),
        )
        .join(models.Order, models.Fulfillment.order_id == models.Order.id)
        .filter(models.Order.store_id == store_id)
        .order_by(models.Fulfillment.created_at.desc())
        .all()
    )