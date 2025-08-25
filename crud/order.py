# crud/order.py

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from typing import List, Dict, Any, Optional
from datetime import datetime

import models
import schemas
from shopify_service import gid_to_id
from .utils import upsert_batch
from sqlalchemy.dialects.postgresql import insert


# ---------------- helpers (dict/attr-safe) ----------------

def _get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _parse_dt(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        s = str(val)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ---------------- committed stock projector helper ----------------

def update_committed_stock_for_order(db: Session, order: models.Order):
    """
    Recalculates and updates the committed stock counts for all barcode groups
    affected by a given order.
    """
    if order.cancelled_at or order.fulfillment_status in ['fulfilled', 'restocked', 'cancelled']:
        return

    line_items = (
        db.query(models.LineItem)
        .options(
            joinedload(models.LineItem.variant).joinedload(models.ProductVariant.group_membership)
        )
        .filter(
            models.LineItem.order_id == order.id,
            models.LineItem.variant_id.isnot(None),
        )
        .all()
    )

    group_deltas: Dict[str, int] = {}
    for item in line_items:
        if item.variant and item.variant.group_membership:
            group_id = item.variant.group_membership.group_id
            group_deltas[group_id] = group_deltas.get(group_id, 0) + int(item.quantity or 0)

    for group_id, qty in group_deltas.items():
        stmt = insert(models.CommittedStock).values(
            group_id=group_id,
            store_id=order.store_id,
            committed_units=qty,
            open_orders_count=1,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['group_id', 'store_id'],
            set_={
                'committed_units': models.CommittedStock.committed_units + stmt.excluded.committed_units,
                'open_orders_count': models.CommittedStock.open_orders_count + 1,
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

    # total_shipping_price_set.shop_money.amount OR sum(shipping_lines.price)
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
    # Optional: hold reason if present on order payloads
    hold_reason = _get(order_data, "hold_reason")
    if hasattr(models.Order, "hold_reason") and hold_reason is not None:
        order_dict["hold_reason"] = hold_reason

    upsert_batch(db, models.Order, [order_dict], ['id'])

    # Ensure products/variants exist for line items
    products_to_create: List[Dict[str, Any]] = []
    variants_to_create: List[Dict[str, Any]] = []

    line_items_src = _get(order_data, "line_items") or []
    required_variant_ids = {int(_get(it, "variant_id")) for it in line_items_src if _get(it, "variant_id")}
    required_product_ids = {int(_get(it, "product_id")) for it in line_items_src if _get(it, "product_id")}

    existing_variants = set()
    existing_products = set()
    if required_variant_ids:
        existing_variants = {
            v[0] for v in db.query(models.ProductVariant.id).filter(models.ProductVariant.id.in_(required_variant_ids)).all()
        }
    if required_product_ids:
        existing_products = {
            p[0] for p in db.query(models.Product.id).filter(models.Product.id.in_(required_product_ids)).all()
        }

    for it in line_items_src:
        pid = _get(it, "product_id")
        vid = _get(it, "variant_id")
        title = _get(it, "title")
        sku = _get(it, "sku")

        if pid and pid not in existing_products:
            products_to_create.append({
                "id": int(pid),
                "store_id": store_id,
                "title": (str(title).split(' - ')[0] if title else None),
                "shopify_gid": f"gid://shopify/Product/{int(pid)}",
                "status": "active",
            })
            existing_products.add(pid)

        if vid and vid not in existing_variants:
            variants_to_create.append({
                "id": int(vid),
                "product_id": int(pid) if pid else None,
                "store_id": store_id,
                "title": title,
                "sku": sku,
                "shopify_gid": f"gid://shopify/ProductVariant/{int(vid)}",
            })
            existing_variants.add(vid)

    if products_to_create:
        upsert_batch(db, models.Product, products_to_create, ['id'])
    if variants_to_create:
        upsert_batch(db, models.ProductVariant, variants_to_create, ['id'])

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
    order = db.query(models.Order).filter(models.Order.id == oid).one_or_none()
    if order:
        update_committed_stock_for_order(db, order)

    db.commit()


def upsert_order_from_webhook(db: Session, store_id: int, order_obj: Any):
    """Backward-compatible alias."""
    return create_or_update_order_from_webhook(db, store_id, order_obj)


def create_or_update_fulfillment_from_webhook(db: Session, store_id: int, fulfillment_data: Any):
    """
    Accepts dict or Pydantic. Minimal columns used.
    """
    order_id = _get(fulfillment_data, "order_id")
    if not order_id:
        return
    order_id = int(order_id)

    # Ensure placeholder order if not present
    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(db, models.Order, [{
            "id": order_id,
            "store_id": store_id,
            "name": f"#{order_id}",
            "shopify_gid": f"gid://shopify/Order/{order_id}",
        }], ['id'])

    fid = _get(fulfillment_data, "id")
    status = _get(fulfillment_data, "status")
    tracking_company = _get(fulfillment_data, "tracking_company")
    tracking_number = _get(fulfillment_data, "tracking_number")
    tracking_url = _get(fulfillment_data, "tracking_url")

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

    # Update order fulfillment_status when appropriate
    order_to_update = db.query(models.Order).filter(models.Order.id == order_id).first()
    if order_to_update:
        if status and str(status).lower() == 'success':
            order_to_update.fulfillment_status = 'fulfilled'
        elif status:
            order_to_update.fulfillment_status = str(status).lower()
        db.commit()


def create_refund_from_webhook(db: Session, store_id: int, refund_data: Any):
    """
    Accepts dict or Pydantic. Persists refund, ensures referenced order + line items exist.
    """
    order_id = _get(refund_data, "order_id")
    if not order_id:
        return
    order_id = int(order_id)

    # Ensure placeholder order
    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(db, models.Order, [{
            "id": order_id,
            "store_id": store_id,
            "name": f"#{order_id}",
            "shopify_gid": f"gid://shopify/Order/{order_id}",
        }], ['id'])

    # Ensure referenced line items exist
    refund_line_items = _get(refund_data, "refund_line_items") or []
    required_line_item_ids = { _get(item, "line_item_id") for item in refund_line_items if _get(item, "line_item_id") }
    existing_line_item_ids = {
        lid for (lid,) in db.query(models.LineItem.id).filter(models.LineItem.id.in_(required_line_item_ids)).all()
    }
    line_items_to_create: List[Dict[str, Any]] = []
    for item in refund_line_items:
        li_id = _get(item, "line_item_id")
        if not li_id or li_id in existing_line_item_ids:
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

    # Refund header
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

        # Refund lines
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

    # Update order financial status according to total refunds
    order_to_update = db.query(models.Order).filter(models.Order.id == order_id).first()
    if order_to_update and order_to_update.total_price is not None:
        current_refunds = (
            db.query(func.sum(models.Refund.total_refunded))
            .filter(models.Refund.order_id == order_id)
            .scalar()
            or 0
        )
        if float(current_refunds) >= float(order_to_update.total_price):
            order_to_update.financial_status = 'refunded'
        else:
            order_to_update.financial_status = 'partially_refunded'
    db.commit()


# ---------------- order HOLD / RELEASE (from webhooks) ----------------

def apply_order_hold_from_webhook(db: Session, store_id: int, payload: Any, on_hold: bool):
    """
    Marks order as on hold (or releases hold). Accepts order payloads or
    fulfillment_order payloads. Best effort to resolve order_id.
    """
    order_id = _get(payload, "order_id")

    # Some hold webhooks may wrap data under 'fulfillment_order'
    if not order_id:
        fo = _get(payload, "fulfillment_order")
        if isinstance(fo, dict):
            order_id = _get(fo, "order_id") or _get(_get(fo, "order"), "id")

    if not order_id:
        # If we cannot resolve the order id, skip safely
        print("[holds] Could not resolve order_id from webhook payload.")
        return

    order_id = int(order_id)

    # Ensure order exists
    if not db.query(models.Order.id).filter(models.Order.id == order_id).first():
        upsert_batch(db, models.Order, [{
            "id": order_id,
            "store_id": store_id,
            "name": f"#{order_id}",
            "shopify_gid": f"gid://shopify/Order/{order_id}",
        }], ['id'])

    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if not order:
        return

    if on_hold:
        order.fulfillment_status = "on_hold"
        reason = _get(payload, "reason") or _get(payload, "hold_reason")
        if hasattr(order, "hold_reason") and reason:
            order.hold_reason = reason
    else:
        # release back to unfulfilled if not fulfilled yet
        if order.fulfillment_status != "fulfilled":
            order.fulfillment_status = "unfulfilled"
        if hasattr(order, "hold_reason"):
            order.hold_reason = None

    db.commit()


# ---------------- bulk GraphQL import (unchanged except guards) ----------------

def create_or_update_orders(db: Session, orders_data: List[schemas.ShopifyOrder], store_id: int):
    if not orders_data:
        return

    all_products: List[Dict[str, Any]] = []
    all_variants: List[Dict[str, Any]] = []
    all_inventory_levels: List[Dict[str, Any]] = []
    all_locations: List[Dict[str, Any]] = []
    all_orders: List[Dict[str, Any]] = []
    all_line_items: List[Dict[str, Any]] = []
    all_fulfillments: List[Dict[str, Any]] = []
    all_fulfillment_events: List[Dict[str, Any]] = []

    processed_product_ids, processed_variant_ids, processed_location_ids, processed_line_item_ids = set(), set(), set(), set()
    order_ids_to_process: List[int] = []

    for order in orders_data:
        order_ids_to_process.append(order.legacy_resource_id)
        payment_gateway_str = ", ".join(order.paymentGatewayNames) if order.paymentGatewayNames else None

        all_orders.append({
            "id": order.legacy_resource_id,
            "shopify_gid": order.id,
            "store_id": store_id,
            "name": order.name,
            "email": order.email,
            "phone": order.phone,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
            "cancelled_at": order.cancelled_at,
            "cancel_reason": order.cancel_reason,
            "closed_at": order.closed_at,
            "processed_at": order.processed_at,
            "financial_status": (order.financial_status or 'pending').lower(),
            "fulfillment_status": (order.fulfillment_status or 'unfulfilled').lower(),
            "currency": order.currency,
            "payment_gateway_names": payment_gateway_str,
            "note": order.note,
            "tags": ", ".join(order.tags) if order.tags else None,
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
                    
                    # FIX: Safely access the featured image URL
                    image_url = None
                    if product.featured_image and isinstance(product.featured_image, dict):
                        image_url = product.featured_image.get('url')

                    all_products.append({
                        "id": product.legacy_resource_id,
                        "shopify_gid": product.id,
                        "store_id": store_id,
                        "title": product.title,
                        "body_html": product.body_html,
                        "vendor": product.vendor,
                        "product_type": product.product_type,
                        "product_category": category_name,
                        "created_at": product.created_at,
                        "handle": product.handle,
                        "updated_at": product.updated_at,
                        "published_at": product.published_at,
                        "status": product.status,
                        "tags": ", ".join(product.tags) if product.tags else None,
                        "image_url": image_url,
                    })

                if variant.legacy_resource_id not in processed_variant_ids:
                    processed_variant_ids.add(variant.legacy_resource_id)
                    inv_item = variant.inventory_item
                    all_variants.append({
                        "id": variant.legacy_resource_id,
                        "shopify_gid": variant.id,
                        "product_id": product.legacy_resource_id if product else None,
                        "store_id": store_id,
                        "title": variant.title,
                        "price": variant.price,
                        "sku": variant.sku,
                        "position": variant.position,
                        "inventory_policy": variant.inventory_policy,
                        "compare_at_price": variant.compare_at_price,
                        "barcode": variant.barcode,
                        "inventory_item_id": inv_item.legacy_resource_id if inv_item else None,
                        "inventory_quantity": variant.inventory_quantity,
                        "created_at": variant.created_at,
                        "updated_at": variant.updated_at,
                        "cost": (inv_item.unit_cost.amount if (inv_item and inv_item.unit_cost) else None),
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
                                "inventory_item_id": inv_item.legacy_resource_id,
                                "location_id": loc.legacy_resource_id,
                                "available": available_qty,
                                "on_hand": on_hand_qty,
                                "updated_at": level.updated_at,
                            })

            all_line_items.append({
                "id": line_item_id,
                "shopify_gid": item.id,
                "order_id": order.legacy_resource_id,
                "variant_id": item.variant.legacy_resource_id if item.variant else None,
                "product_id": item.variant.product.legacy_resource_id if (item.variant and item.variant.product) else None,
                "title": item.title,
                "quantity": item.quantity,
                "sku": item.sku,
                "vendor": item.vendor,
                "price": item.price.amount if item.price else None,
                "total_discount": item.total_discount.amount if item.total_discount else None,
                "taxable": item.taxable,
            })

        for fulfillment in order.fulfillments:
            all_fulfillments.append({
                "id": fulfillment.legacy_resource_id,
                "shopify_gid": fulfillment.id,
                "order_id": order.legacy_resource_id,
                "status": fulfillment.status,
                "created_at": fulfillment.created_at,
                "updated_at": fulfillment.updated_at,
                "tracking_company": fulfillment.tracking_company,
                "tracking_number": fulfillment.tracking_number,
                "tracking_url": str(fulfillment.tracking_url) if fulfillment.tracking_url else None,
            })
            for event in fulfillment.events:
                event_id = gid_to_id(event.id)
                if event_id:
                    all_fulfillment_events.append({
                        "id": event_id,
                        "shopify_gid": event.id,
                        "fulfillment_id": fulfillment.legacy_resource_id,
                        "status": event.status,
                        "happened_at": event.happened_at,
                        "description": event.description,
                    })

    upsert_batch(db, models.Location, all_locations, ['id'])
    upsert_batch(db, models.Product, all_products, ['id'])
    upsert_batch(db, models.ProductVariant, all_variants, ['id'])
    upsert_batch(db, models.InventoryLevel, all_inventory_levels, ['inventory_item_id', 'location_id'])
    upsert_batch(db, models.Order, all_orders, ['id'])
    upsert_batch(db, models.LineItem, all_line_items, ['id'])
    upsert_batch(db, models.Fulfillment, all_fulfillments, ['id'])
    upsert_batch(db, models.FulfillmentEvent, all_fulfillment_events, ['id'])

    db.flush()
    print(f"Updating committed stock for {len(order_ids_to_process)} orders...")
    orders_to_update = db.query(models.Order).filter(models.Order.id.in_(order_ids_to_process)).all()
    for order_obj in orders_to_update:
        update_committed_stock_for_order(db, order_obj)
    db.commit()
    print("Database synchronization and committed stock update complete.")


# ---------------- read helpers ----------------

def get_orders_by_store(db: Session, store_id: int):
    return (
        db.query(models.Order)
        .filter(models.Order.store_id == store_id)
        .order_by(models.Order.created_at.desc())
        .all()
    )


def get_fulfillments_by_store(db: Session, store_id: int):
    return (
        db.query(
            models.Fulfillment.id,
            models.Fulfillment.created_at,
            models.Fulfillment.tracking_company,
            models.Fulfillment.tracking_number,
            models.Fulfillment.status,
            models.Order.name.label("order_name"),
        )
        .join(models.Order, models.Fulfillment.order_id == models.Order.id)
        .filter(models.Order.store_id == store_id)
        .order_by(models.Fulfillment.created_at.desc())
        .all()
    )