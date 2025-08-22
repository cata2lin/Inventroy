# routes/webhooks.py

import base64
import hashlib
import hmac
from typing import Optional, Any, Callable

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal
import models

# Prefer package CRUD
try:
    from crud import store as crud_store
except Exception:  # pragma: no cover
    import store as crud_store  # type: ignore

try:
    from crud import product as crud_product
except Exception:  # pragma: no cover
    import product as crud_product  # type: ignore

# NEW: orders CRUD (for persistence on webhooks)
try:
    from crud import order as crud_order  # type: ignore
except Exception:  # pragma: no cover
    crud_order = None  # type: ignore

# Services
try:
    from services import inventory_sync_service
except Exception:  # pragma: no cover
    import inventory_sync_service  # type: ignore

try:
    from services import commited_projector as committed_projector  # filename has one 't'
except Exception:  # pragma: no cover
    import commited_projector as committed_projector  # type: ignore

try:
    import schemas
except Exception:  # pragma: no cover
    schemas = None  # type: ignore

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)

def _verify_hmac(secret: str, raw_body: bytes, header_hmac: Optional[str]) -> None:
    if not header_hmac:
        raise HTTPException(status_code=400, detail="Missing X-Shopify-Hmac-SHA256 header.")
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    if not hmac.compare_digest(computed, header_hmac):
        raise HTTPException(status_code=401, detail="HMAC verification failed.")

def _call_if_exists(module: Any, names: list[str], *args, **kwargs):
    """Try multiple function names on a module; return True if any ran."""
    if not module:
        return False
    for name in names:
        fn: Optional[Callable] = getattr(module, name, None)
        if callable(fn):
            fn(*args, **kwargs)
            return True
    return False

@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    # Load store & verify
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")
    secret = getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store API secret not configured.")

    topic = request.headers.get("x-shopify-topic")
    event_id = request.headers.get("x-shopify-webhook-id")
    raw_body = await request.body()
    _verify_hmac(secret, raw_body, request.headers.get("x-shopify-hmac-sha256"))

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Always 200 quickly to prevent retries
    response = Response(status_code=200, content="ok")

    # app/uninstalled -> disable store
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # ---------------- Orders: persist + committed-stock projector ----------------
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            # 1) Persist the order row if your CRUD supports it
            if crud_order:
                # Accept multiple possible function names your codebase might use
                persisted = _call_if_exists(
                    crud_order,
                    [
                        "upsert_order_from_webhook",
                        "create_or_update_order_from_webhook",
                        "create_or_update_order",
                        "upsert_order",
                    ],
                    db, store_id, payload
                )
                if not persisted:
                    # Optional log to help diagnose if CRUD symbol isn’t present
                    print("[orders][warn] No CRUD upsert found; skipped order persistence.")

            # 2) Update committed/allocated stock view
            committed_projector.process_order_event(db, store_id, topic, payload)
    except Exception as e:
        db.rollback()
        print(f"[orders][error] store={store_id} topic={topic}: {e}")

    # ---------------- Fulfillments: committed projector ----------------
    try:
        if topic in {"fulfillments/create", "fulfillments/update"}:
            committed_projector.process_fulfillment_event(db, store_id, topic, payload)
    except Exception as e:
        db.rollback()
        print(f"[fulfillment][error] store={store_id} topic={topic}: {e}")

    # ---------------- Products: upsert from webhook ----------------
    try:
        if topic in {"products/create", "products/update"}:
            # best-effort schema parsing
            if schemas and hasattr(schemas, "ShopifyProductWebhook"):
                try:
                    product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
                except Exception:
                    product_data = payload
            else:
                product_data = payload
            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)  # type: ignore[arg-type]
        elif topic == "products/delete":
            # optional: mark as deleted if you maintain soft-deletes
            pass
    except Exception as e:
        db.rollback()
        print(f"[product-upsert][error] store={store_id} topic={topic}: {e}")

    # ---------------- Inventory levels: kick the Golden Sync Loop ----------------
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")
            if inventory_item_id and location_id and event_id:
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db_factory=SessionLocal,           # pass factory; the service manages its own session
                    shop_domain=store.shopify_url,
                    event_id=event_id,
                    inventory_item_id=int(inventory_item_id),
                    location_id=int(location_id),
                )
        except Exception as e:
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    # inventory_items/update (optional enrichment)
    if topic == "inventory_items/update":
        pass

    return response
