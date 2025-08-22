# routes/webhooks.py

import base64
import hashlib
import hmac
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

from database import get_db, SessionLocal

# --- models / crud ---
import models

# Prefer the user's real package layout; fall back to flat files if running standalone
try:
    from crud import store as crud_store
except Exception:  # pragma: no cover
    import store as crud_store  # type: ignore

try:
    from crud import product as crud_product
except Exception:  # pragma: no cover
    import product as crud_product  # type: ignore

try:
    from crud import webhooks as crud_webhook
except Exception:  # pragma: no cover
    import webhooks as crud_webhook  # type: ignore

# --- services ---
# inventory sync service (Golden Loop)
try:
    from services import inventory_sync_service
except Exception:  # pragma: no cover
    import inventory_sync_service  # type: ignore

# committed projector (file name in repo is commited_projector.py)
try:
    from services import commited_projector as committed_projector  # note single 't' in filename
except Exception:  # pragma: no cover
    import commited_projector as committed_projector  # type: ignore

# --- schemas (optional, parsing is best-effort) ---
try:
    import schemas
except Exception:  # pragma: no cover
    schemas = None  # type: ignore

router = APIRouter(
    prefix="/api/webhooks",
    tags=["Webhooks"],
    responses={404: {"description": "Not found"}},
)


def _get_header(request: Request, key: str) -> Optional[str]:
    """Headers are case-insensitive; this helper is a little more forgiving."""
    val = request.headers.get(key)
    if val is not None:
        return val
    # fallback variants used by Shopify docs/samples
    return request.headers.get(key.lower()) or request.headers.get(key.upper())


def _verify_hmac(secret: str, raw_body: bytes, header_hmac: Optional[str]) -> None:
    """
    Verify Shopify webhook HMAC using the store's API secret (shared secret).
    Raises HTTPException if verification fails.
    """
    if not header_hmac:
        raise HTTPException(status_code=400, detail="Missing X-Shopify-Hmac-SHA256 header.")

    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()

    # Timing-safe compare
    if not hmac.compare_digest(computed, header_hmac):
        raise HTTPException(status_code=401, detail="HMAC verification failed.")


@router.post("/{store_id}", include_in_schema=False)
async def receive_webhook(
    store_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Response:
    """
    Central webhook receiver. Verifies HMAC, routes topics, and offloads
    inventory-level work to the Golden Sync Loop in a background task.
    """
    # --- load store ---
    store = crud_store.get_store(db, store_id)
    if not store:
        raise HTTPException(status_code=404, detail="Store not found.")

    # Prefer using api_secret (models.Store in this project always has it)
    secret = getattr(store, "api_secret", None)
    if not secret:
        raise HTTPException(status_code=400, detail="Store API secret not configured.")

    # --- headers & raw body ---
    topic = _get_header(request, "x-shopify-topic")
    event_id = _get_header(request, "x-shopify-webhook-id")
    shop_domain = _get_header(request, "x-shopify-shop-domain") or store.shopify_url
    header_hmac = _get_header(request, "x-shopify-hmac-sha256")

    raw_body = await request.body()

    # --- verify ---
    _verify_hmac(secret, raw_body, header_hmac)

    # --- parse payload (dict); keep this minimal/robust ---
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # --- simple guard: we always return 200 quickly; heavy work happens in background ---
    #     This ensures Shopify doesn’t retry because of our processing latency.
    response = Response(status_code=200, content="ok")

    # --- app/uninstalled: stop all writes immediately ---
    if topic == "app/uninstalled":
        try:
            store.enabled = False
            db.commit()
            print(f"[app/uninstalled] Store '{store.name}' disabled.")
        except Exception as e:
            db.rollback()
            print(f"[app/uninstalled][error] store={store_id}: {e}")
        return response

    # --- order / fulfillment / refunds -> committed projector ---
    try:
        if topic in {"orders/create", "orders/updated", "orders/edited", "orders/cancelled", "orders/delete"}:
            committed_projector.process_order_event(db, store_id, topic, payload)

        elif topic in {"fulfillments/create", "fulfillments/update"}:
            committed_projector.process_fulfillment_event(db, store_id, topic, payload)

        elif topic == "refunds/create":
            # If you count restocks from refund lines, handle them here as well
            pass
    except Exception as e:
        db.rollback()
        # do not fail the webhook; just log
        print(f"[committed_projector][error] store={store_id} topic={topic}: {e}")

    # --- product webhooks -> upsert + group membership updates ---
    try:
        if topic in {"products/create", "products/update"}:
            if schemas and hasattr(schemas, "ShopifyProductWebhook"):
                # Parse strictly if model available; otherwise pass raw dict
                try:
                    product_data = schemas.ShopifyProductWebhook.parse_obj(payload)
                except Exception:
                    product_data = payload  # best-effort fallback
            else:
                product_data = payload  # best-effort fallback

            # NOTE: crud function must not rely on 'admin_graphql_api_id' (use id + shopify_gid instead)
            crud_product.create_or_update_product_from_webhook(db, store.id, product_data)  # type: ignore[arg-type]

        elif topic == "products/delete":
            # Minimal delete handling (schema optional)
            delete_id = None
            if schemas and hasattr(schemas, "DeletePayload"):
                try:
                    delete_id = schemas.DeletePayload.parse_obj(payload).id
                except Exception:
                    delete_id = payload.get("id")
            else:
                delete_id = payload.get("id")
            if delete_id:
                # Optional: crud_webhook.mark_product_as_deleted(db, product_id=delete_id)
                pass
    except Exception as e:
        db.rollback()
        print(f"[product-upsert][error] store={store_id} topic={topic}: {e}")

    # --- inventory_levels/update -> Golden Sync Loop (background) ---
    if topic == "inventory_levels/update":
        try:
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")

            if inventory_item_id and location_id and event_id:
                # IMPORTANT: pass a *factory*, not a live session
                background_tasks.add_task(
                    inventory_sync_service.process_inventory_update_event,
                    db_factory=SessionLocal,  # factory; service owns its session lifecycle
                    shop_domain=shop_domain or store.shopify_url,
                    event_id=str(event_id),
                    inventory_item_id=int(inventory_item_id),
                    location_id=int(location_id),
                )
        except Exception as e:
            # Never fail the webhook; just log
            print(f"[inventory-levels/update][enqueue-error] store={store_id}: {e}")

    # --- inventory_items/update (optional enrichment hook) ---
    if topic == "inventory_items/update":
        # You can enrich cost/tracked flags here if desired
        pass

    return response
