# services/webhook_maintenance.py
"""
Automated webhook maintenance service.
Runs periodically to verify and recreate missing webhooks for all stores.

This prevents the common Shopify issue where webhooks silently disappear
after a few weeks. The service:
1. Checks each enabled store's webhooks against the required topic list.
2. Recreates any missing or misconfigured webhooks.
3. Logs all actions to the audit trail for full traceability.
"""
import time
from typing import List, Dict, Optional
from datetime import datetime, timezone

from database import SessionLocal
from crud import store as crud_store, webhooks as crud_webhook
from shopify_service import ShopifyService
from services import audit_logger
import models


# --- Required webhook topics (must match routes/config.py) ---
ESSENTIAL_WEBHOOK_TOPICS = [
    "inventory_levels/update",
    "products/create",
    "products/update",
    "products/delete",
    "inventory_items/update",
    "inventory_items/delete",
]

# The base URL for webhook callbacks. Set via env or auto-detected.
# This should be the public HTTPS URL where the app is accessible.
import os
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "")


def verify_and_recreate_webhooks(base_url: Optional[str] = None):
    """
    Check all enabled stores and recreate any missing webhooks.
    This is designed to be called by the scheduler on a periodic basis.
    
    Args:
        base_url: The public HTTPS base URL for webhook callbacks.
                  Falls back to WEBHOOK_BASE_URL env var.
    """
    effective_base_url = base_url or WEBHOOK_BASE_URL
    if not effective_base_url:
        audit_logger.log(
            category="SYSTEM",
            action="webhook_maintenance_skipped",
            message="Webhook maintenance skipped: no WEBHOOK_BASE_URL configured",
            severity="WARN",
        )
        print("[WEBHOOK-MAINT] Skipped: no WEBHOOK_BASE_URL configured.")
        return

    db = SessionLocal()
    start_time = time.monotonic()

    total_created = 0
    total_updated = 0
    total_verified = 0
    store_results = []
    errors = []

    try:
        stores = crud_store.get_enabled_stores(db)
        if not stores:
            audit_logger.log(
                category="SYSTEM",
                action="webhook_maintenance_skipped",
                message="No enabled stores found for webhook maintenance",
            )
            return

        audit_logger.log(
            category="CONFIG",
            action="webhook_maintenance_started",
            message=f"Starting webhook verification for {len(stores)} stores",
            details={"store_count": len(stores)},
        )

        for store in stores:
            try:
                result = _verify_store_webhooks(db, store, effective_base_url)
                store_results.append(result)
                total_created += result["created"]
                total_updated += result["updated"]
                total_verified += result["verified"]
            except Exception as e:
                error_msg = f"Failed to verify webhooks for store '{store.name}': {e}"
                errors.append(error_msg)
                audit_logger.log_error(
                    "webhook_maintenance.verify_and_recreate_webhooks",
                    error_msg,
                    details={"store_id": store.id, "store_name": store.name},
                    exc=e,
                )

        duration_ms = int((time.monotonic() - start_time) * 1000)

        summary = (
            f"Webhook maintenance completed: {total_verified} verified, "
            f"{total_created} created, {total_updated} updated, "
            f"{len(errors)} errors across {len(stores)} stores"
        )
        print(f"[WEBHOOK-MAINT] {summary}")

        audit_logger.log_config_change(
            actor="scheduler",
            action="webhook_maintenance_completed",
            message=summary,
            details={
                "total_created": total_created,
                "total_updated": total_updated,
                "total_verified": total_verified,
                "error_count": len(errors),
                "store_results": store_results,
                "errors": errors,
                "duration_ms": duration_ms,
            },
        )

    except Exception as e:
        audit_logger.log_error(
            "webhook_maintenance.verify_and_recreate_webhooks",
            f"Webhook maintenance failed: {e}",
            exc=e,
        )
    finally:
        db.close()


def _verify_store_webhooks(
    db, store: models.Store, base_url: str
) -> Dict:
    """Verify and fix webhooks for a single store."""
    correct_address = f"{base_url.rstrip('/')}/api/webhooks/{store.id}"

    service = ShopifyService(store_url=store.shopify_url, token=store.api_token)
    existing_webhooks = service.get_webhooks()
    existing_map = {
        wh["topic"]: {"id": wh["id"], "address": wh["address"]}
        for wh in existing_webhooks
    }

    created = 0
    updated = 0
    verified = 0

    for topic in ESSENTIAL_WEBHOOK_TOPICS:
        existing = existing_map.get(topic)

        if not existing:
            # Missing webhook — create it
            created_webhook = service.create_webhook(topic=topic, address=correct_address)
            crud_webhook.create_webhook_registration(
                db, store_id=store.id, webhook_data=created_webhook
            )
            created += 1
            audit_logger.log_config_change(
                actor="scheduler",
                action="webhook_recreated",
                message=f"Recreated missing webhook [{topic}] for {store.name}",
                store_id=store.id,
                store_name=store.name,
                details={"topic": topic, "address": correct_address},
            )
        elif existing["address"] != correct_address:
            # Wrong address — delete and recreate
            service.delete_webhook(webhook_id=existing["id"])
            crud_webhook.delete_webhook_registration(
                db, shopify_webhook_id=existing["id"]
            )
            created_webhook = service.create_webhook(topic=topic, address=correct_address)
            crud_webhook.create_webhook_registration(
                db, store_id=store.id, webhook_data=created_webhook
            )
            updated += 1
            audit_logger.log_config_change(
                actor="scheduler",
                action="webhook_address_fixed",
                message=f"Fixed webhook address [{topic}] for {store.name}",
                store_id=store.id,
                store_name=store.name,
                details={
                    "topic": topic,
                    "old_address": existing["address"],
                    "new_address": correct_address,
                },
            )
        else:
            verified += 1

    return {
        "store_id": store.id,
        "store_name": store.name,
        "created": created,
        "updated": updated,
        "verified": verified,
    }
