# product_service.py

import time
import random
from typing import Optional, Dict, Any, List

import requests


class ProductService:
    """
    Service for Shopify product/variant and inventory mutations (GraphQL Admin API).
    """
    def __init__(self, store_url: str, token: str, api_version: str = "2025-04"):
        if not store_url or not token:
            raise ValueError("store_url and token are required")
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        }

    # -------------------- internal helpers --------------------
    def _execute_mutation(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL mutation with retry/backoff for throttling and transient errors.
        """
        payload = {"query": query, "variables": variables or {}}
        max_retries = 7
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                resp = requests.post(self.api_endpoint, headers=self.headers, json=payload, timeout=25)
                resp.raise_for_status()
                data = resp.json()

                # GraphQL-level errors
                if "errors" in data and data["errors"]:
                    errs = data["errors"]
                    print("Shopify API Error Response:", errs)
                    # Retry if throttled
                    is_throttled = any((e.get("extensions", {}) or {}).get("code") == "THROTTLED" for e in errs)
                    if is_throttled and attempt < max_retries - 1:
                        # try to honor throttle status if present (fallback to exp backoff)
                        wait = max(base_delay * (2 ** attempt) + random.uniform(0, 0.5), 1.0)
                        print(f"[throttle] retry in {wait:.2f}s")
                        time.sleep(wait)
                        continue
                    raise ValueError(f"GraphQL API Mutation Error: {errs}")

                return data.get("data") or {}

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    print(f"[network] {e}; retry in {wait:.2f}s")
                    time.sleep(wait)
                else:
                    raise

        raise RuntimeError("Max retries reached for GraphQL mutation")

    # -------------------- product / variant edits (kept minimal) --------------------
    def product_update(self, product_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a product. product_input must include 'id' (a Product GID).
        """
        MUT = """
        mutation productUpdate($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title vendor status tags }
            userErrors { field message }
          }
        }
        """
        data = self._execute_mutation(MUT, {"input": product_input})
        out = (data.get("productUpdate") or {})
        if out.get("userErrors"):
            raise ValueError(f"Shopify User Error: {out['userErrors']}")
        return out.get("product") or {}

    def variants_bulk_update(self, product_id: str, variants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Update multiple variants for a product (uses productVariantsBulkUpdate).
        Each variant dict should include at least 'id' (variant GID) and any fields to change.
        """
        MUT = """
        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id title price compareAtPrice position }
            userErrors { field message }
          }
        }
        """
        data = self._execute_mutation(MUT, {"productId": product_id, "variants": variants})
        out = (data.get("productVariantsBulkUpdate") or {})
        if out.get("userErrors"):
            raise ValueError(f"Shopify User Error: {out['userErrors']}")
        return out.get("productVariants") or []

    # -------------------- inventory mutations --------------------
    def adjust_inventory_quantity(self, inventory_item_id: str, location_id: str, available_delta: int) -> Dict[str, Any]:
        """
        Adjust AVAILABLE by a delta at a location.
        """
        MUT = """
        mutation inventoryAdjustQuantity($input: InventoryAdjustQuantityInput!) {
          inventoryAdjustQuantity(input: $input) {
            inventoryLevel { id }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "inventoryLevelId": f"gid://shopify/InventoryLevel/{inventory_item_id.split('/')[-1]}-{location_id.split('/')[-1]}",
                "availableDelta": int(available_delta)
            }
        }
        print(f"[adjust-available] {inventory_item_id} @ {location_id} Δ {available_delta}")
        data = self._execute_mutation(MUT, variables)
        out = (data.get("inventoryAdjustQuantity") or {})
        if out.get("userErrors"):
            msg = ", ".join(f"{(e.get('field') or '')}: {e.get('message')}" for e in out["userErrors"])
            raise ValueError(f"Shopify Inventory Error: {msg}")
        return out.get("inventoryLevel") or {}
    
    # FIX: Added a new method for more direct inventory adjustments
    def inventory_adjust_quantities(self, inventory_item_id: str, location_id: str, available_delta: int) -> Dict[str, Any]:
        """
        Adjust AVAILABLE by a delta at a location using inventoryAdjustQuantities.
        """
        MUT = """
        mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
          inventoryAdjustQuantities(input: $input) {
            inventoryAdjustmentGroup { id reason }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "changes": [{
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,
                    "delta": int(available_delta),
                }],
            }
        }
        print(f"[adjust-available] {inventory_item_id} @ {location_id} Δ {available_delta}")
        data = self._execute_mutation(MUT, variables)
        out = (data.get("inventoryAdjustQuantities") or {})
        if out.get("userErrors"):
            msg = ", ".join(f"{(e.get('field') or '')}: {e.get('message')}" for e in out["userErrors"])
            raise ValueError(f"Shopify Inventory Error: {msg}")
        return out.get("inventoryAdjustmentGroup") or {}

    def set_inventory_available(self, inventory_item_id: str, location_id: str, target_available: int,
                                reason: str = "correction", ignore_compare: bool = True) -> Dict[str, Any]:
        """
        Set AVAILABLE to an absolute value at a location.
        """
        MUT = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            inventoryAdjustmentGroup { id reason }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "name": "available",
                "reason": reason,
                "ignoreCompareQuantity": bool(ignore_compare),
                "quantities": [{
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,
                    "quantity": int(target_available),
                }],
            }
        }
        print(f"[set-available] {inventory_item_id} @ {location_id} -> {target_available}")
        data = self._execute_mutation(MUT, variables)
        out = (data.get("inventorySetQuantities") or {})
        if out.get("userErrors"):
            msg = ", ".join(f"{(e.get('field') or '')}: {e.get('message')}" for e in out["userErrors"])
            raise ValueError(f"Shopify Inventory Error: {msg}")
        return out.get("inventoryAdjustmentGroup") or {}

    def adjust_on_hand_quantity(self, inventory_item_id: str, location_id: str, on_hand_delta: int) -> Dict[str, Any]:
        """
        Adjust ON_HAND by a delta.
        """
        return self.adjust_inventory_quantity(inventory_item_id, location_id, int(on_hand_delta))