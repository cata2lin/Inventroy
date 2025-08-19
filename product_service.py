# product_service.py

import os
import requests
import time
import random
from typing import Optional, Dict, Any

from shopify_service import gid_to_id

class ProductService:
    """
    A service class to handle product and variant updates via the Shopify Admin API.
    """
    def __init__(self, store_url: str, token: str, api_version: str = "2025-04"):
        if not all([store_url, token]):
            raise ValueError("Store URL and Access Token are required.")
        
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        }

    def _execute_mutation(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Executes a GraphQL mutation with a reliable retry mechanism for throttling.
        """
        payload = {"query": query, "variables": variables or {}}
        max_retries = 7
        base_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_endpoint, headers=self.headers, json=payload, timeout=20)
                response.raise_for_status()
                
                json_response = response.json()

                if "errors" in json_response:
                    print("Shopify API Error Response:", json_response['errors'])
                    is_throttled = any(
                        err.get("extensions", {}).get("code") == "THROTTLED"
                        for err in json_response.get("errors", [])
                    )
                    if is_throttled and attempt < max_retries - 1:
                        cost = json_response.get("extensions", {}).get("cost", {})
                        throttle_status = cost.get("throttleStatus", {})
                        wait_time = throttle_status.get("currentlyAvailable", base_delay) / throttle_status.get("restoreRate", 50) + 1
                        print(f"API throttled. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise ValueError(f"GraphQL API Mutation Error: {json_response['errors']}")

                return json_response.get("data", {})

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Network error: {e}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e

        raise Exception("Max retries reached. Could not complete the API mutation.")

    def update_product(self, product_gid: str, product_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates a product's details using the productUpdate mutation.
        """
        MUTATION_UPDATE_PRODUCT = """
        mutation productUpdate($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id, title, vendor, status, tags }
            userErrors { field, message }
          }
        }
        """
        variables = { "input": { "id": product_gid, **product_input } }
        print(f"Sending product update for {product_gid} with data: {product_input}")
        response_data = self._execute_mutation(MUTATION_UPDATE_PRODUCT, variables)
        result = response_data.get("productUpdate", {})
        if result.get("userErrors"):
            error_message = ", ".join([f"{e['field']}: {e['message']}" for e in result["userErrors"]])
            raise ValueError(f"Shopify User Error: {error_message}")
        return result.get("product", {})

    def update_variant_details(self, product_id: str, variant_updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates a single product variant's details using the productVariantsBulkUpdate mutation.
        """
        MUTATION_UPDATE_VARIANT = """
        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id, title, sku, barcode, price, compareAtPrice, inventoryItem { id } }
            userErrors { field, message }
          }
        }
        """
        variables = { "productId": product_id, "variants": [variant_updates] }
        print(f"Sending variant update for {variant_updates.get('id')} to product {product_id} with data: {variant_updates}")
        response_data = self._execute_mutation(MUTATION_UPDATE_VARIANT, variables)
        result = response_data.get("productVariantsBulkUpdate", {})
        if result.get("userErrors"):
            error_message = ", ".join([f"{e['field']}: {e['message']}" for e in result["userErrors"]])
            raise ValueError(f"Shopify User Error: {error_message}")
        return result.get("productVariants", [{}])[0]

    def adjust_inventory_quantity(self, inventory_item_id: str, location_id: str, available_delta: int) -> Dict[str, Any]:
        """
        Adjusts the 'available' inventory quantity for an inventory item at a location.
        """
        MUTATION_ADJUST_INVENTORY = """
        mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
            inventoryAdjustQuantities(input: $input) {
                inventoryAdjustmentGroup { id, reason }
                userErrors { field, message }
            }
        }
        """
        variables = {
            "input": {
                "reason": "correction", "name": "available",
                "changes": [{"inventoryItemId": inventory_item_id, "locationId": location_id, "delta": available_delta}]
            }
        }
        print(f"Adjusting AVAILABLE inventory for item {inventory_item_id} at {location_id} by {available_delta}")
        response_data = self._execute_mutation(MUTATION_ADJUST_INVENTORY, variables)
        result = response_data.get("inventoryAdjustQuantities", {})
        if result.get("userErrors"):
            error_message = ", ".join([f"{e['field']}: {e['message']}" for e in result["userErrors"]])
            raise ValueError(f"Shopify Inventory Error: {error_message}")
        return result.get("inventoryAdjustmentGroup", {})

    def adjust_on_hand_quantity(self, inventory_item_id: str, location_id: str, on_hand_delta: int) -> Dict[str, Any]:
        """
        Adjusts the 'on hand' quantity by changing the 'available' quantity, which is the reliable method.
        """
        return self.adjust_inventory_quantity(inventory_item_id, location_id, on_hand_delta)