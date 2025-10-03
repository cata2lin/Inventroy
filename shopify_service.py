# shopify_service.py

import time
import requests
import random
from typing import List, Optional, Dict, Any, Generator

# --- (GraphQL Queries remain the same) ---
GET_ALL_PRODUCTS_QUERY = """
query GetAllProducts($cursor: String) {
  products(first: 20, after: $cursor, sortKey: UPDATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        legacyResourceId
        title
        bodyHtml
        vendor
        productType
        status
        createdAt
        handle
        updatedAt
        publishedAt
        status
        tags
        featuredImage { url }
        category { name }
        variants(first: 50) {
          edges {
            node {
              id
              legacyResourceId
              title
              price
              sku
              position
              inventoryPolicy
              compareAtPrice
              barcode
              inventoryQuantity
              createdAt
              updatedAt
              inventoryItem {
                id
                legacyResourceId
                sku
                unitCost { amount }
                inventoryLevels(first: 10) {
                  edges {
                    node {
                      quantities(names: ["available", "on_hand"]) { name quantity }
                      updatedAt
                      location { id legacyResourceId name }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

class ShopifyService:
    def __init__(self, store_url: str, token: str, api_version: str = "2025-10"):
        if not all([store_url, token]):
            raise ValueError("Store URL and Access Token are required.")
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.rest_api_endpoint = f"https://{store_url}/admin/api/{api_version}"
        self.headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}
        self.rest_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    # --- NEW: Webhook Management Methods ---
    def get_webhooks(self) -> List[Dict[str, Any]]:
        url = f"{self.rest_api_endpoint}/webhooks.json"
        response = requests.get(url, headers=self.rest_headers)
        response.raise_for_status()
        return response.json().get("webhooks", [])

    def create_webhook(self, topic: str, address: str) -> Dict[str, Any]:
        url = f"{self.rest_api_endpoint}/webhooks.json"
        payload = {"webhook": {"topic": topic, "address": address, "format": "json"}}
        response = requests.post(url, headers=self.rest_headers, json=payload)
        response.raise_for_status()
        return response.json().get("webhook")

    def delete_webhook(self, webhook_id: int):
        url = f"{self.rest_api_endpoint}/webhooks/{webhook_id}.json"
        response = requests.delete(url, headers=self.rest_headers)
        response.raise_for_status()
        return response.status_code

    # --- (Existing methods remain the same) ---
    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        max_retries = 7
        base_delay = 1
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_endpoint, headers=self.headers, json=payload, timeout=20)
                response.raise_for_status()
                json_response = response.json()
                if "errors" in json_response:
                    is_throttled = any(err.get("extensions", {}).get("code") == "THROTTLED" for err in json_response.get("errors", []))
                    if is_throttled and attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        print(f"API throttled. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"GraphQL API Error: {json_response['errors']}")
                return json_response.get("data")
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Network error: {e}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e
        raise Exception("Max retries reached. Could not complete the API request.")

    def _flatten_edges(self, data: Optional[Dict]) -> List:
        if not data or "edges" not in data:
            return []
        return [edge["node"] for edge in data["edges"]]

    def get_all_products_and_variants(self) -> Generator[List[Dict[str, Any]], None, None]:
        has_next_page = True
        cursor = None
        while has_next_page:
            try:
                data = self._execute_query(GET_ALL_PRODUCTS_QUERY, {"cursor": cursor})
                if not data or "products" not in data:
                    return
                product_connection = data["products"]
                page_info = product_connection.get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")
                products_on_page: List[Dict[str, Any]] = []
                for product_node in self._flatten_edges(product_connection):
                    variants = self._flatten_edges(product_node.pop("variants", {}))
                    for variant_node in variants:
                        inv = variant_node.get("inventoryItem")
                        if inv and isinstance(inv, dict):
                            inv["inventoryLevels"] = self._flatten_edges(inv.get("inventoryLevels"))
                    products_on_page.append({"product": product_node, "variants": variants})
                yield products_on_page
            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during product fetch: {e}. Stopping.")
                return

def gid_to_id(gid: Optional[str]) -> Optional[int]:
    if not gid: return None
    try: return int(str(gid).split('/')[-1])
    except (IndexError, ValueError): return None