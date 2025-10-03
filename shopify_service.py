# shopify_service.py

import os
import time
import requests
import random
from typing import List, Optional, Dict, Any, Generator
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime
import schemas

# --- Helper function ---
def gid_to_id(gid: Optional[str]) -> Optional[int]:
    if not gid:
        return None
    try:
        return int(str(gid).split('/')[-1])
    except (IndexError, ValueError):
        return None

# --- GraphQL Query (Now accepts a query filter) ---
GET_ALL_PRODUCTS_QUERY = """
query GetAllProducts($cursor: String, $query: String) {
  products(first: 50, after: $cursor, sortKey: UPDATED_AT, query: $query) {
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
                unitCost { amount }
                inventoryLevels(first: 20) {
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
        self.headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}

    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        max_retries = 7
        base_delay = 1.0
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_endpoint, headers=self.headers, json=payload, timeout=30)
                response.raise_for_status()
                json_response = response.json()
                if "errors" in json_response and json_response["errors"]:
                    is_throttled = any(err.get("extensions", {}).get("code") == "THROTTLED" for err in json_response["errors"])
                    if is_throttled and attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        print(f"API throttled. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"GraphQL API Error: {json_response['errors']}")
                return json_response.get("data", {})
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
        return [edge["node"] for edge in data.get("edges", [])]

    # --- CORRECTED FUNCTION SIGNATURE ---
    def get_all_products_and_variants(self, cursor: Optional[str] = None, updated_at_max: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Fetches all products and variants, supporting pagination and an upper timestamp limit.
        """
        has_next_page = True
        
        # Build the query filter for the snapshot window
        query_filter = f"updated_at:<='{updated_at_max}'" if updated_at_max else None

        while has_next_page:
            try:
                variables = {"cursor": cursor, "query": query_filter}
                data = self._execute_query(GET_ALL_PRODUCTS_QUERY, variables)
                
                if not data or "products" not in data:
                    yield {"products": [], "pageInfo": {"hasNextPage": False}}
                    return

                product_connection = data["products"]
                page_info = product_connection.get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor") # Update cursor for the next iteration

                products = self._flatten_edges(product_connection)
                for prod in products:
                    prod["variants"] = self._flatten_edges(prod.get("variants"))
                    for variant in prod["variants"]:
                        if variant.get("inventoryItem"):
                            variant["inventoryItem"]["inventoryLevels"] = self._flatten_edges(variant["inventoryItem"].get("inventoryLevels"))
                
                yield {"products": products, "pageInfo": page_info}

            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during product page fetch: {e}. Stopping.")
                yield {"products": [], "pageInfo": {"hasNextPage": False}, "error": str(e)}
                return