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

# --- GraphQL Fragments ---
MONEY_FRAGMENT = "fragment MoneyFragment on MoneyV2 { amount currencyCode }"
LOCATION_FRAGMENT = "fragment LocationFragment on Location { id legacyResourceId name }"
INVENTORY_LEVEL_FRAGMENT = """
fragment InventoryLevelFragment on InventoryLevel {
  quantities(names: ["available", "on_hand"]) { name quantity }
  updatedAt
  location { ...LocationFragment }
}
"""
INVENTORY_ITEM_FRAGMENT = """
fragment InventoryItemFragment on InventoryItem {
  id legacyResourceId sku
  unitCost { amount }
  inventoryLevels(first: 10) { edges { node { ...InventoryLevelFragment } } }
}
"""
PRODUCT_FRAGMENT = """
fragment ProductFragment on Product {
  id legacyResourceId title bodyHtml vendor productType status createdAt handle updatedAt publishedAt status tags
  featuredImage { url }
  category { name }
}
"""
VARIANT_FRAGMENT = """
fragment VariantFragment on ProductVariant {
  id legacyResourceId title price sku position inventoryPolicy compareAtPrice
  barcode inventoryQuantity createdAt updatedAt
  inventoryItem { ...InventoryItemFragment }
  product { ...ProductFragment }
}
"""
LINE_ITEM_FRAGMENT = """
fragment LineItemFragment on LineItem {
  id title quantity sku vendor taxable
  originalUnitPriceSet { shopMoney { ...MoneyFragment } }
  totalDiscountSet { shopMoney { ...MoneyFragment } }
  variant { ...VariantFragment }
}
"""
FULFILLMENT_EVENT_FRAGMENT = "fragment FulfillmentEventFragment on FulfillmentEvent { id message status happenedAt }"
FULFILLMENT_FRAGMENT = """
fragment FulfillmentFragment on Fulfillment {
  id legacyResourceId status createdAt updatedAt
  trackingInfo { company number url }
  events(first: 20) { edges { node { ...FulfillmentEventFragment } } }
}
"""

# --- GraphQL Queries ---
GET_ALL_ORDERS_QUERY = f"""
{MONEY_FRAGMENT}
{LOCATION_FRAGMENT}
{INVENTORY_LEVEL_FRAGMENT}
{INVENTORY_ITEM_FRAGMENT}
{PRODUCT_FRAGMENT}
{VARIANT_FRAGMENT}
{LINE_ITEM_FRAGMENT}
{FULFILLMENT_EVENT_FRAGMENT}
{FULFILLMENT_FRAGMENT}
query GetAllData($cursor: String, $query: String) {{
  orders(first: 5, after: $cursor, sortKey: CREATED_AT, reverse: true, query: $query) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{
      node {{
        id legacyResourceId name createdAt updatedAt cancelledAt cancelReason closedAt processedAt
        displayFinancialStatus displayFulfillmentStatus currencyCode note tags
        paymentGatewayNames
        totalPriceSet {{ shopMoney {{ ...MoneyFragment }} }}
        subtotalPriceSet {{ shopMoney {{ ...MoneyFragment }} }}
        totalTaxSet {{ shopMoney {{ ...MoneyFragment }} }}
        totalDiscountsSet {{ shopMoney {{ ...MoneyFragment }} }}
        totalShippingPriceSet {{ shopMoney {{ ...MoneyFragment }} }}
        lineItems(first: 50) {{ edges {{ node {{ ...LineItemFragment }} }} }}
        fulfillments(first: 10) {{ ...FulfillmentFragment }}
      }}
    }}
  }}
}}
"""

GET_ALL_PRODUCTS_QUERY = f"""
{LOCATION_FRAGMENT}
{INVENTORY_LEVEL_FRAGMENT}
{INVENTORY_ITEM_FRAGMENT}
{PRODUCT_FRAGMENT}
query GetAllProducts($cursor: String) {{
  products(first: 20, after: $cursor, sortKey: UPDATED_AT) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{
      node {{
        ...ProductFragment
        variants(first: 50) {{
          edges {{
            node {{
              id legacyResourceId title price sku position inventoryPolicy compareAtPrice
              barcode inventoryQuantity createdAt updatedAt
              inventoryItem {{ ...InventoryItemFragment }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

class ShopifyService:
    def __init__(self, store_url: str, token: str, api_version: str = "2025-10"):
        if not all([store_url, token]):
            raise ValueError("Store URL and Access Token are required.")
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.rest_api_endpoint = f"https://{store_url}/admin/api/{api_version}"
        
        # CORRECTED: Use single curly braces for dictionaries
        self.headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}
        self.rest_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

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
        print(f"Starting product data fetch from {self.api_endpoint}...")
        while has_next_page:
            try:
                data = self._execute_query(GET_ALL_PRODUCTS_QUERY, {"cursor": cursor})
                if not data or "products" not in data:
                    print("Received no data or malformed products data from API. Stopping.")
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
                    products_on_page.append({
                        "product": product_node,
                        "variants": variants
                    })
                yield products_on_page
            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during product fetch: {e}. Stopping.")
                return
        print("Finished fetching all product pages from Shopify.")