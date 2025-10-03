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

# --- GraphQL Fragments and Queries (unchanged) ---
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
GET_ALL_ORDERS_QUERY = f\"\"\"
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
\"\"\"
GET_ALL_PRODUCTS_QUERY = f\"\"\"
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
\"\"\"
GET_INVENTORY_DETAILS_QUERY = \"\"\"
query GetInventoryDetails($cursor: String) {
  inventoryItems(first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges { node { legacyResourceId tracked unitCost { amount } } }
  }
}
\"\"\"
GET_INVENTORY_LEVELS_QUERY = \"\"\"
query getInventoryLevels($itemIds: [ID!]!) {
  nodes(ids: $itemIds) {
    ... on InventoryItem {
      legacyResourceId
      inventoryLevels(first: 100) {
        edges {
          node {
            location { legacyResourceId }
            quantities(names: ["available", "on_hand"]) { name quantity }
          }
        }
      }
    }
  }
}
\"\"\"

class ShopifyService:
    def __init__(self, store_url: str, token: str, api_version: str = "2025-10"):
        if not all([store_url, token]):
            raise ValueError("Store URL and Access Token are required.")
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.rest_api_endpoint = f"https://{store_url}/admin/api/{api_version}"
        
        # CORRECTED: Use single curly braces for dictionaries
        self.headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}
        self.rest_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    def get_inventory_levels_for_items(self, item_legacy_ids: List[int]) -> List[Dict[str, Any]]:
        if not item_legacy_ids: return []
        item_gids = [f"gid://shopify/InventoryItem/{item_id}" for item_id in item_legacy_ids]
        try:
            variables = {"itemIds": item_gids}
            data = self._execute_query(GET_INVENTORY_LEVELS_QUERY, variables)
            nodes = (data or {}).get("nodes", [])
            if not isinstance(nodes, list): return []
            results: List[Dict[str, Any]] = []
            for node in nodes:
                if not node or "legacyResourceId" not in node: continue
                item_legacy = node.get("legacyResourceId")
                inv_levels = ((node.get("inventoryLevels") or {}).get("edges")) or []
                for lev_edge in inv_levels:
                    lev_node = (lev_edge or {}).get("node") or {}
                    loc_legacy = ((lev_node.get("location") or {}).get("legacyResourceId"))
                    q_list = lev_node.get("quantities") or []
                    available = on_hand = 0
                    for q in q_list:
                        n, qty = q.get("name"), int(q.get("quantity", 0))
                        if n == "available": available = qty
                        elif n == "on_hand": on_hand = qty
                    if not on_hand: on_hand = available
                    results.append({
                        "id": int(item_legacy) if item_legacy else None,
                        "location_id": int(loc_legacy) if loc_legacy else None,
                        "available": int(available), "on_hand": int(on_hand),
                    })
            return results
        except Exception as e:
            print(f"An error occurred during inventory level fetch: {e}")
            return []

    def get_order_id_from_fulfillment_order_gid(self, fulfillment_order_gid: str) -> Optional[int]:
        query = "query($id: ID!) { fulfillmentOrder(id: $id) { order { legacyResourceId } } }"
        variables = {"id": fulfillment_order_gid}
        try:
            data = self._execute_query(query, variables)
            if data and data.get("fulfillmentOrder") and data["fulfillmentOrder"].get("order"):
                return data["fulfillmentOrder"]["order"].get("legacyResourceId")
        except Exception as e:
            print(f"Could not resolve order ID for fulfillment order {fulfillment_order_gid}: {e}")
        return None

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

    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        max_retries = 7
        base_delay = 1
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_endpoint, headers=self.headers, json=payload, timeout=20)
                response.raise_for_status()
                json_response = response.json()
                if "errors" in json_response and json_response.get("errors"):
                    is_throttled = any(err.get("extensions", {}).get("code") == "THROTTLED" for err in json_response["errors"])
                    if is_throttled and attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"GraphQL API Error: {json_response['errors']}")
                return json_response.get("data")
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    raise e
        raise Exception("Max retries reached.")

    def _flatten_edges(self, data: Optional[Dict]) -> List:
        if not data or "edges" not in data:
            return []
        return [edge["node"] for edge in data["edges"]]

    def get_all_products_and_variants(self, cursor: Optional[str] = None, updated_at_max: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        has_next_page = True
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
                cursor = page_info.get("endCursor")

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