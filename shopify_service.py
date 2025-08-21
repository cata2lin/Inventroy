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
    if not gid: return None
    try: return int(str(gid).split('/')[-1])
    except (IndexError, ValueError): return None

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

GET_INVENTORY_DETAILS_QUERY = """
query GetInventoryDetails($cursor: String) {
  inventoryItems(first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges { node { legacyResourceId tracked unitCost { amount } } }
  }
}
"""

# NOTE: Kept for reference, but NOT used anymore (inventoryItems(ids: ...) is invalid).
GET_INVENTORY_LEVELS_QUERY = """
query getInventoryLevels($itemIds: [ID!]!) {
  inventoryItems(ids: $itemIds) {
    edges {
      node {
        legacyResourceId
        inventoryLevels {
          edges {
            node {
              location {
                legacyResourceId
              }
              quantities(names: ["available", "on_hand"]) {
                name
                quantity
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
    def __init__(self, store_url: str, token: str, api_version: str = "2025-04"):
        if not all([store_url, token]):
            raise ValueError("Store URL and Access Token are required.")
        self.api_endpoint = f"https://{store_url}/admin/api/{api_version}/graphql.json"
        self.rest_api_endpoint = f"https://{store_url}/admin/api/{api_version}"
        self.headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}
        self.rest_headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
        
    def get_inventory_levels_for_items(self, item_legacy_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Returns:
          List of dicts: {"id": <inventory_item_legacy_id>, "location_id": <legacy_location_id>,
                          "available": <int>, "on_hand": <int>}
        Implementation uses GraphQL `nodes(ids: [...])` (correct way to fetch specific InventoryItems).
        """
        if not item_legacy_ids:
            return []
        
        # Build GIDs for nodes(ids: ...)
        item_gids = [f"gid://shopify/InventoryItem/{item_id}" for item_id in item_legacy_ids]

        QUERY = """
        query GetInventory($ids: [ID!]!) {
          nodes(ids: $ids) {
            __typename
            ... on InventoryItem {
              id
              legacyResourceId
              inventoryLevels(first: 100) {
                edges {
                  node {
                    location { id legacyResourceId }
                    quantities(names: ["available", "on_hand"]) {
                      name
                      quantity
                    }
                  }
                }
              }
            }
          }
        }
        """

        results: List[Dict[str, Any]] = []
        CHUNK_SIZE = 50  # conservative; nodes() can accept more but this stays safe

        for i in range(0, len(item_gids), CHUNK_SIZE):
            chunk = item_gids[i:i + CHUNK_SIZE]
            variables = {"ids": chunk}
            try:
                data = self._execute_query(QUERY, variables)
            except Exception as e:
                print(f"An error occurred during inventory level fetch: {e}")
                continue

            if not data or "nodes" not in data:
                print("Received no data or malformed nodes data. Skipping chunk.")
                continue

            for node in data["nodes"]:
                # nodes can contain nulls or other types
                if not node or node.get("__typename") != "InventoryItem":
                    continue

                item_legacy_id = node.get("legacyResourceId")
                inv_levels = node.get("inventoryLevels", {}) or {}
                edges = inv_levels.get("edges", []) or []

                for edge in edges:
                    lvl = edge.get("node") or {}
                    loc = (lvl.get("location") or {})
                    loc_legacy_id = loc.get("legacyResourceId")
                    qtys = (lvl.get("quantities") or [])

                    available = 0
                    on_hand = 0
                    for q in qtys:
                        if q.get("name") == "available":
                            try:
                                available = int(q.get("quantity") or 0)
                            except Exception:
                                available = 0
                        elif q.get("name") == "on_hand":
                            try:
                                on_hand = int(q.get("quantity") or 0)
                            except Exception:
                                on_hand = 0

                    if item_legacy_id is not None and loc_legacy_id is not None:
                        results.append({
                            "id": int(item_legacy_id),
                            "location_id": int(loc_legacy_id),
                            "available": int(available),
                            "on_hand": int(on_hand),
                        })

        return results

    def get_order_id_from_fulfillment_order_gid(self, fulfillment_order_gid: str) -> Optional[int]:
        query = """
        query($id: ID!) {
          fulfillmentOrder(id: $id) {
            order {
              legacyResourceId
            }
          }
        }
        """
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

    def get_total_counts(self, created_at_min: Optional[str] = None, created_at_max: Optional[str] = None) -> Dict[str, int]:
        try:
            params = {"status": "any"}
            if created_at_min: params["created_at_min"] = created_at_min
            if created_at_max: params["created_at_max"] = created_at_max
            order_count_url = f"{self.rest_api_endpoint}/orders/count.json"
            product_count_url = f"{self.rest_api_endpoint}/products/count.json"
            order_response = requests.get(order_count_url, headers=self.rest_headers, params=params, timeout=10)
            order_response.raise_for_status()
            product_response = requests.get(product_count_url, headers=self.rest_headers, timeout=10)
            product_response.raise_for_status()
            return { "orders": order_response.json().get("count", 0), "products": product_response.json().get("count", 0) }
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching counts via REST API: {e}")
            return {"orders": 0, "products": 0}

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
        if not data or "edges" not in data: return []
        return [edge["node"] for edge in data["edges"]]

    def get_all_orders_and_related_data(self, created_at_min: Optional[str] = None, created_at_max: Optional[str] = None) -> Generator[List[schemas.ShopifyOrder], None, None]:
        has_next_page = True
        cursor = None
        query_parts = []
        if created_at_min: query_parts.append(f"created_at:>{created_at_min}")
        if created_at_max: query_parts.append(f"created_at:<{created_at_max}")
        query_string = " AND ".join(query_parts) if query_parts else None

        print(f"Starting order data fetch from {self.api_endpoint} with query: {query_string}...")
        while has_next_page:
            try:
                variables = {"cursor": cursor, "query": query_string}
                data = self._execute_query(GET_ALL_ORDERS_QUERY, variables)
                
                if not data or "orders" not in data:
                    print("Received no data or malformed orders data from API. Stopping.")
                    has_next_page = False
                    continue

                order_connection = data["orders"]
                page_info = order_connection.get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")
                orders_on_page = []
                
                for order_node in self._flatten_edges(order_connection):
                    order_node["lineItems"] = self._flatten_edges(order_node.get("lineItems"))
                    for item in order_node["lineItems"]:
                        if item.get("variant") and item["variant"].get("inventoryItem"):
                            item["variant"]["inventoryItem"]["inventoryLevels"] = self._flatten_edges(item["variant"]["inventoryItem"].get("inventoryLevels"))
                        if item.get("originalUnitPriceSet"):
                            item["originalUnitPriceSet"] = item["originalUnitPriceSet"]["shopMoney"]
                        if item.get("totalDiscountSet"):
                             item["totalDiscountSet"] = item["totalDiscountSet"]["shopMoney"]
                    for fulfillment in order_node.get("fulfillments", []):
                        if fulfillment.get("trackingInfo"):
                            tracking_info = fulfillment["trackingInfo"][0] if fulfillment["trackingInfo"] else {}
                            fulfillment["tracking_company"] = tracking_info.get("company")
                            fulfillment["tracking_number"] = tracking_info.get("number")
                            fulfillment["tracking_url"] = str(tracking_info.get("url")) if tracking_info.get("url") else None
                        fulfillment["events"] = self._flatten_edges(fulfillment.get("events"))
                    for key in ["totalPriceSet", "subtotalPriceSet", "totalTaxSet", "totalDiscountsSet", "totalShippingPriceSet"]:
                        if order_node.get(key):
                            order_node[key] = order_node[key]["shopMoney"]
                    orders_on_page.append(schemas.ShopifyOrder.parse_obj(order_node))
                
                yield orders_on_page

            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during order page fetch: {e}. Attempting to continue.")
                has_next_page = False

        print("Finished fetching all order pages from Shopify.")
    
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
                products_on_page = []
                for product_node in self._flatten_edges(product_connection):
                    variants = self._flatten_edges(product_node.pop("variants", {}))
                    for variant_node in variants:
                        if variant_node.get("inventoryItem"):
                            variant_node["inventoryItem"]["inventoryLevels"] = self._flatten_edges(variant_node["inventoryItem"].get("inventoryLevels"))
                    products_on_page.append({
                        "product": schemas.Product.parse_obj(product_node),
                        "variants": [schemas.ProductVariant.parse_obj(v) for v in variants]
                    })
                yield products_on_page
            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during product fetch: {e}. Stopping.")
                return
        print("Finished fetching all product pages from Shopify.")

    def get_all_inventory_details(self) -> Generator[List[Dict[str, Any]], None, None]:
        has_next_page = True
        cursor = None
        print(f"Starting inventory details fetch from {self.api_endpoint}...")
        while has_next_page:
            try:
                data = self._execute_query(GET_INVENTORY_DETAILS_QUERY, {"cursor": cursor})
                if not data or "inventoryItems" not in data:
                    print("Received no data or malformed inventoryItems data. Stopping detail sync.")
                    return
                inventory_connection = data["inventoryItems"]
                page_info = inventory_connection.get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")
                items_on_page = self._flatten_edges(inventory_connection)
                yield items_on_page
            except (ValueError, requests.exceptions.RequestException) as e:
                print(f"An error occurred during inventory details fetch: {e}. Stopping.")
                return
        print("Finished fetching all inventory details.")
