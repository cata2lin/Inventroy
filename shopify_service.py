# shopify_service.py
import time
import requests
import random
from typing import List, Optional, Dict, Any, Generator
from datetime import datetime

def gid_to_id(gid: Optional[str]) -> Optional[int]:
    if not gid:
        return None
    try:
        return int(str(gid).split('/')[-1])
    except (IndexError, ValueError):
        return None

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
  id
  legacyResourceId
  sku
  unitCost { amount }
  inventoryLevels(first: 10) {
    edges { node { ...InventoryLevelFragment } }
  }
}
"""
PRODUCT_FRAGMENT = """
fragment ProductFragment on Product {
  id legacyResourceId title bodyHtml vendor productType status createdAt handle
  updatedAt publishedAt status tags
  featuredImage { url }
  category { name }
}
"""
VARIANT_FRAGMENT = """
fragment VariantFragment on ProductVariant {
  id legacyResourceId title price sku position inventoryPolicy compareAtPrice barcode
  inventoryQuantity createdAt updatedAt
  inventoryItem { ...InventoryItemFragment }
  product { ...ProductFragment }
}
"""
LINE_ITEM_FRAGMENT = """
fragment LineItemFragment on LineItem {
  id title quantity sku
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
              id legacyResourceId title price sku position inventoryPolicy compareAtPrice barcode
              inventoryQuantity createdAt updatedAt
              inventoryItem {{ ...InventoryItemFragment }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

MUTATIONS = {
    "setProductCategory": """
      mutation SetProductCategory($product: ProductUpdateInput!) {
        productUpdate(product: $product) {
          product { id category { id fullName } }
          userErrors { field message }
        }
      }
    """,
    "updateProductType": """
      mutation UpdateProductType($product: ProductUpdateInput!) {
        productUpdate(product: $product) {
          product { id productType }
          userErrors { field message }
        }
      }
    """,
    "updateVariantPrices": """
      mutation UpdateVariantPrices($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
          productVariants { id price }
          userErrors { field message }
        }
      }
    """,
    "updateVariantCompareAt": """
      mutation UpdateVariantCompareAt($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
          productVariants { id compareAtPrice }
          userErrors { field message }
        }
      }
    """,
    "updateVariantBarcode": """
      mutation UpdateVariantBarcode($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
          productVariants { id barcode }
          userErrors { field message }
        }
      }
    """,
    "updateVariantCosts": """
      mutation UpdateVariantCosts($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
          productVariants { id inventoryItem { id unitCost { amount currencyCode } } }
          userErrors { field message }
        }
      }
    """,
    "updateInventoryCost": """
      mutation UpdateInventoryCost($id: ID!, $input: InventoryItemInput!) {
        inventoryItemUpdate(id: $id, input: $input) {
          inventoryItem { id unitCost { amount currencyCode } }
          userErrors { field message }
        }
      }
    """,
    "inventorySetQuantities": """
      mutation SetAbsQty($input: InventorySetQuantitiesInput!) {
        inventorySetQuantities(input: $input) {
          inventoryAdjustmentGroup {
            changes { name delta quantityAfterChange }
          }
          userErrors { field message }
        }
      }
    """,
}

FIND_CATEGORIES_QUERY = """
query FindCategories($q: String!) {
  taxonomy {
    categories(search: $q, first: 10) {
      nodes { id fullName }
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
                if "errors" in json_response and json_response.get("errors"):
                    is_throttled = any(err.get("extensions", {}).get("code") == "THROTTLED" for err in json_response["errors"])
                    if is_throttled and attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        time.sleep(wait_time)
                        continue
                    raise ValueError(f"GraphQL API Error: {json_response['errors']}")
                return json_response.get("data", {})
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    raise e
        raise Exception("Max retries reached. Could not complete the API request.")

    def _flatten_edges(self, data: Optional[Dict]) -> List:
        if not data or "edges" not in data:
            return []
        return [edge["node"] for edge in data.get("edges", [])]

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
                yield {"products": [], "pageInfo": {"hasNextPage": False}, "error": str(e)}
                return

    def execute_mutation(self, mutation_name: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        mutation = MUTATIONS.get(mutation_name)
        if not mutation:
            raise ValueError(f"Mutation '{mutation_name}' not found.")
        return self._execute_query(mutation, variables)

    def find_categories(self, query: str) -> Dict[str, Any]:
        variables = {"q": query}
        return self._execute_query(FIND_CATEGORIES_QUERY, variables)
