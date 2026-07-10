# services/trendyol_client.py
"""
Trendyol Integration API client (apigw.trendyol.com/integration) — the marketplace side of the
Trendyol<->Shopify stock sync. READ endpoints (orders, approved products, batch status) + the ONE
write the sync needs: quantity-only price-and-inventory batches.

API facts this client encodes (from Trendyol's integration-developer-tool + the Scripturi app):
  • Auth: Basic base64(apiKey:apiSecret) + User-Agent "{sellerId} - SelfIntegration".
  • price-and-inventory is ASYNC: returns {"batchRequestId"}; poll GET batch-requests/{id};
    batch "COMPLETED" != success — each item carries its own status + failureReasons; results
    expire in ~4h (callers must persist them).
  • An IDENTICAL request (same barcode, same values) is REJECTED within a 15-minute window —
    callers keep a per-barcode last-sent dedup store (trendyol_sync does).
  • Approved-products paging: max size=100 (yes, 100 — not 1000).
  • Stage environment: set TRENDYOL_BASE_URL=https://stageapigw.trendyol.com/integration.
All methods return structured dicts and NEVER raise into callers ({"ok": False, "error": ...}).
"""
import os
import base64
from typing import Dict, Any, List, Optional

import requests


BASE_URL = os.getenv("TRENDYOL_BASE_URL", "https://apigw.trendyol.com/integration").rstrip("/")
SELLER_ID = os.getenv("TRENDYOL_SELLER_ID", "")
API_KEY = os.getenv("TRENDYOL_API_KEY", "")
API_SECRET = os.getenv("TRENDYOL_API_SECRET", "")
TIMEOUT = int(os.getenv("TRENDYOL_HTTP_TIMEOUT", "30"))

MAX_BATCH_ITEMS = 1000          # price-and-inventory hard limit
MAX_STOCK_PER_PRODUCT = 20000   # Trendyol per-product stock cap
PRODUCTS_PAGE_SIZE = 100        # approved-products hard max page size


def configured() -> bool:
    return bool(SELLER_ID and API_KEY and API_SECRET)


STORE_FRONT_CODE = os.getenv("TRENDYOL_STORE_FRONT_CODE", "RO")


def _session(storefront: bool) -> requests.Session:
    """PRODUCT/INVENTORY endpoints REQUIRE the `storeFrontCode` HTTP HEADER for international
    sellers (without it they return 200 with zero elements — verified live). ORDER endpoints must
    OMIT it (so all countries are returned) — mirrors the proven Scripturi client."""
    s = requests.Session()
    token = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    s.headers.update({
        "Authorization": f"Basic {token}",
        "User-Agent": f"{SELLER_ID} - SelfIntegration",
        "Content-Type": "application/json",
    })
    if storefront:
        s.headers["storeFrontCode"] = STORE_FRONT_CODE
    return s


def _get(path: str, params: Optional[Dict[str, Any]] = None, storefront: bool = True) -> Dict[str, Any]:
    try:
        r = _session(storefront).get(f"{BASE_URL}{path}", params=params or {}, timeout=TIMEOUT)
        if r.status_code != 200:
            return {"ok": False, "status": r.status_code, "error": r.text[:400]}
        return {"ok": True, "data": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _post(path: str, payload: Dict[str, Any], storefront: bool = True) -> Dict[str, Any]:
    try:
        r = _session(storefront).post(f"{BASE_URL}{path}", json=payload, timeout=TIMEOUT)
        if r.status_code not in (200, 202):
            return {"ok": False, "status": r.status_code, "error": r.text[:400]}
        return {"ok": True, "data": r.json() if r.text else {}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_inventory(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Quantity-only stock push (price never enters the sync path). items:
    [{"barcode": <TRENDYOL barcode>, "quantity": int}, ...] (<=1000, caller-enforced).
    Returns {"ok": True, "batch_request_id": ...} — ACCEPTED, not applied; poll get_batch."""
    if not items:
        return {"ok": False, "error": "empty items"}
    if len(items) > MAX_BATCH_ITEMS:
        return {"ok": False, "error": f"batch > {MAX_BATCH_ITEMS} items"}
    res = _post(f"/inventory/sellers/{SELLER_ID}/products/price-and-inventory", {"items": items})
    if not res["ok"]:
        return res
    return {"ok": True, "batch_request_id": (res["data"] or {}).get("batchRequestId")}


def get_batch(batch_request_id: str) -> Dict[str, Any]:
    """Batch status. Returns {"ok", "status", "items": [{"barcode","status","failureReasons"}]}.
    status COMPLETED means PROCESSED — success is per-item (SUCCESS/FAILED)."""
    res = _get(f"/product/sellers/{SELLER_ID}/products/batch-requests/{batch_request_id}")
    if not res["ok"]:
        return res
    d = res["data"] or {}
    items = []
    for it in d.get("items") or []:
        req = it.get("requestItem") or {}
        items.append({"barcode": req.get("barcode") or (it.get("barcode")),
                      "status": it.get("status"),
                      "failureReasons": it.get("failureReasons") or []})
    return {"ok": True, "status": d.get("status"), "items": items,
            "item_count": d.get("itemCount"), "failed_count": d.get("failedItemCount")}


def get_orders(start_ms: int, end_ms: int, page: int = 0, size: int = 200,
               status: Optional[str] = None) -> Dict[str, Any]:
    """Orders in [start,end] (epoch ms), paged. Returns {"ok","content":[...],"total_pages"}."""
    params = {"startDate": start_ms, "endDate": end_ms, "page": page, "size": size,
              "orderByField": "PackageLastModifiedDate", "orderByDirection": "DESC"}
    if status:
        params["status"] = status
    res = _get(f"/order/sellers/{SELLER_ID}/orders", params, storefront=False)
    if not res["ok"]:
        return res
    d = res["data"] or {}
    return {"ok": True, "content": d.get("content") or [], "total_pages": d.get("totalPages", 1),
            "page": d.get("page", page)}


def get_approved_products(page: int = 0, size: int = PRODUCTS_PAGE_SIZE) -> Dict[str, Any]:
    """Products page (needs the storefront header; the server-side `approved` filter returns empty
    for this account — verified live — so callers see ALL items with their `approved` flag and
    filter client-side). Items carry barcode + quantity + approved/onSale/archived."""
    res = _get(f"/product/sellers/{SELLER_ID}/products",
               {"page": page, "size": min(size, PRODUCTS_PAGE_SIZE)})
    if not res["ok"]:
        return res
    d = res["data"] or {}
    return {"ok": True, "content": d.get("content") or [], "total_pages": d.get("totalPages", 1)}
