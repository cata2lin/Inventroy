from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
def root():
    return """<html><body><h2>Welcome to the Shopify Sync Dashboard</h2>
    <a href='/orders/'>Orders</a> | <a href='/products/'>Products</a> | <a href='/inventory/'>Inventory</a></body></html>"""
