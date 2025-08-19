# main.py

import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base
from routes import (
    orders, 
    dashboard_v2, 
    inventory_v2,
    mutations, 
    products, 
    sync_control
)

Base.metadata.create_all(bind=engine)
load_dotenv()

app = FastAPI(
    title="Inventory Intelligence Platform",
    description="A central hub for managing inventory and orders from multiple Shopify stores.",
    version="1.0.0"
)

# Mount static files (CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")
# Point to the new templates directory
templates = Jinja2Templates(directory="templates")

# Include all API routers
app.include_router(orders.router, prefix="/api")
app.include_router(dashboard_v2.router)
app.include_router(inventory_v2.router)
app.include_router(mutations.router)
app.include_router(products.router, prefix="/api")
app.include_router(sync_control.router)

# --- HTML Page Routes ---
@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root():
    return RedirectResponse(url="/dashboard-v2")

@app.get("/dashboard-v2", response_class=HTMLResponse, include_in_schema=False)
async def get_dashboard_v2_page(request: Request):
    return templates.TemplateResponse("dashboard_v2.html", {"request": request, "title": "Dashboard"})

@app.get("/inventory", response_class=HTMLResponse, include_in_schema=False)
async def get_inventory_page(request: Request):
    return templates.TemplateResponse("inventory.html", {"request": request, "title": "Inventory Report"})

@app.get("/bulk-update", response_class=HTMLResponse, include_in_schema=False)
async def get_bulk_update_page(request: Request):
    return templates.TemplateResponse("bulk_update.html", {"request": request, "title": "Bulk Update"})

@app.get("/mutations", response_class=HTMLResponse, include_in_schema=False)
async def get_mutations_page(request: Request):
    return templates.TemplateResponse("mutations.html", {"request": request, "title": "Edit Product"})

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    return templates.TemplateResponse("sync_control.html", {"request": request, "title": "Sync Control"})