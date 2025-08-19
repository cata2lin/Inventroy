# main.py

import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv

# This setup assumes your routes and services are in the correct directories
ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base
# Import all routers, including the new ones
from routes import (
    orders, 
    dashboard, 
    products, 
    mutations, 
    dashboard_v2, 
    inventory_v2,
    bulk_update,
    sync_control
)
# The sync_status router may be in a different location in your new project structure
# For this example, I'll assume it's also under the 'routes' directory
# from routes import sync_status 

Base.metadata.create_all(bind=engine)
load_dotenv()

app = FastAPI(
    title="Inventory Intelligence Platform",
    description="A central hub for managing inventory and orders from multiple Shopify stores.",
    version="1.0.0"
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

# --- Include all API Routers ---
# Ensure all these routers are included and their prefixes are correct
app.include_router(orders.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api/dashboard")
app.include_router(dashboard_v2.router)
app.include_router(mutations.router)
app.include_router(inventory_v2.router)
app.include_router(bulk_update.router)
app.include_router(sync_control.router)
# If you have a sync_status router, include it as well
# app.include_router(sync_status.router)


# --- HTML Page Routes ---
# These are the routes that serve the actual web pages.
# The 404 errors mean these are likely the missing part in your file.

@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root():
    return RedirectResponse(url="/dashboard-v2")

@app.get("/dashboard-v2", response_class=HTMLResponse, include_in_schema=False)
async def get_dashboard_v2_page(request: Request):
    return templates.TemplateResponse("dashboard_v2.html", {"request": request})

@app.get("/products", response_class=HTMLResponse, include_in_schema=False)
async def get_products_page(request: Request):
    return templates.TemplateResponse("products.html", {"request": request})

@app.get("/inventory", response_class=HTMLResponse, include_in_schema=False)
async def get_inventory_page(request: Request):
    return templates.TemplateResponse("inventory.html", {"request": request})

@app.get("/mutations", response_class=HTMLResponse, include_in_schema=False)
async def get_mutations_page(request: Request):
    return templates.TemplateResponse("mutations.html", {"request": request})

@app.get("/bulk-update", response_class=HTMLResponse, include_in_schema=False)
async def get_bulk_update_page(request: Request):
    return templates.TemplateResponse("bulk_update.html", {"request": request})

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    return templates.TemplateResponse("sync_control.html", {"request": request})