# main.py

import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Form, Depends, status, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv

# ADD THESE IMPORTS
from apscheduler.schedulers.background import BackgroundScheduler
import pytz # Import pytz
from database import SessionLocal, get_db
# Ensure you have this file created: jobs/daily_snapshot.py
from jobs.daily_snapshot import run_daily_inventory_snapshot
# END OF ADDED IMPORTS

# NEW IMPORTS FOR LOGIN MIDDLEWARE
import jwt
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
import models

ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base
from routes import (
    dashboard,
    dashboard_v2,
    orders,
    products,
    inventory,       # legacy endpoints
    inventory_v2,    # new endpoints
    bulk_update,
    webhooks,
    sync_control,
    config,
    forecasting,
    sales_analytics
)

load_dotenv()

app = FastAPI(title="Inventory Suite")
app.mount("/static", StaticFiles(directory=str(ROOT_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(ROOT_DIR / "templates"))

# Create tables if needed
Base.metadata.create_all(bind=engine)

# --- ADD THIS BLOCK TO SCHEDULE THE JOB ---
def scheduled_snapshot_job():
    """Wrapper function to handle the database session for the scheduler."""
    db = SessionLocal()
    try:
        run_daily_inventory_snapshot(db)
    finally:
        db.close()

# FIX: Use pytz to handle the timezone robustly
scheduler = BackgroundScheduler(timezone=pytz.utc)
scheduler.add_job(scheduled_snapshot_job, 'cron', hour=1) # Runs every day at 1:00 AM UTC
scheduler.start()
# --- END OF SCHEDULER BLOCK ---

# NEW: Secret key for JWT. This should be stored in a .env file.
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key")

# NEW: Login endpoint (simplified for demonstration)
@app.post("/login")
async def login(response: Response, username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=username).first()
    if not user or not user.verify_password(password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    payload = {
        "sub": user.username,
        "exp": datetime.utcnow() + timedelta(days=1)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    
    response.set_cookie(key="access_token", value=token, httponly=True, samesite="lax", max_age=86400, secure=True)
    return {"message": "Login successful", "access_token": token}

# NEW: Middleware logic
@app.middleware("http")
async def add_login_middleware(request: Request, call_next):
    # Public paths that don't require authentication
    public_paths = ["/login", "/static/", "/api/webhooks/"]
    
    # Check if the request path starts with any public path
    is_public = any(request.url.path.startswith(path) for path in public_paths)

    if not is_public:
        token = request.cookies.get("access_token")
        if not token:
            return RedirectResponse(url="/login_page")
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.state.user = payload.get("sub")
        except jwt.ExpiredSignatureError:
            return RedirectResponse(url="/login_page")
        except jwt.InvalidTokenError:
            return RedirectResponse(url="/login_page")

    response = await call_next(request)
    return response

# NEW: Login Page Route
@app.get("/login_page", response_class=HTMLResponse, include_in_schema=False)
async def get_login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login"})

# Existing routers
app.include_router(dashboard.router)
app.include_router(dashboard_v2.router)
app.include_router(orders.router)
app.include_router(products.router)
app.include_router(inventory.router)
app.include_router(inventory_v2.router)
app.include_router(bulk_update.router)
app.include_router(sync_control.router)
app.include_router(config.router)
app.include_router(webhooks.router)
app.include_router(forecasting.router) 
app.include_router(sales_analytics.router)

# Existing HTML page routes
@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root(request: Request):
    if not request.cookies.get("access_token"):
        return RedirectResponse(url="/login_page")
    return RedirectResponse(url="/dashboard-v2")

@app.get("/dashboard-v2", response_class=HTMLResponse, include_in_schema=False)
async def get_dashboard_v2_page(request: Request):
    return templates.TemplateResponse("dashboard_v2.html", {"request": request, "title": "Dashboard"})

@app.get("/inventory", response_class=HTMLResponse, include_in_schema=False)
async def get_inventory_page(request: Request):
    return templates.TemplateResponse("inventory.html", {"request": request, "title": "Inventory Report"})

@app.get("/forecasting", response_class=HTMLResponse, include_in_schema=False)
async def get_forecasting_page(request: Request):
    return templates.TemplateResponse("forecasting.html", {"request": request, "title": "Forecasting"})

@app.get("/bulk-update", response_class=HTMLResponse, include_in_schema=False)
async def get_bulk_update_page(request: Request):
    return templates.TemplateResponse("bulk_update.html", {"request": request, "title": "Bulk Update"})

@app.get("/mutations", response_class=HTMLResponse, include_in_schema=False)
async def get_mutations_page(request: Request):
    return templates.TemplateResponse("mutations.html", {"request": request, "title": "Edit Product"})

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    return templates.TemplateResponse("sync_control.html", {"request": request, "title": "Sync Control"})

@app.get("/config", response_class=HTMLResponse, include_in_schema=False)
async def get_config_page(request: Request):
    return templates.TemplateResponse("config.html", {"request": request, "title": "Configuration"})

@app.get("/inventory/product/{group_key}", response_class=HTMLResponse, include_in_schema=False)
async def get_product_details_page(request: Request, group_key: str):
    return templates.TemplateResponse(
        "product_details.html",
        {"request": request, "title": f"Product {group_key}", "group_key": group_key},
    )

@app.get("/sales-analytics", response_class=HTMLResponse, include_in_schema=False)
async def get_sales_analytics_page(request: Request):
    return templates.TemplateResponse("sales_analytics.html", {"request": request, "title": "Sales by Product"})