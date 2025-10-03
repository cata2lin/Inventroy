import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request, Form, Depends, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv
from jose import jwt, JOSEError
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import models

# Ensure the project root is in the Python path for clean imports
ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base, get_db
from routes import sync_control, config

# Load environment variables from .env file
load_dotenv()

# Initialize the FastAPI app
app = FastAPI(title="Inventory Suite")

# Mount static files and templates directories
app.mount("/static", StaticFiles(directory=str(ROOT_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(ROOT_DIR / "templates"))

# Create all database tables on application startup if they don't exist
Base.metadata.create_all(bind=engine)

# Secret key for JWT authentication from environment variables
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key-that-is-long-and-secure")

# --- Authentication Endpoints & Middleware ---

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    """Handles user login, verifies credentials, and sets an HTTPOnly cookie."""
    user = db.query(models.User).filter_by(username=username).first()
    if not user or not user.verify_password(password):
        # Redirect back to the login page with an error query parameter
        return RedirectResponse(url="/login_page?error=1", status_code=303)

    # Create the JWT token payload
    payload = {
        "sub": user.username,
        "exp": datetime.utcnow() + timedelta(days=1)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

    # Create a redirect response to the main page
    response = RedirectResponse(url="/", status_code=303)

    # Set the token in a secure, HTTPOnly cookie
    response.set_cookie(
        key="access_token_8002",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400,  # Cookie expires in 1 day
        secure=True     # Ensure the cookie is only sent over HTTPS
    )
    return response

@app.middleware("http")
async def add_login_middleware(request: Request, call_next):
    """
    Middleware to protect routes. It checks for a valid JWT cookie on all
    paths except for a defined list of public paths.
    """
    public_paths = ["/login", "/static/", "/login_page"]
    if any(request.url.path.startswith(path) for path in public_paths):
        return await call_next(request)

    token = request.cookies.get("access_token_8002")
    if not token:
        return RedirectResponse(url="/login_page")

    try:
        # Verify the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        request.state.user = payload.get("sub")
    except JOSEError:
        # If the token is invalid or expired, clear it and redirect to login
        response = RedirectResponse(url="/login_page")
        response.delete_cookie("access_token_8002")
        return response

    return await call_next(request)

# --- HTML Page Routes ---

@app.get("/login_page", response_class=HTMLResponse, include_in_schema=False)
async def get_login_page(request: Request):
    """Serves the login page."""
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login"})

@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root():
    """Redirects the root URL to the main sync control page."""
    return RedirectResponse(url="/sync-control")

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    """Serves the Sync Control page."""
    return templates.TemplateResponse("sync_control.html", {"request": request, "title": "Sync Control"})

@app.get("/config", response_class=HTMLResponse, include_in_schema=False)
async def get_config_page(request: Request):
    """Serves the Configuration page."""
    return templates.TemplateResponse("config.html", {"request": request, "title": "Configuration"})

# --- API Routers ---

# Include the API endpoints from the routes package
app.include_router(sync_control.router)
app.include_router(config.router)