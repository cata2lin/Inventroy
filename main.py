# main.py
import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request, Form, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv
from jose import jwt, JOSEError
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import models

ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base, get_db
from routes import sync_control, config, products, mutations

load_dotenv()

app = FastAPI(title="Inventory Suite")

app.mount("/static", StaticFiles(directory=str(ROOT_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(ROOT_DIR / "templates"))

Base.metadata.create_all(bind=engine)

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key-that-is-long-and-secure")

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=username).first()
    if not user or not user.verify_password(password):
        return RedirectResponse(url="/login_page?error=1", status_code=303)
    payload = {"sub": user.username, "exp": datetime.utcnow() + timedelta(days=1)}
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(key="access_token_8002", value=token, httponly=True, samesite="lax", max_age=86400, secure=True)
    return response

@app.middleware("http")
async def add_login_middleware(request: Request, call_next):
    public_paths = ["/login", "/static/", "/login_page"]
    if any(request.url.path.startswith(path) for path in public_paths):
        return await call_next(request)
    token = request.cookies.get("access_token_8002")
    if not token:
        return RedirectResponse(url="/login_page")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        request.state.user = payload.get("sub")
    except JOSEError:
        response = RedirectResponse(url="/login_page")
        response.delete_cookie("access_token_8002")
        return response
    return await call_next(request)

@app.get("/login_page", response_class=HTMLResponse, include_in_schema=False)
async def get_login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login"})

@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root():
    return RedirectResponse(url="/sync-control")

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    return templates.TemplateResponse("sync_control.html", {"request": request, "title": "Sync Control"})

@app.get("/config", response_class=HTMLResponse, include_in_schema=False)
async def get_config_page(request: Request):
    return templates.TemplateResponse("config.html", {"request": request, "title": "Configuration"})

@app.get("/products", response_class=HTMLResponse, include_in_schema=False)
async def get_products_page(request: Request):
    return templates.TemplateResponse("products.html", {"request": request, "title": "Products"})

@app.get("/mutations", response_class=HTMLResponse, include_in_schema=False)
async def get_mutations_page(request: Request):
    return templates.TemplateResponse("mutations.html", {"request": request, "title": "Mutations"})

# Routers
app.include_router(sync_control.router)
app.include_router(config.router)
app.include_router(products.router)
app.include_router(mutations.router)
