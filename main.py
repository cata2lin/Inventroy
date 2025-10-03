import os
import sys
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Form, Depends, status, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv
from jose import jwt, JOSEError
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
import models

ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

from database import engine, Base, get_db
from routes import sync_control, config

load_dotenv()

app = FastAPI(title="Inventory Suite")
app.mount("/static", StaticFiles(directory=str(ROOT_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(ROOT_DIR / "templates"))
Base.metadata.create_all(bind=engine)

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key")

@app.post("/login")
async def login(response: Response, username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=username).first()
    if not user or not user.verify_password(password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    payload = { "sub": user.username, "exp": datetime.utcnow() + timedelta(days=1) }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    
    response.set_cookie(key="access_token_8002", value=token, httponly=True, samesite="lax", max_age=86400, secure=True)
    return {"message": "Login successful", "access_token": token}

@app.middleware("http")
async def add_login_middleware(request: Request, call_next):
    public_paths = ["/login", "/static/", "/login_page"]
    is_public = any(request.url.path.startswith(path) for path in public_paths)

    if not is_public:
        token = request.cookies.get("access_token_8002")
        if not token:
            return RedirectResponse(url="/login_page")
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.state.user = payload.get("sub")
        except JOSEError:
            return RedirectResponse(url="/login_page")

    response = await call_next(request)
    return response

@app.get("/login_page", response_class=HTMLResponse, include_in_schema=False)
async def get_login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login"})

app.include_router(sync_control.router)
app.include_router(config.router)

@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def read_root(request: Request):
    if not request.cookies.get("access_token_8002"):
        return RedirectResponse(url="/login_page")
    return RedirectResponse(url="/sync-control")

@app.get("/sync-control", response_class=HTMLResponse, include_in_schema=False)
async def get_sync_control_page(request: Request):
    return templates.TemplateResponse("sync_control.html", {"request": request, "title": "Sync Control"})