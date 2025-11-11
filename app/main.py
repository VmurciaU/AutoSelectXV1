# app/main.py
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os

from app.routers import user_routes, upload_routes, admin_routes, nav_routes
# ⬇️ importa los routers nuevos
from app.routers import cases_routes, documents_routes

from app.utils.auth import get_current_user_id
from app.database.conection import SessionLocal
from app.models.user import User

app = FastAPI(title="AutoSelectX App")

# Routers existentes
app.include_router(user_routes.router)
app.include_router(upload_routes.router)
app.include_router(admin_routes.router)

# ⬇️ Routers nuevos (MVP casos + documentos)
app.include_router(cases_routes.router)
app.include_router(documents_routes.router)

app.include_router(nav_routes.router)

# Static y templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/health")
def health():
    return {"status": "ok"}

# Página principal
@app.get("/", response_class=HTMLResponse)
async def mostrar_inicio(request: Request):
    user_id = get_current_user_id(request)
    user_name = None
    user_rol = None

    if user_id:
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.id == user_id).first()
            if user:
                user_name = user.nombre
                user_rol = user.rol
        finally:
            db.close()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "user_name": user_name,
        "user_rol": user_rol
    })

@app.get("/caracteristicas", response_class=HTMLResponse)
async def mostrar_caracteristicas(request: Request):
    user_id = get_current_user_id(request)
    user_name = None
    user_rol = None

    if user_id:
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.id == user_id).first()
            if user:
                user_name = user.nombre
                user_rol = user.rol
        finally:
            db.close()

    return templates.TemplateResponse("caracteristicas.html", {
        "request": request,
        "user_name": user_name,
        "user_rol": user_rol
    })
