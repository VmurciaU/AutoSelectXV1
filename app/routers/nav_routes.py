# app/routers/nav_routes.py
from fastapi import APIRouter, Request, Depends
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.templating import Jinja2Templates
from app.database.conection import SessionLocal
from sqlalchemy.orm import Session
from app.utils.auth import get_current_user_id
from app.models.user import User

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _ctx(request: Request, db: Session):
    user_id = get_current_user_id(request)
    user_name, user_rol = None, None
    if user_id:
        u = db.query(User).filter(User.id == user_id).first()
        user_name = getattr(u, "nombre", None)
        user_rol  = getattr(u, "rol", None)
    return {"request": request, "user_name": user_name, "user_rol": user_rol}

@router.get("/", response_class=HTMLResponse)
def home(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("index.html", _ctx(request, db))

@router.get("/features", response_class=HTMLResponse)
def features(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("features.html", _ctx(request, db))

# Placeholders navegables (sin lógica aún)
@router.get("/cases", response_class=HTMLResponse)
def cases_list(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wip.html", {**_ctx(request, db), "title": "Casos (lista)"})

@router.get("/quotes", response_class=HTMLResponse)
def quotes_list(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wip.html", {**_ctx(request, db), "title": "Cotizaciones"})

@router.get("/products", response_class=HTMLResponse)
def products_catalog(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wip.html", {**_ctx(request, db), "title": "Productos"})

@router.get("/customers", response_class=HTMLResponse)
def customers_catalog(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wip.html", {**_ctx(request, db), "title": "Clientes"})

@router.get("/delivery-terms", response_class=HTMLResponse)
def delivery_terms_catalog(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wip.html", {**_ctx(request, db), "title": "Términos de Entrega"})

@router.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request, db: Session = Depends(get_db)):
    ctx = _ctx(request, db)
    if ctx["user_rol"] != "admin":
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("wip.html", {**ctx, "title": "Gestión de Usuarios"})
