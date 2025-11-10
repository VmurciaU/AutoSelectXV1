# app/routers/user_routes.py
from fastapi import APIRouter, Request, Form, status, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates
from passlib.context import CryptContext
from itsdangerous import URLSafeSerializer

from app.database.conection import SessionLocal
from app.models.user import User
from app.utils.auth import get_current_user_id

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ⚠️ Para producción, lee del entorno (no hardcodear)
SECRET_KEY = "autoselectx_secret_key_2024"
serializer = URLSafeSerializer(SECRET_KEY, salt="session")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/register", response_class=HTMLResponse)
def show_register_form(request: Request):
    user_id = get_current_user_id(request)
    if user_id:
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse("register.html", {
        "request": request,
        "user_name": None,
        "user_rol": None
    })

@router.post("/register")
def register_user(
    request: Request,
    nombre: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    if db.query(User).filter(User.email == email).first():
        return templates.TemplateResponse("register.html", {"request": request, "error": "El correo ya está registrado."})

    hashed_password = pwd_context.hash(password)
    new_user = User(nombre=nombre, email=email, password_hash=hashed_password, activo=False)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    print(f"✅ Usuario registrado: {new_user.email}")
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

@router.get("/login", response_class=HTMLResponse)
def show_login_form(request: Request):
    user_id = get_current_user_id(request)
    if user_id:
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse("login.html", {
        "request": request,
        "user_name": None,
        "user_rol": None
    })

@router.post("/login")
def login_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    error = None
    user = db.query(User).filter(User.email == email).first()
    if not user:
        error = "Usuario no registrado."
    elif not pwd_context.verify(password, user.password_hash):
        error = "Contraseña incorrecta."
    elif not user.activo:
        error = "Usuario inactivo. Solicita activación al administrador."

    if error:
        return templates.TemplateResponse("login.html", {"request": request, "error": error})

    print(f"✅ Usuario autenticado: {user.email}")

    response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    signed_id = serializer.dumps(str(user.id))
    response.set_cookie(key="session_token", value=signed_id, httponly=True)
    return response

@router.get("/quien-soy", response_class=HTMLResponse)
def quien_soy(request: Request):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    return HTMLResponse(f"<h1>Sesión activa para el usuario con ID: {user_id}</h1>")

@router.get("/logout")
def logout(request: Request):
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("session_token")
    return response
