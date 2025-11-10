# app/routers/admin_routes.py
from fastapi import APIRouter, Request, Depends, Form, status
from fastapi.responses import RedirectResponse, HTMLResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.models.user import User
from app.utils.auth import get_current_user_id

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/usuarios", response_class=HTMLResponse)
def gestionar_usuarios(request: Request, db: Session = Depends(get_db)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    usuario = db.query(User).filter(User.id == user_id).first()
    if not usuario or usuario.rol != "admin":
        return RedirectResponse(url="/", status_code=302)

    usuarios = db.query(User).all()
    return templates.TemplateResponse("usuarios.html", {
        "request": request,
        "usuarios": usuarios,
        "user_name": usuario.nombre,
        "user_rol": usuario.rol
    })

@router.post("/usuarios/estado/{user_id}")
def cambiar_estado(user_id: int, db: Session = Depends(get_db)):
    usuario = db.query(User).filter(User.id == user_id).first()
    if usuario:
        usuario.activo = not usuario.activo
        db.commit()
    return RedirectResponse(url="/usuarios", status_code=status.HTTP_302_FOUND)

@router.post("/usuarios/rol/{user_id}")
def cambiar_rol(user_id: int, db: Session = Depends(get_db)):
    usuario = db.query(User).filter(User.id == user_id).first()
    if usuario:
        usuario.rol = "admin" if usuario.rol == "usuario" else "usuario"
        db.commit()
    return RedirectResponse(url="/usuarios", status_code=status.HTTP_302_FOUND)
