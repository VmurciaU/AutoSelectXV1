# app/routers/admin_routes.py
from fastapi import APIRouter, Request, Depends, status, Query
from fastapi.responses import RedirectResponse, HTMLResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.models.user import User
from app.utils.auth import get_current_user_id

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# -----------------------------------------------------------
# Conexión DB
# -----------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------------------------------------------
# Helper: verificar si el usuario es admin
# -----------------------------------------------------------
def _require_admin(request: Request, db: Session):
    """Devuelve el usuario admin si existe, de lo contrario None."""
    user_id = get_current_user_id(request)
    if not user_id:
        return None
    usuario = db.query(User).filter(User.id == user_id).first()
    if not usuario or usuario.rol != "admin":
        return None
    return usuario

# -----------------------------------------------------------
# LISTA / BÚSQUEDA / FILTROS
# -----------------------------------------------------------
@router.get("/usuarios", response_class=HTMLResponse)
@router.get("/admin/users", response_class=HTMLResponse)
def gestionar_usuarios(
    request: Request,
    q: str | None = Query(None, description="Buscar nombre o correo"),
    role: str | None = Query(None, description="Filtrar por rol"),
    estado: str | None = Query(None, description="Filtrar por estado"),
    db: Session = Depends(get_db),
):
    admin = _require_admin(request, db)
    if not admin:
        return RedirectResponse(url="/", status_code=302)

    query = db.query(User)

    # --- Filtros dinámicos ---
    if q:
        like = f"%{q.strip()}%"
        query = query.filter((User.nombre.ilike(like)) | (User.email.ilike(like)))

    if role in {"admin", "usuario"}:
        query = query.filter(User.rol == role)

    if estado == "activo":
        query = query.filter(User.activo.is_(True))
    elif estado == "inactivo":
        query = query.filter(User.activo.is_(False))

    usuarios = query.order_by(User.id.asc()).all()

    return templates.TemplateResponse(
        "usuarios.html",
        {
            "request": request,
            "usuarios": usuarios,
            "user_name": admin.nombre,
            "user_rol": admin.rol,
            "q": q or "",
            "role": role or "",
            "estado": estado or "",
        },
    )

# -----------------------------------------------------------
# ACTIVAR / DESACTIVAR USUARIO
# -----------------------------------------------------------
@router.post("/usuarios/estado/{user_id}")
def cambiar_estado(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = _require_admin(request, db)
    if not admin:
        return RedirectResponse(url="/", status_code=302)

    usuario = db.query(User).filter(User.id == user_id).first()
    if usuario:
        usuario.activo = not bool(usuario.activo)
        db.commit()

    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

# -----------------------------------------------------------
# CAMBIAR ROL (alternar admin <-> usuario)
# -----------------------------------------------------------
@router.post("/usuarios/rol/{user_id}")
def cambiar_rol(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = _require_admin(request, db)
    if not admin:
        return RedirectResponse(url="/", status_code=302)

    usuario = db.query(User).filter(User.id == user_id).first()
    if usuario:
        usuario.rol = "admin" if usuario.rol != "admin" else "usuario"
        db.commit()

    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)
