# app/routers/cases_routes.py
import os
from datetime import datetime
from typing import List, Dict, Any
from fastapi import (
    APIRouter, Depends, HTTPException, status, Request, Form
)
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from app.database.deps import get_db
from app.models.cases import Case
from app.models.user import User
from app.schemas.case_schema import CaseCreate, CaseOut
from app.utils.auth import get_current_user_id


from sqlalchemy import func
from sqlalchemy.orm import Session
from fastapi.responses import RedirectResponse, HTMLResponse


router = APIRouter(prefix="/cases", tags=["cases"])
templates = Jinja2Templates(directory="app/templates")

# ----------------------
# Utilidades
# ----------------------
def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _ctx_user(request: Request, db: Session):
    """(user_id, user, user_name, user_rol)"""
    user_id = get_current_user_id(request)
    user = db.query(User).filter(User.id == user_id).first() if user_id else None
    return (
        user_id,
        user,
        getattr(user, "nombre", None) if user else None,
        getattr(user, "rol", None) if user else None,
    )

def _is_admin(user_rol: str | None) -> bool:
    return (user_rol or "").lower() == "admin"

def _status_badge(status: str | None) -> Dict[str, str]:
    s = (status or "").upper()
    mapping = {
        "NEW":         {"text": "Nuevo",       "bg": "bg-blue-600/20",    "color": "text-blue-300"},
        "QUEUED":      {"text": "En cola",     "bg": "bg-blue-600/20",    "color": "text-blue-300"},
        "PROCESSING":  {"text": "Procesando",  "bg": "bg-amber-600/20",   "color": "text-amber-300"},
        "READY":       {"text": "Listo",       "bg": "bg-emerald-600/20", "color": "text-emerald-300"},
        "ERROR":       {"text": "Error",       "bg": "bg-rose-600/20",    "color": "text-rose-300"},
    }
    return mapping.get(s, {"text": s or "—", "bg": "bg-gray-600/20", "color": "text-gray-300"})

def _can_edit_or_delete(user_id: int | None, user_rol: str | None, case: Case) -> bool:
    if not user_id:
        return False
    if _is_admin(user_rol):
        return True
    # dueño
    return getattr(case, "user_id", None) == user_id

def _row_from_case(c: Case, user_id: int | None, user_rol: str | None) -> Dict[str, Any]:
    """Estandariza lo que la plantilla necesita (badges + permisos)."""
    return {
        "id": c.id,
        "title": getattr(c, "name", None) or getattr(c, "title", None) or f"Caso #{c.id}",
        "description": getattr(c, "notes", None) or getattr(c, "description", None),
        "owner_display": getattr(c, "owner_name", None) or getattr(c, "owner_email", None) or getattr(c, "user_id", None),
        "updated": getattr(c, "updated_at", None) or getattr(c, "created_at", None),
        "status_badge": _status_badge(getattr(c, "status", None)),
        "can_edit": _can_edit_or_delete(user_id, user_rol, c),
        "can_delete": _can_edit_or_delete(user_id, user_rol, c),
    }

# =========================================================
# ==============   VISTAS HTML (web)   ====================
# =========================================================


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
def list_cases_html(request: Request, db: Session = Depends(get_db)):
    user_id, _, user_name, user_rol = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    from app.models.documents import Document  # Import local necesario

    is_admin = (user_rol or "").lower() == "admin"

    # ----------------------------
    # 1) Parámetros de paginación
    # ----------------------------
    page_size = 10
    try:
        page = int(request.query_params.get("page", 1))
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    # ----------------------------
    # 2) Base query de casos
    # ----------------------------
    base_q = db.query(Case).enable_eagerloads(False)

    # Ignorar casos eliminados (soft delete)
    base_q = base_q.filter(Case.status != "deleted")

    if not is_admin:
        base_q = base_q.filter(Case.user_id == user_id)

    
        # ----------------------------
    # 2-bis) Filtros (ID, nombre, notas)
    # ----------------------------
    q_id_raw = request.query_params.get("q_id", "").strip()
    q_name = request.query_params.get("q_name", "").strip()
    q_notes = request.query_params.get("q_notes", "").strip()

    # Filtro por ID exacto (si es número válido)
    if q_id_raw:
        try:
            q_id = int(q_id_raw)
            base_q = base_q.filter(Case.id == q_id)
        except ValueError:
            # Si no es número, dejamos base_q sin cambios o podríamos forzar sin resultados.
            pass

    # Filtro por nombre (LIKE)
    if q_name:
        base_q = base_q.filter(Case.name.ilike(f"%{q_name}%"))

    # Filtro por notas / descripción (LIKE)
    if q_notes:
        base_q = base_q.filter(Case.notes.ilike(f"%{q_notes}%"))


    # ----------------------------
    # 3) Total de casos (para paginación)
    # ----------------------------
    total_cases = base_q.count()
    total_pages = max(1, (total_cases + page_size - 1) // page_size)

    # Ajustar si la página excede
    if page > total_pages:
        page = total_pages

    offset = (page - 1) * page_size

    # ----------------------------
    # 4) Subquery: conteo real de documentos (ignorando los deleted)
    # ----------------------------
    docs_sq = (
            db.query(
                    Document.case_id.label("case_id"),
                    func.count(Document.id).label("doc_count")    
            )
            .filter(Document.status != "deleted")   # <<–– ESTE ES EL CAMBIO CLAVE
            .group_by(Document.case_id)
            .subquery()
)

    # ----------------------------
    # 5) Query final paginada con doc_count
    # ----------------------------
    q = (
        base_q
        .outerjoin(docs_sq, docs_sq.c.case_id == Case.id)
        .with_entities(
            Case,
            func.coalesce(docs_sq.c.doc_count, 0).label("doc_count")
        )
        .order_by(Case.updated_at.desc())
        .offset(offset)
        .limit(page_size)
    )

    # ----------------------------
    # 6) Construir filas para plantilla
    # ----------------------------
    rows = []
    for case_obj, real_doc_count in q.all():
        r = _row_from_case(case_obj, user_id, user_rol)
        r["doc_count"] = int(real_doc_count or 0)

        # Propietario (admin)
        if is_admin:
            owner = db.query(User).filter(User.id == case_obj.user_id).first()
            r["owner_display"] = (
                getattr(owner, "nombre", None)
                or getattr(owner, "email", None)
                or f"ID {case_obj.user_id}"
            )
        else:
            r["owner_display"] = None

        rows.append(r)

    # ----------------------------
    # 7) Render de la lista
    # ----------------------------
    return templates.TemplateResponse(
        "cases_list.html",
        {
            "request": request,
            "rows": rows,
            "user_name": user_name,
            "user_rol": user_rol,
            "page": page,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
        },
    )



@router.post("/new", response_class=HTMLResponse)
def create_case_html(
    request: Request,
    name: str = Form("Caso Nuevo"),
    notes: str = Form("Creado desde /cases"),
    db: Session = Depends(get_db),
):
    """
    Crea un nuevo caso usando case.id para construir rutas.
    """
    # 1. Validar usuario
    user_id, _, _, _ = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # 2. Crear el caso con rutas vacías (placeholder)
    new_case = Case(
        user_id=user_id,
        customer_id=None,
        name=name[:200],
        status="queued",
        input_dir="",
        index_dir="",
        rag_version="pc1-6@2025.11.09",
        notes=notes,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(new_case)
    db.commit()
    db.refresh(new_case)   # <-- YA tenemos new_case.id

    # 3. Construir rutas reales usando case.id
    base_dir = "./shared_data"

    final_input_dir = os.path.join(base_dir, f"inbox/{new_case.id}/original")
    final_index_dir = os.path.join(base_dir, f"index/{new_case.id}")

    # Crear carpetas físicas
    _ensure_dir(final_input_dir)
    _ensure_dir(final_index_dir)

    # 4. Actualizar el caso con las rutas correctas
    new_case.input_dir = final_input_dir
    new_case.index_dir = final_index_dir
    new_case.updated_at = datetime.utcnow()

    db.commit()

    # 5. Redirigir a la vista del caso
    return RedirectResponse(url=f"/cases/{new_case.id}/view", status_code=302)


@router.get("/{case_id}/view", response_class=HTMLResponse)
def view_case_html(case_id: int, request: Request, db: Session = Depends(get_db)):
    user_id, _, user_name, user_rol = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        return RedirectResponse(url="/cases", status_code=302)
    
    # No permitir ver casos eliminados
    if case.status == "deleted":
        return RedirectResponse(url="/cases", status_code=302)

    # admin o dueño
    if not _is_admin(user_rol) and case.user_id != user_id:
        return RedirectResponse(url="/cases", status_code=302)

    row = _row_from_case(case, user_id, user_rol)

    return templates.TemplateResponse(
        "case_detail.html",
        {
            "request": request,
            "case": case,   # compat
            "row": row,     # útil si quieres reusar badges en el detalle
            "user_name": user_name,
            "user_rol": user_rol,
        },
    )


@router.get("/{case_id}/edit", response_class=HTMLResponse)
def edit_case_form(
    case_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user_id, _, user_name, user_rol = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        return RedirectResponse(url="/cases", status_code=302)

    # No permitir editar casos eliminados
    if case.status == "deleted":
        return RedirectResponse(url="/cases", status_code=302)

    # permisos: admin o dueño
    if not _can_edit_or_delete(user_id, user_rol, case):
        return RedirectResponse(url="/cases", status_code=302)

    row = _row_from_case(case, user_id, user_rol)

    return templates.TemplateResponse(
        "case_edit.html",
        {
            "request": request,
            "case": case,
            "row": row,
            "user_name": user_name,
            "user_rol": user_rol,
        },
    )


@router.post("/{case_id}/edit", response_class=HTMLResponse)
def edit_case_submit(
    case_id: int,
    request: Request,
    name: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    user_id, _, _, user_rol = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        return RedirectResponse(url="/cases", status_code=302)

    # No permitir editar casos eliminados
    if case.status == "deleted":
        return RedirectResponse(url="/cases", status_code=302)

    # permisos: admin o dueño
    if not _can_edit_or_delete(user_id, user_rol, case):
        return RedirectResponse(url="/cases", status_code=302)

    # Solo actualizamos nombre y notas
    case.name = name[:200] if name else case.name
    case.notes = notes

    # Actualizar timestamp
    if hasattr(case, "touch"):
        case.touch()
    else:
        case.updated_at = datetime.utcnow()

    db.commit()

    return RedirectResponse(
        url=f"/cases/{case.id}/view",
        status_code=302,
    )



@router.post("/{case_id}/delete", response_class=HTMLResponse)
def delete_case_html(
    case_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user_id, _, _, user_rol = _ctx_user(request, db)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        return RedirectResponse(url="/cases", status_code=302)

    # permisos: admin o dueño
    if not _can_edit_or_delete(user_id, user_rol, case):
        return RedirectResponse(url="/cases", status_code=302)

    # Soft delete: solo marcamos el status, no borramos de la BD
    case.status = "deleted"
    if hasattr(case, "touch"):
        case.touch()
    else:
        case.updated_at = datetime.utcnow()

    db.commit()

    return RedirectResponse(url="/cases", status_code=302)


# Enlaza a tu pantalla de carga existente (documentos + proceso)
@router.get("/{case_id}/upload")
def case_upload_redirect(case_id: int):
    return RedirectResponse(url=f"/upload?selected_case_id={case_id}", status_code=302)

# =========================================================
# ==============   API REST (JSON)      ===================
# =========================================================
# API separada bajo /cases/api

@router.post("/api", response_model=CaseOut, status_code=status.HTTP_201_CREATED)
def create_case_api(payload: CaseCreate, request: Request, db: Session = Depends(get_db)):
    # para pruebas, fallback si no hay sesión (igual que antes)
    user_id = get_current_user_id(request) or 3

    if not payload.input_dir or not payload.index_dir:
        raise HTTPException(status_code=400, detail="input_dir y index_dir son requeridos")

    _ensure_dir(payload.input_dir)
    _ensure_dir(payload.index_dir)

    case = Case(
        user_id=user_id,
        customer_id=payload.customer_id,
        name=payload.name,
        status=payload.status,
        input_dir=payload.input_dir,
        index_dir=payload.index_dir,
        rag_version=payload.rag_version,
        notes=payload.notes,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(case)
    db.commit()
    db.refresh(case)
    return case

@router.get("/api", response_model=List[CaseOut])
def list_cases_api(db: Session = Depends(get_db)):
    return db.query(Case).order_by(Case.id.asc()).all()

@router.get("/api/{case_id}", response_model=CaseOut)
def get_case_api(case_id: int, db: Session = Depends(get_db)):
    case = db.get(Case, case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case no encontrado")
    return case

