from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates

from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session, joinedload

from app.database.conection import SessionLocal
from app.utils.auth import get_current_user_id

from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document

import os
import shutil

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# --- Directorios base (.env con defaults) ---
FILES_BASE_DIR = Path(os.getenv("FILES_BASE_DIR", "./shared_data")).resolve()
INBOX_DIR      = Path(os.getenv("INBOX_DIR", str(FILES_BASE_DIR / "inbox"))).resolve()
INDEX_DIR      = Path(os.getenv("INDEX_DIR", str(FILES_BASE_DIR / "index"))).resolve()

# --- DB session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Helpers ----------
def _is_admin(role: str | None) -> bool:
    return (role or "").lower() == "admin"

def _safe_remove_path(path: Path) -> None:
    try:
        if path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass

def _recount_docs(db: Session, case_id: int) -> int:
    cnt = (
        db.query(func.count(Document.id))
        .filter(Document.case_id == case_id, Document.status != "deleted")
        .scalar()
    )
    return int(cnt or 0)

# ===============================
# ==========  VISTAS  ===========
# ===============================

@router.get("/cases", response_class=HTMLResponse)
def list_cases_html(
    request: Request,
    q: Optional[str] = None,
    notes_q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    current_user = db.query(User).filter(User.id == user_id).first()
    user_name = getattr(current_user, "nombre", None) or getattr(current_user, "username", None) or "Invitado"
    is_admin = _is_admin(getattr(current_user, "rol", None))

    # Base: si no es admin, solo sus casos
    query = db.query(Case).options(joinedload(Case.user))
    if not is_admin:
        query = query.filter(Case.user_id == user_id)

    # Filtros de texto (opcionales)
    if q:
        query = query.filter(Case.name.ilike(f"%{q}%"))
    if notes_q:
        # si tu modelo tiene "notes"
        query = query.filter(Case.notes.ilike(f"%{notes_q}%"))

    # Subquery para contar docs (no 'deleted')
    docs_count_sq = (
        select(func.count(Document.id))
        .where(Document.case_id == Case.id, Document.status != "deleted")
        .correlate(Case)
        .scalar_subquery()
    )

    rows = (
        db.query(Case, docs_count_sq.label("docs_count"))
        .order_by(desc(Case.id))
        .all()
    )

    # Armamos payload para el template (incluye notes ya normalizado y owner_name)
    cases_payload = []
    for case, docs_count in rows:
        owner = getattr(case, "user", None)
        owner_name = None
        if owner is not None:
            owner_name = getattr(owner, "nombre", None) or getattr(owner, "username", None)

        # Normalizamos notas (string o None)
        notes_val = getattr(case, "notes", None)
        if notes_val is not None:
            try:
                # Evitar que queden notas con solo espacios/saltos de línea
                notes_val = str(notes_val).strip()
            except Exception:
                pass
            if notes_val == "":
                notes_val = None

        # Fecha de actualización: si no existe, cae a created_at
        updated_val = getattr(case, "updated_at", None) or getattr(case, "created_at", None)

        cases_payload.append({
            "id": case.id,
            "name": case.name,
            "status": case.status,
            "owner": case.user_id,
            "owner_name": owner_name,
            "docs_count": int(docs_count or 0),
            "notes": notes_val,
            "updated_at": updated_val,
        })

    return templates.TemplateResponse(
        "cases_list.html",
        {
            "request": request,
            "cases": cases_payload,
            "is_admin": is_admin,
            "user_name": user_name,
        },
    )

@router.post("/cases/create")
def create_case(
    request: Request,
    name: str = Form("Caso Demo"),
    notes: str = Form("creado desde /cases"),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case_obj = Case(
        user_id=user_id,
        customer_id=None,
        name=name[:200] if name else None,
        status="queued",
        input_dir="",
        index_dir="",
        rag_version=None,
        doc_count=0,
        notes=notes,
        # si tu modelo tiene created_at / updated_at, se setean; si no, no pasa nada
        **({ "created_at": datetime.utcnow(), "updated_at": datetime.utcnow() }
           if hasattr(Case, "created_at") and hasattr(Case, "updated_at") else {})
    )
    db.add(case_obj)
    db.flush()

    input_dir = INBOX_DIR / str(case_obj.id)
    index_dir = INDEX_DIR / str(case_obj.id)
    (input_dir / "original").mkdir(parents=True, exist_ok=True)
    index_dir.mkdir(parents=True, exist_ok=True)

    case_obj.input_dir = str(input_dir)
    case_obj.index_dir = str(index_dir)
    db.add(case_obj)
    db.commit()

    return RedirectResponse(url="/cases", status_code=302)

@router.post("/cases/{case_id}/delete")
def delete_case(
    request: Request,
    case_id: int,
    delete_files: bool = Form(True),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case_obj = db.query(Case).filter(Case.id == case_id).first()
    if not case_obj:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    user = db.query(User).filter(User.id == user_id).first()
    if not (_is_admin(getattr(user, "rol", None)) or case_obj.user_id == user_id):
        raise HTTPException(status_code=403, detail="No autorizado")

    db.delete(case_obj)
    db.commit()

    if delete_files:
        if getattr(case_obj, "input_dir", None):
            _safe_remove_path(Path(case_obj.input_dir))
        if getattr(case_obj, "index_dir", None):
            _safe_remove_path(Path(case_obj.index_dir))

    return RedirectResponse(url="/cases", status_code=302)

@router.get("/cases/{case_id}")
def case_detail(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    case_obj = db.query(Case).filter(Case.id == case_id).first()
    if not case_obj:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    user = db.query(User).filter(User.id == user_id).first()
    if not (_is_admin(getattr(user, "rol", None)) or case_obj.user_id == user_id):
        raise HTTPException(status_code=403, detail="No autorizado")

    docs_count = _recount_docs(db, case_id)
    if hasattr(case_obj, "doc_count") and case_obj.doc_count != docs_count:
        case_obj.doc_count = docs_count
        if hasattr(case_obj, "updated_at"):
            setattr(case_obj, "updated_at", datetime.utcnow())
        db.add(case_obj)
        db.commit()

    return JSONResponse(
        {
            "id": case_obj.id,
            "name": case_obj.name,
            "status": case_obj.status,
            "doc_count": getattr(case_obj, "doc_count", docs_count),
            "input_dir": getattr(case_obj, "input_dir", None),
            "index_dir": getattr(case_obj, "index_dir", None),
            "notes": getattr(case_obj, "notes", None),
            "updated_at": (
                getattr(case_obj, "updated_at", None).isoformat()
                if getattr(case_obj, "updated_at", None) else None
            ),
        }
    )

@router.get("/cases/{case_id}/upload")
def open_upload_for_case(case_id: int):
    return RedirectResponse(url=f"/upload?selected_case_id={case_id}", status_code=302)
