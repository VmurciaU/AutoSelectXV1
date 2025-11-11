# app/routers/upload_routes.py
from __future__ import annotations

from fastapi import (
    APIRouter, Request, UploadFile, File, Form, status,
    BackgroundTasks, Depends, HTTPException
)
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates

from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from pathlib import Path
from datetime import datetime
from typing import List, Optional
import os, io

from app.utils.auth import get_current_user_id
from app.database.conection import SessionLocal

# Modelos
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# --- Directorios base (.env con defaults) ---
FILES_BASE_DIR = Path(os.getenv("FILES_BASE_DIR", "./shared_data")).resolve()
INBOX_DIR      = Path(os.getenv("INBOX_DIR", str(FILES_BASE_DIR / "inbox"))).resolve()
INDEX_DIR      = Path(os.getenv("INDEX_DIR", str(FILES_BASE_DIR / "index"))).resolve()
RAG_VERSION    = os.getenv("RAG_VERSION", "pc1-6@2025.11.09")

# --- DB session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Utilidades ----------
def _safe_filename(name: str) -> str:
    keep = (" ", ".", "_", "-", "(", ")")
    cleaned = "".join(c for c in name if c.isalnum() or c in keep)
    return cleaned.strip().replace("..", ".")

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def _count_pdf_pages(data: bytes) -> Optional[int]:
    """Intenta contar páginas con pdfplumber si está disponible; si falla => None."""
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return None
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return len(pdf.pages)
    except Exception:
        return None

def _latest_case_for_user(db: Session, user_id: int) -> Optional[Case]:
    return (
        db.query(Case)
        .filter(Case.user_id == user_id)
        .order_by(desc(Case.id))
        .first()
    )

def _is_admin(role: str | None) -> bool:
    return (role or "").lower() == "admin"

def _load_case(db: Session, case_id: int | None) -> Optional[Case]:
    if not case_id:
        return None
    return db.query(Case).filter(Case.id == case_id).first()

# ===============================
# ==========  VISTAS  ===========
# ===============================
@router.get("/upload", response_class=HTMLResponse)
def show_upload_form(
    request: Request,
    processing_status: str | None = None,
    selected_case_id: int | None = None,  # ?selected_case_id=#
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_name = getattr(user, "nombre", None)
    user_role = getattr(user, "rol", None)

    # Casos del usuario (propios)
    cases_list = (
        db.query(Case)
        .filter(Case.user_id == user_id)
        .order_by(Case.id.desc())
        .all()
    )

    # Caso seleccionado: la query manda; si no viene, último caso del usuario
    sel_case = None
    if selected_case_id:
        sel_case = next((c for c in cases_list if c.id == selected_case_id), None)
    if not sel_case and cases_list:
        sel_case = cases_list[0]

    # Documentos desde DB (no eliminados)
    docs = []
    if sel_case:
        docs = (
            db.query(Document)
            .filter(Document.case_id == sel_case.id, Document.status != "deleted")
            .order_by(Document.id.asc())
            .all()
        )

    return templates.TemplateResponse(
        "carga.html",   # <-- tu plantilla existente
        {
            "request": request,
            "docs": [
                {
                    "id": d.id,
                    "filename": d.filename,
                    "stored_path": d.stored_path,
                    "size_bytes": d.size_bytes,
                    "pages": d.pages,
                    "status": d.status,
                    "created_at": d.created_at,
                }
                for d in docs
            ],
            "cases_list": [
                {
                    "id": c.id,
                    "name": getattr(c, "name", None),
                    "status": getattr(c, "status", None),
                    "doc_count": getattr(c, "doc_count", 0),
                    # ¡Ojo! campos opcionales: usar getattr para no romper
                    "notes": getattr(c, "notes", None),
                    "updated_at": getattr(c, "updated_at", None),
                }
                for c in cases_list
            ],
            "selected_case_id": sel_case.id if sel_case else None,
            "processing_status": processing_status,
            "user_name": user_name,
            "user_rol": user_role,
        },
    )

@router.post("/upload", response_class=HTMLResponse)
async def handle_upload(
    request: Request,
    file: UploadFile | None = File(None),
    files: List[UploadFile] | None = File(None),
    case_id: int | None = Form(None),             # hidden del form
    case_name: str = Form("Caso Demo"),
    notes: str = Form("creado desde /upload"),
    db: Session = Depends(get_db),
):
    """
    Si llega case_id (o selected_case_id en la query), se usa ese CASE EXISTENTE.
    Si no llega, se crea un caso NUEVO (manteniendo compatibilidad).
    """
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # Unificar fuente de case_id: form > querystring
    if case_id is None:
        q_case = request.query_params.get("selected_case_id")
        if q_case and str(q_case).isdigit():
            case_id = int(q_case)

    # Normalizar lista de archivos
    upload_list: List[UploadFile] = []
    if file is not None:
        upload_list.append(file)
    if files:
        upload_list.extend([f for f in files if f is not None])
    if not upload_list:
        return JSONResponse({"error": "No se recibió ningún archivo."}, status_code=400)

    # --- Cargar/crear el caso ---
    if case_id:
        # Usar caso existente
        case_obj = _load_case(db, case_id)
        if not case_obj:
            return JSONResponse({"error": "Case no existe."}, status_code=404)

        # Seguridad: dueño o admin
        user = db.query(User).filter(User.id == user_id).first()
        if not (_is_admin(getattr(user, "rol", None)) or getattr(case_obj, "user_id", None) == user_id):
            return JSONResponse({"error": "No autorizado para este case."}, status_code=403)

        # Asegurar rutas del caso (estándar por CASE_ID)
        input_dir = Path(getattr(case_obj, "input_dir", "") or (INBOX_DIR / str(case_obj.id)))
        index_dir = Path(getattr(case_obj, "index_dir", "") or (INDEX_DIR / str(case_obj.id)))
        original_dir = input_dir / "original"
        _ensure_dir(original_dir)
        _ensure_dir(index_dir)

        case_obj.input_dir = str(input_dir)
        case_obj.index_dir = str(index_dir)

    else:
        # Crear NUEVO caso SOLO si no vino case_id
        case_obj = Case(
            user_id=user_id,
            customer_id=None,
            name=(case_name or "Caso").strip()[:200],
            status="queued",
            input_dir="",
            index_dir="",
            # Campos opcionales: si el modelo no los tiene no pasa nada
            **({"rag_version": RAG_VERSION} if hasattr(Case, "rag_version") else {}),
            **({"doc_count": 0} if hasattr(Case, "doc_count") else {}),
            **({"notes": notes} if hasattr(Case, "notes") else {}),
            **({"created_at": datetime.utcnow()} if hasattr(Case, "created_at") else {}),
            **({"updated_at": datetime.utcnow()} if hasattr(Case, "updated_at") else {}),
        )
        db.add(case_obj)
        db.flush()  # obtiene id

        # Estándar por CASE_ID
        input_dir = INBOX_DIR / str(case_obj.id)
        index_dir = INDEX_DIR / str(case_obj.id)
        original_dir = input_dir / "original"
        _ensure_dir(input_dir)
        _ensure_dir(original_dir)
        _ensure_dir(index_dir)

        case_obj.input_dir = str(input_dir)
        case_obj.index_dir = str(index_dir)

    # --- Guardar archivos en disco + registrar en DB ---
    added = 0
    for f in upload_list:
        filename = _safe_filename(Path(f.filename or "").name)
        if not filename or not filename.lower().endswith(".pdf"):
            db.rollback()
            return JSONResponse(
                {"error": f"Solo PDF. Archivo inválido: {filename or '(sin nombre)'}"},
                status_code=400,
            )

        file_bytes = await f.read()
        stored_path = Path(case_obj.input_dir) / "original" / filename
        _ensure_dir(stored_path.parent)

        with open(stored_path, "wb") as buffer:
            buffer.write(file_bytes)

        pages = _count_pdf_pages(file_bytes)
        mime_type = getattr(f, "content_type", None)
        size_bytes = len(file_bytes) if file_bytes is not None else None

        doc = Document(
            case_id=case_obj.id,
            user_id=user_id,
            filename=filename,                # basename en DB
            original_path=str(stored_path),   # ruta completa (puede ser igual a stored_path)
            stored_path=str(stored_path),     # ruta completa
            mime_type=mime_type,
            size_bytes=size_bytes,
            pages=pages,
            status="uploaded",
            **({"created_at": datetime.utcnow()} if hasattr(Document, "created_at") else {}),
            **({"updated_at": datetime.utcnow()} if hasattr(Document, "updated_at") else {}),
        )
        db.add(doc)
        added += 1

    # Actualizar contadores/timestamps si existen
    if hasattr(case_obj, "doc_count"):
        case_obj.doc_count = int(getattr(case_obj, "doc_count", 0) or 0) + added
    if hasattr(case_obj, "updated_at"):
        case_obj.updated_at = datetime.utcnow()

    db.commit()

    # Volver a la misma pantalla del case seleccionado
    return RedirectResponse(
        url=f"/upload?selected_case_id={case_obj.id}",
        status_code=status.HTTP_302_FOUND,
    )

@router.post("/delete-file")
def delete_file(
    request: Request,
    doc_id: Optional[int] = Form(None),      # preferido
    filename: Optional[str] = Form(None),    # fallback legado
    case_id: int | None = Form(None),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # Resolver el case (form > último del usuario)
    sel_case = None
    if case_id:
        sel_case = db.query(Case).filter(Case.id == case_id).first()
    if not sel_case:
        sel_case = _latest_case_for_user(db, user_id)
    if not sel_case:
        return RedirectResponse(url="/upload", status_code=302)

    # Permisos: dueño o admin
    user = db.query(User).filter(User.id == user_id).first()
    if not (_is_admin(getattr(user, "rol", None)) or getattr(sel_case, "user_id", None) == user_id):
        return RedirectResponse(url="/cases", status_code=302)

    # --- Localizar doc ---
    doc: Optional[Document] = None
    if doc_id:
        doc = (
            db.query(Document)
            .filter(Document.id == doc_id, Document.case_id == sel_case.id)
            .first()
        )
    elif filename:
        # modo legado por nombre (en el caso activo)
        basename = Path(filename).name
        doc = (
            db.query(Document)
            .filter(Document.case_id == sel_case.id, Document.filename == basename)
            .first()
        )

    if not doc:
        return RedirectResponse(url=f"/upload?selected_case_id={sel_case.id}", status_code=302)

    # Borrar físico si existe
    try:
        p = Path(doc.stored_path) if getattr(doc, "stored_path", None) else None
        if p and p.exists():
            p.unlink()
    except Exception:
        pass

    # Soft delete en DB
    doc.status = "deleted"
    if hasattr(doc, "updated_at"):
        doc.updated_at = datetime.utcnow()
    db.add(doc)

    # Recalcular conteo real (solo no-deleted)
    remaining = (
        db.query(func.count(Document.id))
        .filter(Document.case_id == sel_case.id, Document.status != "deleted")
        .scalar()
    )
    if hasattr(sel_case, "doc_count"):
        sel_case.doc_count = int(remaining or 0)
    if hasattr(sel_case, "updated_at"):
        sel_case.updated_at = datetime.utcnow()
    db.add(sel_case)

    db.commit()

    return RedirectResponse(
        url=f"/upload?selected_case_id={sel_case.id}",
        status_code=302,
    )

# ---- Procesamiento (placeholder para indexación) ----
@router.post("/procesar")
def procesar_archivos(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    last_case = _latest_case_for_user(db, user_id)
    if not last_case:
        return RedirectResponse(url="/upload?processing_status=no_case", status_code=302)

    # Aquí iría la tarea asíncrona real
    last_case.status = "processing"
    if hasattr(last_case, "updated_at"):
        last_case.updated_at = datetime.utcnow()
    db.add(last_case)
    db.commit()

    return RedirectResponse(url="/upload?processing_status=in_progress", status_code=302)

# ---- Progreso (mock sencillo) ----
@router.get("/progress")
def consultar_progreso(request: Request):
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)
    # Placeholder simple
    return JSONResponse(content={"progress": {"percent": 0}})
