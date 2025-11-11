# app/routers/upload_routes.py
from __future__ import annotations

from fastapi import APIRouter, Request, UploadFile, File, Form, status, BackgroundTasks, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import desc
import os, shutil, io
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from app.utils.auth import get_current_user_id
from app.database.conection import SessionLocal

# === MODELOS (plural, tal como en tu repo) ===
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# --- Directorios base desde .env (con defaults) ---
FILES_BASE_DIR = Path(os.getenv("FILES_BASE_DIR", "./shared_data")).resolve()
INBOX_DIR      = Path(os.getenv("INBOX_DIR", str(FILES_BASE_DIR / "inbox"))).resolve()
INDEX_DIR      = Path(os.getenv("INDEX_DIR", str(FILES_BASE_DIR / "index"))).resolve()
RAG_VERSION    = os.getenv("RAG_VERSION", "pc1-6@2025.11.09")

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
    """Intenta contar páginas con pdfplumber si está instalado; si falla, devuelve None."""
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
    return db.query(Case).filter(Case.user_id == user_id).order_by(desc(Case.id)).first()

# ---------- Vistas ----------
@router.get("/upload", response_class=HTMLResponse)
def show_upload_form(
    request: Request,
    processing_status: str = None,
    selected_case_id: int | None = None,  # 👈 permite llegar con ?selected_case_id=#
    db: Session = Depends(get_db)
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_name = getattr(user, "nombre", None)
    user_rol  = getattr(user, "rol", None)

    # 👉 Traer lista de cases del usuario (últimos primero)
    cases_list = (
        db.query(Case)
        .filter(Case.user_id == user_id)
        .order_by(Case.id.desc())
        .all()
    )

    # Determinar case seleccionado: parámetro > último case
    if selected_case_id:
        sel_case = next((c for c in cases_list if c.id == selected_case_id), None)
    else:
        sel_case = cases_list[0] if cases_list else None

    files = []
    base_folder: Path
    if sel_case and sel_case.input_dir:
        base_folder = Path(sel_case.input_dir)
    else:
        base_folder = INBOX_DIR / str(user_id)  # fallback histórico

    if base_folder.exists():
        for f in sorted(base_folder.rglob("*.pdf")):
            if f.is_file():
                # mostrar relativo al input_dir del case
                if sel_case and sel_case.input_dir and f.is_relative_to(base_folder):
                    files.append(str(f.relative_to(base_folder)))
                else:
                    files.append(f.name)

    return templates.TemplateResponse("upload.html", {
        "request": request,
        "files": files,
        "user_name": user_name,
        "user_rol": user_rol,
        "processing_status": processing_status,
        "cases_list": [{"id": c.id, "name": c.name, "status": c.status, "doc_count": c.doc_count} for c in cases_list],
        "selected_case_id": sel_case.id if sel_case else None,
    })


@router.post("/upload")
async def handle_upload(
    request: Request,
    file: UploadFile | None = File(None),
    files: List[UploadFile] | None = File(None),
    case_id: int | None = Form(None),       # 👈 nuevo
    case_name: str = Form("Caso Demo"),
    notes: str = Form("creado desde /upload"),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    upload_list = []
    if file is not None:
        upload_list.append(file)
    if files:
        upload_list.extend(files)
    upload_list = [f for f in upload_list if f is not None]

    if not upload_list:
        return JSONResponse({"error": "No se recibió ningún archivo."}, status_code=400)

    # 👉 Si viene case_id, lo usamos; si no, creamos uno nuevo
    if case_id:
        new_case = db.query(Case).filter(Case.id == case_id, Case.user_id == user_id).first()
        if not new_case:
            return JSONResponse({"error": "Case inválido o no pertenece al usuario."}, status_code=403)
        # aseguramos rutas
        input_dir = Path(new_case.input_dir) if new_case.input_dir else (INBOX_DIR / str(new_case.id) / "original")
        index_dir = Path(new_case.index_dir) if new_case.index_dir else (INDEX_DIR / str(new_case.id))
        _ensure_dir(input_dir)
        _ensure_dir(index_dir)
        new_case.input_dir = str(input_dir)
        new_case.index_dir = str(index_dir)
    else:
        new_case = Case(
            user_id=user_id,
            customer_id=None,
            name=case_name[:200],
            status="queued",
            input_dir="",
            index_dir="",
            rag_version=RAG_VERSION,
            doc_count=0,
            notes=notes,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(new_case)
        db.flush()
        input_dir = INBOX_DIR / str(new_case.id) / "original"
        index_dir = INDEX_DIR / str(new_case.id)
        _ensure_dir(input_dir)
        _ensure_dir(index_dir)
        new_case.input_dir = str(input_dir)
        new_case.index_dir = str(index_dir)

    # Guardar PDFs como antes
    added = 0
    for f in upload_list:
        filename = _safe_filename(Path(f.filename).name)
        if not filename.lower().endswith(".pdf"):
            db.rollback()
            return JSONResponse({"error": f"Solo PDF. Archivo inválido: {filename}"}, status_code=400)

        file_bytes = await f.read()
        stored_path = input_dir / filename
        with open(stored_path, "wb") as buffer:
            buffer.write(file_bytes)

        pages = _count_pdf_pages(file_bytes)
        mime_type = getattr(f, "content_type", None)
        size_bytes = len(file_bytes) if file_bytes is not None else None

        doc = Document(
            case_id=new_case.id,
            user_id=user_id,
            filename=filename,
            original_path=str(stored_path),
            stored_path=str(stored_path),
            mime_type=mime_type,
            size_bytes=size_bytes,
            pages=pages,
            status="uploaded",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(doc)
        added += 1

    new_case.doc_count = (new_case.doc_count or 0) + added
    new_case.touch()
    db.commit()

    # 👈 vuelve a /upload con el case seleccionado
    return RedirectResponse(
        url=f"/upload?selected_case_id={new_case.id}",
        status_code=status.HTTP_302_FOUND
    )

@router.post("/delete-file")
def delete_file(
    request: Request,
    filename: str = Form(...),
    case_id: int | None = Form(None),            # 👈 nuevo
    db: Session = Depends(get_db)
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # Prioriza el case recibido; si no, último case del usuario
    sel_case = None
    if case_id:
        sel_case = db.query(Case).filter(Case.id == case_id, Case.user_id == user_id).first()
    if not sel_case:
        sel_case = _latest_case_for_user(db, user_id)

    base_folder = Path(sel_case.input_dir) if (sel_case and sel_case.input_dir) else (INBOX_DIR / str(user_id))
    file_path = base_folder / filename
    if file_path.exists():
        file_path.unlink()

    if sel_case:
        doc = db.query(Document).filter(
            Document.case_id == sel_case.id,
            Document.filename == filename
        ).first()
        if doc:
            doc.status = "deleted"
            doc.touch()
            db.commit()

    # Mantener el case seleccionado en la vista
    redirect_url = "/upload"
    if sel_case:
        redirect_url += f"?selected_case_id={sel_case.id}"
    return RedirectResponse(url=redirect_url, status_code=302)


# ---- Procesamiento (placeholder: luego disparará LightRAG /index) ----
@router.post("/procesar")
def procesar_archivos(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    last_case = _latest_case_for_user(db, user_id)
    if not last_case:
        return RedirectResponse(url="/upload?processing_status=no_case", status_code=302)

    # Ejemplo futuro:
    # background_tasks.add_task(llamar_lightrag_index, last_case.input_dir, last_case.index_dir, last_case.id)

    last_case.status = "processing"
    last_case.touch()
    db.commit()
    return RedirectResponse(url="/upload?processing_status=in_progress", status_code=302)

# ---- Progreso (placeholder) ----
@router.get("/progress")
def consultar_progreso(request: Request):
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    progreso = {"percent": 0, "status": "pending"}
    return JSONResponse(content={"progress": progreso})

# ---- API extra: listar documentos por case (JSON) ----
@router.get("/cases/{case_id}/documents")
def list_documents_by_case(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db)
):
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    case_obj = db.query(Case).filter(Case.id == case_id).first()
    if not case_obj:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    if getattr(case_obj, "user_id", None) != user_id:
        raise HTTPException(status_code=403, detail="No autorizado para este Case")

    docs = db.query(Document).filter(Document.case_id == case_id).order_by(Document.id.asc()).all()
    return [
        {
            "id": d.id,
            "case_id": d.case_id,
            "filename": d.filename,
            "original_path": d.original_path,
            "stored_path": d.stored_path,
            "mime_type": d.mime_type,
            "size_bytes": d.size_bytes,
            "status": d.status,
            "pages": d.pages,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in docs
    ]
