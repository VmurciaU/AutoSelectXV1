# app/routers/upload_routes.py
from __future__ import annotations

from fastapi import (
    APIRouter,
    Request,
    UploadFile,
    File,
    Form,
    status,
    BackgroundTasks,
    Depends,
    HTTPException,
)
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import desc
import os
import shutil
import io
import sys
import subprocess
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from app.utils.auth import get_current_user_id
from app.database.conection import SessionLocal

# === MODELOS ===
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

logger = logging.getLogger(__name__)

# --- Directorios base ---
FILES_BASE_DIR = Path(os.getenv("FILES_BASE_DIR", "./shared_data")).resolve()
INBOX_DIR = Path(os.getenv("INBOX_DIR", str(FILES_BASE_DIR / "inbox"))).resolve()
INDEX_DIR = Path(os.getenv("INDEX_DIR", str(FILES_BASE_DIR / "index"))).resolve()
RAG_VERSION = os.getenv("RAG_VERSION", "pc1-6@2025.11.09")


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
    try:
        import pdfplumber
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


def _sync_documents_for_case(
    db: Session,
    case: Case,
    base_folder: Path,
    user_id: int,
) -> None:
    """Sincroniza Document en DB con PDFs físicos."""
    from app.models.documents import Document

    if not base_folder.exists():
        return

    pdf_files = []
    for f in base_folder.rglob("*.pdf"):
        if f.is_file():
            pdf_files.append(f)
    file_names = {f.name for f in pdf_files}

    existing_docs = db.query(Document).filter(Document.case_id == case.id).all()
    docs_by_filename = {d.filename: d for d in existing_docs}

    for f in pdf_files:
        if f.name not in docs_by_filename:
            new_doc = Document(
                case_id=case.id,
                user_id=user_id,
                filename=f.name,
                original_path=str(f),
                stored_path=str(f),
                mime_type="application/pdf",
                size_bytes=f.stat().st_size,
                pages=None,
                status="uploaded",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(new_doc)

    for d in existing_docs:
        if d.filename not in file_names and d.status != "deleted":
            d.status = "deleted"
            if hasattr(d, "touch"):
                d.touch()
            else:
                d.updated_at = datetime.utcnow()

    db.commit()


# ============================================================
#        ** NUEVO **  MANEJO REAL DE ERROR + error.json
# ============================================================
def _write_error_file(case: Case, message: str):
    """
    Crea un archivo error.json dentro del index_dir del caso.
    """
    try:
        index_path = Path(case.index_dir) if case.index_dir else None
        if not index_path:
            return

        _ensure_dir(index_path)

        error_file = index_path / "error.json"
        data = {"error_message": message}

        with open(error_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    except Exception as ex:
        logger.exception("No se pudo escribir error.json para case_id=%s: %s", case.id, ex)


def _read_error_file(case: Case) -> Optional[str]:
    """
    Intenta leer error.json si existe; retorna mensaje.
    """
    try:
        index_path = Path(case.index_dir)
        error_file = index_path / "error.json"
        if error_file.exists():
            with open(error_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("error_message")
    except Exception:
        return None

    return None


# ============================================================
#                      PIPELINE REAL
# ============================================================
def _run_case_pipeline(case_id: int) -> None:
    """
    Ejecuta el pipeline raggrafo.
    Si falla: 
        - cambia estado a 'error'
        - escribe error.json
    """
    cmd = [
        sys.executable,
        "-m",
        "raggrafo.scripts.run_pipeline_for_case",
        "--case-id",
        str(case_id),
        "--use-core",
    ]

    logger.info("Iniciando pipeline para case_id=%s: %s", case_id, " ".join(cmd))

    try:
        subprocess.run(cmd, check=True)

        # Pipeline OK
        db = SessionLocal()
        try:
            case = db.query(Case).filter(Case.id == case_id).first()
            if case:
                case.status = "done"
                case.touch()
                db.commit()
        finally:
            db.close()

        logger.info("Pipeline completado correctamente para case_id=%s", case_id)

    except Exception as e:
        # ===========
        #    ERROR
        # ===========
        logger.exception("Pipeline error para case_id=%s: %s", case_id, e)

        db = SessionLocal()
        try:
            case = db.query(Case).filter(Case.id == case_id).first()
            if case:
                case.status = "error"

                # Mensaje genérico seguro
                short_msg = "El procesamiento falló. Verifica tu configuración o tus documentos."

                # Guardar en el campo notes
                case.notes = (case.notes or "") + f"\n[Error] {short_msg}"
                case.touch()
                db.commit()

                # NUEVO: escribir error.json
                _write_error_file(case, short_msg)

        finally:
            db.close()


# ============================================================
#                PAGINA /upload
# ============================================================
@router.get("/upload", response_class=HTMLResponse)
def show_upload_form(
    request: Request,
    processing_status: str | None = None,
    case_id: int | None = None,
    selected_case_id: int | None = None,
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    if case_id is not None and selected_case_id is None:
        selected_case_id = case_id

    user = db.query(User).filter(User.id == user_id).first()
    user_rol = getattr(user, "rol", None)
    is_admin = (user_rol or "").lower() == "admin"

    if is_admin:
        cases_list_db = db.query(Case).order_by(Case.id.desc()).all()
    else:
        cases_list_db = (
            db.query(Case)
            .filter(Case.user_id == user_id)
            .order_by(Case.id.desc())
            .all()
        )

    sel_case = None

    if selected_case_id:
        sel_case = db.query(Case).filter(Case.id == selected_case_id).first()
        if sel_case and (not is_admin) and (sel_case.user_id != user_id):
            sel_case = None

    if not sel_case and cases_list_db:
        sel_case = cases_list_db[0]

    files: list[str] = []

    if sel_case:
        if sel_case.input_dir:
            base_folder = Path(sel_case.input_dir)
        else:
            base_folder = INBOX_DIR / str(sel_case.id) / "original"
            _ensure_dir(base_folder)
            sel_case.input_dir = str(base_folder)
            db.commit()

        _sync_documents_for_case(db, sel_case, base_folder, user_id)

        if base_folder.exists():
            for f in sorted(base_folder.rglob("*.pdf")):
                if f.is_file():
                    files.append(str(f.name))
    else:
        files = []

    cases_list = [
        {
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "doc_count": c.doc_count,
        }
        for c in cases_list_db
    ]

    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "files": files,
            "user_name": getattr(user, "nombre", None),
            "user_rol": user_rol,
            "processing_status": processing_status,
            "cases_list": cases_list,
            "selected_case_id": sel_case.id if sel_case else None,
        },
    )


# ============================================================
#              SUBIR PDF
# ============================================================
@router.post("/upload")
async def handle_upload(
    request: Request,
    file: UploadFile | None = File(None),
    files: List[UploadFile] | None = File(None),
    case_id: int | None = Form(None),
    case_name: str = Form("Caso Demo"),
    notes: str = Form("creado desde /upload"),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_rol = getattr(user, "rol", None)

    upload_list: list[UploadFile] = []
    if file: upload_list.append(file)
    if files: upload_list.extend(files)
    upload_list = [f for f in upload_list if f]

    if not upload_list:
        return JSONResponse({"error": "No se recibió archivo."}, status_code=400)

    # Determinar case
    if case_id is not None:
        new_case = db.query(Case).filter(Case.id == case_id).first()
        if not new_case:
            return JSONResponse({"error": "Case no encontrado."}, status_code=404)

        if new_case.user_id != user_id and user_rol != "admin":
            return JSONResponse({"error": "No autorizado."}, status_code=403)

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

    # Guardar PDFs
    added = 0
    for f in upload_list:
        filename = _safe_filename(Path(f.filename).name)
        if not filename.lower().endswith(".pdf"):
            db.rollback()
            return JSONResponse({"error": f"Solo PDF: {filename}"}, status_code=400)

        file_bytes = await f.read()
        stored_path = Path(new_case.input_dir) / filename

        with open(stored_path, "wb") as bf:
            bf.write(file_bytes)

        pages = _count_pdf_pages(file_bytes)
        mime_type = getattr(f, "content_type", None)
        size_bytes = len(file_bytes)

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

    return RedirectResponse(
        url=f"/upload?case_id={new_case.id}",
        status_code=302,
    )


# ============================================================
#               BORRAR ARCHIVO
# ============================================================
@router.post("/delete-file")
def delete_file(
    request: Request,
    filename: str = Form(...),
    case_id: int | None = Form(None),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_rol = getattr(user, "rol", None)

    sel_case = None
    if case_id is not None:
        sel_case = db.query(Case).filter(Case.id == case_id).first()
        if sel_case and sel_case.user_id != user_id and user_rol != "admin":
            return JSONResponse({"error": "No autorizado."}, status_code=403)

    if not sel_case:
        sel_case = _latest_case_for_user(db, user_id)

    if sel_case and sel_case.input_dir:
        base_folder = Path(sel_case.input_dir)
    else:
        base_folder = INBOX_DIR / str(user_id)

    file_path = base_folder / filename
    if file_path.exists():
        file_path.unlink()

    if sel_case:
        doc = (
            db.query(Document)
            .filter(
                Document.case_id == sel_case.id,
                Document.filename == filename,
            )
            .first()
        )
        if doc:
            doc.status = "deleted"
            doc.touch()
            db.commit()

    redirect_url = "/upload"
    if sel_case:
        redirect_url += f"?case_id={sel_case.id}"
    return RedirectResponse(redirect_url, status_code=302)


# ============================================================
#               PROCESAR /procesar
# ============================================================
@router.post("/procesar")
def procesar_archivos(
    request: Request,
    background_tasks: BackgroundTasks,
    case_id: int | None = Form(None),
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_rol = getattr(user, "rol", None)

    target_case = None
    if case_id is not None:
        target_case = db.query(Case).filter(Case.id == case_id).first()
        if not target_case:
            return RedirectResponse(url="/upload?processing_status=no_case", status_code=302)
        if target_case.user_id != user_id and user_rol != "admin":
            return RedirectResponse(url="/upload?processing_status=forbidden", status_code=302)
    else:
        target_case = _latest_case_for_user(db, user_id)
        if not target_case:
            return RedirectResponse(url="/upload?processing_status=no_case", status_code=302)

    docs_q = (
        db.query(Document)
        .filter(
            Document.case_id == target_case.id,
            Document.status != "deleted",
        )
    )
    if docs_q.count() == 0:
        return RedirectResponse(url=f"/upload?case_id={target_case.id}&processing_status=no_docs", status_code=302)

    target_case.status = "indexing"
    if hasattr(target_case, "processing_started_at"):
        target_case.processing_started_at = datetime.utcnow()
    target_case.touch()
    db.commit()

    background_tasks.add_task(_run_case_pipeline, target_case.id)

    return RedirectResponse(
        url=f"/upload?case_id={target_case.id}&processing_status=in_progress",
        status_code=302,
    )


# ============================================================
#               PROGRESS /progress
# ============================================================
@router.get("/progress")
def consultar_progreso(
    request: Request,
    case_id: int | None = None,
    db: Session = Depends(get_db),
):
    """
    Retorna:
    {
      "case_id": ...,
      "status": ...,
      "progress": 0-100,
      "error_message": "..."   (solo si status=error)
    }
    """
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    user = db.query(User).filter(User.id == user_id).first()
    user_rol = getattr(user, "rol", None)

    # --- localizar case ---
    if case_id is not None:
        case_obj = db.query(Case).filter(Case.id == case_id).first()
        if not case_obj:
            return JSONResponse({"error": "Case no encontrado"}, status_code=404)
        if case_obj.user_id != user_id and user_rol != "admin":
            return JSONResponse({"error": "No autorizado"}, status_code=403)
    else:
        case_obj = _latest_case_for_user(db, user_id)
        if not case_obj:
            return JSONResponse({"error": "Sin casos"}, status_code=404)

    status_val = case_obj.status or ""

    # ---------------------------------------------------
    # 1) Comprobar si hay error.json (fuente de verdad)
    # ---------------------------------------------------
    err_msg = _read_error_file(case_obj)

    if status_val == "error" or err_msg:
        # Si por alguna razón el status sigue en 'indexing' pero
        # ya existe error.json, lo corregimos aquí.
        if status_val != "error":
            case_obj.status = "error"
            case_obj.touch()
            try:
                db.commit()
            except Exception:
                db.rollback()

        return {
            "case_id": case_obj.id,
            "status": "error",
            "progress": 100,
            "error_message": err_msg or "Ocurrió un problema al procesar los documentos.",
        }

    # ---------------------------------------------------
    # 2) Caso normal: progreso simbólico
    # ---------------------------------------------------
    progress = 0

    if status_val in ("done", "quoted", "archived"):
        progress = 100

    elif status_val in ("indexing", "queued"):
        start_ts = getattr(case_obj, "processing_started_at", None)
        if not start_ts:
            start_ts = getattr(case_obj, "updated_at", None) or getattr(case_obj, "created_at", None)

        if start_ts:
            elapsed = (datetime.utcnow() - start_ts).total_seconds()
            total = 180.0  # tiempo estimado
            raw = (elapsed / total) * 100.0
            progress = int(max(5, min(raw, 95)))
        else:
            progress = 5

    return {
        "case_id": case_obj.id,
        "status": status_val,
        "progress": progress,
    }


# ============================================================
#               LISTAR DOCUMENTOS
# ============================================================
@router.get("/cases/{case_id}/documents")
def list_documents_by_case(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
):
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    case_obj = db.query(Case).filter(Case.id == case_id).first()
    if not case_obj:
        raise HTTPException(status_code=404, detail="Case no encontrado")

    if case_obj.user_id != user_id:
        raise HTTPException(status_code=403, detail="No autorizado")

    docs = (
        db.query(Document)
        .filter(Document.case_id == case_id)
        .order_by(Document.id.asc())
        .all()
    )
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
