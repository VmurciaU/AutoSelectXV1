# app/routers/upload_routes.py
from fastapi import APIRouter, Request, UploadFile, File, Form, status, BackgroundTasks, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
import os, shutil
from pathlib import Path

from app.utils.auth import get_current_user_id
from app.database.conection import SessionLocal
from app.models.user import User

# Si más adelante quieres reusar el pipeline local, importas aquí:
# from lightrag.pipelines.pipeline_procesamiento_pdf import procesar_pdfs_por_usuario
# from lightrag.pipelines.progress_tracker import get_progress, reset_progress

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Directorios base desde .env (con defaults)
FILES_BASE_DIR = Path(os.getenv("FILES_BASE_DIR", "./shared_data")).resolve()
INBOX_DIR      = Path(os.getenv("INBOX_DIR", str(FILES_BASE_DIR / "inbox"))).resolve()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/upload", response_class=HTMLResponse)
def show_upload_form(request: Request, processing_status: str = None, db: Session = Depends(get_db)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user = db.query(User).filter(User.id == user_id).first()
    user_name = user.nombre if user else None
    user_rol  = user.rol if user else None

    user_folder = INBOX_DIR / str(user_id)
    files = []
    if user_folder.exists():
        for f in sorted(user_folder.iterdir()):
            if f.is_file() and f.suffix.lower() == ".pdf":
                files.append(f.name)

    return templates.TemplateResponse("upload.html", {
        "request": request,
        "files": files,
        "user_name": user_name,
        "user_rol": user_rol,
        "processing_status": processing_status
    })

@router.post("/upload")
def handle_upload(request: Request, file: UploadFile = File(...)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # Validar extensión
    filename = Path(file.filename).name
    if not filename.lower().endswith(".pdf"):
        return JSONResponse({"error": "Solo se permiten archivos PDF."}, status_code=400)

    user_folder = INBOX_DIR / str(user_id)
    user_folder.mkdir(parents=True, exist_ok=True)

    file_path = user_folder / filename
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return RedirectResponse(url="/upload", status_code=status.HTTP_302_FOUND)

@router.post("/delete-file")
def delete_file(request: Request, filename: str = Form(...)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user_folder = INBOX_DIR / str(user_id)
    file_path = user_folder / filename

    if file_path.exists():
        file_path.unlink()

    return RedirectResponse(url="/upload", status_code=302)

# ---- Procesamiento (placeholder) ----
# Más adelante disparará LightRAG vía HTTP: POST /index
@router.post("/procesar")
def procesar_archivos(request: Request, background_tasks: BackgroundTasks):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    # Placeholder: aquí luego llamaremos a LightRAG /index
    # background_tasks.add_task(llamar_lightrag_index, input_dir, index_dir)

    return RedirectResponse(url="/upload?processing_status=in_progress", status_code=302)

# ---- Progreso (placeholder) ----
@router.get("/progress")
def consultar_progreso(request: Request):
    user_id = get_current_user_id(request)
    if not user_id:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    # Placeholder: cuando tengamos tracker real
    progreso = {"percent": 0, "status": "pending"}
    return JSONResponse(content={"progress": progreso})
