from fastapi import APIRouter, Request, UploadFile, File, Form, status, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from starlette.templating import Jinja2Templates
import os, shutil
from utils.auth import get_current_user_id
from database.conection import SessionLocal
from models.user import User
from scripts.pipeline_procesamiento_pdf import procesar_pdfs_por_usuario
from scripts.progress_tracker import get_progress, reset_progress

router = APIRouter()
templates = Jinja2Templates(directory="templates")

UPLOAD_BASE_DIR = "outputs/session_files"


# --- RENDER UPLOAD PAGE ---
@router.get("/upload", response_class=HTMLResponse)
def show_upload_form(request: Request, processing_status: str = None):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    db = SessionLocal()
    user = db.query(User).filter(User.id == user_id).first()
    user_name = user.nombre if user else None
    user_rol = user.rol if user else None
    db.close()

    user_folder = os.path.join(UPLOAD_BASE_DIR, str(user_id))
    files = [
        f for f in os.listdir(user_folder)
        if os.path.isfile(os.path.join(user_folder, f)) and f.lower().endswith(".pdf")
    ] if os.path.exists(user_folder) else []

    return templates.TemplateResponse("upload.html", {
        "request": request,
        "files": files,
        "user_name": user_name,
        "user_rol": user_rol,
        "processing_status": processing_status
    })


# --- CARGAR ARCHIVO ---
@router.post("/upload")
def handle_upload(request: Request, file: UploadFile = File(...)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user_folder = os.path.join(UPLOAD_BASE_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)

    file_path = os.path.join(user_folder, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return RedirectResponse(url="/upload", status_code=status.HTTP_302_FOUND)


# --- ELIMINAR ARCHIVO ---
@router.post("/delete-file")
def delete_file(request: Request, filename: str = Form(...)):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    user_folder = os.path.join(UPLOAD_BASE_DIR, str(user_id))
    file_path = os.path.join(user_folder, filename)

    if os.path.exists(file_path):
        os.remove(file_path)

    return RedirectResponse(url="/upload", status_code=302)


# --- PROCESAR ARCHIVOS ---
@router.post("/procesar")
def procesar_archivos(request: Request, background_tasks: BackgroundTasks):
    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)

    reset_progress(user_id)  # Reinicia el progreso

    # Ejecutar el procesamiento en segundo plano
    background_tasks.add_task(procesar_pdfs_por_usuario, user_id)

    # Redirige con la bandera que activa el seguimiento del progreso
    return RedirectResponse(url="/upload?processing_status=in_progress", status_code=302)


# --- PROGRESO PARA BARRA AJAX ---
@router.get("/progress")
def consultar_progreso():
    user_id = 3  # 🔧 Forzar manualmente
    print(f"🔧 Consulta manual de progreso para el usuario: {user_id}")
    progreso = get_progress(user_id)
    return JSONResponse(content={"progress": progreso})
