# app/routers/chat_routes.py
from __future__ import annotations

# ============================================================
#  IMPORTS
# ============================================================

from datetime import datetime
import json
import base64
import asyncio

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.utils.auth import get_current_user_id
from app.models.cases import Case

from app.models.user import User 


# Pipelines EXTRACT-LIST + NORMALIZE
from raggrafo.pipelines.extract_and_normalize import run_extract_and_normalize

# HTML builders (RAW + NORMALIZED)
from raggrafo.pipelines.printPumpsJson import build_all_messages

# Motor RAG (modo engineering)
from raggrafo.scripts.rag_service import run_rag_case

import os


def load_requirements_from_storage(case_id: int):
    """Carga las bombas normalizadas desde rag_storage si existen."""
    path = f"raggrafo/rag_storage/case_{case_id}/normalized.json"
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data.get("pumps", [])
    except:
        return []

# ============================================================
#  CONFIGURACIÓN BASE
# ============================================================

router = APIRouter(tags=["assistant"])   # IMPORTANTE — debe declararse antes que @router.get/post
templates = Jinja2Templates(directory="app/templates")
MAX_HISTORY = 50

# ============================================================
#  DB SESSION
# ============================================================

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============================================================
#  HISTORIAL: ENCODE / DECODE / CLAMP
# ============================================================

def encode_history(history: list[dict]) -> str:
    """Convierte el historial a Base64 seguro."""
    try:
        return base64.b64encode(json.dumps(history).encode()).decode()
    except Exception:
        return ""

def decode_history(encoded: str | None) -> list[dict]:
    """Convierte Base64 → lista. Si falla, retorna lista vacía."""
    if not encoded:
        return []
    try:
        raw = base64.b64decode(encoded.encode()).decode()
        return json.loads(raw)
    except Exception:
        return []

def build_initial_history() -> list[dict]:
    """Historial inicial vacío."""
    return []

def clamp_history(history: list[dict]) -> list[dict]:
    """Evita que el historial crezca indefinidamente."""
    if len(history) <= MAX_HISTORY:
        return history
    return history[-MAX_HISTORY:]

# ============================================================
#  GET — CARGA INICIAL DE LA PÁGINA DEL ASISTENTE
# ============================================================
@router.get(
    "/cases/{case_id}/assistant",
    response_class=HTMLResponse,
)
async def get_case_assistant(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Carga la página completa del asistente del caso.
    Aquí NO hay HTMX: se carga el layout completo.
    El JSON de bombas se lee desde disco (si existe).
    """
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    current_user = db.query(User).get(current_user_id)

    # Historial inicial vacío (el chat parcial HTMX lo modificará después)
    chat_history = build_initial_history()
    history_serialized = encode_history(chat_history)

    # Cargar bombas normalizadas previas (si hay JSON guardado)
    requirements = load_requirements_from_storage(case_id)

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,
        "requirements": requirements,
        "requirements_json": requirements,
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "pipeline_summary": None,
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,
    }

    return templates.TemplateResponse("case_assistant.html", context)



@router.get("/cases/{case_id}/chat-panel", response_class=HTMLResponse)
def load_chat_panel(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
):
    """Carga SOLO el panel de chat (HTMX)."""

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    # Cargar historial guardado (si existe)
    history = []

    # Renderizar solo el panel del chat
    return templates.TemplateResponse(
        "assistant/_chat_panel.html",
        {
            "request": request,
            "case_id": case_id,
            "history": history,
        },
    )



@router.post("/cases/{case_id}/send-message", response_class=HTMLResponse)
async def chat_send_message(
    request: Request,
    case_id: int,
    message: str = Form(...),
    history_encoded: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    Procesa un mensaje del usuario vía HTMX.
    No guarda nada en BD, solo renderiza nuevamente el panel del chat.
    """

    # 1. Decodificar historial temporal
    history = decode_history(history_encoded)
    if not history:
        history = []

    # 2. Añadir mensaje del usuario
    clean_user_msg = message.strip()
    if clean_user_msg:
        history.append({
            "role": "user",
            "content": clean_user_msg,
        })

    # 3. Ejecutar motor RAG (consulta normal)
    try:
        rag_result = await run_rag_case(
            case_id=case_id,
            question=clean_user_msg,
            mode="engineering",
        )
        rag_answer = rag_result.get("answer") or "⚠️ Respuesta vacía."
    except Exception as e:
        rag_answer = f"⚠️ Error procesando pregunta: {e}"

    # 4. Añadir respuesta del asistente
    history.append({
        "role": "assistant",
        "content": rag_answer,
    })

    # 5. Recortar historial si es necesario
    history = clamp_history(history)

    # 6. Codificar historial nuevamente (base64)
    history_serialized = encode_history(history)

    # 7. Retornar SOLO el panel del chat
    return templates.TemplateResponse(
        "assistant/_chat_panel.html",
        {
            "request": request,
            "case_id": case_id,
            "history": history,
            "chat_history_serialized": history_serialized,
        },
    )






# ============================================================
#  POST — PROCESA MENSAJES DEL ASISTENTE
# ============================================================
@router.post(
    "/cases/{case_id}/assistant",
    response_class=HTMLResponse,
)
async def post_case_assistant(
    request: Request,
    case_id: int,
    user_message: str = Form(""),
    history: str = Form(None),
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    POST clásico del asistente.
    Sigue refrescando todo el layout para:
       - actualizar el banner de bombas al ejecutar @buscar
       - mantener el sistema funcionando como hasta ahora
    Más adelante lo reemplazaremos gradualmente por HTMX.
    """

    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    current_user = db.query(User).get(current_user_id)

    # ================================================
    # 1) Reconstruir historial
    # ================================================
    chat_history = decode_history(history)
    if not chat_history:
        chat_history = build_initial_history()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    clean_user_msg = (user_message or "").strip()

    if clean_user_msg:
        chat_history.append({
            "role": "user",
            "content": clean_user_msg,
            "timestamp": now_str,
        })

    assistant_text = None
    requirements_list = None  # ← IMPORTANTE: permite la prioridad en context

    # ================================================
    # 2) Comando especial: @buscar_bombas_items
    # ================================================
    if clean_user_msg == "@buscar_bombas_items":
        try:
            pregunta = (
                "Extrae TODAS las bombas dosificadoras con caudal nominal, "
                "presión, viscosidad, turndown y TAG. Devuelve el JSON limpio."
            )

            task = run_extract_and_normalize(case_id, pregunta)
            if asyncio.iscoroutine(task) or isinstance(task, asyncio.Task):
                comando_result = await task
            else:
                comando_result = task

            raw_json = comando_result.get("raw", {})
            normalized_json = comando_result.get("normalized", {})

            # Guardar en almacenamiento del RAG
            path = f"raggrafo/rag_storage/case_{case_id}/normalized.json"
            with open(path, "w") as f:
                json.dump(normalized_json, f, indent=2)

            # Lista de bombas detectadas para el banner
            requirements_list = normalized_json.get("pumps", [])

            # Mensajes formateados para el chat
            msgs = build_all_messages(raw_json, normalized_json)
            for key in ["summary", "raw", "normalized"]:
                chat_history.append({
                    "role": "assistant",
                    "content": msgs[key],
                    "timestamp": now_str,
                })

            assistant_text = None  # ya agregamos mensajes

        except Exception as e:
            assistant_text = f"⚠️ Error ejecutando @buscar_bombas_items\n\n{e}"

    # ================================================
    # 3) Consulta normal RAG
    # ================================================
    else:
        if clean_user_msg:
            try:
                rag_result = await run_rag_case(
                    case_id=case.id,
                    question=clean_user_msg,
                    mode="engineering",
                )

                rag_answer = (rag_result.get("answer") or "").strip()
                rag_error = rag_result.get("error")

                if rag_answer:
                    assistant_text = rag_answer
                elif rag_error:
                    assistant_text = f"⚠️ Error en motor RAG: {rag_error}"
                else:
                    assistant_text = "⚠️ Respuesta vacía del motor RAG."

            except Exception as e:
                assistant_text = f"⚠️ Error ejecutando consulta: {e}"

    # ================================================
    # 4) Agregar respuesta del asistente
    # ================================================
    if assistant_text:
        chat_history.append({
            "role": "assistant",
            "content": assistant_text,
            "timestamp": now_str,
        })

    # ================================================
    # 5) Serializar e hidratar contexto completo
    # ================================================
    chat_history = clamp_history(chat_history)
    history_serialized = encode_history(chat_history)

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,

        # REGLA DE ORO (se mantiene intacta):
        # Si acabo de tener requirements_list → úsalo
        # si NO → lee el JSON
        "requirements": (
            requirements_list
            if requirements_list is not None
            else load_requirements_from_storage(case_id)
        ),

        "requirements_json": (
            requirements_list
            if requirements_list is not None
            else []
        ),

        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "pipeline_summary": None,
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,
    }

    return templates.TemplateResponse("case_assistant.html", context)
