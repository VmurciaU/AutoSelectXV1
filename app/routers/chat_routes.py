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
    history: str | None = None,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id)
):
    """Carga la interfaz del asistente con historial vacío o previo."""
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")
    
    current_user = db.query(User).get(current_user_id)

    chat_history = decode_history(history)
    if not chat_history:
        chat_history = build_initial_history()

    


    history_serialized = encode_history(chat_history)

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,
        "requirements": requirements_list if 'requirements_list' in locals() else [],
        "requirements_json": requirements_list if 'requirements_list' in locals() else [],
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "pipeline_summary": None,
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,
    }

    return templates.TemplateResponse("case_assistant.html", context)

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
    user_message: str = Form(...),
    history: str | None = Form(None),
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    # -----------------------------------
    # 1. Validar caso
    # -----------------------------------
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")
    current_user = db.query(User).get(current_user_id)


    # -----------------------------------
    # 2. Reconstruir historial
    # -----------------------------------
    chat_history = decode_history(history)
    if not chat_history:
        chat_history = build_initial_history()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    clean_user_msg = (user_message or "").strip()

    # Evitar mensajes vacíos
    if not clean_user_msg:
        assistant_text = "⚠️ Mensaje vacío. Escribe una pregunta o usa @buscar_bombas_items."
    else:
        # -----------------------------------
        # 3. Agregar mensaje del usuario al historial
        # -----------------------------------
        chat_history.append({
            "role": "user",
            "content": clean_user_msg,
            "timestamp": now_str,
        })

        assistant_text = None

        # ============================================================
        # 4. Comando especial: @buscar_bombas_items
        # ============================================================
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

                requirements_list = normalized_json.get("pumps", [])

                msgs = build_all_messages(raw_json, normalized_json)

                for key in ["summary", "raw", "normalized"]:
                    chat_history.append({
                        "role": "assistant",
                        "content": msgs[key],
                        "timestamp": now_str,
                    })

                assistant_text = None

            except Exception as e:
                assistant_text = (
                    f"⚠️ Error ejecutando @buscar_bombas_items\n\n"
                    f"Detalle técnico: {type(e).__name__}: {e}"
                )

        # ============================================================
        # 5. Consulta normal al motor RAG (modo engineering)
        # ============================================================
        else:
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
                    assistant_text = (
                        "⚠️ Error al consultar el motor RAG.\n\n"
                        f"Detalle: {rag_error}"
                    )
                else:
                    assistant_text = (
                        "⚠️ Respuesta vacía del motor RAG.\n"
                        "Verifica que el pipeline PC1–PC7 esté ejecutado."
                    )

            except Exception as e:
                assistant_text = (
                    "⚠️ Error al procesar la consulta.\n\n"
                    f"Detalle: {type(e).__name__}: {e}"
                )

    # ============================================================
    # 6. Agregar respuesta del asistente (si existe)
    # ============================================================
    if assistant_text:
        chat_history.append({
            "role": "assistant",
            "content": assistant_text,
            "timestamp": now_str,
        })

    # ============================================================
    # 7. Serializar historial y renderizar template
    # ============================================================
    chat_history = clamp_history(chat_history)
    history_serialized = encode_history(chat_history)

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,
        "requirements": requirements_list if 'requirements_list' in locals() else [],
        "requirements_json": requirements_list if 'requirements_list' in locals() else [],
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "pipeline_summary": None,
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,   
    }

    return templates.TemplateResponse("case_assistant.html", context)
