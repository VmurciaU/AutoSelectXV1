# app/routers/chat_routes.py
from __future__ import annotations

from datetime import datetime
import json
import base64

from fastapi import (
    APIRouter,
    Request,
    Depends,
    Form,
    HTTPException,
)
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.utils.auth import get_current_user_id
from app.models.cases import Case

# Cliente RAG por caso
from raggrafo.scripts.case_rag_client import query_case_rag

router = APIRouter(tags=["assistant"])

templates = Jinja2Templates(directory="app/templates")

MAX_HISTORY = 50  # por si el chat crece mucho, recortamos un poco


# --- Dependencia local para obtener sesión de BD ---
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Helpers para serializar/deserializar historial en un campo hidden ---

def encode_history(history: list[dict]) -> str:
    """
    Serializa el historial a JSON y lo codifica en base64
    para que pueda viajar seguro en un <input hidden>.
    """
    try:
        raw = json.dumps(history, ensure_ascii=False)
        b64 = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
        return b64
    except Exception:
        return ""


def decode_history(encoded: str | None) -> list[dict]:
    """
    Decodifica el historial desde base64+JSON.
    Si algo falla, devuelve lista vacía.
    """
    if not encoded:
        return []
    try:
        raw = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
        history = json.loads(raw)
        if isinstance(history, list):
            return history
        return []
    except Exception:
        return []


def build_initial_history() -> list[dict]:
    """
    Mensaje inicial del asistente cuando se abre el chat por primera vez.
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return [
        {
            "role": "assistant",
            "content": (
                "🧪 Asistente técnico de AutoSelect-X.\n\n"
                "El motor RAG del caso ya puede responder preguntas basadas en los documentos "
                "procesados (pipeline PC1–PC7). Escribe, por ejemplo:\n"
                "«Resume los requisitos principales de caudal y presión que aparecen en la Hoja de Datos»."
            ),
            "timestamp": now_str,
        }
    ]


def clamp_history(history: list[dict]) -> list[dict]:
    """
    Limita el tamaño del historial para que el hidden no explote.
    """
    if len(history) > MAX_HISTORY:
        return history[-MAX_HISTORY:]
    return history


# ===============================
#   RUTA GET: ver asistente
# ===============================
@router.get(
    "/cases/{case_id}/assistant",
    response_class=HTMLResponse,
)
def get_case_assistant(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Muestra la pantalla del asistente del caso (chat + banner).
    """
    case = (
        db.query(Case)
        .filter(Case.id == case_id)
        .first()
    )
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    # TODO: validar que el caso pertenezca al usuario o que sea admin.

    # Historial inicial con un mensaje de bienvenida del asistente
    chat_messages: list[dict] = build_initial_history()
    history_serialized = encode_history(clamp_history(chat_messages))

    # Placeholder para secciones del banner (luego vendrán de la BD / RAG)
    requirements = []       # lista de requisitos técnicos detectados
    quote_items = []        # lista de ítems de bomba seleccionados
    selected_client = None  # cliente seleccionado
    selected_terms = None   # condiciones comerciales seleccionadas
    quote_notes = ""        # notas internas / de la cotización

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_messages,
        "chat_history_serialized": history_serialized,
        "requirements": requirements,
        "quote_items": quote_items,
        "selected_client": selected_client,
        "selected_terms": selected_terms,
        "quote_notes": quote_notes,
        "pipeline_summary": None,  # luego podremos traer estado PC1–PC7
    }
    return templates.TemplateResponse("case_assistant.html", context)


# ==========================================
#   RUTA POST: enviar mensaje al asistente
# ==========================================
@router.post(
    "/cases/{case_id}/assistant",
    response_class=HTMLResponse,
)
def post_case_assistant(
    request: Request,
    case_id: int,
    user_message: str = Form(...),
    history: str | None = Form(None),
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Recibe el mensaje del usuario, reconstruye el historial
    desde el hidden, agrega el mensaje del usuario y trata
    de consultar el RAG del caso.
    """
    case = (
        db.query(Case)
        .filter(Case.id == case_id)
        .first()
    )
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    # Reconstruir historial previo; si viene vacío, usar el mensaje inicial
    chat_history = decode_history(history)
    if not chat_history:
        chat_history = build_initial_history()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Agregamos mensaje del usuario
    clean_user_msg = (user_message or "").strip()
    if clean_user_msg:
        chat_history.append(
            {
                "role": "user",
                "content": clean_user_msg,
                "timestamp": now_str,
            }
        )

    # Intentar respuesta vía RAG del caso
    try:
        # OJO: usamos mode="core" porque así validaste el pipeline manualmente:
        # python -m raggrafo.pipelines.rag_case_query --case-id 14 --mode core --question "PREGUNTA" --raw
        rag_result = query_case_rag(
            case_id=case.id,
            question=clean_user_msg,
            mode="core",
            top_k=6,
        )
        rag_answer = (rag_result.get("answer") or "").strip()
        rag_raw = rag_result.get("raw") or {}

        if rag_answer:
            assistant_text = rag_answer
        else:
            rag_error = ""
            if isinstance(rag_raw, dict):
                rag_error = rag_raw.get("error", "")

            if rag_error:
                assistant_text = (
                    "⚠️ Ocurrió un problema al obtener la respuesta del asistente técnico "
                    "de este caso (el motor RAG devolvió una respuesta vacía).\n\n"
                    f"Detalle técnico (RAG): {rag_error}"
                )
            else:
                assistant_text = (
                    "⚠️ El motor RAG del caso no devolvió contenido útil para esta pregunta. "
                    "Revisa que el pipeline se haya ejecutado correctamente con '--use-core'."
                )

    except Exception as e:
        assistant_text = (
            "⚠️ Ocurrió un error al consultar el asistente técnico del caso.\n\n"
            f"Detalle técnico: {type(e).__name__}: {e}"
        )

    # Añadir mensaje del asistente al historial
    chat_history.append(
        {
            "role": "assistant",
            "content": assistant_text,
            "timestamp": now_str,
        }
    )

    # Limitar tamaño del historial y serializar para el hidden
    chat_history = clamp_history(chat_history)
    history_serialized = encode_history(chat_history)

    # Banner lateral (por ahora placeholders)
    requirements = []
    quote_items = []
    selected_client = None
    selected_terms = None
    quote_notes = ""

    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,
        "requirements": requirements,
        "quote_items": quote_items,
        "selected_client": selected_client,
        "selected_terms": selected_terms,
        "quote_notes": quote_notes,
        "pipeline_summary": None,
    }
    return templates.TemplateResponse("case_assistant.html", context)
