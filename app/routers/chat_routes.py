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

from raggrafo.pipelines.extract_and_normalize import run_extract_and_normalize

from raggrafo.pipelines.printPumpsJson import build_all_messages


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
    Recibe el mensaje del usuario, reconstruye el historial,
    agrega el mensaje del usuario y consulta el RAG del caso.
    """
    case = (
        db.query(Case)
        .filter(Case.id == case_id)
        .first()
    )
    if not case:
        raise HTTPException(status_code=404, detail="Caso no encontrado")

    # --- Reconstruir historial previo ---
    chat_history = decode_history(history)
    if not chat_history:
        chat_history = build_initial_history()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # --- Agregar mensaje del usuario ---
    clean_user_msg = (user_message or "").strip()
    if clean_user_msg:
        chat_history.append(
            {
                "role": "user",
                "content": clean_user_msg,
                "timestamp": now_str,
            }
        )
    # ======================================================
    #   Resolver comandos o ejecutar RAG normal
    # ======================================================
    try:

        # --- COMANDOS ESPECIALES ---
        if clean_user_msg.startswith("@"):

            # ==========================================
            # NUEVO: @buscar_bombas_items
            # ==========================================
            if clean_user_msg == "@buscar_bombas_items":
                pregunta = (
                    "Extrae TODAS las bombas dosificadoras con caudal nominal, presión, "
                    "viscosidad, turndown y TAG. Devuelve el JSON limpio."
                )

                # Ejecuta extract + normalize
                comando_result = run_extract_and_normalize(case_id, pregunta)

                # Obtiene RAW y NORMALIZED
                raw_json = comando_result.get("raw", {})
                normalized_json = comando_result.get("normalized", {})

                # Construye los 3 mensajes en HTML
                from raggrafo.pipelines.printPumpsJson import build_all_messages
                msgs = build_all_messages(raw_json, normalized_json)

                # Agregamos 3 mensajes del asistente al historial
                chat_history.append({
                    "role": "assistant",
                    "content": msgs["summary"],
                    "timestamp": now_str,
                })

                chat_history.append({
                    "role": "assistant",
                    "content": msgs["raw"],
                    "timestamp": now_str,
                })

                chat_history.append({
                    "role": "assistant",
                    "content": msgs["normalized"],
                    "timestamp": now_str,
                })

                # Saltar el flujo normal para NO duplicar mensajes
                assistant_text = None

            # ==========================================
            # @extraer_requisitos_item_1  (igual que antes)
            # ==========================================
            elif clean_user_msg == "@extraer_requisitos_item_1":
                pregunta = (
                    "Extrae una tabla con Tag, caudal nominal, presión de descarga, "
                    "viscosidad y número de bombas para este caso."
                )
                comando_result = run_extract_and_normalize(case_id, pregunta)
                assistant_text = json.dumps(comando_result, indent=2, ensure_ascii=False)

            # ==========================================
            # @resumen_hd  (igual que antes)
            # ==========================================
            elif clean_user_msg == "@resumen_hd":
                pregunta = "Resume los requisitos principales de caudal y presión de la Hoja de Datos."
                comando_result = run_extract_and_normalize(case_id, pregunta)
                assistant_text = json.dumps(comando_result, indent=2, ensure_ascii=False)

            else:
                assistant_text = (
                    "⚠️ Comando no reconocido. Intenta con:\n\n"
                    "- @buscar_bombas_items\n"
                    "- @extraer_requisitos_item_1\n"
                    "- @resumen_hd\n"
                )

        # ==================================================
        #   MODO NORMAL (flujo RAG estándar)
        # ==================================================
        else:
            rag_result = query_case_rag(
                case_id=case.id,
                question=clean_user_msg,
                mode="engineering",
                top_k=6,
            )

            rag_answer = (rag_result.get("answer") or "").strip()
            rag_raw = rag_result.get("raw") or {}

            if rag_answer:
                assistant_text = rag_answer

            else:
                rag_error = rag_raw.get("error", "") if isinstance(rag_raw, dict) else ""
                if rag_error:
                    assistant_text = (
                        "⚠️ Error al obtener respuesta del asistente.\n\n"
                        f"Detalle técnico (RAG): {rag_error}"
                    )
                else:
                    assistant_text = (
                        "⚠️ El motor RAG devolvió una respuesta vacía. "
                        "Verifica que el pipeline PC1–PC7 esté ejecutado."
                    )

    except Exception as e:
        assistant_text = (
            "⚠️ Error al consultar el asistente.\n\n"
            f"Detalle técnico: {type(e).__name__}: {e}"
        )

    # ======================================================
    # Agregar mensaje del asistente SOLO si hay texto
    # (para @buscar_bombas_items ya agregamos los 3 HTML)
    # ======================================================
    if assistant_text:
        chat_history.append(
            {
                "role": "assistant",
                "content": assistant_text,
                "timestamp": now_str,
            }
        )

    # --- Limitar historial y serializar ---
    chat_history = clamp_history(chat_history)
    history_serialized = encode_history(chat_history)

    # Banner lateral
    context = {
        "request": request,
        "case": case,
        "chat_messages": chat_history,
        "chat_history_serialized": history_serialized,
        "requirements": [],
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "pipeline_summary": None,
    }

    return templates.TemplateResponse("case_assistant.html", context)
