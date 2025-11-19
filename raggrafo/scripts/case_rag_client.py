# raggrafo/scripts/case_rag_client.py
# Cliente RAG por caso para ser usado desde el chat del backend.
#
# NO vuelve a correr el pipeline PC1–PC7.
# Solo abre el work_dir de LightRAG para ese case_id y hace consultas.

from __future__ import annotations

import os
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

try:
    # LightRAG instalado vía pip / local
    from lightrag import LightRAG, QueryParam
except ImportError as exc:
    raise ImportError(
        "No se pudo importar 'lightrag'. "
        "Asegúrate de tenerlo instalado en el venv:  pip install lightrag"
    ) from exc


logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Paths base: asumimos que este archivo vive en raggrafo/scripts/
# y que rag_storage/ está en raggrafo/rag_storage
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAG_STORAGE = PROJECT_ROOT / "rag_storage"


def _get_work_dir_for_case(case_id: int) -> Path:
    """
    Devuelve la carpeta de trabajo (work_dir) de LightRAG para un caso dado.

    Por defecto:  raggrafo/rag_storage/case_<case_id>
    """
    base_dir = Path(
        os.getenv("RAGGRAFO_STORAGE_DIR", str(DEFAULT_RAG_STORAGE))
    ).expanduser()

    work_dir = base_dir / f"case_{case_id}"
    if not work_dir.exists():
        raise FileNotFoundError(
            f"No se encontró el work_dir de RAG para el caso {case_id}: {work_dir}"
        )
    return work_dir


# -------------------------------------------------------------------
# Caché de clientes por case_id (para no re-crear LightRAG cada query)
# -------------------------------------------------------------------
@lru_cache(maxsize=64)
def get_case_rag_client(case_id: int) -> LightRAG:
    """
    Carga (o recupera desde caché) un cliente LightRAG para un case_id.
    Usa el mismo work_dir que creó PC7 al hacer la ingesta.

    Lee configuración básica desde variables de entorno:
      - OPENAI_API_KEY
      - LLM_MODEL (opcional)
      - EMBEDDING_MODEL (opcional)
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY no está definido en el entorno. "
            "Exporta la clave antes de usar el chat RAG."
        )

    work_dir = _get_work_dir_for_case(case_id)

    # IMPORTANTE:
    # No pasamos llm_model ni embedding_model como keywords,
    # porque la versión instalada de LightRAG no los acepta.
    # Solo enviamos el work_dir como primer argumento posicional
    # y dejamos que LightRAG lea el resto desde el entorno / defaults.
    rag = LightRAG(str(work_dir))

    return rag


# -------------------------------------------------------------------
# Función sencilla para lanzar una consulta desde el chat
# -------------------------------------------------------------------
def query_case_rag(
    case_id: int,
    question: str,
    *,
    mode: str = "hybrid",
    top_k: int = 6,
) -> Dict[str, Any]:
    """
    Lanza una consulta RAG contra el caso dado.

    Devuelve SIEMPRE un dict con al menos:
      {
        "answer": <texto o cadena vacía>,
        "raw": <objeto original devuelto por LightRAG o dict con 'error'>
      }

    Si LightRAG falla o devuelve algo raro (None, "None", etc.), se normaliza a
    answer = "" para que el router pueda mostrar un mensaje de fallback.
    """
    question = (question or "").strip()
    if not question:
        raise ValueError("La pregunta está vacía.")

    rag = get_case_rag_client(case_id)

    param = QueryParam(
        mode=mode,
        top_k=top_k,
    )

    # ---- Ejecutar query con manejo de errores ----
    try:
        # Intento 1: usando QueryParam (API más nueva)
        raw = rag.query(question, param=param)
    except TypeError as e:
        # Muchas versiones viejas de LightRAG no aceptan param=QueryParam.
        # En ese caso reintentamos sin el parámetro para ser compatibles.
        logger.warning(
            "LightRAG.query falló con TypeError usando QueryParam; "
            "reintentando sin 'param'. Error: %s: %s",
            type(e).__name__,
            e,
        )
        try:
            raw = rag.query(question)
        except Exception as e2:
            logger.error(
                "RAG query failed even without QueryParam for case %s: %s: %s",
                case_id,
                type(e2).__name__,
                e2,
            )
            return {
                "answer": "",
                "raw": {"error": f"{type(e2).__name__}: {e2}"},
            }
    except Exception as e:
        # Cualquier otro error general
        logger.error(
            "RAG query failed for case %s: %s: %s",
            case_id,
            type(e).__name__,
            e,
        )
        return {
            "answer": "",
            "raw": {"error": f"{type(e).__name__}: {e}"},
        }

    # ---- Normalizar la respuesta a texto usable ----
    answer_text: str

    if raw is None:
        answer_text = ""
    elif isinstance(raw, str):
        if raw.strip().lower() == "none":
            answer_text = ""
        else:
            answer_text = raw
    elif isinstance(raw, dict):
        answer_text = (
            raw.get("answer")
            or raw.get("response")
            or ""
        )
        if isinstance(answer_text, str):
            if answer_text.strip().lower() == "none":
                answer_text = ""
        else:
            answer_text = ""
    else:
        text = str(raw)
        answer_text = "" if text.strip().lower() == "none" else text

    return {
        "answer": answer_text,
        "raw": raw,
    }


# -------------------------------------------------------------------
# Pequeño main opcional para probar desde consola:
#   python -m raggrafo.scripts.case_rag_client --case-id 20 "pregunta..."
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Consulta rápida al RAG de un caso."
    )
    parser.add_argument(
        "--case-id",
        type=int,
        required=True,
        help="ID del caso (case_id) ya procesado por PC1–PC7.",
    )
    parser.add_argument(
        "question",
        type=str,
        nargs="+",
        help="Pregunta para el asistente.",
    )

    args = parser.parse_args()
    q = " ".join(args.question)

    result = query_case_rag(args.case_id, q)
    print("\n=== RESPUESTA ===\n")
    print(result["answer"])
    print("\n=== RAW ===\n")
    print(result["raw"])
