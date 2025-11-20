# raggrafo/scripts/case_rag_client.py
# Cliente RAG por caso para ser usado desde el chat del backend.
#
# IMPORTANTE:
# - NO corre el pipeline PC1–PC7.
# - Solo ejecuta el mismo comando que usas manualmente:
#
#   python -m raggrafo.pipelines.rag_case_query \
#       --case-id 14 \
#       --mode core \
#       --question "PREGUNTA" \
#       --raw
#
# De esta forma, el chat reutiliza EXACTAMENTE el mismo flujo ya probado.

from __future__ import annotations

import logging
import subprocess
import sys
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _build_cmd(case_id: int, question: str, mode: str) -> list[str]:
    """
    Construye el comando para llamar al script raggrafo.pipelines.rag_case_query
    usando el mismo patrón que validaste manualmente.
    """
    return [
        sys.executable,
        "-m",
        "raggrafo.pipelines.rag_case_query",
        "--case-id",
        str(case_id),
        "--mode",
        mode,
        "--question",
        question,
        "--raw",
    ]


def query_case_rag(
    case_id: int,
    question: str,
    *,
    mode: str = "core",
    top_k: int = 6,  # por compatibilidad de firma, aunque no lo usamos aquí
) -> Dict[str, Any]:
    """
    Lanza una consulta RAG contra el caso dado llamando al script
    raggrafo.pipelines.rag_case_query vía subprocess.

    Devuelve SIEMPRE un dict con al menos:
      {
        "answer": <texto o cadena vacía>,
        "raw": {
            "stdout": <salida estándar>,
            "stderr": <error estándar>,
            "returncode": <código de salida>,
            "error": <mensaje de error si hubo fallo, opcional>
        }
      }
    """
    question = (question or "").strip()
    if not question:
        raise ValueError("La pregunta está vacía.")

    cmd = _build_cmd(case_id=case_id, question=question, mode=mode)

    logger.info("Ejecutando RAG vía subprocess: %s", " ".join(cmd))

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        logger.error(
            "Error ejecutando raggrafo.pipelines.rag_case_query para case_id=%s: %s: %s",
            case_id,
            type(e).__name__,
            e,
        )
        return {
            "answer": "",
            "raw": {
                "stdout": "",
                "stderr": "",
                "returncode": -1,
                "error": f"{type(e).__name__}: {e}",
            },
        }

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    rc = proc.returncode

    raw_dict: Dict[str, Any] = {
        "stdout": stdout,
        "stderr": stderr,
        "returncode": rc,
    }

    if rc != 0:
        # El script falló; devolvemos error para que el chat muestre fallback.
        msg = (
            f"rag_case_query salió con código {rc}. "
            f"STDERR: {stderr.strip()}"
        )
        logger.error(
            "rag_case_query failed for case_id=%s, mode=%s: %s",
            case_id,
            mode,
            msg,
        )
        raw_dict["error"] = msg
        return {
            "answer": "",
            "raw": raw_dict,
        }

    # Si el script devuelve 0, asumimos que stdout es la respuesta "raw"
    answer_text = stdout.strip()
    if answer_text.lower() == "none":
        answer_text = ""

    return {
        "answer": answer_text,
        "raw": raw_dict,
    }


# -------------------------------------------------------------------
# Pequeño main opcional para probar desde consola:
#   python -m raggrafo.scripts.case_rag_client --case-id 20 "pregunta..."
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Consulta rápida al RAG de un caso usando rag_case_query."
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
