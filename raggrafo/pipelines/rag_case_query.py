# raggrafo/pipelines/rag_case_query.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)

# QueryParam es la forma “oficial” de elegir el modo de búsqueda:
# naive | local | global | hybrid
try:
    from lightrag import QueryParam  # type: ignore[attr-defined]
except Exception:  # por si en el futuro cambian el nombre
    QueryParam = None  # type: ignore[assignment]

CaseId = Union[int, str]


def load_rag_for_case(case_id: CaseId) -> Tuple[Any, Path]:
    """
    Carga una instancia de LightRAG para un caso específico,
    usando el storage construido por PC6/PC7 en:

        rag_storage/case_<case_id>/

    Retorna (rag_instance, storage_dir).
    """
    case_id_str = str(case_id)
    storage_dir = (RAG_STORAGE_DIR / f"case_{case_id_str}").resolve()

    if not storage_dir.exists():
        raise FileNotFoundError(
            f"[load_rag_for_case] No existe el storage del caso {case_id_str}: {storage_dir} "
            f"(¿ya corriste PC7 para este caso?)."
        )

    rag = _make_rag(storage_dir)

    # Intentamos inicializar el Core (aunque hoy puede ser no-op, mantenemos el flujo)
    try:
        if inspect.iscoroutinefunction(_core_initialize):
            try:
                asyncio.run(_core_initialize(rag))
            except RuntimeError:
                # Si ya hay un event loop corriendo (ej. dentro de FastAPI),
                # usamos el loop actual.
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(_core_initialize(rag))
                else:
                    loop.run_until_complete(_core_initialize(rag))
        else:
            # Versión síncrona de _core_initialize
            _core_initialize(rag)
    except Exception as e:
        print(f"[load_rag_for_case] Aviso: no se pudo inicializar Core correctamente: {e}")

    return rag, storage_dir


def _build_query_param(mode: str) -> Optional[Any]:
    """
    Construye un QueryParam(mode=...) compatible con la versión de LightRAG.

    - Si QueryParam no existe, devuelve None.
    - Si mode == "core", lo mapeamos a "hybrid" (alias lógico).
    """
    if QueryParam is None:
        return None

    # Normalizamos el modo
    normalized = mode.strip().lower() if mode else "hybrid"
    if normalized == "core":
        normalized = "hybrid"

    # Modos típicos soportados por LightRAG: naive | local | global | hybrid
    try:
        return QueryParam(mode=normalized)
    except Exception:
        # Si por alguna razón falla (cambio de API), no forzamos el uso
        return None


def ask_rag(
    case_id: CaseId,
    question: str,
    mode: str = "core",
    history: Optional[list[dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Realiza una pregunta al RAG del caso dado.

    - Usa load_rag_for_case(case_id) para obtener la instancia.
    - Intenta usar los métodos estándar de LightRAG:
        - query(..., param=QueryParam(mode="hybrid"|"local"|...))
        - aquery(...)
        - chat(..., history=...)
    - Devuelve un dict normalizado con:
        - case_id
        - question
        - mode        (modo solicitado)
        - answer      (texto "plano" extraído de la respuesta)
        - raw         (respuesta completa tal como la devolvió LightRAG)
        - storage_dir
    """
    rag, storage_dir = load_rag_for_case(case_id)

    answer_raw: Any = None
    param = _build_query_param(mode)

    try:
        # 1) Preferencia: API oficial .query con QueryParam
        if hasattr(rag, "query") and callable(getattr(rag, "query")):
            if param is not None:
                try:
                    answer_raw = rag.query(
                        question,
                        param=param,
                    )
                except TypeError:
                    # Por si la versión de LightRAG no acepta param=
                    answer_raw = rag.query(question)
            else:
                # Sin QueryParam disponible: uso mínimo
                answer_raw = rag.query(question)

        # 2) Fallback: versión asíncrona aquery()
        elif hasattr(rag, "aquery") and callable(getattr(rag, "aquery")):
            coro = rag.aquery(question, param=param) if param is not None else rag.aquery(question)
            answer_raw = asyncio.run(coro)

        # 3) Fallback: interfaz tipo chat()
        elif hasattr(rag, "chat") and callable(getattr(rag, "chat")):
            if history is None:
                answer_raw = rag.chat(question)
            else:
                answer_raw = rag.chat(question, history=history)

        else:
            raise RuntimeError(
                "La instancia de LightRAG no define 'query', 'aquery' ni 'chat'. "
                "Revisa la API de tu versión."
            )

    except Exception as e:
        raise RuntimeError(
            f"[ask_rag] Error consultando el RAG para el caso {case_id}: {e}"
        ) from e

    # Normalizar la respuesta a un string legible
    answer_text: Optional[str] = None

    if isinstance(answer_raw, str):
        answer_text = answer_raw
    elif isinstance(answer_raw, dict):
        # Buscamos campos típicos
        for key in ("answer", "response", "output", "result", "text", "message"):
            val = answer_raw.get(key)
            if isinstance(val, str):
                answer_text = val
                break

    if answer_text is None:
        # Último recurso: convertir a string
        answer_text = str(answer_raw)

    return {
        "case_id": str(case_id),
        "question": question,
        "mode": mode,
        "answer": answer_text,
        "raw": answer_raw,
        "storage_dir": str(storage_dir),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse
    import json
    import sys
    import textwrap

    parser = argparse.ArgumentParser(
        description="Realiza una consulta RAG sobre un caso específico (storage de PC7)."
    )
    parser.add_argument(
        "--case-id",
        required=True,
        help="ID del caso (por ejemplo 14).",
    )
    parser.add_argument(
        "--mode",
        default="core",
        help=(
            "Modo de consulta para QueryParam. "
            "Alias: 'core' → 'hybrid'. "
            "Modos típicos: naive | local | global | hybrid | core."
        ),
    )
    parser.add_argument(
        "--question",
        "-q",
        required=True,
        help="Pregunta en lenguaje natural a realizar sobre el caso.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Si se indica, imprime además la respuesta RAW en JSON.",
    )

    args = parser.parse_args()

    # case_id puede venir como string, lo dejamos así para mantener compatibilidad
    case_id = args.case_id
    question = args.question
    mode = args.mode

    try:
        result = ask_rag(case_id=case_id, question=question, mode=mode)
    except Exception as e:
        print(f"[rag_case_query] ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)

    print("==============================================================")
    print(f"[RAG CASE QUERY] case_id={result['case_id']}  mode={result['mode']}")
    print("--------------------------------------------------------------")
    print("Pregunta:")
    print(f"  {result['question']}")
    print("--------------------------------------------------------------")
    print("Respuesta:")
    print(textwrap.fill(result["answer"], width=100))
    print("--------------------------------------------------------------")
    print(f"Storage dir: {result['storage_dir']}")
    if args.raw:
        print("--------------------------------------------------------------")
        print("Respuesta RAW (JSON):")
        print(json.dumps(result["raw"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
