# lightrag/pipelines/rag_case_query.py
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

    # Intentamos inicializar el Core (si está disponible)
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


def ask_rag(
    case_id: CaseId,
    question: str,
    history: Optional[list[dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Realiza una pregunta al RAG del caso dado.

    - Usa load_rag_for_case(case_id) para obtener la instancia.
    - Intenta usar los métodos estándar de LightRAG:
        - query(...)
        - aquery(...)
        - chat(..., history=...)
    - Devuelve un dict normalizado con:
        - case_id
        - question
        - answer  (texto "plano" extraído de la respuesta)
        - raw     (respuesta completa tal como la devolvió LightRAG)
        - storage_dir
    """
    rag, storage_dir = load_rag_for_case(case_id)

    answer_raw: Any = None

    # Intentamos varios métodos posibles, según la versión de LightRAG
    try:
        if hasattr(rag, "query") and callable(getattr(rag, "query")):
            # Modo síncrono clásico
            answer_raw = rag.query(question)

        elif hasattr(rag, "aquery") and callable(getattr(rag, "aquery")):
            # Versión asíncrona
            coro = rag.aquery(question)
            answer_raw = asyncio.run(coro)

        elif hasattr(rag, "chat") and callable(getattr(rag, "chat")):
            # Algunos forks usan interfaz tipo chat
            if history is None:
                answer_raw = rag.chat(question)
            else:
                # Estructura de history depende de tu fork;
                # aquí solo la pasamos si existe.
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
        "answer": answer_text,
        "raw": answer_raw,
        "storage_dir": str(storage_dir),
    }
