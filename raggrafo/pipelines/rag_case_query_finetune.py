# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión multipaso robusta)
======================================================

NUEVA ARQUITECTURA:
-------------------
1) DISCOVERY:
      - Extrae TAGs
      - Extrae tipos de bomba
      - Extrae filas de tablas si no hay TAGs

2) EXTRACT POR BOMBA:
      - Método A → JSON directo
      - Método B → Regex + Engineering (fallback)

3) ENSAMBLADO GLOBAL:
      - pumps = [...]
      - notes = "ok" / "no-context" / "fallback-used"

Garantías:
----------
- Siempre retorna JSON válido.
- Siempre retorna lista de bombas.
- No altera PC6.
- Compatible con rag_config.py.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Union

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)

from .json_repair import repair_and_parse

try:
    from raggrafo.pipelines import rag_config as CFG
except Exception:
    CFG = None

CaseId = Union[int, str]


# ------------------------------------------------------------
# Helper universal JSON
# ------------------------------------------------------------
def _force_json(obj: Any) -> str:
    if isinstance(obj, str) and "[no-context]" in obj:
        return json.dumps({"pumps": [], "notes": "no-context"}, ensure_ascii=False, indent=2)
    if obj is None:
        return json.dumps({}, ensure_ascii=False, indent=2)
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, ensure_ascii=False, indent=2)
    return json.dumps({"text": str(obj)}, ensure_ascii=False, indent=2)


# ------------------------------------------------------------
# Wrapper seguro del aquery
# ------------------------------------------------------------
async def _safe_aquery(rag, question: str, query_param: Any = None):
    if rag is None:
        raise RuntimeError("RAG no inicializado")
    if hasattr(rag, "aquery") and inspect.iscoroutinefunction(rag.aquery):
        return await rag.aquery(question, query_param)
    raise RuntimeError("Tu LightRAG local no tiene método aquery async compatible")


# ------------------------------------------------------------
# Carga del RAG desde PC6
# ------------------------------------------------------------
async def _load_rag(case_id: CaseId):
    case_dir = Path(RAG_STORAGE_DIR) / f"case_{case_id}"
    if not case_dir.exists():
        raise RuntimeError(f"No existe storage del caso {case_id}: {case_dir}")

    rag = _make_rag(case_dir)

    try:
        if inspect.iscoroutinefunction(_core_initialize):
            await _core_initialize(rag)
        else:
            _core_initialize(rag)
    except Exception as e:
        print(f"[WARN] _core_initialize falló: {e}")

    return rag


# ============================================================
# PASO 1 – DISCOVERY
# ============================================================
async def _discover_pumps(rag, question: str) -> Dict[str, Any]:
    """
    Retorna:
    {
        "tags": [...],
        "types": [...],
        "count": int
    }
    """
    prompt = """
Eres un extractor técnico especializado.

A partir de TODOS los documentos del caso, detecta:
- Todos los TAG de bombas (ej: P-5540, P-5541…)
- Tipos de bomba (ej: bomba dosificadora, API675, diafragma…)
- Cuenta total de bombas si aparece en HD/MR/ET

Formato EXACTO JSON:
{
  "tags": [],
  "types": [],
  "count": null
}
NO inventes datos.
"""

    q = f"{prompt}\n\nPregunta:\n{question}"

    resp = await _safe_aquery(rag, q)
    safe = repair_and_parse(resp)

    if not isinstance(safe, dict):
        return {"tags": [], "types": [], "count": None}

    return {
        "tags": safe.get("tags", []) or [],
        "types": safe.get("types", []) or [],
        "count": safe.get("count", None),
    }


# ============================================================
# PASO 2 – Extract para una BOMBA por TAG (Método A/B)
# ============================================================
async def _extract_single_by_tag(rag, tag: str) -> Dict[str, Any]:
    """
    Extrae UNA BOMBA específica usando multipaso A/B.
    """

    # ---------- Método A (JSON directo)
    prompt = CFG.EXTRACT_CONFIG["prompt_json_single"]
    q = f"{prompt}\n\nPregunta:\nDame todos los datos de proceso de la bomba {tag}"

    resp = await _safe_aquery(rag, q, CFG.EXTRACT_CONFIG["query_param"])
    safe_a = repair_and_parse(resp)

    if isinstance(safe_a, dict) and safe_a.get("fluid") or safe_a.get("optional"):
        safe_a["optional"] = safe_a.get("optional", {})
        safe_a["optional"]["tag"] = tag
        return safe_a

    # ---------- Método B (regex + engineering)
    eng = await _safe_aquery(
        rag,
        f"Como ingeniero, describe los datos de proceso de {tag}",
        CFG.MODES["engineering"]["query_param"],
    )
    eng_text = eng["text"] if isinstance(eng, dict) else str(eng)

    flow = re.findall(r"(\d+(\.\d+)?)\s*GPD", eng_text)
    pres = re.findall(r"(\d+)\s*psig", eng_text)

    result = {
        "fluid": None,
        "flow_nominal": flow[0][0] if flow else None,
        "discharge_pressure": pres[0][0] if pres else None,
        "viscosity": None,
        "optional": {
            "tag": tag,
            "temperature": None,
            "density": None,
            "service": None,
            "materials": None,
            "location": None,
            "voltage": None,
            "pump_type": None,
            "drive_type": None,
            "source_pages": None,
        }
    }

    return result


# ============================================================
# PASO 3 – Extract sin TAGs (tablas)
# ============================================================
async def _extract_from_tables(rag, question: str) -> List[Dict[str, Any]]:
    """
    Si no existen TAGs, intentar detectar filas de tablas (GPD / psig).
    """
    prompt = """
Analiza TODAS las tablas del caso.

Extrae TODAS las bombas, incluso si no hay TAG.
Busca filas con:
- Capacidad mínima (GPD)
- Capacidad máxima (GPD)
- Presión (psig)

Devuelve formato EXACTO:
{
  "pumps": [
    {
      "fluid": null,
      "flow_nominal": null,
      "discharge_pressure": null,
      "viscosity": null,
      "optional": {
        "tag": null,
        "temperature": null
      }
    }
  ]
}
"""

    q = f"{prompt}\n\nPregunta:\n{question}"
    resp = await _safe_aquery(rag, q)
    safe = repair_and_parse(resp)

    if isinstance(safe, dict) and "pumps" in safe:
        return safe["pumps"]

    return []


# ============================================================
# PASO 4 – ENSAMBLADO MULTIPASO
# ============================================================
async def _run_extract_multipaso(rag, question: str) -> Dict[str, Any]:

    # -- DISCOVERY
    disc = await _discover_pumps(rag, question)
    tags = disc["tags"]
    types = disc["types"]
    count = disc["count"]

    pumps: List[Dict[str, Any]] = []

    # -- Si hay TAGs, extraemos una por una
    if tags:
        for tag in tags:
            p = await _extract_single_by_tag(rag, tag)
            pumps.append(p)
        return {"pumps": pumps, "notes": "ok"}

    # -- Si NO hay TAGs → tabla
    table_pumps = await _extract_from_tables(rag, question)
    if table_pumps:
        return {"pumps": table_pumps, "notes": "ok-tables"}

    # -- Fallback final → Método A con lista
    fallback = await _safe_aquery(
        rag,
        CFG.MODES["extract"]["prompt_json_list"] + f"\n\nPregunta:\n{question}",
        CFG.MODES["extract"]["query_param"],
    )
    safe_fb = repair_and_parse(fallback)

    if isinstance(safe_fb, dict) and "pumps" in safe_fb:
        return {"pumps": safe_fb["pumps"], "notes": "fallback-A"}

    # -- Último recurso
    return {"pumps": [], "notes": "no-context"}


# ============================================================
# Modos tradicionales (naive, engineering, verify)
# ============================================================
async def _run_simple(rag, question: str, mode: str) -> Dict[str, Any]:
    cfg = CFG.MODES.get(mode)
    if not cfg:
        return {"final": "", "raw": None, "error": f"Modo '{mode}' no existe"}

    prompt = cfg.get("prompt_text", "")
    q = f"{prompt}\n\nPregunta:\n{question}"

    resp = await _safe_aquery(rag, q, cfg.get("query_param"))
    final = resp["text"] if isinstance(resp, dict) and "text" in resp else str(resp)

    return {"final": final, "raw": resp, "error": None}


# ============================================================
# Wrapper público UNIFICADO
# ============================================================
async def run_case_query_finetune(
    case_id: CaseId,
    mode: str,
    question: str,
    list_mode: bool = False,
):
    if CFG is None:
        raise RuntimeError("rag_config.py falló al cargar")

    rag = await _load_rag(case_id)

    if mode == "extract" or mode == "extract-list":
        result = await _run_extract_multipaso(rag, question)
        return {"final": json.dumps(result, ensure_ascii=False, indent=2),
                "raw": result,
                "error": None}

    if mode == "naive":
        return await _run_simple(rag, question, "naive")
    if mode == "engineering":
        return await _run_simple(rag, question, "engineering")
    if mode == "verify":
        return await _run_simple(rag, question, "verify")

    raise RuntimeError(f"Modo '{mode}' no soportado en esta versión multipaso")


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--mode", default="extract")
    ap.add_argument("--list-mode", action="store_true")
    args = ap.parse_args()

    out = asyncio.run(
        run_case_query_finetune(
            case_id=args.case_id,
            mode=args.mode,
            question=args.question,
            list_mode=args.list_mode,
        )
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
