# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión multipaso con unidades RAW)
===============================================================

Arquitectura robusta de 4 pasos:
1) DISCOVERY (tags, types, count)
2) EXTRACT BY TAG (Método A + Método B)
3) EXTRACT FROM TABLES (cuando no hay TAGs)
4) FALLBACK A JSON DIRECTO

Nuevo: Captura de unidades RAW para:
- Flujo (GPD, LPH, m3/d…)
- Presión (psig, bar…)
- Temperatura (°F, °C)
- Viscosidad (cP, cSt…)
- Densidad (kg/m3, g/cm3…)

Salida estable:
{
  "pumps": [
      { "flow_nominal": { "value": ..., "unit": ... }, ... }
  ],
  "notes": "ok|fallback-A|no-context"
}

Compatible 100% con:
- pc6_lightrag.py
- rag_config.py
- extract / extract-list
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


#####################################################################
# HELPERS JSON
#####################################################################
def _wrap_json(value: Any) -> str:
    """Siempre devuelve JSON string."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2)
    if value is None:
        return json.dumps({}, ensure_ascii=False, indent=2)
    return json.dumps({"text": str(value)}, ensure_ascii=False, indent=2)


#####################################################################
# AQUERY WRAPPER
#####################################################################
async def _safe_aquery(rag, question: str, query_param: Any = None):
    if rag is None:
        raise RuntimeError("RAG no inicializado")

    if hasattr(rag, "aquery") and inspect.iscoroutinefunction(rag.aquery):
        return await rag.aquery(question, query_param)

    raise RuntimeError("El LightRAG local no implementa aquery async")


#####################################################################
# LOAD PC6 RAG
#####################################################################
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
    except:
        pass

    return rag


#####################################################################
# REGEX DE UNIDADES
#####################################################################
FLOW_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(GPD|LPH|m3/d|BPD|LPM)", re.IGNORECASE)
PRESSURE_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(psig|psi|bar|kg/cm2)", re.IGNORECASE)
TEMP_PATTERN = re.compile(r"(-?\d+(\.\d+)?)\s*(°F|°C|F|C)", re.IGNORECASE)
VISC_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(cP|cSt)", re.IGNORECASE)
DENS_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(kg/m3|g/cm3)", re.IGNORECASE)


def _extract_unit(pattern, text: str):
    m = pattern.search(text)
    if not m:
        return {"value": None, "unit": None}
    return {"value": float(m.group(1)), "unit": m.group(3)}


#####################################################################
# PASO 1 – DISCOVERY
#####################################################################
async def _discover_pumps(rag, question: str):
    prompt = """
A partir de TODOS los documentos (HD, MR, ET), devuelve EXACTAMENTE:

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


#####################################################################
# PASO 2 – EXTRACT SINGLE BY TAG
#####################################################################
async def _extract_single_by_tag(rag, tag: str) -> Dict[str, Any]:
    """
    Extrae UNA bomba específica usando Método A y fallback B.
    """

    # MÉTODO A
    prompt = CFG.EXTRACT_CONFIG["prompt_json_single"]
    q = f"{prompt}\n\nPregunta:\nDame todos los datos de proceso de la bomba {tag}"

    resp = await _safe_aquery(rag, q, CFG.EXTRACT_CONFIG["query_param"])
    safe_a = repair_and_parse(resp)

    # Si método A devolvió algo válido
    if isinstance(safe_a, dict):
        # Captura unidades desde texto bruto
        raw_text = str(resp)

        return {
            "fluid": safe_a.get("fluid"),
            "flow_nominal": _extract_unit(FLOW_PATTERN, raw_text),
            "discharge_pressure": _extract_unit(PRESSURE_PATTERN, raw_text),
            "viscosity": _extract_unit(VISC_PATTERN, raw_text),
            "optional": {
                "tag": tag,
                "temperature": _extract_unit(TEMP_PATTERN, raw_text),
                "density": _extract_unit(DENS_PATTERN, raw_text),
                "service": safe_a.get("optional", {}).get("service"),
                "materials": safe_a.get("optional", {}).get("materials"),
                "location": safe_a.get("optional", {}).get("location"),
                "voltage": safe_a.get("optional", {}).get("voltage"),
                "pump_type": safe_a.get("optional", {}).get("pump_type"),
                "drive_type": safe_a.get("optional", {}).get("drive_type"),
                "source_pages": safe_a.get("optional", {}).get("source_pages"),
            }
        }

    # MÉTODO B – Regex + engineering
    eng = await _safe_aquery(
        rag,
        f"Describe técnicamente los datos de proceso de {tag}",
        CFG.MODES["engineering"]["query_param"],
    )
    text = eng["text"] if isinstance(eng, dict) else str(eng)

    return {
        "fluid": None,
        "flow_nominal": _extract_unit(FLOW_PATTERN, text),
        "discharge_pressure": _extract_unit(PRESSURE_PATTERN, text),
        "viscosity": _extract_unit(VISC_PATTERN, text),
        "optional": {
            "tag": tag,
            "temperature": _extract_unit(TEMP_PATTERN, text),
            "density": _extract_unit(DENS_PATTERN, text),
            "service": None,
            "materials": None,
            "location": None,
            "voltage": None,
            "pump_type": None,
            "drive_type": None,
            "source_pages": None,
        }
    }


#####################################################################
# PASO 3 – EXTRACT ONLY FROM TABLES
#####################################################################
async def _extract_from_tables(rag, question: str) -> List[Dict[str, Any]]:
    prompt = """
Extrae TODAS las bombas desde TABLAS, incluso si no hay TAGs.

Debe devolver EXACTAMENTE:
{
  "pumps": [
    {
      "fluid": null,
      "flow_nominal": { "value": ..., "unit": "..." },
      "discharge_pressure": { "value": ..., "unit": "..." },
      "viscosity": { "value": ..., "unit": "..." },
      "optional": { "tag": null }
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


#####################################################################
# PASO 4 – MULTIPASO ENSAMBLE
#####################################################################
async def _run_extract_multipaso(rag, question: str) -> Dict[str, Any]:

    disc = await _discover_pumps(rag, question)
    tags = disc["tags"]

    pumps = []

    # Si hay TAGs → extraemos cada una
    if tags:
        for tag in tags:
            p = await _extract_single_by_tag(rag, tag)
            pumps.append(p)
        return {"pumps": pumps, "notes": "ok"}

    # Si no hay TAGs → tablas
    table_pumps = await _extract_from_tables(rag, question)
    if table_pumps:
        return {"pumps": table_pumps, "notes": "ok-tables"}

    # Fallback A → JSON directo lista
    fallback = await _safe_aquery(
        rag,
        CFG.MODES["extract"]["prompt_json_list"] + f"\n\nPregunta:\n{question}",
        CFG.MODES["extract"]["query_param"],
    )
    safe_fb = repair_and_parse(fallback)
    if isinstance(safe_fb, dict) and "pumps" in safe_fb:
        return {"pumps": safe_fb["pumps"], "notes": "fallback-A"}

    return {"pumps": [], "notes": "no-context"}


#####################################################################
# RUN PUBLICO
#####################################################################
async def run_case_query_finetune(
    case_id: CaseId,
    mode: str,
    question: str,
    list_mode: bool = False,
):
    rag = await _load_rag(case_id)

    if mode in ["extract", "extract-list"]:
        result = await _run_extract_multipaso(rag, question)
        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": result,
            "error": None,
        }

    # Modos texto
    return await _safe_aquery(rag, question)


#####################################################################
# CLI
#####################################################################
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
