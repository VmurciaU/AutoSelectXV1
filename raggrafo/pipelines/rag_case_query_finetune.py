# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión INDUSTRIAL con multipaso robusto)
====================================================================

Arquitectura reforzada en 5 pasos:
1) DISCOVERY INDEPENDIENTE DE LA PREGUNTA (tags y conteo real del caso)
2) FALLBACK DISCOVERY 2 (clasificador por fluido / servicio / producto químico)
3) EXTRACT BY TAG (Método A + Método B)
4) EXTRACT FROM TABLES (doble intento)
5) FALLBACK FINAL “ALL-BOMBS” (garantizado)

+ Captura de unidades RAW
+ Salida estable SIEMPRE con pumps[ ] 
+ Logs detallados modo desarrollo

Compatible 100% con:
- pc6_lightrag.py
- rag_config.py
- extract_and_normalize.py
- extract / extract-list
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

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
# PASO 1 – DISCOVERY REAL (NO depende de la pregunta)
#####################################################################
async def _discover_pumps_primary(rag) -> Dict[str, Any]:
    """
    Pregunta neutral que SIEMPRE descubre tags reales del caso.
    """
    prompt = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID). 
Devuelve SOLO y EXACTAMENTE:

{
  "tags": [],
  "types": [],
  "count": null
}

NO inventes nada.
"""

    q = prompt

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
# PASO 2 – DISCOVERY SECUNDARIO (clasificador)
#####################################################################
async def _discover_pumps_secondary(rag) -> List[str]:
    """
    Clasifica por fluido, servicio o producto cuando no hay tags en el discovery primario.
    """
    prompt = """
Lista TODOS los equipos dosificadores del paquete QUÍMICO. 
Devuelve SOLO:

{
  "tags": []
}

Sin inventar.
"""

    resp = await _safe_aquery(rag, prompt)
    safe = repair_and_parse(resp)

    if isinstance(safe, dict) and "tags" in safe:
        return safe["tags"]

    return []


#####################################################################
# PASO 3 – EXTRACT SINGLE BY TAG (Método A + B)
#####################################################################
async def _extract_single_by_tag(rag, tag: str) -> Dict[str, Any]:

    # MÉTODO A
    prompt = CFG.EXTRACT_CONFIG["prompt_json_single"]
    q = f"{prompt}\n\nPregunta:\nDame todos los datos de proceso de la bomba {tag}"

    resp = await _safe_aquery(rag, q, CFG.EXTRACT_CONFIG["query_param"])
    safe_a = repair_and_parse(resp)

    if isinstance(safe_a, dict):
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

    # MÉTODO B
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
# PASO 4 – EXTRACT FROM TABLES (doble intento)
#####################################################################
async def _extract_from_tables(rag, question: str) -> List[Dict[str, Any]]:

    prompt = """
Extrae TODAS las bombas desde TABLAS del paquete químico.

Formato EXACTO:
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

    resp = await _safe_aquery(rag, f"{prompt}\n\nPregunta:\n{question}")
    safe = repair_and_parse(resp)

    if isinstance(safe, dict) and "pumps" in safe:
        return safe["pumps"]

    return []


#####################################################################
# PASO 5 – MULTIPASO INDUSTRIAL COMPLETO
#####################################################################
async def _run_extract_multipaso(rag, question: str, mode: str = "extract-list") -> Dict[str, Any]:

    #################################################################
    # 1) DISCOVERY PRIMARIO
    #################################################################
    primary = await _discover_pumps_primary(rag)
    tags = primary["tags"]

    if tags:
        pumps = []
        for t in tags:
            pumps.append(await _extract_single_by_tag(rag, t))
        return {"pumps": pumps, "notes": "ok"}


    #################################################################
    # 2) DISCOVERY SECUNDARIO (CLASIFICADOR)
    #################################################################
    secondary_tags = await _discover_pumps_secondary(rag)
    if secondary_tags:
        pumps = []
        for t in secondary_tags:
            pumps.append(await _extract_single_by_tag(rag, t))
        return {"pumps": pumps, "notes": "ok-secondary"}


    #################################################################
    # 3) TABLAS INTENTO 1 (con la pregunta)
    #################################################################
    tbl1 = await _extract_from_tables(rag, question)
    if tbl1:
        return {"pumps": tbl1, "notes": "ok-tables"}


    #################################################################
    # 4) TABLAS INTENTO 2 (generic)
    #################################################################
    tbl2 = await _extract_from_tables(
        rag,
        "Listado completo de bombas dosificadoras del paquete químico con datos de proceso."
    )
    if tbl2:
        return {"pumps": tbl2, "notes": "heuristic-all"}


    #################################################################
    # 5) FALLBACK FINAL usando prompt_json_list
    #################################################################
    mode_cfg = None
    if CFG and "MODES" in CFG.__dict__:
        if mode in CFG.MODES:
            mode_cfg = CFG.MODES[mode]
        else:
            mode_cfg = CFG.MODES.get("extract")

    if mode_cfg:
        fb = await _safe_aquery(
            rag,
            mode_cfg["prompt_json_list"] + f"\n\nPregunta:\n{question}",
            mode_cfg["query_param"],
        )
        safe_fb = repair_and_parse(fb)
        if isinstance(safe_fb, dict) and "pumps" in safe_fb:
            return {"pumps": safe_fb["pumps"], "notes": "fallback-A"}

    return {"pumps": [], "notes": "no-context"}



#####################################################################
# INTERFAZ PÚBLICA USADA POR extract_and_normalize
#####################################################################
async def extract_query(
    case_id: int,
    question: str,
    mode: str = "extract-list",
    top_k: int = 6,
    return_raw_dict: bool = False,
) -> Dict[str, Any]:

    rag = await _load_rag(case_id)

    if mode in ["extract", "extract-list"]:
        result = await _run_extract_multipaso(rag, question, mode=mode)
        if return_raw_dict:
            return result
        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": result,
            "error": None,
        }

    txt = await _safe_aquery(rag, question)
    if isinstance(txt, dict):
        return {"text": txt.get("text"), "notes": "text-mode"}

    return {"text": str(txt), "notes": "text-mode"}



#####################################################################
# LEGACY CLI (mantener compatibilidad)
#####################################################################
async def run_case_query_finetune(
    case_id: CaseId,
    mode: str,
    question: str,
    list_mode: bool = False,
):
    rag = await _load_rag(case_id)

    if mode in ["extract", "extract-list"]:
        result = await _run_extract_multipaso(rag, question, mode=mode)
        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": result,
            "error": None,
        }

    return await _safe_aquery(rag, question)



#####################################################################
# CLI
#####################################################################
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--mode", default="extract-list")
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
