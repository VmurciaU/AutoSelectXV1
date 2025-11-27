# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión INDUSTRIAL + 80/20 LISTA COMPLETA)
=======================================================================

Regla 80/20 aplicada:
---------------------
- SIEMPRE extraemos TODAS LAS BOMBAS DEL PAQUETE PRIMERO.
- La pregunta NO controla cuántas bombas salen.
- La pregunta solo afecta el DISPATCHER de fallback.
- Esto garantiza estabilidad para selección, cálculo y normalización.

Arquitectura:
1) DISCOVERY PRIMARIO (tags reales del caso)
2) DISCOVERY SECUNDARIO (clasificador por fluidos / servicio)
3) EXTRACT ALL BY TAGS (Método A + Método B)
4) TABLE PASS 1 (si no hay tags)
5) TABLE PASS 2 (consulta genérica “all bombs”)
6) FALLBACK JSON LIST

Compatibilidad:
- pc6_lightrag.py
- rag_config.py
- extract_and_normalize.py
- modos: extract / extract-list (+ texto libre)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Union
import copy

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)
from .json_repair import repair_and_parse

try:
    # Config central del RAG (prompts, modos, etc.)
    from raggrafo.pipelines import rag_config as CFG
except Exception:  # pragma: no cover
    CFG = None

CaseId = Union[int, str]


# ================================================================
# HELPERS BÁSICOS
# ================================================================

async def _safe_aquery(rag, question: str, query_param=None):
    """
    Wrapper seguro para rag.aquery().

    - Verifica que RAG exista
    - Verifica que aquery sea async
    - Si no se pasa query_param, usa uno seguro por defecto desde rag_config
    """
    if rag is None:
        raise RuntimeError("RAG no inicializado")

    # Si no tenemos query_param, intentamos usar el de 'naive' (o 'engineering')
    if query_param is None and CFG is not None and hasattr(CFG, "MODES"):
        default_cfg = CFG.MODES.get("naive") or CFG.MODES.get("engineering") or {}
        qp = default_cfg.get("query_param")
        if qp is not None:
            query_param = qp

    if hasattr(rag, "aquery") and inspect.iscoroutinefunction(rag.aquery):
        return await rag.aquery(question, query_param)

    raise RuntimeError("LightRAG local no implementa aquery async")



async def _load_rag(case_id: CaseId):
    """
    Carga el RAG construido por PC6/PC7 para un caso específico.
    """
    case_dir = Path(RAG_STORAGE_DIR) / f"case_{case_id}"
    if not case_dir.exists():
        raise RuntimeError(f"No existe storage del caso {case_id}")

    rag = _make_rag(case_dir)

    # Inicialización extendida (si aplica)
    try:
        if inspect.iscoroutinefunction(_core_initialize):
            await _core_initialize(rag)
        else:
            _core_initialize(rag)
    except Exception:
        # No rompemos si la inicialización extra falla
        pass

    return rag


# ================================================================
# REGEX de unidades (para pistas adicionales RAW)
# ================================================================
FLOW_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(GPD|LPH|m3/d|BPD|LPM)", re.IGNORECASE)
PRESSURE_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(psig|psi|bar|kg/cm2)", re.IGNORECASE)
TEMP_PATTERN = re.compile(r"(-?\d+(\.\d+)?)\s*(°F|°C|F|C)", re.IGNORECASE)
VISC_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(cP|cSt)", re.IGNORECASE)
DENS_PATTERN = re.compile(r"(\d+(\.\d+)?)\s*(kg/m3|g/cm3)", re.IGNORECASE)


def _extract_unit(pattern, text: str) -> Dict[str, Any]:
    """
    Extrae valor + unidad del texto, usando un patrón regex.

    Devuelve:
        {"value": float | None, "unit": str | None}
    """
    if not text:
        return {"value": None, "unit": None}

    m = pattern.search(text)
    if not m:
        return {"value": None, "unit": None}
    return {"value": float(m.group(1)), "unit": m.group(3)}


def _extract_numbers_from_text(text: str) -> List[float]:
    """
    Extrae todos los números (decimales con punto o coma) de un texto.

    Se usa como heurística para reconstruir min/max cuando el LLM
    solo describe "capacidad mínima / máxima" pero no llena bien el JSON.
    """
    if not text:
        return []

    nums_raw = re.findall(r"(-?\d+(?:[.,]\d+)?)", text)
    out: List[float] = []
    for n in nums_raw:
        try:
            out.append(float(n.replace(",", ".")))
        except ValueError:
            continue
    return out


# ================================================================
# FIX GENÉRICO DE FLOW (para TODAS las bombas)
# ================================================================

def _fix_pump_flow(pump: Dict[str, Any]) -> Dict[str, Any]:
    """
    Corrige de forma genérica el diccionario 'flow' de una bomba.

    Objetivo:
    - Si el texto RAW menciona "mínima / máxima" pero NO "nominal":
        * Reconstruimos min y max desde los números del texto.
        * Si nominal coincide con min o max, lo anulamos (None).
    - No depende del origen (tags, tablas, fallback JSON-list).
    """
    flow = pump.get("flow") or {}
    if not isinstance(flow, dict):
        flow = {"min": None, "nominal": None, "max": None, "raw": None}

    raw_flow_text = flow.get("raw")
    if not isinstance(raw_flow_text, str):
        # Sin texto RAW fiable no tocamos nada
        pump["flow"] = flow
        return pump

    raw_lower = raw_flow_text.lower()
    nums = _extract_numbers_from_text(raw_flow_text)
    nominal_in_text = "nominal" in raw_lower

    fmin = flow.get("min")
    fnom = flow.get("nominal")
    fmax = flow.get("max")

    # Caso industrial genérico:
    # - RAW tiene "mínima ... X; máxima ... Y"
    # - RAW NO menciona "nominal"
    # Regla de la casa:
    #   -> reconstruimos min/max si faltan
    #   -> PERO NUNCA aceptamos un "nominal" inventado si el texto no dice "nominal".
    if not nominal_in_text and len(nums) >= 2:
        # Rellenar min / max si faltan
        if fmin is None:
            fmin = nums[0]
            flow["min"] = fmin
        if fmax is None:
            fmax = nums[1]
            flow["max"] = fmax

        # Cualquier valor de nominal en este contexto es sospechoso → lo anulamos.
        if fnom is not None:
            flow["nominal"] = None


    pump["flow"] = flow
    return pump


def _postprocess_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aplica _fix_pump_flow a todas las bombas del resultado.
    """
    pumps = result.get("pumps") or []
    fixed: List[Dict[str, Any]] = []
    for p in pumps:
        if isinstance(p, dict):
            fixed.append(_fix_pump_flow(p))
        else:
            fixed.append(p)
    result["pumps"] = fixed
    return result


# ================================================================
# DISCOVERY DE TAGS
# ================================================================

async def _discover_pumps_primary(rag) -> List[str]:
    """
    DISCOVERY PRIMARIO:
    -------------------
    Descubre los TAGs REALES del caso a partir de TODOS los documentos.

    No depende de la pregunta del usuario.
    """
    prompt = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID).

Tu tarea es ENCONTRAR TODAS las bombas dosificadoras de químicos del paquete
(incluyendo bombas en paralelo, stand-by, redundantes, etc.).

Devuelve SOLO los TAGs de esas bombas en este formato JSON ESTRICTO:

{
  "tags": ["P-101", "P-102", "P-103"]
}

Reglas IMPORTANTES:
- Si encuentras una TABLA de datos de proceso (por ejemplo en la ET o en la HD)
donde aparezcan varios TAG de bombas dosificadoras (como P-5540, P-5541, P-5542, etc.),
  debes incluir TODOS esos TAGs en la lista.
- No agrupes varias bombas en un solo TAG.
- No mezcles TAG de otros equipos (tanques, válvulas, etc.), solo bombas dosificadoras.
- NO inventes TAGs. Solo devuelve los que realmente aparezcan en las tablas o texto.
- Si no estás seguro, usa una lista vacía: "tags": [].
"""

    resp = await _safe_aquery(rag, prompt)
    safe = repair_and_parse(resp)

    if isinstance(safe, dict):
        tags = safe.get("tags") or []
        if isinstance(tags, list):
            # Normalizamos a str siempre
            return [str(t).strip() for t in tags if str(t).strip()]
    return []


async def _discover_pumps_secondary(rag) -> List[str]:
    """
    DISCOVERY SECUNDARIO:
    ---------------------
    Clasificador auxiliar cuando no hay tags del discovery primario.
    Puede basarse en servicio, fluido, etc.
    """
    prompt = """
Lista TODOS los equipos dosificadores del paquete químico
en formato JSON estricto:

{
  "tags": ["P-101", "P-102"]
}

NO inventes datos. Si no hay equipos claros, usa una lista vacía.
"""
    resp = await _safe_aquery(rag, prompt)
    safe = repair_and_parse(resp)

    if isinstance(safe, dict):
        tags = safe.get("tags") or []
        if isinstance(tags, list):
            return [str(t).strip() for t in tags if str(t).strip()]
    return []


# ================================================================
# EXTRAER UNA BOMBA POR TAG (Método A + Método B)
# ================================================================

async def _extract_single_by_tag(rag, tag: str) -> Dict[str, Any]:
    """
    EXTRAER UNA BOMBA COMPLETA POR TAG
    -----------------------------------
    Método A (principal):
        - Usa prompt_json_single de CFG.EXTRACT_CONFIG
        - Espera JSON estricto con campos:
            fluid, flow, discharge_pressure, viscosity, optional

    Método B (fallback engineering):
        - Usa modo "engineering" de CFG.MODES
        - Intenta extraer parámetros desde texto técnico

    Además:
        - Adjunta campos *_raw con pistas de unidades detectadas.
        - Aplica CORRECCIÓN INDUSTRIAL de flujo (via _fix_pump_flow).
    """
    if CFG is None or not hasattr(CFG, "EXTRACT_CONFIG"):
        raise RuntimeError("rag_config.EXTRACT_CONFIG no está disponible")

    # ----------------------
    # Método A — JSON literal
    # ----------------------
    base_prompt = CFG.EXTRACT_CONFIG.get("prompt_json_single", "")
    q = f"{base_prompt}\n\nPregunta:\nDame todos los datos de proceso de la bomba {tag}"

    resp = await _safe_aquery(rag, q, CFG.EXTRACT_CONFIG.get("query_param"))
    safe_a = repair_and_parse(resp)

    if isinstance(safe_a, dict):
        # Por si LightRAG devuelve dict con 'text'
        raw_text = safe_a.get("text")
        if raw_text is None:
            raw_text = str(resp)

        flow = safe_a.get("flow", {}) or {}
        if not isinstance(flow, dict):
            flow = {}

        # Usamos flow["raw"] si el LLM la dio; si no, usamos el texto general
        raw_flow_text = flow.get("raw")
        if not isinstance(raw_flow_text, str):
            raw_flow_text = raw_text

        # Montamos pump base
        pump = {
            "fluid": safe_a.get("fluid"),
            "flow": {
                "min": flow.get("min"),
                "nominal": flow.get("nominal"),
                "max": flow.get("max"),
                "raw": raw_flow_text,
            },
            "discharge_pressure": safe_a.get("discharge_pressure"),
            "viscosity": safe_a.get("viscosity"),
            "optional": safe_a.get("optional", {}),
            # Pistas de unidades desde texto bruto:
            "flow_nominal_raw": _extract_unit(FLOW_PATTERN, raw_flow_text),
            "pressure_raw": _extract_unit(PRESSURE_PATTERN, raw_text),
            "viscosity_raw": _extract_unit(VISC_PATTERN, raw_text),
            "temperature_raw": _extract_unit(TEMP_PATTERN, raw_text),
            "density_raw": _extract_unit(DENS_PATTERN, raw_text),
        }

        # Aplicamos corrección genérica de flow
        return _fix_pump_flow(pump)

    # --------------------------
    # Método B — Fallback "engineering"
    # --------------------------
    if CFG is None or not hasattr(CFG, "MODES"):
        raise RuntimeError("rag_config.MODES no está disponible")

    eng_mode = CFG.MODES.get("engineering", {})
    eng_qp = eng_mode.get("query_param")

    eng = await _safe_aquery(
        rag,
        f"Describe técnicamente los datos de proceso de {tag}",
        eng_qp,
    )
    text = eng.get("text") if isinstance(eng, dict) else str(eng)

    pump = {
        "fluid": None,
        "flow": {"min": None, "nominal": None, "max": None, "raw": text},
        "discharge_pressure": None,
        "viscosity": None,
        "optional": {"tag": tag},
        "flow_nominal_raw": _extract_unit(FLOW_PATTERN, text),
        "pressure_raw": _extract_unit(PRESSURE_PATTERN, text),
        "viscosity_raw": _extract_unit(VISC_PATTERN, text),
        "temperature_raw": _extract_unit(TEMP_PATTERN, text),
        "density_raw": _extract_unit(DENS_PATTERN, text),
    }

    return _fix_pump_flow(pump)


# ================================================================
# TABLAS (cuando no hay tags claros)
# ================================================================

async def _extract_from_tables(rag, question: str) -> List[Dict[str, Any]]:
    """
    Extrae bombas a partir de TABLAS de los documentos.

    Se usa como plan B/C cuando no se consiguen TAGs claros.
    """
    prompt = """
Extrae TODAS las bombas dosificadoras únicamente desde TABLAS
(en hojas de datos, MR, ET, etc.) y responde en JSON estricto:

{
  "pumps": [
    {
      "fluid": "...",
      "flow": { "min": ..., "nominal": ..., "max": ..., "raw": "..." },
      "discharge_pressure": ...,
      "viscosity": ...,
      "optional": { "tag": "...", "service": "...", "temperature": ... }
    }
  ]
}
"""
    resp = await _safe_aquery(rag, f"{prompt}\n\nPregunta:\n{question}")
    safe = repair_and_parse(resp)

    pumps = []
    if isinstance(safe, dict) and isinstance(safe.get("pumps"), list):
        for p in safe["pumps"]:
            if isinstance(p, dict):
                pumps.append(_fix_pump_flow(p))
            else:
                pumps.append(p)
    return pumps


# ================================================================
# MULTIPASO 80/20 (PIPELINE INDUSTRIAL)
# ================================================================

async def _run_extract_multipaso(rag, question: str, mode: str) -> Dict[str, Any]:
    """
    Orquestador industrial 80/20:

    1) Discovery primario de TAGs reales
    2) Discovery secundario (clasificador por equipo)
    3) Extracción desde tablas (pregunta del usuario)
    4) Extracción desde tablas genérica "todas las bombas"
    5) Fallback JSON list según CFG.MODES[mode]
    """

    # 1 — TAGs REALES
    tags = await _discover_pumps_primary(rag)
    if tags:
        pumps: List[Dict[str, Any]] = []
        for t in tags:
            pumps.append(await _extract_single_by_tag(rag, t))
        return {
            "pumps": pumps,
            "notes": "ok-primary",
        }

    # 2 — TAGs secundarios
    tags2 = await _discover_pumps_secondary(rag)
    if tags2:
        pumps: List[Dict[str, Any]] = []
        for t in tags2:
            pumps.append(await _extract_single_by_tag(rag, t))
        return {
            "pumps": pumps,
            "notes": "ok-secondary",
        }

    # 3 — Tablas usando la pregunta del usuario
    tbl1 = await _extract_from_tables(rag, question)
    if tbl1:
        return {"pumps": tbl1, "notes": "ok-tables-question"}

    # 4 — Tablas genéricas (consulta "all bombs")
    tbl2 = await _extract_from_tables(
        rag,
        "Listado completo de bombas dosificadoras del paquete químico con todos "
        "sus datos de proceso (caudal mínimo, nominal y máximo, presión, "
        "temperatura, viscosidad, servicio y TAG).",
    )
    if tbl2:
        return {"pumps": tbl2, "notes": "ok-tables-generic"}

    # 5 — Fallback JSON LIST (modo extract / extract-list)
    if CFG is None or not hasattr(CFG, "MODES"):
        raise RuntimeError("rag_config.MODES no está disponible")

    mode_cfg = CFG.MODES.get(mode) or CFG.MODES.get("extract") or {}

    fb_prompt = mode_cfg.get("prompt_json_list", "")
    fb_qp = mode_cfg.get("query_param")

    # Pregunta canónica interna: SIEMPRE extrae todas las bombas
    canonical_q = (
        "Listado completo de bombas dosificadoras del paquete químico con todos "
        "sus datos de proceso (caudal mínimo, nominal y máximo, presión, "
        "temperatura, viscosidad, servicio y TAG)."
    )

    fb = await _safe_aquery(
        rag,
        fb_prompt + f"\n\nPregunta:\n{canonical_q}",
        fb_qp,
    )

    safe_fb = repair_and_parse(fb)

    if isinstance(safe_fb, dict) and isinstance(safe_fb.get("pumps"), list):
        res = {
            "pumps": safe_fb["pumps"],
            "notes": "fallback-A",
            "_fallback_text": fb,
        }
    else:
        res = {
            "pumps": [],
            "notes": "fallback-invalid",
            "_fallback_text": fb,
        }

    return res

    






    # Sin contexto suficiente
    return {"pumps": [], "notes": "no-context"}


# ================================================================
# API PÚBLICA (usada por extract_and_normalize.py)
# ================================================================

async def extract_query(
    case_id: int,
    question: str,
    mode: str = "extract-list",
    top_k: int = 6,  # se mantiene por compatibilidad de firma
    return_raw_dict: bool = False,
):
    """
    Punto de entrada principal para consultas RAG estructuradas.

    - Para modos "extract" / "extract-list": devuelve siempre JSON con
      TODAS las bombas del caso, más un campo "notes".
    - Para otros modos: actúa como un proxy a texto libre.
    """
    rag = await _load_rag(case_id)

    # Modos estructurados
    if mode in ("extract", "extract-list"):
        
        result = await _run_extract_multipaso(rag, question, mode)


        # 1) Postproceso industrial obligatorio
        result = _postprocess_result(result)

        # 2) NUEVO: duplicador de bombas FINAL
        if isinstance(result, dict) and isinstance(result.get("pumps"), list):
            # Usamos el texto crudo que pasó por fallback
            # El multipaso mete el fb original dentro de `notes` o en el safe_fb... 
            # pero para modos extract-list SIEMPRE tenemos full_str en result["final"].
            # Tomar el texto del fallback si existe
            fallback_text = result.get("_fallback_text") or ""
            result["pumps"] = apply_pump_count_postprocessor(result["pumps"], fallback_text)



        if return_raw_dict:
            return result
        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": result,
            "error": None,
        }

    # Modos de texto libre (naive, mix, combo, engineering, verify, etc.)
    # Usamos la configuración específica del modo si está definida en rag_config.
    query_param = None
    if CFG is not None and hasattr(CFG, "MODES"):
        mode_cfg = CFG.MODES.get(mode) or {}
        query_param = mode_cfg.get("query_param")

    out = await _safe_aquery(rag, question, query_param)

    # Normalizamos la salida a texto + raw + error=None
    if isinstance(out, dict):
        # Muchos modelos devuelven {"text": "..."}; si no, serializamos.
        text = out.get("text") or out.get("answer")
        if text is None:
            text = json.dumps(out, ensure_ascii=False)
        raw = out
    else:
        text = str(out)
        raw = {"text": text}

    return {
        "final": text,
        "raw": raw,
        "error": None,
    }


# ================================================================
# CLI LEGACY
# ================================================================

async def run_case_query_finetune(case_id: CaseId, mode: str, question: str):
    """
    Compatibilidad con scripts tipo:

        python -m raggrafo.pipelines.rag_case_query_finetune \\
            --case-id 2 \\
            --mode extract-list \\
            --question "¿Cuál es el caudal nominal?"
    """
    return await extract_query(int(case_id), question, mode)


import re
import copy

def apply_pump_count_postprocessor(pumps: list, fb_text: str) -> list:
    """
    Duplica bombas cuando el fallback detecta 1 pero el texto menciona múltiples.
    - No modifica prompts
    - No modifica el JSON original
    - No afecta normalizer
    """

    # Si ya hay más de 1 bomba, no hacer nada
    if pumps is None or len(pumps) != 1:
        return pumps

    text = fb_text.lower()

    # ============================================================
    # 1. Detectar patrones numéricos explícitos: "3 bombas", "3 metering pumps"
    # ============================================================
    pat_num = re.search(r"\b(\d+)\s*(bombas?|metering pumps?)\b", text)
    if pat_num:
        n = int(pat_num.group(1))
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # ============================================================
    # 2. Detectar formato "2+1", "3+0", etc.
    # ============================================================
    pat_plus = re.search(r"(\d+)\s*\+\s*(\d+)", text)
    if pat_plus:
        a = int(pat_plus.group(1))
        b = int(pat_plus.group(2))
        n = a + b
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # ============================================================
    # 3. Detectar cantidad textual: "dos bombas", "tres bombas"
    # ============================================================
    WORD2NUM = {
        "una": 1, "un": 1,
        "dos": 2,
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7
    }

    pat_word = re.search(r"\b(una|un|dos|tres|cuatro|cinco|seis|siete)\s+bombas?\b", text)
    if pat_word:
        w = pat_word.group(1)
        n = WORD2NUM.get(w, 1)
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # ============================================================
    # 4. Detectar “dos bombas de operación y una de respaldo”
    # ============================================================
    pat_oper = re.search(
        r"(una|un|dos|tres|cuatro)\s+bombas?\s+de\s+operaci[oó]n\s+y\s+(una|un|dos|tres|cuatro)",
        text
    )
    if pat_oper:
        n1 = WORD2NUM.get(pat_oper.group(1), 1)
        n2 = WORD2NUM.get(pat_oper.group(2), 1)
        n = n1 + n2
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # ============================================================
    # Si nada coincide: devolver la bomba única detectada
    # ============================================================
    return pumps



if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--mode", default="extract-list")
    args = ap.parse_args()

    out = asyncio.run(run_case_query_finetune(
        case_id=args.case_id,
        mode=args.mode,
        question=args.question,
    ))

    print(json.dumps(out, ensure_ascii=False, indent=2))

