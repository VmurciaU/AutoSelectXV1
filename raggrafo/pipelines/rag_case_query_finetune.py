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
2.5) DISCOVERY TABLAS: SKID -> TAG BOMBA (si existe)
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
import copy
import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

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
# POSTPROCESSOR (duplicación por fallback-text) — debe existir ANTES del uso
# ================================================================

def apply_pump_count_postprocessor(pumps: list, fb_text: str) -> list:
    """
    Duplica bombas cuando el fallback detecta 1 pero el texto menciona múltiples.
    - No modifica prompts
    - No modifica el JSON original (solo duplica deepcopies)
    - No afecta normalizer directamente (le llegan bombas repetidas)
    """
    if not pumps or not isinstance(pumps, list):
        return pumps

    # Si ya hay != 1 bomba, no hacer nada
    if len(pumps) != 1:
        return pumps

    text = (fb_text or "").lower()

    # 1) "3 bombas", "3 metering pumps"
    pat_num = re.search(r"\b(\d+)\s*(bombas?|metering pumps?)\b", text)
    if pat_num:
        n = int(pat_num.group(1))
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # 2) "2+1", "3 + 0"
    pat_plus = re.search(r"\b(\d+)\s*\+\s*(\d+)\b", text)
    if pat_plus:
        a = int(pat_plus.group(1))
        b = int(pat_plus.group(2))
        n = a + b
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # 3) "dos bombas", "tres bombas"
    WORD2NUM = {
        "una": 1, "un": 1,
        "dos": 2,
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
    }
    pat_word = re.search(r"\b(una|un|dos|tres|cuatro|cinco|seis|siete)\s+bombas?\b", text)
    if pat_word:
        w = pat_word.group(1)
        n = WORD2NUM.get(w, 1)
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # 4) “dos bombas de operación y una de respaldo”
    pat_oper = re.search(
        r"\b(una|un|dos|tres|cuatro|cinco)\s+bombas?\s+de\s+operaci[oó]n\s+y\s+(una|un|dos|tres|cuatro|cinco)\b",
        text
    )
    if pat_oper:
        n1 = WORD2NUM.get(pat_oper.group(1), 1)
        n2 = WORD2NUM.get(pat_oper.group(2), 1)
        n = n1 + n2
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    return pumps


# ================================================================
# HELPERS BÁSICOS
# ================================================================

async def _safe_aquery(rag, question: str, query_param=None):
    """
    Wrapper seguro para rag.aquery().

    - Verifica que RAG exista
    - Soporta aquery async o sync (por si cambia implementación)
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

    if not hasattr(rag, "aquery"):
        raise RuntimeError("RAG no expone aquery()")

    fn = rag.aquery

    # async
    if inspect.iscoroutinefunction(fn):
        return await fn(question, query_param)

    # sync (por compatibilidad)
    return fn(question, query_param)


async def _load_rag(case_id: CaseId):
    """
    Carga el RAG construido por PC6/PC7 para un caso específico.
    """
    case_dir = Path(RAG_STORAGE_DIR) / f"case_{case_id}"
    if not case_dir.exists():
        raise RuntimeError(f"No existe storage del caso {case_id}: {case_dir}")

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

# --- Flow helpers (dual-unit equivalence / up-to detection) ---
_FLOW_PAIR_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(GPH|GPD|LPH|LPD)\b", re.IGNORECASE)

def _extract_flow_pairs(raw: str) -> List[Tuple[float, str]]:
    s = raw or ""
    pairs: List[Tuple[float, str]] = []
    for m in _FLOW_PAIR_RE.finditer(s):
        v = float(m.group(1).replace(",", "."))
        u = m.group(2).upper()
        pairs.append((v, u))
    return pairs

def _flow_to_gph(v: float, u: str) -> Optional[float]:
    u = (u or "").upper()
    if u == "GPH":
        return v
    if u == "GPD":
        return v / 24.0
    if u == "LPH":
        return v / 3.78541
    if u == "LPD":
        return (v / 24.0) / 3.78541
    return None

def _same_flow_dual_unit(a: Tuple[float, str], b: Tuple[float, str], rel_tol: float = 0.06) -> bool:
    ag = _flow_to_gph(a[0], a[1])
    bg = _flow_to_gph(b[0], b[1])
    if ag is None or bg is None:
        return False
    denom = max(abs(ag), abs(bg), 1e-9)
    return abs(ag - bg) / denom <= rel_tol


def _fix_pump_flow(pump: Dict[str, Any]) -> Dict[str, Any]:
    """
    Corrige de forma genérica el diccionario 'flow' de una bomba.

    Problema real (caso Ecopetrol típico):
    - Textos tipo: "Hasta 72 GPD (3 GPH)" traen 2 números que NO son rango.
      Son el MISMO caudal en 2 unidades. Si lo tratamos como min/max:
        min=72, max=3  -> luego el normalizer asume GPD para ambos y queda incoherente.

    Objetivos:
    - Mantener flow.raw EXACTO (no tocarlo).
    - NO inventar nominal.
    - Si detectamos equivalencia dual-unit (GPD/GPH, LPD/LPH), NO construir rango.
      Preferimos dejar SOLO max (para textos "hasta"/"up to") o un único valor.
    - Si el texto sugiere límite superior ("hasta", "up to", "máximo"), dejamos max y min=None.
    - Si NO hay nominal explícito y el texto sí es rango real (dos números en misma unidad),
      reconstruimos min/max cuando faltan.
    """
    flow = pump.get("flow") or {}
    if not isinstance(flow, dict):
        flow = {"min": None, "nominal": None, "max": None, "raw": None}

    raw_flow_text = flow.get("raw")
    if not isinstance(raw_flow_text, str) or not raw_flow_text.strip():
        pump["flow"] = flow
        return pump

    raw_lower = raw_flow_text.lower()

    # flags
    nominal_in_text = ("nominal" in raw_lower) or ("rated" in raw_lower)
    up_to_in_text = bool(re.search(r"\b(hasta|up to|máx\.?|max\.?|maximum)\b", raw_lower))

    # extracted numbers
    nums = _extract_numbers_from_text(raw_flow_text)
    pairs = _extract_flow_pairs(raw_flow_text)

    fmin = flow.get("min")
    fnom = flow.get("nominal")
    fmax = flow.get("max")

    # 1) Dual-unit equivalence: "72 GPD (3 GPH)" etc.
    if len(pairs) >= 2 and _same_flow_dual_unit(pairs[0], pairs[1]):
        # No es rango. Conservamos raw y dejamos un solo valor.
        flow["min"] = None
        flow["nominal"] = None if not nominal_in_text else fnom
        # Si el texto sugiere "hasta", tomamos el primer valor como max.
        # (No convertimos aquí: el normalizer usa raw para unit detection)
        flow["max"] = pairs[0][0]
        pump["flow"] = flow
        return pump

    # 2) "Hasta X ..." con un valor (o incluso varios números no-rango): tratar como máximo
    #    Nota: si hay explícito un rango con '-', lo dejamos para la lógica de rango.
    has_dash_range = bool(re.search(r"\d\s*[-–—]\s*\d", raw_flow_text))
    if up_to_in_text and not has_dash_range and nums:
        # Preferimos poner max y min None (evita falsos rangos).
        if fmax is None:
            flow["max"] = nums[0]
        flow["min"] = None
        if not nominal_in_text:
            flow["nominal"] = None
        pump["flow"] = flow
        return pump

    # 3) Rango real (sin nominal explícito): si hay >=2 números, reconstruir min/max si faltan, anular nominal.
    if not nominal_in_text and len(nums) >= 2:
        if fmin is None:
            flow["min"] = nums[0]
        if fmax is None:
            flow["max"] = nums[1]
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
    DISCOVERY PRIMARIO: descubre TAGs REALES del caso.
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
            return [str(t).strip() for t in tags if str(t).strip()]
    return []


async def _discover_pumps_secondary(rag) -> List[str]:
    """
    DISCOVERY SECUNDARIO: cuando no hay tags del discovery primario.
    """
    prompt = """
Lista TODOS los equipos dosificadores del paquete químico en formato JSON estricto:

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


async def _discover_skids_and_pump_tags(case_id: CaseId, rag) -> Dict[str, List[str]]:
    """
    Descubre mapeo SKID -> TAGS de BOMBA de forma determinística y GENÉRICA.

    Principios:
    - Fuente primaria: master_tables.csv (PC4) + corpus.jsonl (PC4/PC5) si existe.
    - No sobreajuste: NO asumimos formatos fijos tipo P-####; extraemos candidatos y
      elegimos la columna de "bomba" por estructura/frecuencia.
    - Guardrail anti-alucinación: si luego se usa LLM, solo puede escoger tags que ya
      existan literalmente en tablas/texto.
    - Salida: {skid_tag: [pump_tag1, pump_tag2, ...]} (solo strings no vacíos).
    """
    # 1) Intento determinístico (TABLAS)
    mapping = _discover_skid_pump_mapping_from_pc4(case_id)
    if mapping:
        return mapping

    # 2) Fallback determinístico (CORPUS texto) – útil si las tablas vienen rotas
    mapping = _discover_skid_pump_mapping_from_corpus(case_id)
    if mapping:
        return mapping

    # 3) Último recurso: LLM (pero con guardrails)
    #    - Solo tablas (como antes) y filtrando tags que existan en candidatos.
    candidates = _collect_tag_candidates_from_case(case_id)
    prompt = """
Usa SOLAMENTE información literal de TABLAS (no texto narrativo) en TODOS los documentos del caso.

Objetivo:
- Encontrar tablas tipo “LISTADO SKID ...” u otras equivalentes
- Extraer el mapeo completo: TAG SKID -> lista de TAG BOMBA

Devuelve JSON ESTRICTO:

{
  "skids": [
    {"skid_tag": "...", "pump_tags": ["...","..."]}
  ]
}

Reglas:
- NO inventes tags.
- Si un tag no existe literalmente en las tablas/texto, NO lo incluyas.
"""
    out: Dict[str, List[str]] = {}
    try:
        res = await _safe_aquery(rag, prompt, mode="ok-tables-all")
        data = json.loads(res) if isinstance(res, str) else res
        for rec in (data or {}).get("skids", []) if isinstance(data, dict) else []:
            skid = str(rec.get("skid_tag") or "").strip()
            pts = rec.get("pump_tags") or []
            if not skid or not isinstance(pts, list):
                continue
            clean = []
            for t in pts:
                ts = str(t).strip()
                if not ts:
                    continue
                # Guardrail: solo tags vistos
                if candidates and ts not in candidates:
                    continue
                if ts == skid:
                    continue
                clean.append(ts)
            if skid and clean:
                out[skid] = sorted(list(set(clean)))
    except Exception:
        return {}
    return out


# ================================================================
# DISCOVERY determinístico desde PC4 (master_tables.csv)
# ================================================================

_TAG_TOKEN_RE = re.compile(r'\b[A-Z0-9]{1,10}(?:-[A-Z0-9]{1,12}){1,8}\b')
_BOMBA_WORD_TAG_RE = re.compile(r'\b(BOMBA|PUMP)\s*[-]?\s*(\d+)\b', re.IGNORECASE)

def _safe_upper(s: Any) -> str:
    try:
        return str(s).strip()
    except Exception:
        return ""

def _extract_tag_tokens(text: str) -> List[str]:
    """
    Extrae tokens tipo TAG de un texto. Genérico: no asume prefijos fijos.
    Reglas anti-ruido:
    - Debe contener al menos un dígito (para evitar palabras comunes).
    - Longitud razonable.
    """
    if not text:
        return []
    t = _safe_upper(text).upper()

    tokens = set()
    for m in _TAG_TOKEN_RE.finditer(t):
        tok = m.group(0).strip().upper()
        if any(ch.isdigit() for ch in tok) and 3 <= len(tok) <= 30:
            tokens.add(tok)

    # BOMBA 1 / PUMP 2 -> BOMBA-1
    for m in _BOMBA_WORD_TAG_RE.finditer(t):
        tokens.add(f"{m.group(1).upper()}-{m.group(2)}")

    # Filtrar falsos positivos obvios (documentos / normas)
    bad_prefixes = ("DNC-", "SER-", "EDP-", "API-", "ISO-", "IEC-")
    tokens = {x for x in tokens if not x.startswith(bad_prefixes)}

    return sorted(tokens)

def _parse_pressure_pair(text: str) -> Optional[Dict[str, Any]]:
    """
    Detecta presión tipo "2000 / 1800" o "500/450" (psig implícito en tabla).
    Devuelve {"max": int/float, "min": int/float, "raw": "..."} o None.
    """
    if not text:
        return None
    s = _safe_upper(text)
    # encontrar pares con "/"
    m = re.search(r'(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', s)
    if m:
        a = float(m.group(1)); b = float(m.group(2))
        mx, mn = (a, b) if a >= b else (b, a)
        # preferir int si es entero
        def to_num(x):
            return int(x) if abs(x - int(x)) < 1e-9 else x
        return {"max": to_num(mx), "min": to_num(mn), "raw": s.strip()}
    # single number
    m2 = re.search(r'\b(\d+(?:\.\d+)?)\b', s)
    if m2:
        v = float(m2.group(1))
        v = int(v) if abs(v - int(v)) < 1e-9 else v
        return {"max": v, "min": None, "raw": s.strip()}
    return None

def _find_case_outputs_dir(case_id: CaseId) -> Optional[Path]:
    """
    Encuentra outputs/cases/<case_id> en layouts típicos (Render/WSL).
    """
    env = os.getenv("RAG_CASE_OUTPUTS_DIR", "").strip()
    candidates: List[Path] = []
    if env:
        candidates.append(Path(env))
        candidates.append(Path(env) / str(case_id))
        candidates.append(Path(env) / f"{case_id}")

    try:
        rs = Path(RAG_STORAGE_DIR).resolve()
        # .../raggrafo/rag_storage -> .../raggrafo/outputs/cases/<id>
        candidates.append(rs.parent / "outputs" / "cases" / str(case_id))
        candidates.append(rs.parent / "outputs" / "cases" / f"{case_id}")
        candidates.append(rs.parent.parent / "outputs" / "cases" / str(case_id))
    except Exception:
        pass

    for p in candidates:
        try:
            if p and p.exists():
                return p
        except Exception:
            continue
    return None

def _discover_skid_pump_mapping_from_pc4(case_id: CaseId) -> Dict[str, List[str]]:
    """
    Construye mapping SKID -> [pump_tags...] desde master_tables.csv sin LLM.
    Detecta tablas por estructura (columnas con tags largos vs tags cortos).
    """
    out: Dict[str, List[str]] = {}
    case_out = _find_case_outputs_dir(case_id)
    if not case_out:
        return out

    # buscar master_tables.csv en cualquier subcarpeta (pc4, pc4_merged_tables, etc.)
    mt_path = None
    for p in case_out.rglob("master_tables.csv"):
        mt_path = p
        break
    if not mt_path or not mt_path.exists():
        return out

    try:
        df = pd.read_csv(mt_path)
    except Exception:
        return out

    ccols = [c for c in df.columns if re.match(r"^c\d+$", str(c))]
    if not ccols:
        return out

    # Procesar por tabla_uid
    for table_uid, g in df.groupby("table_uid", dropna=False):
        g2 = g.sort_values(by=["_page", "_table_idx", "_row"], kind="stable")
        # construir columna score
        col_stats = {}
        for c in ccols:
            vals = [_safe_upper(v) for v in g2[c].tolist()]
            # tokens por celda
            tok_lists = [ _extract_tag_tokens(v) for v in vals ]
            tok_count = sum(len(tl) for tl in tok_lists)
            uniq = len(set([t for tl in tok_lists for t in tl]))
            avg_len = np.mean([len(t) for tl in tok_lists for t in tl]) if tok_count else 0
            # señales de skid
            skid_hits = sum(1 for v in vals if any(k in v.upper() for k in ["INY", "QUIM", "SKID", "LOC"]))
            col_stats[c] = {"tok_count": tok_count, "uniq": uniq, "avg_len": avg_len, "skid_hits": skid_hits}

        # elegir skid_col: muchos tokens, avg_len alto, o hits de skid
        skid_col = max(col_stats.keys(), key=lambda c: (col_stats[c]["skid_hits"], col_stats[c]["avg_len"], col_stats[c]["tok_count"])) if col_stats else None
        if not skid_col:
            continue

        # elegir pump_col: tokens pero menos "skid_hits" y/o menor avg_len, no igual skid_col
        pump_candidates = [c for c in ccols if c != skid_col and col_stats[c]["tok_count"] > 0]
        if not pump_candidates:
            continue
        pump_col = max(pump_candidates, key=lambda c: (col_stats[c]["tok_count"], -col_stats[c]["skid_hits"], -col_stats[c]["avg_len"]))

        # intentar identificar una columna de presión: contiene pares con "/"
        press_col = None
        best_press = 0
        for c in ccols:
            vals = [_safe_upper(v) for v in g2[c].tolist()]
            hits = sum(1 for v in vals if re.search(r'\d+\s*/\s*\d+', v))
            if hits > best_press:
                best_press = hits
                press_col = c
        # columna de notas/servicio (si existe) – usar la más "textual"
        note_col = None
        note_best = 0
        for c in ccols:
            vals = [_safe_upper(v) for v in g2[c].tolist()]
            # ratio de letras
            score = sum(1 for v in vals if len(v) >= 8 and re.search(r'[A-Z]', v.upper()))
            if score > note_best:
                note_best = score
                note_col = c

        # recorrer filas
        last_ctx_by_skid: Dict[str, Dict[str, Any]] = {}
        for _, row in g2.iterrows():
            skid_tokens = _extract_tag_tokens(row.get(skid_col, ""))
            pump_tokens = _extract_tag_tokens(row.get(pump_col, ""))

            if not skid_tokens or not pump_tokens:
                continue

            # Heurística: skid = token más largo en skid_tokens
            skid = max(skid_tokens, key=len).strip()
            if not skid:
                continue

            # presión
            pinfo = _parse_pressure_pair(row.get(press_col, "")) if press_col else None
            # notas
            note = _safe_upper(row.get(note_col, "")) if note_col else ""

            ctx = last_ctx_by_skid.get(skid, {})
            if pinfo:
                ctx["discharge_pressure_raw"] = pinfo["raw"]
                ctx["discharge_pressure_max"] = pinfo["max"]
                ctx["discharge_pressure_min"] = pinfo["min"]
            if note:
                ctx["note"] = note
            last_ctx_by_skid[skid] = ctx

            # limpiar tokens de bomba: no debe incluir skid
            clean = [t for t in pump_tokens if t and t != skid]
            if not clean:
                continue
            out.setdefault(skid, [])
            out[skid].extend(clean)

        # dedup final por skid
    for skid, pts in list(out.items()):
        out[skid] = sorted(list(dict.fromkeys([p.strip() for p in pts if p.strip()])))
    return out

def _discover_skid_pump_mapping_from_corpus(case_id: CaseId) -> Dict[str, List[str]]:
    """
    Fallback: intenta encontrar líneas tipo "SKIDTAG ... PUMP_TAG" dentro de corpus.jsonl.
    No asume formatos de tag; usa heurística por co-ocurrencia en misma línea.
    """
    out: Dict[str, List[str]] = {}
    case_out = _find_case_outputs_dir(case_id)
    if not case_out:
        return out
    corpus_path = None
    for p in case_out.rglob("corpus.jsonl"):
        corpus_path = p
        break
    if not corpus_path or not corpus_path.exists():
        return out

    try:
        with open(corpus_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                txt = _safe_upper(obj.get("text", ""))
                if not txt:
                    continue
                # buscamos co-ocurrencias de múltiples tags en una misma línea
                for ln in txt.splitlines():
                    toks = _extract_tag_tokens(ln)
                    if len(toks) < 2:
                        continue
                    # elegir skid como el más largo (suele ser el sistema/skid)
                    skid = max(toks, key=len)
                    pumps = [t for t in toks if t != skid]
                    if pumps:
                        out.setdefault(skid, [])
                        out[skid].extend(pumps)
    except Exception:
        return {}

    for skid, pts in list(out.items()):
        out[skid] = sorted(list(dict.fromkeys([p.strip() for p in pts if p.strip()])))
    return out

def _collect_tag_candidates_from_case(case_id: CaseId) -> Optional[set]:
    """
    Colecciona un conjunto de tags candidatos vistos en tablas/corpus.
    Sirve para guardrails de LLM (no inventar).
    """
    cand = set()
    m1 = _discover_skid_pump_mapping_from_pc4(case_id)
    for skid, pts in m1.items():
        cand.add(skid); cand.update(pts)
    m2 = _discover_skid_pump_mapping_from_corpus(case_id)
    for skid, pts in m2.items():
        cand.add(skid); cand.update(pts)
    return cand if cand else None

# ================================================================
# EXTRAER UNA BOMBA POR TAG (Método A + Método B)
# ================================================================

def _looks_like_valid_single_pump_dict(d: Dict[str, Any]) -> bool:
    """
    Valida si el dict parece realmente un JSON de bomba (y no un dict wrapper tipo {"text": "..."}).
    """
    if not isinstance(d, dict):
        return False

    # Debe tener al menos flow dict o discharge_pressure o viscosity o fluid.
    has_any = any(k in d for k in ("flow", "discharge_pressure", "viscosity", "fluid", "optional"))
    if not has_any:
        return False

    flow = d.get("flow")
    if flow is not None and not isinstance(flow, dict):
        return False

    # Si solo viene "text" y nada más, no sirve
    only_text = (set(d.keys()) <= {"text", "answer"})
    if only_text:
        return False

    return True


async def _extract_single_by_tag(rag, tag: str, skid_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    EXTRAER UNA BOMBA COMPLETA POR TAG

    Método A (principal): prompt_json_single de CFG.EXTRACT_CONFIG
    Método B (fallback): modo "engineering" de CFG.MODES

    Además:
    - Adjunta campos *_raw con pistas de unidades detectadas.
    - Aplica corrección industrial de flow (via _fix_pump_flow).
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

    if isinstance(safe_a, dict) and _looks_like_valid_single_pump_dict(safe_a):
        raw_text = safe_a.get("text")
        if raw_text is None:
            raw_text = str(resp)

        flow = safe_a.get("flow", {}) or {}
        if not isinstance(flow, dict):
            flow = {}

        raw_flow_text = flow.get("raw")
        if not isinstance(raw_flow_text, str) or not raw_flow_text.strip():
            raw_flow_text = raw_text

        optional = safe_a.get("optional", {})
        if not isinstance(optional, dict):
            optional = {}
        # Enforce TAG (no debe ser skid) y anexar skid_tag al service sin cambiar schema.
        optional["tag"] = tag
        if skid_tag:
            svc = str(optional.get("service") or "").strip()
            skid_txt = f"Skid {skid_tag}"
            if skid_txt not in svc:
                optional["service"] = (svc + (" | " if svc else "") + skid_txt).strip()

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
            "optional": optional,
            "flow_nominal_raw": _extract_unit(FLOW_PATTERN, raw_flow_text),
            "pressure_raw": _extract_unit(PRESSURE_PATTERN, raw_text),
            "viscosity_raw": _extract_unit(VISC_PATTERN, raw_text),
            "temperature_raw": _extract_unit(TEMP_PATTERN, raw_text),
            "density_raw": _extract_unit(DENS_PATTERN, raw_text),
        }

        # asegurar tag siempre
        pump["optional"]["tag"] = pump["optional"].get("tag") or tag

        return _fix_pump_flow(pump)

    # --------------------------
    # Método B — Fallback "engineering"
    # --------------------------
    if CFG is None or not hasattr(CFG, "MODES"):
        raise RuntimeError("rag_config.MODES no está disponible")

    eng_mode = CFG.MODES.get("engineering", {}) or {}
    eng_qp = eng_mode.get("query_param")

    eng = await _safe_aquery(rag, f"Describe técnicamente los datos de proceso de {tag}", eng_qp)
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

    pumps: List[Dict[str, Any]] = []
    if isinstance(safe, dict) and isinstance(safe.get("pumps"), list):
        for p in safe["pumps"]:
            if isinstance(p, dict):
                pumps.append(_fix_pump_flow(p))
    return pumps


# ================================================================
# MULTIPASO 80/20 (PIPELINE INDUSTRIAL)
# ================================================================

async def _run_extract_multipaso(case_id: int, rag, question: str, mode: str) -> Dict[str, Any]:
    """
    Orquestador industrial 80/20:

    1) Discovery primario de TAGs reales
    2) Discovery secundario (clasificador por equipo)
    2.5) Discovery por TABLAS: SKID -> TAG BOMBA
    3) Tablas usando la pregunta del usuario
    4) Tablas genéricas (consulta "all bombs")
    5) Fallback JSON LIST según CFG.MODES[mode]
    """

    # 1 — TAGs REALES
    tags = await _discover_pumps_primary(rag)
    print(f"[DISCOVERY] primary tags count={len(tags)} head={tags[:10]}")

    if tags:
        pumps: List[Dict[str, Any]] = []
        for t in tags:
            pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(t) if 'pump_to_skid' in locals() else None))
        return {"pumps": pumps, "notes": "ok-primary"}

    # 2 — TAGs secundarios
    tags2 = await _discover_pumps_secondary(rag)
    print(f"[DISCOVERY] secondary tags count={len(tags2)} head={tags2[:10]}")

    if tags2:
        pumps: List[Dict[str, Any]] = []
        for t in tags2:
            pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(t) if 'pump_to_skid' in locals() else None))
        return {"pumps": pumps, "notes": "ok-secondary"}

    # 2.5 — Discovery por TABLAS: SKID -> TAG BOMBA
    skid_map = await _discover_skids_and_pump_tags(case_id, rag)
    print(f"[DISCOVERY] skid_map count={len(skid_map)}")
    if skid_map:
        sample = list(skid_map.items())[:3]
        print(f"[DISCOVERY] skid_map sample={sample}")

        pump_tags: List[str] = []
        pump_to_skid: Dict[str, str] = {}
        for skid, pts in skid_map.items():
            for pt in (pts or []):
                if not pt:
                    continue
                pump_tags.append(pt)
                # si un tag aparece en varios skids, conservamos el primero (estable)
                pump_to_skid.setdefault(pt, skid)

        pump_tags = sorted(list({t for t in pump_tags if t}))
        print(f"[DISCOVERY] pump_tags total={len(pump_tags)} head={pump_tags[:15]}")

        if pump_tags:
            pumps: List[Dict[str, Any]] = []
            for t in pump_tags:
                pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(t) if 'pump_to_skid' in locals() else None))
            return {"pumps": pumps, "notes": "ok-skid-map"}

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

    canonical_q = (
        "Listado completo de bombas dosificadoras del paquete químico con todos "
        "sus datos de proceso (caudal mínimo, nominal y máximo, presión, "
        "temperatura, viscosidad, servicio y TAG)."
    )

    fb = await _safe_aquery(rag, fb_prompt + f"\n\nPregunta:\n{canonical_q}", fb_qp)
    safe_fb = repair_and_parse(fb)

    if isinstance(safe_fb, dict) and isinstance(safe_fb.get("pumps"), list):
        return {
            "pumps": safe_fb["pumps"],
            "notes": "fallback-A",
            "_fallback_text": fb,
        }

    return {
        "pumps": [],
        "notes": "fallback-invalid",
        "_fallback_text": fb,
    }


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
        result = await _run_extract_multipaso(case_id, rag, question, mode)

        # 1) Postproceso industrial obligatorio (flow fix)
        result = _postprocess_result(result)

        # 2) Postproceso opcional: duplicar si fallback-text dice múltiples
        #    (solo aplica si venimos de fallback y el JSON devolvió 1 bomba)
        if isinstance(result, dict) and isinstance(result.get("pumps"), list):
            fb_text = result.get("_fallback_text") or ""
            # Guardrail anti-alucinación:
            # Solo ajustar conteo por texto si NO tenemos tags explícitos.
            has_explicit_tags = any(
                isinstance(p, dict) and (p.get("optional") or {}).get("tag")
                for p in (result.get("pumps") or [])
            )
            if (not has_explicit_tags) and fb_text:
                result["pumps"] = apply_pump_count_postprocessor(result["pumps"], fb_text)

        if return_raw_dict:
            return result

        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": result,
            "error": None,
        }

    # Modos de texto libre (naive, mix, combo, engineering, verify, etc.)
    query_param = None
    if CFG is not None and hasattr(CFG, "MODES"):
        mode_cfg = CFG.MODES.get(mode) or {}
        query_param = mode_cfg.get("query_param")

    out = await _safe_aquery(rag, question, query_param)

    # Normalizamos la salida a texto + raw + error=None
    if isinstance(out, dict):
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


if __name__ == "__main__":
    ap = __import__("argparse").ArgumentParser()
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
