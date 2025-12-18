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
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Tuple

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)
from .json_repair import repair_and_parse


# ================================================================
# DETECCIÓN DETERMINÍSTICA DE TAGs (sin LLM)
# ================================================================
# Objetivo: maximizar recall de bombas SIN depender de que el LLM "entienda tablas"
# Fuente primaria: pc4/corpus.jsonl + master_tables.csv (si existen en outputs del caso)
# Fallback: discovery por LLM (métodos existentes) SOLO si no se encuentran tags.

TAG_TOKEN_RE = re.compile(r"\b[A-Z0-9]{1,10}(?:[-/][A-Z0-9]{1,10}){1,6}\b")
PUMP_HINT_RE = re.compile(r"\b(BOMBA|BOMBAS|DOSIFIC|DOSIFICAD|METERING|PUMP|DOSING|INJECTION PUMP)\b", re.I)
NON_PUMP_HINT_RE = re.compile(r"\b(TANQUE|TANK|TK\b|TNK|VALV|VÁLV|LINEA|LINE|PIPING|TUBER|FILTRO|FILTER)\b", re.I)


BLACKLIST_SEGMENTS = {
    "HD", "MR", "ET", "EDP", "ECP", "IEC", "ISO", "ASTM", "NEMA", "API", "ATEX",
    "DOC", "REV", "VERSION",
}

def _looks_like_doc_or_standard(token: str) -> bool:
    t = token.upper()
    if not t:
        return True
    # cosas tipo 220-127V, 94/9/EC
    if t[0].isdigit():
        return True
    if re.match(r"\d{2,4}/\d{1,2}/[A-Z]{1,4}$", t):
        return True
    parts = re.split(r"[-/]", t)
    if any(p in BLACKLIST_SEGMENTS for p in parts):
        return True
    # tokens de 2 segmentos: solo aceptamos si se parecen a tags de equipo (P-#### o M156CIP-####, etc.)
    if len(parts) == 2:
        if parts[0] == "P":
            return False
        if re.match(r"^[A-Z]\d{2,4}[A-Z]{0,4}$", parts[0]):  # M156CIP
            return False
        if parts[0].startswith("M") and re.search(r"\d", parts[0]):
            return False
        # si llega aquí, suele ser código de paquete/documento (DNC-00001, QUIM-TN05, etc.)
        return True
    # muchos estándares tienen patrón XXX-XXX-XXX (todo uppercase corto) y no parecen tags de equipo
    if len(parts) >= 3 and all(re.match(r"^[A-Z]{2,6}\d*$", p) for p in parts):
        return True
    return False

def _case_outputs_root() -> Path:
    """
    Intenta localizar outputs/cases de forma robusta.
    - Env: RAG_CASE_OUTPUTS_DIR
    - Default relativo al repo: raggrafo/outputs/cases
    """
    env = os.getenv("RAG_CASE_OUTPUTS_DIR")
    if env:
        return Path(env)
    # Heurística: .../raggrafo/pipelines/ -> .../raggrafo/outputs/cases
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if p.name == "raggrafo":
            return p / "outputs" / "cases"
    # fallback: cwd
    return Path("raggrafo") / "outputs" / "cases"

def _find_case_artifact(case_id: int, pattern: str) -> Optional[Path]:
    root = _case_outputs_root() / str(case_id)
    if not root.exists():
        return None
    hits = list(root.rglob(pattern))
    if not hits:
        return None
    # elegimos el más grande (suele ser el "real")
    hits.sort(key=lambda p: p.stat().st_size, reverse=True)
    return hits[0]

def _load_case_corpus_text(case_id: int) -> Tuple[str, List[str]]:
    """
    Retorna texto concatenado de corpus + lista de hashes para salt determinístico.
    """
    corpus_path = _find_case_artifact(case_id, "corpus.jsonl")
    if corpus_path is None:
        return "", []
    texts = []
    hashes = []
    try:
        with corpus_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                t = obj.get("text") or ""
                if t:
                    texts.append(t)
                h = obj.get("hash")
                if h:
                    hashes.append(str(h)[:16])
    except Exception:
        return "", []
    return "\n\n".join(texts), hashes

def _load_master_tables(case_id: int) -> Optional[pd.DataFrame]:
    path = _find_case_artifact(case_id, "master_tables.csv")
    if path is None:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def _extract_tag_tokens(text: str) -> List[str]:
    if not text:
        return []
    up = text.upper()
    out = []
    for m in TAG_TOKEN_RE.finditer(up):
        tok = m.group(0)
        if not (re.search(r"[A-Z]", tok) and re.search(r"\d", tok)):
            continue
        # excluye fechas tipo 20/11/2017
        if re.match(r"\d{1,2}/\d{1,2}/\d{2,4}$", tok):
            continue
        out.append(tok)
    return out

def _score_tag_context(text: str, tag: str, window: int = 90) -> int:
    """score simple: +2 pump-hint, -2 non-pump-hint, +1 si contiene INY/QUIM"""
    tag = tag.upper()
    score = 0
    if "INY" in tag or "QUIM" in tag:
        score += 1
    # buscamos ocurrencias con contexto
    for m in re.finditer(re.escape(tag), text.upper()):
        a = max(0, m.start() - window)
        b = min(len(text), m.end() + window)
        ctx = text[a:b]
        if PUMP_HINT_RE.search(ctx):
            score += 2
        if NON_PUMP_HINT_RE.search(ctx):
            score -= 2
    return score

def _deterministic_discover_pump_tags(case_id: int, debug: bool = True) -> Tuple[List[str], Dict[str, Any]]:
    """
    Devuelve lista de TAGs candidatos a bombas (alta cobertura) + métricas debug.
    """
    corpus_text, hashes = _load_case_corpus_text(case_id)
    df = _load_master_tables(case_id)

    candidates = set(_extract_tag_tokens(corpus_text))

    # también desde tablas (fila completa)
    if df is not None and not df.empty:
        # columnas c1..c50
        ccols = [c for c in df.columns if c.startswith("c")]
        for _, row in df[ccols].iterrows():
            s = " ".join(str(x) for x in row.values if isinstance(x, str) or (isinstance(x, (int,float)) and not pd.isna(x)))
            for tok in _extract_tag_tokens(s):
                candidates.add(tok)

    # scoring
    scored = []
    for t in candidates:
        t_up = t.upper()
        sc = _score_tag_context(corpus_text, t_up)

        # Inclusión por patrones fuertes (alta prioridad)
        strong = (
            t_up.startswith("P-") or
            t_up.startswith("M-INY-QUIM-") or
            re.match(r"^[A-Z]\d{2,4}[A-Z]{0,4}-\d{3,6}$", t_up)  # ej: M156CIP-5503
        )

        if _looks_like_doc_or_standard(t_up) and not strong:
            continue

        # heurística final: requerimos evidencia de "bomba" en contexto, salvo strong
        if strong or sc >= 3:
            scored.append((sc, t_up))
    scored.sort(key=lambda x: (-x[0], x[1]))

    tags = [t for _, t in scored]
    dbg = {
        "case_id": case_id,
        "corpus_hashes": hashes,
        "candidates_total": len(candidates),
        "selected_total": len(tags),
        "selected_head": tags[:20],
    }
    if debug:
        print(f"[DET] corpus_hashes={hashes[:3]} candidates={len(candidates)} selected={len(tags)} head={tags[:10]}")
    return tags, dbg

def _salt_from_hashes(case_id: int, hashes: List[str]) -> str:
    if not hashes:
        return f"[CASE_SALT:{case_id}]"
    return f"[CASE_SALT:{case_id}:{'-'.join(hashes[:4])}]"


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

def _fix_pump_flow(pump: Dict[str, Any]) -> Dict[str, Any]:
    """
    Corrige de forma genérica el diccionario 'flow' de una bomba.

    Objetivo:
    - Si el texto RAW menciona min/max pero NO "nominal":
        * Reconstruimos min y max desde los números del texto (si faltan)
        * Anulamos nominal si vino inventado
    """
    flow = pump.get("flow") or {}
    if not isinstance(flow, dict):
        flow = {"min": None, "nominal": None, "max": None, "raw": None}

    raw_flow_text = flow.get("raw")
    if not isinstance(raw_flow_text, str) or not raw_flow_text.strip():
        pump["flow"] = flow
        return pump

    raw_lower = raw_flow_text.lower()
    nums = _extract_numbers_from_text(raw_flow_text)

    nominal_in_text = ("nominal" in raw_lower) or ("rated" in raw_lower)

    fmin = flow.get("min")
    fnom = flow.get("nominal")
    fmax = flow.get("max")

    # Regla industrial:
    # si NO se menciona nominal y hay >=2 números, reconstruir min/max si faltan y anular nominal.
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


async def _discover_skids_and_pump_tags(rag) -> Dict[str, List[str]]:
    prompt = """
Usa SOLAMENTE información literal de TABLAS (no texto narrativo) en TODOS los documentos del caso.

Objetivo:
- Encontrar tablas tipo “LISTADO SKID DE INYECCIÓN DE QUÍMICA” u otras equivalentes
- Extraer el mapeo completo: TAG SKID -> lista de TAG BOMBA

Devuelve JSON ESTRICTO:

{
  "skids": [
    {"skid_tag": "M-INY-QUIM-LOC05", "pump_tags": ["P-58","P-64","P-65"]},
    {"skid_tag": "M-INY-QUIM-CB06", "pump_tags": ["CB-16","CB-17"]}
  ]
}

Reglas duras:
- pump_tags SOLO puede contener tags con estos patrones (tal cual aparecen):
  • P-<número>  (ej: P-58, P-121)
  • CB-<número> (ej: CB-16)
  • DT-<número> (ej: DT-194)
  • PG-<texto/número> (ej: PG-05R, PG-23)
  • LOC-<texto/número> (ej: LOC-05, LOC-8E)
- skid_tag debe ser el TAG SKID literal de la tabla (ej: M-INY-QUIM-...).
- No inventes tags. Si un skid no tiene bombas claras, pon pump_tags=[].
- Debe cubrir TODOS los skids listados en la tabla (si existe).
- Si no encuentras ninguna tabla con ambos campos, devuelve: {"skids": []}.
"""
    resp = await _safe_aquery(rag, prompt)
    safe = repair_and_parse(resp)

    out: Dict[str, List[str]] = {}
    if isinstance(safe, dict) and isinstance(safe.get("skids"), list):
        for rec in safe["skids"]:
            if not isinstance(rec, dict):
                continue
            skid = str(rec.get("skid_tag") or "").strip()
            pts = rec.get("pump_tags") or []
            if skid and isinstance(pts, list):
                clean = [str(t).strip() for t in pts if str(t).strip()]
                # Filtrar si el tag skid aparece también en la lista de bombas
                clean = [tag for tag in clean if tag and tag != skid]
                if clean:
                    out[skid] = sorted(list(set(clean)))
    return out


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


async def _extract_single_by_tag(rag, tag: str, salt: str = "") -> Dict[str, Any]:
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
    if salt:
        q = q + "\n\n" + salt

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

    # 1 — TAGs DETERMINÍSTICOS (pc4/corpus + master_tables)
    det_tags, det_dbg = _deterministic_discover_pump_tags(case_id, debug=True)
    corpus_text, hashes = _load_case_corpus_text(case_id)
    salt = _salt_from_hashes(case_id, hashes)

    tags_all: List[str] = []
    if det_tags:
        tags_all.extend(det_tags)

    # 2 — (Opcional) Discovery por LLM SOLO si no hallamos tags determinísticos
    if not tags_all:
        tags_llm = await _discover_pumps_primary(rag)
        print(f"[DISCOVERY] primary tags count={len(tags_llm)} head={tags_llm[:10]}")
        tags_all.extend(tags_llm)

        tags_llm2 = await _discover_pumps_secondary(rag)
        print(f"[DISCOVERY] secondary tags count={len(tags_llm2)} head={tags_llm2[:10]}")
        tags_all.extend(tags_llm2)

    # 2.5 — Discovery por TABLAS: SKID -> TAG BOMBA (suma, no reemplaza)
    skid_map = await _discover_skids_and_pump_tags(rag)
    print(f"[DISCOVERY] skid_map count={len(skid_map)}")
    if skid_map:
        sample = list(skid_map.items())[:3]
        print(f"[DISCOVERY] skid_map sample={sample}")
        for _, pump_tags in skid_map.items():
            tags_all.extend([t for t in pump_tags if t])

    # Normaliza / dedupe
    seen = set()
    tags_all_norm: List[str] = []
    for t in tags_all:
        t = (t or "").strip()
        if not t:
            continue
        t_up = t.upper()
        if t_up in seen:
            continue
        seen.add(t_up)
        tags_all_norm.append(t_up)

    print(f"[DISCOVERY] tags_final count={len(tags_all_norm)} head={tags_all_norm[:12]}")

    if tags_all_norm:
        pumps: List[Dict[str, Any]] = []
        for t in tags_all_norm:
            pumps.append(await _extract_single_by_tag(rag, t, salt=salt))
        return {"pumps": pumps, "notes": "ok-tags-deterministic" if det_tags else "ok-tags-llm"}

    # 3 — Tablas genéricas (fallback SOLO si no hubo tags)
    tbl2 = await _extract_from_tables(
        rag,
        "Listado completo de bombas dosificadoras del paquete químico con todos "
        "sus datos de proceso (caudal mínimo, nominal y máximo, presión, "
        "temperatura, viscosidad, servicio y TAG).\n\n" + salt,
    )
    if tbl2:
        return {"pumps": tbl2, "notes": "ok-tables-generic"}

    # 4 — Fallback JSON LIST (modo extract / extract-list)
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
        result = await _run_extract_multipaso(int(case_id), rag, question, mode)

        # 1) Postproceso industrial obligatorio (flow fix)
        result = _postprocess_result(result)

        # 2) Postproceso opcional: duplicar si fallback-text dice múltiples
        #    (solo aplica si venimos de fallback y el JSON devolvió 1 bomba)
        if isinstance(result, dict) and isinstance(result.get("pumps"), list):
            fb_text = result.get("_fallback_text") or ""
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
