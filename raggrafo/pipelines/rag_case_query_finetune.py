# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (INDUSTRIAL + 80/20 LISTA COMPLETA) – v4.7
====================================================================

Fixes principales:
- ✅ Group-safe: elimina `IndexError: no such group` en _extract_unit.
- ✅ Discovery mejorado: soporta tags tipo "1 M-INY-QUIM-LOC05" (con índice + tag),
  tags con saltos de línea, y tags alfanuméricos largos.
- ✅ Fallback determinístico adicional: si NO existe outputs/cases/<id> (PC4/PC5),
  intenta extraer tags directamente desde PDFs del caso (shared_data/inbox, rag_storage, etc.)
- ✅ No limita artificialmente cantidad de bombas: si detecta N tags, extrae N.

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
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Iterable

import pandas as pd
import numpy as np

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)
from .json_repair import repair_and_parse

try:
    from raggrafo.pipelines import rag_config as CFG
except Exception:  # pragma: no cover
    CFG = None

CaseId = Union[int, str]

# ================================================================
# POSTPROCESSOR (duplicación por fallback-text)
# ================================================================

def apply_pump_count_postprocessor(pumps: list, fb_text: str) -> list:
    """
    Duplica bombas cuando el fallback detecta 1 pero el texto menciona múltiples.
    Guardrail: SOLO duplica si hay exactamente 1 bomba.
    """
    if not pumps or not isinstance(pumps, list):
        return pumps
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

    # 2) "2+1"
    pat_plus = re.search(r"\b(\d+)\s*\+\s*(\d+)\b", text)
    if pat_plus:
        n = int(pat_plus.group(1)) + int(pat_plus.group(2))
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    # 3) "dos bombas", "tres bombas"
    WORD2NUM = {"una": 1, "un": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6, "siete": 7, "ocho": 8, "nueve": 9}
    pat_word = re.search(r"\b(una|un|dos|tres|cuatro|cinco|seis|siete|ocho|nueve)\s+bombas?\b", text)
    if pat_word:
        n = WORD2NUM.get(pat_word.group(1), 1)
        if n > 1:
            base = pumps[0]
            return [copy.deepcopy(base) for _ in range(n)]

    return pumps

# ================================================================
# HELPERS BÁSICOS
# ================================================================

async def _safe_aquery(rag, question: str, query_param=None, **kwargs):
    """Wrapper seguro para rag.aquery() (async o sync)."""
    if rag is None:
        raise RuntimeError("RAG no inicializado")

    # kwargs ignorados (compatibilidad futura)
    if query_param is None and CFG is not None and hasattr(CFG, "MODES"):
        default_cfg = CFG.MODES.get("naive") or CFG.MODES.get("engineering") or {}
        qp = default_cfg.get("query_param")
        if qp is not None:
            query_param = qp

    if not hasattr(rag, "aquery"):
        raise RuntimeError("RAG no expone aquery()")

    fn = rag.aquery
    if inspect.iscoroutinefunction(fn):
        return await fn(question, query_param)
    return fn(question, query_param)

async def _load_rag(case_id: CaseId):
    """Carga el RAG construido por PC6/PC7 para un caso específico."""
    case_dir = Path(RAG_STORAGE_DIR) / f"case_{case_id}"
    if not case_dir.exists():
        raise RuntimeError(f"No existe storage del caso {case_id}: {case_dir}")

    rag = _make_rag(case_dir)

    try:
        if inspect.iscoroutinefunction(_core_initialize):
            await _core_initialize(rag)
        else:
            _core_initialize(rag)
    except Exception:
        pass

    return rag

# ================================================================
# REGEX de unidades (pistas adicionales RAW)
# ================================================================

FLOW_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(GPD|GPH|LPH|LPD|m3/d|BPD|LPM)\b", re.IGNORECASE)
PRESSURE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(psig|psi|bar|kg/cm2)\b", re.IGNORECASE)
TEMP_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*(°F|°C|F|C)\b", re.IGNORECASE)
VISC_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(cP|cSt)\b", re.IGNORECASE)
DENS_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(kg/m3|g/cm3)\b", re.IGNORECASE)

def _extract_unit(pattern, text: str) -> Dict[str, Any]:
    """
    Extrae valor + unidad del texto usando un patrón regex.

    FIX (group-safe):
    - Evita `IndexError: no such group` cuando el patrón NO tiene group(3).
    - Soporta patrones con grupos extra.
    """
    if not text:
        return {"value": None, "unit": None}

    m = pattern.search(text)
    if not m:
        return {"value": None, "unit": None}

    try:
        value = float(m.group(1).replace(",", "."))
    except Exception:
        return {"value": None, "unit": None}

    unit = None
    try:
        if getattr(m, "lastindex", 0) >= 2:
            unit = m.group(m.lastindex)
    except Exception:
        unit = None

    return {"value": value, "unit": unit}

def _extract_numbers_from_text(text: str) -> List[float]:
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
    Corrige el dict 'flow' de una bomba:
    - Si el texto RAW menciona min/max pero NO "nominal":
        * Reconstruye min y max desde los números del texto (si faltan)
        * Anula nominal si vino inventado
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
    pumps = result.get("pumps") or []
    fixed: List[Dict[str, Any]] = []
    for p in pumps:
        fixed.append(_fix_pump_flow(p) if isinstance(p, dict) else p)
    result["pumps"] = fixed
    return result

# ================================================================
# DISCOVERY DE TAGS (LLM)
# ================================================================

async def _discover_pumps_primary(rag) -> List[str]:
    prompt = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID).

Tu tarea es ENCONTRAR TODAS las bombas dosificadoras de químicos del paquete
(incluyendo bombas en paralelo, stand-by, redundantes, etc.).

Devuelve SOLO los TAGs de esas bombas en este formato JSON ESTRICTO:

{
  "tags": ["P-101", "P-102", "P-103"]
}

Reglas IMPORTANTES:
- Si encuentras una TABLA de datos de proceso donde aparezcan varios TAG de bombas,
  debes incluir TODOS esos TAGs.
- No mezcles TAG de otros equipos (tanques, válvulas, etc.), solo bombas dosificadoras.
- NO inventes TAGs. Solo devuelve los que realmente aparezcan.
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
    prompt = """
Lista TODOS los equipos dosificadores del paquete químico en formato JSON estricto:
{
  "tags": ["P-101", "P-102"]
}
NO inventes datos. Si no hay equipos claros, usa "tags": [].
"""
    resp = await _safe_aquery(rag, prompt)
    safe = repair_and_parse(resp)
    if isinstance(safe, dict):
        tags = safe.get("tags") or []
        if isinstance(tags, list):
            return [str(t).strip() for t in tags if str(t).strip()]
    return []

# ================================================================
# DISCOVERY determinístico: tablas/corpus + PDFs
# ================================================================

# Token tipo TAG con guiones (sin espacios)
_TAG_TOKEN_RE = re.compile(r"\b[A-Z0-9]{1,12}(?:-[A-Z0-9]{1,16}){1,10}\b")
# Index + TAG (permite "1 M-INY-QUIM-LOC05")
_INDEXED_TAG_RE = re.compile(r"\b(\d{1,3})\s+([A-Z][A-Z0-9]{0,10}(?:-[A-Z0-9]{1,16}){1,10})\b")
# Une tags partidos por salto de línea: "M-INY-QUIM-\nLOC05" -> "M-INY-QUIM-LOC05"
_BROKEN_HYPHEN_RE = re.compile(r"([A-Z0-9])-\s*\n\s*([A-Z0-9])")

_BAD_PREFIXES = ("DNC-", "SER-", "EDP-", "API-", "ISO-", "IEC-")

def _safe_upper(s: Any) -> str:
    try:
        return str(s).strip()
    except Exception:
        return ""

def _normalize_text_for_tags(text: str) -> str:
    if not text:
        return ""
    t = str(text)
    t = t.replace("\r", "\n")
    t = _BROKEN_HYPHEN_RE.sub(r"\1-\2", t)
    t = re.sub(r"[ \t]+", " ", t)
    return t

def _extract_tag_tokens(text: str) -> List[str]:
    """
    Extrae tokens tipo TAG de un texto:
    - Tags con guiones (sin espacios)
    - Tags indexados "1 M-INY-QUIM-LOC05" (se conserva el string completo)
    """
    if not text:
        return []
    t = _normalize_text_for_tags(_safe_upper(text)).upper()

    tokens: set[str] = set()

    # index + tag -> "1 <TAG>"
    for m in _INDEXED_TAG_RE.finditer(t):
        idx = m.group(1)
        tag = m.group(2)
        if any(ch.isdigit() for ch in tag) and not tag.startswith(_BAD_PREFIXES):
            tokens.add(f"{idx} {tag}")

    # tags sin índice
    for m in _TAG_TOKEN_RE.finditer(t):
        tok = m.group(0).strip().upper()
        if any(ch.isdigit() for ch in tok) and 3 <= len(tok) <= 40 and not tok.startswith(_BAD_PREFIXES):
            tokens.add(tok)

    return sorted(tokens)

def _find_case_outputs_dir(case_id: CaseId) -> Optional[Path]:
    """Encuentra outputs/cases/<case_id> en layouts típicos (Render/WSL)."""
    env = os.getenv("RAG_CASE_OUTPUTS_DIR", "").strip()
    candidates: List[Path] = []
    if env:
        candidates += [Path(env), Path(env) / str(case_id), Path(env) / f"{case_id}"]

    try:
        rs = Path(RAG_STORAGE_DIR).resolve()
        candidates += [
            rs.parent / "outputs" / "cases" / str(case_id),
            rs.parent.parent / "outputs" / "cases" / str(case_id),
        ]
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
    Heurística genérica: elige columnas por estructura (tokens + hits).
    """
    out: Dict[str, List[str]] = {}
    case_out = _find_case_outputs_dir(case_id)
    if not case_out:
        return out

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
    if not ccols or "table_uid" not in df.columns:
        return out

    for _table_uid, g in df.groupby("table_uid", dropna=False):
        g2 = g.sort_values(by=[c for c in ["_page", "_table_idx", "_row"] if c in g.columns], kind="stable")

        col_stats = {}
        for c in ccols:
            vals = [_safe_upper(v) for v in g2[c].tolist()]
            tok_lists = [_extract_tag_tokens(v) for v in vals]
            tok_count = sum(len(tl) for tl in tok_lists)
            uniq = len(set([t for tl in tok_lists for t in tl]))
            avg_len = np.mean([len(t) for tl in tok_lists for t in tl]) if tok_count else 0
            skid_hits = sum(1 for v in vals if any(k in str(v).upper() for k in ["INY", "QUIM", "SKID", "LOC"]))
            col_stats[c] = {"tok_count": tok_count, "uniq": uniq, "avg_len": avg_len, "skid_hits": skid_hits}

        if not col_stats:
            continue

        skid_col = max(col_stats.keys(), key=lambda c: (col_stats[c]["skid_hits"], col_stats[c]["avg_len"], col_stats[c]["tok_count"]))
        pump_candidates = [c for c in ccols if c != skid_col and col_stats[c]["tok_count"] > 0]
        if not pump_candidates:
            continue
        pump_col = max(pump_candidates, key=lambda c: (col_stats[c]["tok_count"], -col_stats[c]["skid_hits"], -col_stats[c]["avg_len"]))

        for _, row in g2.iterrows():
            skid_tokens = _extract_tag_tokens(row.get(skid_col, ""))
            pump_tokens = _extract_tag_tokens(row.get(pump_col, ""))
            if not skid_tokens or not pump_tokens:
                continue

            # skid = token más largo (estable)
            skid = max(skid_tokens, key=len).strip()
            clean = [t for t in pump_tokens if t and t != skid]
            if not clean:
                continue
            out.setdefault(skid, [])
            out[skid].extend(clean)

    for skid, pts in list(out.items()):
        out[skid] = sorted(list(dict.fromkeys([p.strip() for p in pts if p and str(p).strip()])))
    return out

def _discover_skid_pump_mapping_from_corpus(case_id: CaseId) -> Dict[str, List[str]]:
    """Fallback: co-ocurrencias de tags en líneas de corpus.jsonl."""
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
                for ln in str(txt).splitlines():
                    toks = _extract_tag_tokens(ln)
                    if len(toks) < 2:
                        continue
                    skid = max(toks, key=len)
                    pumps = [t for t in toks if t != skid]
                    if pumps:
                        out.setdefault(skid, [])
                        out[skid].extend(pumps)
    except Exception:
        return {}

    for skid, pts in list(out.items()):
        out[skid] = sorted(list(dict.fromkeys([p.strip() for p in pts if p and str(p).strip()])))
    return out

def _collect_tag_candidates_from_case(case_id: CaseId) -> Optional[set]:
    cand: set[str] = set()
    for m in (_discover_skid_pump_mapping_from_pc4(case_id), _discover_skid_pump_mapping_from_corpus(case_id)):
        for skid, pts in (m or {}).items():
            cand.add(str(skid).strip().upper())
            for p in (pts or []):
                cand.add(str(p).strip().upper())
    return cand if cand else None

def _find_case_pdfs(case_id: CaseId) -> List[Path]:
    """
    Intenta localizar PDFs del caso cuando no hay outputs/cases.
    Busca en:
      - shared_data/inbox (varias variantes)
      - rag_storage/case_<id> (si guarda originales)
      - variable RAG_CASE_PDF_DIR (si existe)
    """
    roots: List[Path] = []
    env = os.getenv("RAG_CASE_PDF_DIR", "").strip()
    if env:
        roots.append(Path(env))

    # variantes comunes en tu proyecto
    roots += [
        Path("shared_data") / "inbox",
        Path("/opt/render/project/src/shared_data/inbox"),
        Path("/home/user/AutoSelectXV1/shared_data/inbox"),
        Path(RAG_STORAGE_DIR),
    ]

    candidates: List[Path] = []
    for r in roots:
        try:
            if not r.exists():
                continue
            # buscar pdfs cercanos al case_id
            for p in r.rglob("*.pdf"):
                # heurística: si el nombre o la ruta contiene case_id
                if str(case_id) in p.name or f"case_{case_id}" in str(p.parent) or f"/{case_id}/" in str(p):
                    candidates.append(p)
            # si no encontró por case id, igual lista (para casos donde inbox no separa por id)
            if not candidates and r.name.lower() == "inbox":
                candidates.extend(list(r.glob("*.pdf")))
        except Exception:
            continue

    # dedup estable
    uniq: List[Path] = []
    seen = set()
    for p in candidates:
        try:
            rp = str(p.resolve())
        except Exception:
            rp = str(p)
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)
    return uniq

def _harvest_tags_from_pdfs(case_id: CaseId) -> List[str]:
    """
    Extrae tags desde PDFs (texto + tablas) usando pdfplumber si está disponible.
    Se usa como último determinístico ANTES del LLM.
    """
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return []

    pdfs = _find_case_pdfs(case_id)
    if not pdfs:
        return []

    found: set[str] = set()

    def _add_from_text(txt: str):
        for t in _extract_tag_tokens(txt):
            found.add(t)

    for pdf_path in pdfs:
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page in pdf.pages:
                    txt = page.extract_text() or ""
                    _add_from_text(txt)

                    # tablas
                    try:
                        tables = page.extract_tables() or []
                    except Exception:
                        tables = []
                    for tb in tables:
                        for row in tb or []:
                            for cell in row or []:
                                if cell:
                                    _add_from_text(str(cell))
        except Exception:
            continue

    # Filtrar candidatos demasiado genéricos (muy cortos) y limpiar
    cleaned = []
    for t in sorted(found):
        ts = str(t).strip()
        if not ts:
            continue
        # descartar tokens puramente numéricos
        if re.fullmatch(r"\d+", ts):
            continue
        cleaned.append(ts)
    return cleaned

def _harvest_pump_tags_deterministic(case_id: CaseId) -> List[str]:
    """
    Extrae tags SIN LLM desde:
      1) mapping SKID->PUMP (tablas/corpus)
      2) corpus.jsonl completo
      3) PDFs del caso (si no hay outputs/cases)
    NO filtra por prefijos rígidos.
    """
    out: set[str] = set()

    # 1) mapping skid->pump
    for m in (_discover_skid_pump_mapping_from_pc4(case_id), _discover_skid_pump_mapping_from_corpus(case_id)):
        for skid, pts in (m or {}).items():
            out.add(str(skid).strip().upper())
            for p in (pts or []):
                out.add(str(p).strip().upper())

    # 2) scan corpus completo (si existe)
    case_out = _find_case_outputs_dir(case_id)
    if case_out:
        corpus_path = next(iter(case_out.rglob("corpus.jsonl")), None)
        if corpus_path and corpus_path.exists():
            try:
                with open(corpus_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        txt = obj.get("text") or ""
                        for tg in _extract_tag_tokens(txt):
                            out.add(tg.upper())
            except Exception:
                pass
    else:
        # 3) si no hay outputs, intenta PDFs
        for tg in _harvest_tags_from_pdfs(case_id):
            out.add(tg.upper())

    # Filtro anti-ruido final: descartar docs/normas y tags sin dígitos
    cleaned = []
    for t in sorted(out):
        if not t:
            continue
        if t.startswith(_BAD_PREFIXES):
            continue
        # permitir "BOMBA-1" etc; pero exigir al menos un dígito global
        if not any(ch.isdigit() for ch in t):
            continue
        # limitar tamaño
        if len(t) > 60:
            continue
        cleaned.append(t)
    return cleaned

# ================================================================
# EXTRAER UNA BOMBA POR TAG (Método A + Método B)
# ================================================================

def _looks_like_valid_single_pump_dict(d: Dict[str, Any]) -> bool:
    if not isinstance(d, dict):
        return False
    has_any = any(k in d for k in ("flow", "discharge_pressure", "viscosity", "fluid", "optional"))
    if not has_any:
        return False
    flow = d.get("flow")
    if flow is not None and not isinstance(flow, dict):
        return False
    only_text = (set(d.keys()) <= {"text", "answer"})
    return not only_text

async def _extract_single_by_tag(rag, tag: str, skid_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Extraer una bomba por TAG:
      - Método A: CFG.EXTRACT_CONFIG.prompt_json_single (JSON)
      - Método B: modo engineering (texto) con heurísticas de unidad
    """
    if CFG is None or not hasattr(CFG, "EXTRACT_CONFIG"):
        raise RuntimeError("rag_config.EXTRACT_CONFIG no está disponible")

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
            raw_flow_text = str(raw_text)

        optional = safe_a.get("optional", {}) or {}
        if not isinstance(optional, dict):
            optional = {}

        optional["tag"] = tag
        if skid_tag:
            svc = str(optional.get("service") or "").strip()
            skid_txt = f"Skid {skid_tag}"
            if skid_txt not in svc:
                optional["service"] = (svc + (" | " if svc else "") + skid_txt).strip()

        pump = {
            "fluid": safe_a.get("fluid"),
            "flow": {"min": flow.get("min"), "nominal": flow.get("nominal"), "max": flow.get("max"), "raw": raw_flow_text},
            "discharge_pressure": safe_a.get("discharge_pressure"),
            "viscosity": safe_a.get("viscosity"),
            "optional": optional,
            "flow_nominal_raw": _extract_unit(FLOW_PATTERN, raw_flow_text),
            "pressure_raw": _extract_unit(PRESSURE_PATTERN, str(raw_text)),
            "viscosity_raw": _extract_unit(VISC_PATTERN, str(raw_text)),
            "temperature_raw": _extract_unit(TEMP_PATTERN, str(raw_text)),
            "density_raw": _extract_unit(DENS_PATTERN, str(raw_text)),
        }
        pump["optional"]["tag"] = pump["optional"].get("tag") or tag
        return _fix_pump_flow(pump)

    # Método B — engineering
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
    Orquestador 80/20:
    0) Determinístico (tablas/corpus/PDFs) -> tags
    1) LLM primary tags
    2) LLM secondary tags
    3) Tablas por pregunta
    4) Tablas genéricas
    5) Fallback JSON LIST
    """
    # 0 — determinístico
    det_tags = _harvest_pump_tags_deterministic(case_id)
    print(f"[DISCOVERY] deterministic tags count={len(det_tags)} head={det_tags[:15]}")

    pump_to_skid: Dict[str, str] = {}
    try:
        skid_map_det = _discover_skid_pump_mapping_from_pc4(case_id) or _discover_skid_pump_mapping_from_corpus(case_id) or {}
        for skid, pts in skid_map_det.items():
            for pt in (pts or []):
                pump_to_skid.setdefault(str(pt).strip().upper(), str(skid).strip().upper())
    except Exception:
        pump_to_skid = {}

    if det_tags:
        pumps: List[Dict[str, Any]] = []
        for t in det_tags:
            pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(str(t).upper())))
        return {"pumps": pumps, "notes": "ok-deterministic"}

    # 1 — primary LLM
    tags = await _discover_pumps_primary(rag)
    print(f"[DISCOVERY] primary tags count={len(tags)} head={tags[:10]}")
    if tags:
        pumps: List[Dict[str, Any]] = []
        for t in tags:
            pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(str(t).upper())))
        return {"pumps": pumps, "notes": "ok-primary"}

    # 2 — secondary LLM
    tags2 = await _discover_pumps_secondary(rag)
    print(f"[DISCOVERY] secondary tags count={len(tags2)} head={tags2[:10]}")
    if tags2:
        pumps: List[Dict[str, Any]] = []
        for t in tags2:
            pumps.append(await _extract_single_by_tag(rag, t, pump_to_skid.get(str(t).upper())))
        return {"pumps": pumps, "notes": "ok-secondary"}

    # 3 — tablas con pregunta
    tbl1 = await _extract_from_tables(rag, question)
    if tbl1:
        return {"pumps": tbl1, "notes": "ok-tables-question"}

    # 4 — tablas genéricas
    tbl2 = await _extract_from_tables(
        rag,
        "Listado completo de bombas dosificadoras del paquete químico con todos "
        "sus datos de proceso (caudal mínimo, nominal y máximo, presión, "
        "temperatura, viscosidad, servicio y TAG).",
    )
    if tbl2:
        return {"pumps": tbl2, "notes": "ok-tables-generic"}

    # 5 — fallback JSON LIST
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
        return {"pumps": safe_fb["pumps"], "notes": "fallback-A", "_fallback_text": fb}

    return {"pumps": [], "notes": "fallback-invalid", "_fallback_text": fb}

# ================================================================
# API PÚBLICA
# ================================================================

async def extract_query(
    case_id: int,
    question: str,
    mode: str = "extract-list",
    top_k: int = 6,
    return_raw_dict: bool = False,
):
    """
    Punto de entrada principal para consultas RAG estructuradas.
    """
    rag = await _load_rag(case_id)

    if mode in ("extract", "extract-list"):
        result = await _run_extract_multipaso(case_id, rag, question, mode)

        result = _postprocess_result(result)

        if isinstance(result, dict) and isinstance(result.get("pumps"), list):
            fb_text = result.get("_fallback_text") or ""
            has_explicit_tags = any(isinstance(p, dict) and (p.get("optional") or {}).get("tag") for p in (result.get("pumps") or []))
            if (not has_explicit_tags) and fb_text:
                result["pumps"] = apply_pump_count_postprocessor(result["pumps"], fb_text)

        if return_raw_dict:
            return result

        return {"final": json.dumps(result, ensure_ascii=False, indent=2), "raw": result, "error": None}

    # Modos texto libre
    query_param = None
    if CFG is not None and hasattr(CFG, "MODES"):
        mode_cfg = CFG.MODES.get(mode) or {}
        query_param = mode_cfg.get("query_param")

    out = await _safe_aquery(rag, question, query_param)

    if isinstance(out, dict):
        text = out.get("text") or out.get("answer")
        if text is None:
            text = json.dumps(out, ensure_ascii=False)
        raw = out
    else:
        text = str(out)
        raw = {"text": text}

    return {"final": text, "raw": raw, "error": None}

# ================================================================
# CLI LEGACY
# ================================================================

async def run_case_query_finetune(case_id: CaseId, mode: str, question: str):
    return await extract_query(int(case_id), question, mode)

if __name__ == "__main__":
    ap = __import__("argparse").ArgumentParser()
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--mode", default="extract-list")
    args = ap.parse_args()

    out = asyncio.run(run_case_query_finetune(case_id=args.case_id, mode=args.mode, question=args.question))
    print(json.dumps(out, ensure_ascii=False, indent=2))
