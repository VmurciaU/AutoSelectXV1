# -*- coding: utf-8 -*-
"""
NORMALIZER v4.5 – AutoSelect-X (industrial, group-safe)
======================================================

Objetivos clave:
- NO inventar flow_nominal cuando no existe explícito en los documentos.
- Si solo hay rango mínimo–máximo en el texto (sin palabra "nominal"):
    → solo se normalizan min y max, y flow_nominal_std = None.
- Manejo robusto para textos con múltiples unidades (p.ej. "72 GPD (3 GPH)").
- Uso de LLM SOLO si está disponible (SDK + API key) y NO hay número explícito.
- Conversión a unidades estándar: GPH, PSI, °C, cP.
- API 100% compatible con extract_and_normalize.py:
    - async normalize_result(raw_json) -> dict
    - save_normalized(normalized, case_id)
"""

from __future__ import annotations

import json
import os
import re
import sys
import asyncio
from typing import Dict, Any, Optional, Tuple, List

# OpenAI client (opcional). Si no está instalado o no hay API key, el normalizador NO usa LLM.
try:
    from openai import OpenAI  # type: ignore
    _client = OpenAI()
except Exception:
    OpenAI = None  # type: ignore
    _client = None

# ================================================================
# CONFIGURACIÓN UNIDADES
# ================================================================

FLOW_UNITS = {"GPH", "LPH", "GPD", "LPD"}
PRESSURE_UNITS = {"PSI", "PSIG", "BAR"}
TEMP_UNITS = {"C", "°C", "F", "°F"}
VISC_UNITS = {"CP"}

FLOW_STD = "GPH"
PRESSURE_STD = "PSI"
TEMP_STD = "°C"
VISC_STD = "cP"

BASE_STORAGE = os.path.join("raggrafo", "rag_storage")

# Regex value+unit
_FLOW_VU_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*(GPH|GPD|LPH|LPD)\b", re.IGNORECASE)
_PRESS_VU_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*(PSIG?|BAR)\b", re.IGNORECASE)
_TEMP_VU_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*(°?C|°?F)\b", re.IGNORECASE)
_VISC_VU_RE = re.compile(r"(-?\d+(?:[.,]\d+)?)\s*(CP)\b", re.IGNORECASE)

# ================================================================
# UTILS
# ================================================================

def _clean(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = str(raw)
    s = raw.strip()
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("º", "°")
    s = re.sub(r"\s+", " ", s)
    return s


def _to_float(num_str: str) -> Optional[float]:
    try:
        return float(num_str.replace(",", "."))
    except Exception:
        return None


def _extract_nominal(raw_value: str) -> Optional[float]:
    """Extrae número o promedio si es rango."""
    try:
        nums = re.findall(r"-?\d+(?:[.,]\d+)?", raw_value)
        vals = [v for v in (_to_float(x) for x in nums) if v is not None]
        if not vals:
            return None
        if len(vals) >= 2:
            return (vals[0] + vals[1]) / 2.0
        return vals[0]
    except Exception:
        return None


def _parse_value_and_unit(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (valor_o_rango_str, unidad_detectada) o (None, None)
    """
    if raw is None:
        return None, None

    s = _clean(raw)
    if not s:
        return None, None

    # RANGO tipo "0.1 - 2.0 GPD"
    if "-" in s:
        parts = s.split("-")
        right = parts[-1].strip()
        m = re.search(r"([a-zA-Z°]+)", right)
        u = m.group(1).upper() if m else None
        return s, u

    # VALOR único
    s2 = re.sub(r"(\d)([a-zA-Z°])", r"\1 \2", s)
    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*([a-zA-Z°]+)", s2)
    if not m:
        return s, None

    return m.group(1), m.group(2).upper()


# ================================================================
# CONVERSIONES
# ================================================================

def _convert_flow_value(value: float, unit: str) -> Optional[float]:
    unit = (unit or "").upper()
    if unit == "GPH":
        return value
    if unit == "LPH":
        return value / 3.78541
    if unit == "GPD":
        return value / 24.0
    if unit == "LPD":
        return (value / 24.0) / 3.78541
    return None


def _convert_flow(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None:
        return None
    return _convert_flow_value(nominal, unit)


def _convert_pressure(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None:
        return None
    unit = (unit or "").upper()
    if unit in {"PSI", "PSIG"}:
        return nominal
    if unit == "BAR":
        return nominal * 14.5038
    return None


def _convert_temp(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None:
        return None
    unit = (unit or "").upper()
    if unit in {"C", "°C"}:
        return nominal
    if unit in {"F", "°F"}:
        return (nominal - 32.0) * 5.0 / 9.0
    return None


def _convert_visc(raw: str, unit: str) -> Optional[float]:
    unit = (unit or "").upper()
    if unit == "CP":
        return _extract_nominal(raw)
    return None


# ================================================================
# LLM (opcional)
# ================================================================

async def _ask_llm_for_value_and_unit(raw: str, field: str) -> Optional[Dict[str, Any]]:
    """Solo se usa cuando NO hay número explícito y el cliente existe."""
    if _client is None:
        return None
    try:
        prompt = f"""
Convierte el parámetro '{field}' en JSON estricto:
{{"value": NUMERO, "unit": "UNIDAD"}}

Unidades válidas:
- Flujo: GPH, LPH, GPD, LPD
- Presión: PSI, PSIG, BAR
- Temperatura: °C, °F
- Viscosidad: cP

Entrada: "{raw}"

Si NO puede inferirse, responde: null
"""
        res = _client.chat.completions.create(
            model=os.getenv("NORMALIZER_LLM_MODEL", "gpt-4o-mini"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        txt = (res.choices[0].message["content"] or "").strip()
        if txt.lower() == "null":
            return None
        data = json.loads(txt)
        if isinstance(data, dict) and "value" in data and "unit" in data:
            return data
        return None
    except Exception:
        return None


async def _ask_llm_for_unit_only(raw: str, field: str) -> Optional[str]:
    if _client is None:
        return None
    try:
        prompt = f"""
Identifica SOLO la unidad del parámetro '{field}'.
Entrada: "{raw}"
Responde únicamente la unidad (ej: "GPH") o null.
"""
        res = _client.chat.completions.create(
            model=os.getenv("NORMALIZER_LLM_MODEL", "gpt-4o-mini"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        unit = (res.choices[0].message["content"] or "").strip().upper()
        all_units = FLOW_UNITS | PRESSURE_UNITS | TEMP_UNITS | VISC_UNITS
        return unit if unit in all_units else None
    except Exception:
        return None


# ================================================================
# NORMALIZAR PARÁMETRO ESCALAR
# ================================================================

async def _normalize_param(raw_value: Any, field: str, allowed_units: set, convert_func, std_unit: str):
    """
    Normaliza un parámetro escalar (pressure, temperature, viscosity) a su unidad estándar.
    """
    if raw_value is None:
        return None, None

    # Caso: el RAG ya devolvió un número "crudo" sin unidad → asumimos estándar.
    if isinstance(raw_value, (int, float)):
        return float(raw_value), std_unit

    raw_str = _clean(str(raw_value))
    if not raw_str:
        return None, None

    raw_num, raw_unit = _parse_value_and_unit(raw_str)

    # 1) Hay número explícito
    if raw_num and _extract_nominal(raw_num) is not None:
        # unidad explícita válida
        if raw_unit in allowed_units:
            std = convert_func(raw_str, raw_unit)
            return std, std_unit

        # pedir unidad al LLM (solo si está disponible)
        unit2 = await _ask_llm_for_unit_only(raw_str, field)
        if unit2:
            std = convert_func(raw_str, unit2)
            return std, std_unit

        return None, None

    # 2) No hay número → LLM semántico (si hay cliente)
    sem = await _ask_llm_for_value_and_unit(raw_str, field)
    if sem is None:
        return None, None

    fake_raw = f"{sem['value']} {sem['unit']}"
    std = convert_func(fake_raw, sem["unit"])
    return std, std_unit


# ================================================================
# NORMALIZAR FLUJO ROBUSTO
# ================================================================

def _extract_flow_values_gph(raw_text: str) -> List[float]:
    """
    Extrae TODAS las ocurrencias value+unit en flow.raw y las convierte a GPH.
    Maneja textos con múltiples unidades (p.ej. "72 GPD (3 GPH)").
    """
    s = _clean(raw_text or "") or ""
    vals: List[float] = []
    for m in _FLOW_VU_RE.finditer(s):
        v = _to_float(m.group(1))
        u = m.group(2).upper()
        if v is None:
            continue
        gph = _convert_flow_value(v, u)
        if gph is not None:
            vals.append(gph)
    return vals


def _text_mentions_nominal(raw_text: str) -> bool:
    return bool(re.search(r"\b(nominal|rated)\b", raw_text or "", re.IGNORECASE))


# ================================================================
# NORMALIZAR BOMBA COMPLETA
# ================================================================

async def normalize_pump(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p)

    # ------------------------------------------------------------
    # FLUJO (min–max–nominal) con heurística industrial robusta
    # ------------------------------------------------------------
    flow_raw = p.get("flow", {}) or {}
    if not isinstance(flow_raw, dict):
        flow_raw = {}

    raw_min = flow_raw.get("min")
    raw_nom = flow_raw.get("nominal")
    raw_max = flow_raw.get("max")
    raw_text = str(flow_raw.get("raw") or "")

    # 1) Si vienen min/max numéricos, convertirlos usando unidad deducida del texto
    # 2) Si NO vienen, intentar deducir desde value+unit en texto (convertidos a GPH)
    vals_gph = _extract_flow_values_gph(raw_text)

    def _coerce_flow(v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            # OJO: si viene ya numérico, se asume que está en la unidad del texto si detectable,
            # pero para evitar inventar, si no hay unidad en texto asumimos GPH.
            unit = None
            m = re.search(r"\b(GPH|GPD|LPH|LPD)\b", raw_text.upper())
            unit = m.group(1) if m else "GPH"
            return _convert_flow(str(v), unit)
        try:
            v2 = _to_float(str(v))
            if v2 is None:
                return None
            m = re.search(r"\b(GPH|GPD|LPH|LPD)\b", raw_text.upper())
            unit = m.group(1) if m else "GPH"
            return _convert_flow(str(v2), unit)
        except Exception:
            return None

    fmin_std = _coerce_flow(raw_min)
    fmax_std = _coerce_flow(raw_max)

    if fmin_std is None or fmax_std is None:
        if vals_gph:
            # si hay múltiples valores, usar min/max de los valores convertidos
            mn = min(vals_gph)
            mx = max(vals_gph)
            if fmin_std is None:
                fmin_std = mn
            if fmax_std is None:
                fmax_std = mx
    # Heurísticas industriales adicionales:
    # - Si el texto indica "hasta / up to / max" y los valores convertidos colapsan al mismo número,
    #   interpretarlo como un máximo (sin mínimo).
    if vals_gph and re.search(r"\b(hasta|up to|máx|max)\b", raw_text, re.IGNORECASE):
        mx = max(vals_gph)
        fmax_std = mx
        # si no hay pista clara de mínimo, lo anulamos
        if not re.search(r"\b(desde|min(?:\.|imo)?|mín(?:\.|imo)?)\b", raw_text, re.IGNORECASE):
            fmin_std = None

    # - Si por errores del extractor vienen invertidos, intercambiar
    if fmin_std is not None and fmax_std is not None and fmin_std > fmax_std:
        fmin_std, fmax_std = fmax_std, fmin_std


    # nominal: SOLO si el texto menciona nominal explícito y existe raw_nom
    nominal_std = None
    if raw_nom is not None and _text_mentions_nominal(raw_text):
        # evitar duplicar min/max
        try:
            if raw_min is not None and raw_nom == raw_min:
                nominal_std = None
            elif raw_max is not None and raw_nom == raw_max:
                nominal_std = None
            else:
                nominal_std = _coerce_flow(raw_nom)
        except Exception:
            nominal_std = None

    out["flow_min_std"] = fmin_std
    out["flow_nominal_std"] = nominal_std
    out["flow_max_std"] = fmax_std
    out["flow_unit_std"] = FLOW_STD

    # ------------------------------------------------------------
    # PRESIÓN
    # ------------------------------------------------------------
    dp_raw = p.get("discharge_pressure")
    dp_std, _ = await _normalize_param(dp_raw, "pressure", PRESSURE_UNITS, _convert_pressure, PRESSURE_STD)
    out["discharge_pressure_std"] = dp_std
    out["discharge_pressure_unit"] = PRESSURE_STD

    # ------------------------------------------------------------
    # TEMPERATURA
    # ------------------------------------------------------------
    temp_raw = (p.get("optional") or {}).get("temperature")
    temp_std, _ = await _normalize_param(temp_raw, "temperature", TEMP_UNITS, _convert_temp, TEMP_STD)
    out["temperature_std"] = temp_std
    out["temperature_unit"] = TEMP_STD

    # ------------------------------------------------------------
    # VISCOSIDAD
    # ------------------------------------------------------------
    visc_raw = p.get("viscosity")
    visc_std, _ = await _normalize_param(visc_raw, "viscosity", VISC_UNITS, _convert_visc, VISC_STD)
    out["viscosity_std"] = visc_std
    out["viscosity_unit"] = VISC_STD

    return out


# ================================================================
# API PRINCIPAL PARA extract_and_normalize.py
# ================================================================

async def normalize_result(raw_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recibe el dict crudo generado por el pipeline RAG:
    {
      "pumps": [ ... ],
      "notes": "...",
      ...
    }
    """
    pumps = raw_json.get("pumps", []) or []
    pumps_norm = []
    for p in pumps:
        if isinstance(p, dict):
            pumps_norm.append(await normalize_pump(p))
    normalized = {
        "pumps": pumps_norm,
        "notes": raw_json.get("notes", ""),
        "units": {
            "flow": FLOW_STD,
            "pressure": PRESSURE_STD,
            "temperature": TEMP_STD,
            "viscosity": VISC_STD,
        },
    }
    return normalized


def save_normalized(normalized: Dict[str, Any], case_id: int):
    """Guarda normalized.json en raggrafo/rag_storage/case_{case_id}/normalized.json"""
    case_dir = os.path.join(BASE_STORAGE, f"case_{case_id}")
    os.makedirs(case_dir, exist_ok=True)
    out_path = os.path.join(case_dir, "normalized.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)
    print(f"\n✔ Normalizado guardado en: {out_path}\n")


# ================================================================
# CLI MANUAL (opcional)
# ================================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso:")
        print("  python -m raggrafo.pipelines.normalizer RAW_JSON_PATH CASE_ID")
        sys.exit(1)

    raw_path = sys.argv[1]
    case_id = int(sys.argv[2])

    with open(raw_path, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    # Si viene envuelto como {"raw": {...}}
    if "raw" in raw_json and isinstance(raw_json["raw"], dict):
        raw_json = raw_json["raw"]

    out = asyncio.run(normalize_result(raw_json))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    save_normalized(out, case_id)
