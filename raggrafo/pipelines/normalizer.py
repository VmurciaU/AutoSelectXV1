# -*- coding: utf-8 -*-
"""
NORMALIZER v4.4 – AutoSelect-X (FINAL, industrial)
==================================================

Objetivos clave:
- NO inventar flow_nominal cuando no existe explícito en los documentos.
- Si solo hay rango mínimo–máximo en el texto (sin palabra "nominal"):
    → solo se normalizan min y max, y flow_nominal_std = None.
- Si el texto contiene explícitamente "nominal" y el JSON trae flow.nominal:
    → se normaliza flow_nominal_std.
- Manejo robusto para rangos típicos de especificaciones API-675.
- Uso de LLM solo cuando no hay número explícito para presión/temperatura/viscosidad.
- Conversión a unidades estándar: GPH, PSI, °C, cP.
- API 100% compatible con extract_and_normalize.py:
    - async normalize_result(raw_json) -> dict
    - save_normalized(normalized, case_id)
"""

import json
import os
import re
import sys
import asyncio
from typing import Dict, Any, Optional, Tuple, List

from openai import OpenAI
client = OpenAI()

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


def _extract_nominal(raw_value: str) -> Optional[float]:
    """Extrae número o promedio si es rango."""
    try:
        nums = re.findall(r"[\d\.]+", raw_value)
        if not nums:
            return None
        if len(nums) >= 2:
            return (float(nums[0]) + float(nums[1])) / 2.0
        return float(nums[0])
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
        right = parts[1].strip()
        m = re.search(r"([a-zA-Z°]+)", right)
        u = m.group(1).upper() if m else None
        return s, u

    # VALOR único
    s2 = re.sub(r"(\d)([a-zA-Z°])", r"\1 \2", s)
    m = re.search(r"([\d\.]+)\s*([a-zA-Z°]+)", s2)
    if not m:
        return s, None

    return m.group(1), m.group(2).upper()


def _extract_flow_numbers_and_unit(raw_text: str) -> Tuple[List[float], Optional[str]]:
    """
    Extrae TODOS los números y una unidad de caudal (si existe) del campo flow.raw.
    """
    s = _clean(raw_text or "")
    if not s:
        return [], None

    nums = [float(x) for x in re.findall(r"[\d\.]+", s)]
    m = re.search(r"\b(GPH|LPH|GPD|LPD)\b", s.upper())
    unit = m.group(1) if m else None
    return nums, unit

# ================================================================
# LLM SEMÁNTICO
# ================================================================

async def _ask_llm_for_value_and_unit(raw: str, field: str) -> Optional[Dict[str, Any]]:
    """
    Solo se usa cuando NO hay número explícito.
    """
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
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        txt = res.choices[0].message["content"].strip()

        if txt.lower() == "null":
            return None

        data = json.loads(txt)
        if isinstance(data, dict) and "value" in data and "unit" in data:
            return data
        return None

    except Exception:
        return None


async def _ask_llm_for_unit_only(raw: str, field: str) -> Optional[str]:
    try:
        prompt = f"""
Identifica SOLO la unidad del parámetro '{field}'.
Entrada: "{raw}"
Responde únicamente la unidad (ej: "GPH") o null.
"""
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        unit = res.choices[0].message["content"].strip().upper()

        all_units = FLOW_UNITS | PRESSURE_UNITS | TEMP_UNITS | VISC_UNITS
        if unit in all_units:
            return unit
        return None

    except Exception:
        return None

# ================================================================
# CONVERSIONES
# ================================================================

def _convert_flow(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None:
        return None

    unit = (unit or "").upper()
    if unit == "GPH":
        return nominal
    if unit == "LPH":
        return nominal / 3.78541
    if unit == "GPD":
        return nominal / 24.0
    if unit == "LPD":
        return (nominal / 24.0) / 3.78541
    return None


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
# NORMALIZAR PARÁMETRO ESCALAR
# ================================================================

async def _normalize_param(raw_value: Any, field: str, allowed_units: set, convert_func):
    """
    Normaliza un parámetro escalar (pressure, temperature, viscosity)
    a su unidad estándar.
    """
    if raw_value is None:
        return None, None

    # Caso: el RAG ya devolvió un número "crudo" sin unidad → asumimos estándar.
    if isinstance(raw_value, (int, float)):
        val = float(raw_value)
        # Asumimos que ya viene en la unidad estándar del campo.
        if field == "pressure":
            return val, PRESSURE_STD
        if field == "temperature":
            return val, TEMP_STD
        if field == "viscosity":
            return val, VISC_STD
        return val, None

    raw_str = _clean(str(raw_value))
    if not raw_str:
        return None, None

    raw_num, raw_unit = _parse_value_and_unit(raw_str)

    # 1) Hay número explícito
    if raw_num and _extract_nominal(raw_num) is not None:
        # unidad explícita válida
        if raw_unit in allowed_units:
            std = convert_func(raw_str, raw_unit)
            return std, list(allowed_units)[0]

        # pedir unidad al LLM
        unit2 = await _ask_llm_for_unit_only(raw_str, field)
        if unit2:
            std = convert_func(raw_str, unit2)
            return std, list(allowed_units)[0]

        return None, None

    # 2) No hay número → LLM semántico
    sem = await _ask_llm_for_value_and_unit(raw_str, field)
    if sem is None:
        return None, None

    fake_raw = f"{sem['value']} {sem['unit']}"
    std = convert_func(fake_raw, sem["unit"])
    return std, list(allowed_units)[0]

# ================================================================
# NORMALIZAR BOMBA COMPLETA
# ================================================================

async def normalize_pump(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p)

    # ------------------------------------------------------------
    # FLUJO (min–max–nominal) con heurística industrial
    # ------------------------------------------------------------
    flow_raw = p.get("flow", {}) or {}
    raw_min = flow_raw.get("min")
    raw_nom = flow_raw.get("nominal")
    raw_max = flow_raw.get("max")
    raw_text = flow_raw.get("raw") or ""

    nums, unit_txt = _extract_flow_numbers_and_unit(raw_text)
    unit_txt = unit_txt or "GPH"  # fallback conservador

    # Detectar si el texto menciona "nominal"
    has_nominal_word = bool(re.search(r"nominal", raw_text, re.IGNORECASE))

    # === MIN ===
    if raw_min is not None:
        min_val_str = str(raw_min)
    elif len(nums) >= 1:
        min_val_str = str(nums[0])
    else:
        min_val_str = None

    fmin_std = _convert_flow(min_val_str, unit_txt) if min_val_str is not None else None

    # === MAX ===
    if raw_max is not None:
        max_val_str = str(raw_max)
    elif len(nums) >= 2:
        max_val_str = str(nums[1])
    else:
        max_val_str = None

    fmax_std = _convert_flow(max_val_str, unit_txt) if max_val_str is not None else None

    # === NOMINAL ===
    nominal_std = None
    if raw_nom is not None and has_nominal_word:
        # Solo consideramos nominal si el texto menciona nominal explícitamente
        # y el valor no coincide con min/max (para evitar promedios inventados).
        same_as_min = (raw_min is not None and raw_nom == raw_min)
        same_as_max = (raw_max is not None and raw_nom == raw_max)
        if not same_as_min and not same_as_max:
            nominal_std = _convert_flow(str(raw_nom), unit_txt)

    out["flow_min_std"] = fmin_std
    out["flow_nominal_std"] = nominal_std
    out["flow_max_std"] = fmax_std
    out["flow_unit_std"] = FLOW_STD

    # ------------------------------------------------------------
    # PRESIÓN
    # ------------------------------------------------------------
    dp_raw = p.get("discharge_pressure")
    dp_std, _unit_dp = await _normalize_param(
        dp_raw,
        "pressure",
        PRESSURE_UNITS,
        _convert_pressure,
    )
    out["discharge_pressure_std"] = dp_std
    out["discharge_pressure_unit"] = PRESSURE_STD

    # ------------------------------------------------------------
    # TEMPERATURA
    # ------------------------------------------------------------
    temp_raw = (p.get("optional") or {}).get("temperature")
    temp_std, _unit_t = await _normalize_param(
        temp_raw,
        "temperature",
        TEMP_UNITS,
        _convert_temp,
    )
    out["temperature_std"] = temp_std
    out["temperature_unit"] = TEMP_STD

    # ------------------------------------------------------------
    # VISCOSIDAD
    # ------------------------------------------------------------
    visc_raw = p.get("viscosity")
    visc_std, _unit_v = await _normalize_param(
        visc_raw,
        "viscosity",
        VISC_UNITS,
        _convert_visc,
    )
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

    Devuelve:
    {
      "pumps": [ bombas con *_std ],
      "notes": "...",
      "units": { ... }
    }
    """
    pumps = raw_json.get("pumps", []) or []
    pumps_norm = []

    for p in pumps:
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
    """
    Guarda normalized.json en:
        raggrafo/rag_storage/case_{case_id}/normalized.json
    """
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
