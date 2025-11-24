# -*- coding: utf-8 -*-
"""
NORMALIZER v4 – AutoSelect-X (FINAL)
====================================

Mejoras sobre v3:
- LLM semántico: convierte lenguaje natural → número + unidad (p. ej. "cuatro galones por hora")
- Fallback robusto: si el LLM devuelve null → None
- Manejo unificado de:
    - rangos
    - valores con unidad pegada
    - inputs ambiguos
- Flujo: GPH
- Presión: PSI
- Temp: °C
- Viscosidad: cP
- Guarda normalized.json en:
      raggrafo/rag_storage/case_{id}/normalized.json
"""

import json
import os
import re
import sys
import asyncio
from typing import Dict, Any, Optional, Tuple

from openai import OpenAI
client = OpenAI()

# ================================================================
# CONFIGURACIÓN
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
# UTILIDADES
# ================================================================

def _clean(raw: Optional[str]) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("º", "°")
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_nominal(raw_value: str) -> Optional[float]:
    """Extrae número nominal (promedio si es rango)."""
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
    Retorna:
      - valor crudo (o rango)
      - unidad detectada (STRING) o None
    """
    if not raw or not isinstance(raw, str):
        return None, None

    s = _clean(raw)

    # --- RANGO ---
    if "-" in s:
        parts = s.split("-")
        left = parts[0].strip()
        right = parts[1].strip()

        m = re.search(r"([a-zA-Z°]+)", right)
        u = m.group(1).upper() if m else None

        return s, u

    # --- VALOR ÚNICO ---
    # unidad pegada
    s2 = re.sub(r"(\d)([a-zA-Z°])", r"\1 \2", s)
    m = re.search(r"([\d\.]+)\s*([a-zA-Z°]+)", s2)

    if not m:
        # no detectó número explícito
        return s, None

    return m.group(1), m.group(2).upper()


# ================================================================
# LLM SEMÁNTICO: obtener number+unit desde lenguaje natural
# ================================================================

async def _ask_llm_for_value_and_unit(raw: str, field: str) -> Optional[Dict[str, Any]]:
    """
    Semántico: convierte lenguaje natural → número + unidad.
    Formato esperado:
      {"value": 4, "unit": "GPH"}
    o null si no se puede inferir.
    """
    try:
        prompt = f"""
Eres experto en ingeniería de bombas dosificadoras.

Convierte el valor del parámetro '{field}' a un JSON estricto:
{{"value": NUMERO, "unit": "UNIDAD"}}

Unidades válidas:
- Flujo: GPH, LPH, GPD, LPD
- Presión: PSI, PSIG, BAR
- Temperatura: °C, °F
- Viscosidad: cP

Entrada original: "{raw}"

Si NO puedes inferir un número y una unidad, responde EXACTAMENTE:
null
"""

        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )

        txt = res.choices[0].message["content"].strip()

        if txt.lower() == "null":
            return None

        try:
            data = json.loads(txt)
            if not isinstance(data, dict):
                return None
            if "value" not in data or "unit" not in data:
                return None
            return data
        except Exception:
            return None

    except Exception:
        return None


async def _ask_llm_for_unit_only(raw: str, field: str) -> Optional[str]:
    """
    Último fallback: sólo unidad si ya tenemos número pero no unidad.
    """
    try:
        prompt = f"""
Identifica SOLO la unidad esperada del parámetro '{field}'.
Responde con solo la unidad (ej: "GPH") o null.

Entrada: "{raw}"
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

    if unit == "GPH": return nominal
    if unit == "LPH": return nominal / 3.78541
    if unit == "GPD": return nominal / 24.0
    if unit == "LPD": return (nominal / 24.0) / 3.78541
    return None


def _convert_pressure(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None: return None

    if unit in {"PSI", "PSIG"}:
        return nominal
    if unit == "BAR":
        return nominal * 14.5038
    return None


def _convert_temp(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None: return None

    if unit in {"C", "°C"}:
        return nominal
    if unit in {"F", "°F"}:
        return (nominal - 32.0) * 5.0 / 9.0
    return None


def _convert_visc(raw: str, unit: str) -> Optional[float]:
    return _extract_nominal(raw) if unit == "CP" else None


# ================================================================
# NORMALIZAR UNA BOMBA
# ================================================================

async def _normalize_param(raw_value: Optional[str], field: str, allowed_units: set, convert_func):
    """
    Normaliza UN parámetro de bomba (flow/pressure/temp/viscosity).
    """

    if not raw_value:
        return None, None

    raw_value = _clean(raw_value)
    raw_num, raw_unit = _parse_value_and_unit(raw_value)

    # 1. SI YA TIENE NÚMERO EXPLÍCITO
    if raw_num and _extract_nominal(raw_num) is not None:

        # 1A. unidad explícita válida
        if raw_unit in allowed_units:
            std_val = convert_func(raw_value, raw_unit)
            return std_val, list(allowed_units)[0]

        # 1B. no hay unidad → pedirla
        unit2 = await _ask_llm_for_unit_only(raw_value, field)
        if unit2:
            std_val = convert_func(raw_value, unit2)
            return std_val, list(allowed_units)[0]

        return None, None

    # 2. SI NO HAY NÚMERO → usar LLM semántico
    sem = await _ask_llm_for_value_and_unit(raw_value, field)
    if sem is None:
        return None, None

    # convertir a string para que el convertor procese
    raw_fake = f"{sem['value']} {sem['unit']}"
    std = convert_func(raw_fake, sem["unit"].upper())
    return std, list(allowed_units)[0]


async def normalize_pump(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p)

    # FLOW
    val, unit = await _normalize_param(
        p.get("flow_nominal"),
        "flow",
        FLOW_UNITS,
        _convert_flow
    )
    out["flow_nominal_std"] = val
    out["flow_nominal_unit"] = FLOW_STD

    # PRESSURE
    val, unit = await _normalize_param(
        p.get("discharge_pressure"),
        "pressure",
        PRESSURE_UNITS,
        _convert_pressure
    )
    out["discharge_pressure_std"] = val
    out["discharge_pressure_unit"] = PRESSURE_STD

    # TEMPERATURE
    val, unit = await _normalize_param(
        p.get("optional", {}).get("temperature"),
        "temperature",
        TEMP_UNITS,
        _convert_temp
    )
    out["temperature_std"] = val
    out["temperature_unit"] = TEMP_STD

    # VISCOSITY
    val, unit = await _normalize_param(
        p.get("viscosity"),
        "viscosity",
        VISC_UNITS,
        _convert_visc
    )
    out["viscosity_std"] = val
    out["viscosity_unit"] = VISC_STD

    return out


# ================================================================
# NORMALIZAR RESULTADO COMPLETO
# ================================================================

async def normalize_result(raw_json: Dict[str, Any]) -> Dict[str, Any]:
    pumps = []
    for p in raw_json.get("pumps", []):
        pumps.append(await normalize_pump(p))

    return {
        "pumps": pumps,
        "notes": raw_json.get("notes", ""),
        "units": {
            "flow": FLOW_STD,
            "pressure": PRESSURE_STD,
            "temperature": TEMP_STD,
            "viscosity": VISC_STD,
        }
    }


# ================================================================
# GUARDADO
# ================================================================

def save_normalized(normalized: Dict[str, Any], case_id: int):
    case_dir = os.path.join(BASE_STORAGE, f"case_{case_id}")
    os.makedirs(case_dir, exist_ok=True)

    out_path = os.path.join(case_dir, "normalized.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)

    print(f"\n✔ Normalizado guardado en: {out_path}\n")


# ================================================================
# CLI
# ================================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python -m raggrafo.pipelines.normalizer raw.json CASE_ID")
        sys.exit(1)

    raw_path = sys.argv[1]
    case_id = int(sys.argv[2])

    with open(raw_path, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    if "raw" in raw_json:
        raw_json = raw_json["raw"]

    out = asyncio.run(normalize_result(raw_json))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    save_normalized(out, case_id)
