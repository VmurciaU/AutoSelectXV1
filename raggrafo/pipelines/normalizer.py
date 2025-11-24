# -*- coding: utf-8 -*-
"""
NORMALIZER v3 – AutoSelect-X (FINAL)
===================================

Robusto, endurecido, tolerante a variaciones de formato RAW.
Convierte TODAS las unidades extraídas del RAG (RAW) a UNIDADES ESTÁNDAR:

- Flujo     → GPH
- Presión   → PSI
- Temp      → °C
- Viscosity → cP   (siempre esta unidad)

Acepta SOLO:
- Flujo: GPH, LPH, GPD, LPD
- Presión: PSI, PSIG, BAR
- T°: °C, °F
- Visc: cP

Incluye:
- Rango "min–max",
- Unidades pegadas o separadas,
- Conversión vía LLM cuando no detecta unidad,
- Guardado automático del JSON normalizado en:
      raggrafo/rag_storage/case_{id}/normalized.json
"""

import json
import os
import re
import sys
import asyncio
from typing import Dict, Any, Optional, Tuple

# Para conversión por LLM si la unidad no se detecta
from openai import OpenAI
client = OpenAI()

# ================================================================
# CONFIGURACIÓN CENTRAL
# ================================================================

FLOW_UNITS = {"GPH", "LPH", "GPD", "LPD"}
PRESSURE_UNITS = {"PSI", "PSIG", "BAR"}
TEMP_UNITS = {"C", "°C", "F", "°F"}
VISC_UNITS = {"CP"}

FLOW_STD = "GPH"
PRESSURE_STD = "PSI"
TEMP_STD = "°C"
VISC_STD = "cP"

# Ruta base del storage
BASE_STORAGE = os.path.join("raggrafo", "rag_storage")


# ================================================================
# LIMPIEZA
# ================================================================

def _clean(raw: Optional[str]) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None

    s = raw.strip()
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("º", "°")
    s = re.sub(r"\s+", " ", s)
    return s


# ================================================================
# PARSER DE VALOR/UNIDAD
# ================================================================

def _parse_value_and_unit(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw or not isinstance(raw, str):
        return None, None

    s = _clean(raw)

    # --- RANGO ---
    if "-" in s:
        parts = s.split("-")
        left, right = parts[0].strip(), parts[1].strip()

        # tomar unidad desde right
        m = re.search(r"([a-zA-Z°]+)", right)
        unit = m.group(1).upper() if m else None

        return s, unit

    # --- VALOR ÚNICO ---
    s2 = re.sub(r"(\d)([a-zA-Z°])", r"\1 \2", s)

    m = re.search(r"([\d\.]+)\s*([a-zA-Z°]+)", s2)
    if not m:
        return s2, None

    return m.group(1), m.group(2).upper()


# ================================================================
# FALLBACK LLM PARA UNIDADES NO DETECTADAS
# ================================================================

async def _ask_llm_for_unit(value: str, field: str) -> Optional[str]:
    """
    Usa LLM para inferir unidad cuando no se detecta.
    """
    try:
        prompt = f"""
Eres experto técnico en bombas dosificadoras.

El valor '{value}' corresponde al parámetro '{field}'.
Indica SOLO la unidad esperada entre:

- Flujo: GPH, LPH, GPD, LPD
- Presión: PSI, PSIG, BAR
- Temperatura: °C, °F
- Viscosidad: cP

Si no sabes, responde: null
"""

        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        unit = res.choices[0].message["content"].strip().upper()
        if unit in FLOW_UNITS | PRESSURE_UNITS | TEMP_UNITS | VISC_UNITS:
            return unit
        return None

    except Exception:
        return None


# ================================================================
# CONVERSIONES
# ================================================================

def _extract_nominal(raw_value: str) -> Optional[float]:
    """Extrae promedio en caso de rango o valor único."""
    try:
        nums = re.findall(r"[\d\.]+", raw_value)
        if not nums:
            return None
        if len(nums) >= 2:
            return (float(nums[0]) + float(nums[1])) / 2.0
        return float(nums[0])
    except Exception:
        return None


def _convert_flow(raw: str, unit: str) -> Optional[float]:
    nominal = _extract_nominal(raw)
    if nominal is None: return None

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

async def normalize_pump(p: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(p)

    # ---------- FLOW ----------
    raw_flow = p.get("flow_nominal")
    _, unit = _parse_value_and_unit(raw_flow)

    if unit not in FLOW_UNITS:
        unit = await _ask_llm_for_unit(raw_flow, "flow")

    out["flow_nominal_std"] = _convert_flow(raw_flow, unit) if unit else None
    out["flow_nominal_unit"] = FLOW_STD

    # ---------- PRESSURE ----------
    raw_p = p.get("discharge_pressure")
    _, unit = _parse_value_and_unit(raw_p)

    if unit not in PRESSURE_UNITS:
        unit = await _ask_llm_for_unit(raw_p, "pressure")

    out["discharge_pressure_std"] = _convert_pressure(raw_p, unit) if unit else None
    out["discharge_pressure_unit"] = PRESSURE_STD

    # ---------- TEMPERATURE ----------
    raw_t = p.get("optional", {}).get("temperature")
    _, unit = _parse_value_and_unit(raw_t)

    if unit not in TEMP_UNITS:
        unit = await _ask_llm_for_unit(raw_t, "temperature")

    out["temperature_std"] = _convert_temp(raw_t, unit) if unit else None
    out["temperature_unit"] = TEMP_STD

    # ---------- VISCOSITY ----------
    raw_v = p.get("viscosity")
    _, unit = _parse_value_and_unit(raw_v)

    if unit not in VISC_UNITS:
        unit = await _ask_llm_for_unit(raw_v, "viscosity")

    out["viscosity_std"] = _convert_visc(raw_v, unit) if unit else None
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
# GUARDAR EN CASE STORAGE
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

    # Puede venir envuelto como {"final": "...json...", "raw": {...}}
    if "raw" in raw_json:
        raw_json = raw_json["raw"]

    out = asyncio.run(normalize_result(raw_json))

    # imprimir resultado en pantalla
    print(json.dumps(out, ensure_ascii=False, indent=2))

    # además guardar
    save_normalized(out, case_id)
