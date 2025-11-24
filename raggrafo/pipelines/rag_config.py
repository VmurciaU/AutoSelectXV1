# -*- coding: utf-8 -*-
"""
RAG CONFIG – AutoSelect-X (LOCAL, versión final con bloque FLOW)
================================================================

- Nuevo bloque flow: {min, nominal, max, raw}
- El resto del schema se mantiene idéntico
- 100% compatible con rag_case_query_finetune y PC6/PC7
- Sin f-strings → seguro para Prompt JSON
"""

from __future__ import annotations
from typing import Any, Dict
import json

# ============================================================
# QueryParam seguro
# ============================================================

try:
    from lightrag import QueryParam
except Exception:
    QueryParam = None


def build_query_param(**kwargs) -> Any:
    if QueryParam is None:
        return {"__type__": "QueryParam", **kwargs}
    try:
        return QueryParam(**kwargs)
    except Exception:
        return {"__type__": "QueryParam", **kwargs}


# ============================================================
# UNIDADES PERMITIDAS (normalizador)
# ============================================================

UNITS_ALLOWED = {
    "flow": ["GPH", "LPH", "GPD", "LPD"],
    "pressure": ["PSI", "PSIG", "BAR"],
    "temperature": ["°C", "C", "°F", "F"],
    "viscosity": ["CP"]
}

UNITS_STANDARD = {
    "flow": "GPH",
    "pressure": "PSI",
    "temperature": "°C",
    "viscosity": "cP"
}

# ============================================================
# SCHEMA MINIMAL (NUEVA VERSIÓN CON FLOW COMPLETO)
# ============================================================

PUMP_SCHEMA_MINIMAL_EXPANDED = """
{
  "fluid": null,

  "flow": {
    "min": null,
    "nominal": null,
    "max": null,
    "raw": null
  },

  "discharge_pressure": null,
  "viscosity": null,

  "optional": {
    "density": null,
    "temperature": null,
    "tag": null,
    "service": null,
    "materials": null,
    "area_classification": null,
    "voltage": null,
    "source_pages": null,
    "location": null,
    "pump_type": null,
    "drive_type": null
  }
}
"""

PUMP_SCHEMA_MINIMAL_LIST_EXPANDED = """
{
  "pumps": [
    {
      "fluid": null,

      "flow": {
        "min": null,
        "nominal": null,
        "max": null,
        "raw": null
      },

      "discharge_pressure": null,
      "viscosity": null,

      "optional": {
        "density": null,
        "temperature": null,
        "tag": null,
        "service": null,
        "materials": null,
        "area_classification": null,
        "voltage": null,
        "source_pages": null,
        "location": null,
        "pump_type": null,
        "drive_type": null
      }
    }
  ],
  "notes": null
}
"""

# Para validación interna
PUMP_SCHEMA_MINIMAL = json.loads(PUMP_SCHEMA_MINIMAL_EXPANDED)
PUMP_SCHEMA_MINIMAL_LIST = json.loads(PUMP_SCHEMA_MINIMAL_LIST_EXPANDED)


# ============================================================
# PROMPTS TEXTUALES
# ============================================================

PROMPT_NAIVE = """
Actúa como asistente técnico. Responde claro y sin inventar datos.
Usa exclusivamente la información existente en los documentos del caso.
"""

PROMPT_ENGINEERING = """
Actúa como ingeniero especializado en sistemas de inyección química.
Usa valores reales del caso y cita páginas cuando existan.
No inventes datos. Si no hay información, dilo.
"""

PROMPT_VERIFY = """
Verifica consistencia entre HD, MR, ET y P&ID:
- Caudal, presión, viscosidad
- Materiales
- Requisitos eléctricos
Indica contradicciones y señala qué valores son confiables.
"""


# ============================================================
# PROMPTS JSON
# ============================================================

PROMPT_JSON_SINGLE = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve UNA ÚNICA BOMBA (la más relevante para la pregunta),
con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL

NO inventes datos.
Solo llena información textual literal encontrada en los documentos.
"""

PROMPT_JSON_SINGLE = PROMPT_JSON_SINGLE.replace(
    "REEMPLAZAR_AQUI_SCHEMA_MINIMAL",
    PUMP_SCHEMA_MINIMAL_EXPANDED
)

PROMPT_JSON_LIST = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve TODAS LAS BOMBAS encontradas (incluso si es una sola),
con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL_LIST

Reglas:
- Usa SOLO valores literales de los documentos.
- NO inventes mínimos, nominales o máximos.
- Si NO existe explícitamente, deja null.
- El campo "flow.raw" SIEMPRE debe contener el texto EXACTO encontrado.

"""

PROMPT_JSON_LIST = PROMPT_JSON_LIST.replace(
    "REEMPLAZAR_AQUI_SCHEMA_MINIMAL_LIST",
    PUMP_SCHEMA_MINIMAL_LIST_EXPANDED
)


# ============================================================
# MODES
# ============================================================

_MODES: Dict[str, Dict[str, Any]] = {
    "naive": {
        "prompt_text": PROMPT_NAIVE,
        "query_param": build_query_param(),
    },

    "engineering": {
        "prompt_text": PROMPT_ENGINEERING,
        "query_param": build_query_param(),
    },

    "verify": {
        "prompt_text": PROMPT_VERIFY,
        "query_param": build_query_param(),
    },

    "extract": {
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,
        "schema_single": PUMP_SCHEMA_MINIMAL,
        "schema_list": PUMP_SCHEMA_MINIMAL_LIST,

        "query_param": build_query_param(),
        "list_mode": True,
    },

    "extract-list": {
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,
        "schema_single": PUMP_SCHEMA_MINIMAL,
        "schema_list": PUMP_SCHEMA_MINIMAL_LIST,

        "query_param": build_query_param(),
        "list_mode": True,
    },

    "mix": {
        "submodes": ["naive", "engineering"]
    },

    "combo": {
        "submodes": ["naive", "engineering", "extract", "verify"]
    },

    "mix-v2": {
        "submodes": ["engineering", "extract", "naive"],
        "weights": {"engineering": 0.6, "extract": 0.3, "naive": 0.1},
    },
}

MODES = _MODES
MIX_V2_WEIGHTS = _MODES["mix-v2"]["weights"]

EXTRACT_CONFIG: Dict[str, Any] = {
    "prompt_json_single": PROMPT_JSON_SINGLE,
    "prompt_json_list": PROMPT_JSON_LIST,
    "schema_single": PUMP_SCHEMA_MINIMAL,
    "schema_list": PUMP_SCHEMA_MINIMAL_LIST,
    "query_param": build_query_param(),
    "list_mode": True,
}

EXTRACT_METHOD = "A"
