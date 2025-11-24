# -*- coding: utf-8 -*-
"""
RAG CONFIG – AutoSelect-X (INDUSTRIAL, versión final estable)
==============================================================

- Define TODOS los prompts y schemas usados por el pipeline.
- Mantiene consistencia absoluta con:
    • rag_case_query_finetune.py  (multipaso industrial)
    • extract_and_normalize.py
    • normalizer v4
    • PC6/PC7 LightRAG local

- Incluye:
    ✔ Schema expandido con flow {min, nominal, max, raw}
    ✔ Versiones SINGLE y LIST
    ✔ Prompts universalmente compatibles (sin f-strings)
    ✔ QueryParam seguro para LightRAG
"""

from __future__ import annotations
from typing import Any, Dict
import json


# ============================================================
# QueryParam seguro (funciona incluso si LightRAG falla)
# ============================================================
try:
    from lightrag import QueryParam
except Exception:
    QueryParam = None


def build_query_param(**kwargs) -> Any:
    """Crea QueryParam seguro sin romper si LightRAG está ausente."""
    if QueryParam is None:
        return {"__type__": "QueryParam", **kwargs}

    try:
        return QueryParam(**kwargs)
    except Exception:
        return {"__type__": "QueryParam", **kwargs}


# ============================================================
# UNIDADES PERMITIDAS (para Normalizer v4)
# ============================================================
UNITS_ALLOWED = {
    "flow": ["GPH", "LPH", "GPD", "LPD"],
    "pressure": ["PSI", "PSIG", "BAR"],
    "temperature": ["°C", "C", "°F", "F"],
    "viscosity": ["CP", "cP"],
}

UNITS_STANDARD = {
    "flow": "GPH",
    "pressure": "PSI",
    "temperature": "°C",
    "viscosity": "cP",
}


# ============================================================
# SCHEMA (EXPANDIDO INDUSTRIAL)
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

# Objetos JSON reales
PUMP_SCHEMA_MINIMAL = json.loads(PUMP_SCHEMA_MINIMAL_EXPANDED)
PUMP_SCHEMA_MINIMAL_LIST = json.loads(PUMP_SCHEMA_MINIMAL_LIST_EXPANDED)


# ============================================================
# PROMPTS TEXTUALES
# ============================================================

PROMPT_NAIVE = """
Actúa como asistente técnico. Responde sin inventar datos.
Usa exclusivamente valores literales presentes en HD, MR, ET y P&ID.
"""

PROMPT_ENGINEERING = """
Actúa como ingeniero especializado en sistemas de inyección química.
Cita páginas y tablas cuando sea posible. NO inventes datos.
Si la información no existe explícitamente, deja null.
"""

PROMPT_VERIFY = """
Verifica consistencia entre HD, MR, ET y P&ID:
- Caudal, presión, viscosidad, temperatura
- Materiales y servicio
- Requisitos eléctricos
Indica contradicciones sin inventar información.
"""


# ============================================================
# PROMPTS JSON (SINGLE / LIST)
# ============================================================

PROMPT_JSON_SINGLE = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve UNA ÚNICA BOMBA, con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL

Reglas:
- Usa SOLO valores literales encontrados.
- Si un campo no existe, déjalo null.
- NO inventes mínimos, nominales o máximos.
- flow.raw SIEMPRE debe contener el texto literal.
"""

PROMPT_JSON_SINGLE = PROMPT_JSON_SINGLE.replace(
    "REEMPLAZAR_AQUI_SCHEMA_MINIMAL",
    PUMP_SCHEMA_MINIMAL_EXPANDED
)


PROMPT_JSON_LIST = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve TODAS las bombas encontradas (una o varias),
con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL_LIST

Reglas:
- Usa SOLO valores literales del caso.
- NO inventes mínimos, nominales o máximos.
- flow.raw SIEMPRE debe contener el texto literal encontrado.
- Si no existe un valor explícito, déjalo en null.
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

    # Modo extract (1 sola bomba si aplica)
    "extract": {
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,

        "schema_single": PUMP_SCHEMA_MINIMAL,
        "schema_list": PUMP_SCHEMA_MINIMAL_LIST,

        "query_param": build_query_param(),
    },

    # Modo extract-list (siempre todas las bombas)
    "extract-list": {
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,

        "schema_single": PUMP_SCHEMA_MINIMAL,
        "schema_list": PUMP_SCHEMA_MINIMAL_LIST,

        "query_param": build_query_param(),
    },

    "mix": {
        "submodes": ["naive", "engineering"],
    },

    "combo": {
        "submodes": ["naive", "engineering", "extract", "verify"],
    },

    "mix-v2": {
        "submodes": ["engineering", "extract", "naive"],
        "weights": {"engineering": 0.6, "extract": 0.3, "naive": 0.1},
        "query_param": build_query_param(),
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
}

EXTRACT_METHOD = "A"  # Método por defecto
