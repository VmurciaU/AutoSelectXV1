# -*- coding: utf-8 -*-
"""
RAG CONFIG – AutoSelect-X (LOCAL, versión estable sin f-strings)
================================================================

- Schema MINIMAL (obligatorio + opcional)
- Sin f-strings → NO hay errores por llaves {}
- Prompts simplificados → LLM más preciso y menos alucinaciones
- extract SIEMPRE devuelve una lista de bombas, incluso si es una sola.
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
# SCHEMA MINIMAL (OBLIGATORIO + OPCIONAL)
# ============================================================

PUMP_SCHEMA_MINIMAL_EXPANDED = """
{
  "fluid": null,
  "flow_nominal": null,
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
      "flow_nominal": null,
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

# Convertir a dict (para comparaciones / validaciones)
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

# Versión "single" (una bomba) – la dejamos disponible por si algún
# modo o herramienta la requiere, aunque por diseño usamos listas.
PROMPT_JSON_SINGLE = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve UNA ÚNICA BOMBA (la más relevante para la pregunta),
con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL

Usa el esquema EXACTO del modelo.
Los campos obligatorios SIEMPRE deben aparecer.
Los opcionales pueden quedar en null.

NO inventes datos.
"""

PROMPT_JSON_SINGLE = PROMPT_JSON_SINGLE.replace(
    "REEMPLAZAR_AQUI_SCHEMA_MINIMAL",
    PUMP_SCHEMA_MINIMAL_EXPANDED
)

# Versión lista (múltiples bombas) – esta es la principal
PROMPT_JSON_LIST = """
Analiza TODOS los documentos del caso (HD, MR, ET, P&ID)
y devuelve TODAS LAS BOMBAS encontradas, incluso si es una sola,
con el SIGUIENTE FORMATO EXACTO:

REEMPLAZAR_AQUI_SCHEMA_MINIMAL_LIST

Cada bomba debe usar el esquema EXACTO del modelo.
Los campos obligatorios SIEMPRE deben aparecer.
Los opcionales pueden quedar en null.

NO inventes datos.
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

    # EXTRACT — siempre trabajamos con LISTA de bombas
    # (pero definimos también las claves *_single para compatibilidad
    #  con el pipeline finetune).
    "extract": {
        # Compatibilidad con pipelines antiguos
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        # Claves esperadas por rag_case_query_finetune.py (método A)
        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,
        "schema_single": PUMP_SCHEMA_MINIMAL,
        "schema_list": PUMP_SCHEMA_MINIMAL_LIST,

        "query_param": build_query_param(),
        # Forzamos list_mode=True para que extract devuelva lista
        "list_mode": True,
    },

    "extract-list": {
        # Compatibilidad
        "prompt_json": PROMPT_JSON_LIST,
        "schema": PUMP_SCHEMA_MINIMAL_LIST,

        # Misma configuración que extract, pero semánticamente explícito
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

# Config específica para extract, útil si el pipeline la busca por nombre
EXTRACT_CONFIG: Dict[str, Any] = {
    "prompt_json_single": PROMPT_JSON_SINGLE,
    "prompt_json_list": PROMPT_JSON_LIST,
    "schema_single": PUMP_SCHEMA_MINIMAL,
    "schema_list": PUMP_SCHEMA_MINIMAL_LIST,
    "query_param": build_query_param(),
    "list_mode": True,
}

# Método A — JSON directo
EXTRACT_METHOD = "A"
