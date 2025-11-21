# raggrafo/pipelines/rag_config.py
# -*- coding: utf-8 -*-
"""
RAG CONFIG – AutoSelect-X (LOCAL)
=================================
Configuración central del RAG local. NO modifica chunking ni embeddings.
Define modos, prompts y JSON schemas para todos los modos soportados:

    naive
    engineering
    verify
    extract
    extract-list
    mix
    combo
    mix-v2

Este file ES EL CORAZÓN del ajuste fino.
"""

from __future__ import annotations
from typing import Any, Dict, Optional

# ============================================================
# QueryParam (import seguro)
# ============================================================

try:
    from lightrag import QueryParam   # este sí existe en tu versión cargada localmente
except Exception:
    QueryParam = None


def build_query_param(**kwargs) -> Any:
    """
    Construcción segura de QueryParam:
    - Si existe QueryParam real: usarlo
    - Si no existe o falla: devolver dict marcador
    """
    if QueryParam is None:
        return {"__type__": "QueryParam", **kwargs}

    try:
        return QueryParam(**kwargs)
    except Exception:
        return {"__type__": "QueryParam", **kwargs}



# ============================================================
# JSON schemas FULL-FIELDS ALWAYS
# ============================================================

PUMP_FIELDS = {
    "tag": None,
    "service": None,
    "fluid": None,
    "location": None,
    "flow_nominal": None,
    "flow_min": None,
    "flow_max": None,
    "discharge_pressure": None,
    "suction_pressure": None,
    "delta_pressure": None,
    "temperature": None,
    "viscosity": None,
    "density": None,
    "npsha": None,
    "npshr": None,
    "material_head": None,
    "material_diaphragm_or_seal": None,
    "material_valves": None,
    "material_plunger_or_piston": None,
    "pump_type": None,
    "drive_type": None,
    "connections": None,
    "stroke": None,
    "voltage": None,
    "frequency": None,
    "motor_power": None,
    "motor_current": None,
    "start_mode": None,
    "electrical_protection": None,
    "standards": None,
    "tests": None,
    "certifications": None,
    "source_pages": None,
    "source_sections": None,
}

PUMP_LIST_SCHEMA = {
    "pumps": [PUMP_FIELDS],
    "notes": None,
}

# ============================================================
# Prompts texto
# ============================================================

PROMPT_NAIVE = """
Actúa como asistente técnico. Responde claro, directo y sin inventar datos.
Usa exclusivamente la información existente en los documentos del caso.
"""

PROMPT_ENGINEERING = """
Actúa como ingeniero de proyectos de sistemas de inyección de químicos.
Responde usando valores del caso, citando páginas/secciones cuando existan.
No inventes datos. Si algo no está en los documentos, dilo explícitamente.
"""

PROMPT_VERIFY = """
Verifica consistencia entre HD, MR, ET y P&ID:
- caudal, presión, temperatura
- materiales
- requisitos eléctricos
Indica contradicciones y qué datos son confiables.
"""

PROMPT_COMBO = """
Resume las perspectivas naive + engineering + extract + verify en un
informe compacto para cotización. Destaca riesgos y datos faltantes.
"""

# ============================================================
# Prompts JSON
# ============================================================

PROMPT_JSON_SINGLE = f"""
Extrae SOLO LA BOMBA PRINCIPAL en formato JSON.

Reglas:
1) NO inventes datos. Todo lo que no aparezca, déjalo en null.
2) Usa exclusivamente los documentos del caso.
3) Si hay varias bombas, elige la más asociada al servicio químico principal.
4) Devuelve SOLO JSON. Nada de texto adicional.

Esquema esperado:
{PUMP_FIELDS}
"""

PROMPT_JSON_LIST = f"""
Extrae TODAS LAS BOMBAS en LISTA JSON.

Reglas:
1) NO inventes datos. Campos faltantes → null.
2) Devuelve:
   {{
       "pumps": [ {{...}}, {{...}} ],
       "notes": "texto" o null
   }}
3) Si no hay bombas → lista vacía.
4) Devuelve SOLO JSON.

Esquema por bomba:
{PUMP_FIELDS}
"""

# ============================================================
# MODES
# ============================================================

_MODES: Dict[str, Dict[str, Any]] = {
    "naive": {
        "description": "Respuesta directa básica.",
        "prompt_text": PROMPT_NAIVE,
        "query_param": build_query_param(),
    },
    "engineering": {
        "description": "Respuesta técnica detallada.",
        "prompt_text": PROMPT_ENGINEERING,
        "query_param": build_query_param(),
    },
    "verify": {
        "description": "Verificación de coherencia entre documentos.",
        "prompt_text": PROMPT_VERIFY,
        "query_param": build_query_param(),
    },
    "extract": {
        "description": "Extracción JSON – bomba única.",
        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,
        "schema_single": PUMP_FIELDS,
        "schema_list": PUMP_LIST_SCHEMA,
        "query_param": build_query_param(),
    },
    "extract-list": {
        "description": "Extracción JSON – lista completa de bombas.",
        "prompt_json_single": PROMPT_JSON_SINGLE,
        "prompt_json_list": PROMPT_JSON_LIST,
        "schema_single": PUMP_FIELDS,
        "schema_list": PUMP_LIST_SCHEMA,
        "query_param": build_query_param(),
        "list_mode": True,
    },
    "mix": {
        "description": "Mezcla naive + engineering.",
        "submodes": ["naive", "engineering"],
    },
    "combo": {
        "description": "naive + engineering + extract + verify.",
        "submodes": ["naive", "engineering", "extract", "verify"],
        "prompt_text": PROMPT_COMBO,
    },
    "mix-v2": {
        "description": "Fusión ponderada avanzada.",
        "submodes": ["engineering", "extract", "naive"],
        "weights": {
            "engineering": 0.6,
            "extract": 0.3,
            "naive": 0.1,
        },
    },
}

# ============================================================
# API pública
# ============================================================

def get(mode: str) -> Dict[str, Any]:
    return _MODES.get(mode, {})

def list_modes():
    return _MODES

# EXPOSICIÓN DE CONSTANTES (para compatibilidad con tu finetune viejo)
MODES = _MODES
MIX_V2_WEIGHTS = _MODES["mix-v2"]["weights"]
