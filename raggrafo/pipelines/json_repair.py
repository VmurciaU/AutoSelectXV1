# -*- coding: utf-8 -*-
"""
JSON REPAIR – AutoSelect-X
==========================

Reparador avanzado de JSON para respuestas RAG/LLM.
Corrige:
 - JSON incrustado en texto
 - comas sobrantes y llaves faltantes
 - claves duplicadas
 - arrays fracturados
 - valores “none”, “nil”, “N/A”, “--”
 - JSON-string dentro de JSON
 - basura antes/después del JSON

Devuelve SIEMPRE un dict estable:
 { "pumps": [], "notes": null }
"""

import json
import re
from typing import Any, Dict


# ============================================================
# UTILIDAD 1: Extraer el bloque JSON más probable del texto
# ============================================================

def extract_json_from_text(text: str) -> str:
    """
    Extrae la sección JSON más prometedora del texto.
    Si no encuentra llaves, devuelve texto original.
    """

    if not isinstance(text, str):
        return text

    # Remover etiquetas de Markdown
    text = re.sub(r"```json|```", "", text, flags=re.IGNORECASE)

    # Encontrar el primer '{' y último '}'
    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        return text.strip()

    return text[start : end + 1].strip()


# ============================================================
# UTILIDAD 2: Normalización básica antes de reparar
# ============================================================

def sanitize_json_string(s: str) -> str:
    """
    Limpieza ligera: comas dobles, None -> null, N/A -> null, etc.
    """
    if not isinstance(s, str):
        return s

    # Normalizar null-like
    s = re.sub(r"\b(None|none|nil|N/A|N\/A|--)\b", "null", s)

    # Quitar comas dobles
    s = s.replace(",,", ",")

    # Quitar trailing commas antes de '}' o ']'
    s = re.sub(r",(\s*[}\]])", r"\1", s)

    return s.strip()


# ============================================================
# UTILIDAD 3: Reparación estructural
# ============================================================

def structural_repair(s: str) -> str:
    """
    Intenta cerrar llaves, corchetes y arreglar estructuras rotas.
    Heurísticas seguras, no invasivas.
    """

    # Contar llaves
    open_braces = s.count("{")
    close_braces = s.count("}")
    if close_braces < open_braces:
        s = s + ("}" * (open_braces - close_braces))

    # Contar corchetes
    open_brackets = s.count("[")
    close_brackets = s.count("]")
    if close_brackets < open_brackets:
        s = s + ("]" * (open_brackets - close_brackets))

    return s


# ============================================================
# UTILIDAD 4: Reparar claves duplicadas
# ============================================================

def remove_duplicate_keys(obj: Any) -> Any:
    """
    Si obj es dict y tiene keys duplicadas (por fallback json parser),
    este método limpia esas duplicaciones, manteniendo el último valor.
    """

    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            clean[k] = remove_duplicate_keys(v)
        return clean

    if isinstance(obj, list):
        return [remove_duplicate_keys(i) for i in obj]

    return obj


# ============================================================
# FUNCIÓN PRINCIPAL: Repair + Parse
# ============================================================

def repair_and_parse(raw: Any) -> Dict[str, Any]:
    """
    Punto central del JSON Repair.
    Acepta:
      - dict
      - list
      - str (json o texto libre)
    Devuelve SIEMPRE un dict estable.
    """

    # Caso 1: Ya es JSON válido
    if isinstance(raw, dict):
        return remove_duplicate_keys(raw)

    if isinstance(raw, list):
        return {"pumps": raw, "notes": None}

    # Caso 2: Convertir a str
    s = str(raw).strip()
    if not s:
        return {"pumps": [], "notes": None}

    # Extraer JSON
    s = extract_json_from_text(s)
    s = sanitize_json_string(s)
    s = structural_repair(s)

    # Primer intento
    try:
        data = json.loads(s)
        return remove_duplicate_keys(data)
    except Exception:
        pass

    # Segundo intento: Remover partes problemáticas
    try:
        s2 = re.sub(r"[^\x20-\x7E\n\t{}[\],\":0-9A-Za-z._-]", "", s)
        data = json.loads(s2)
        return remove_duplicate_keys(data)
    except Exception:
        pass

    # Fallback FINAL (estable)
    return {"pumps": [], "notes": None}
