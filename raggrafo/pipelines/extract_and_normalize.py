# -*- coding: utf-8 -*-
"""
EXTRACT & NORMALIZE – AutoSelect-X
==================================

Función unificada para:
1. Ejecutar el pipeline RAG (cualquier modo)
2. Guardar raw.json
3. Normalizar el resultado (Normalizer v4)
4. Guardar normalized.json
5. Imprimir ambos en pantalla
6. Retornar un dict limpio para la UI

Usa:
- rag_case_query_finetune.extract_query()
- normalizer.normalize_result()
- normalizer.save_normalized()
"""

from __future__ import annotations

import os
import json
import asyncio
from typing import Any, Dict

# Pipeline RAG
from . import rag_case_query_finetune as RAG

# Normalizador v4
from . import normalizer as NORM


# ================================================================
# UTILIDAD: GUARDAR RAW.JSON
# ================================================================

def _save_raw(case_id: int, raw: Dict[str, Any]) -> str:
    base_dir = os.path.join("raggrafo", "rag_storage", f"case_{case_id}")
    os.makedirs(base_dir, exist_ok=True)

    out_path = os.path.join(base_dir, "raw.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    print(f"\n✔ RAW guardado en: {out_path}\n")
    return out_path


# ================================================================
# FUNCIÓN PRINCIPAL
# ================================================================

async def extract_and_normalize(
    case_id: int,
    question: str,
    mode: str = "extract-list",
    top_k: int = 6
) -> Dict[str, Any]:
    """
    Ejecuta:
      1. RAG (modo configurable)
      2. Guarda raw.json
      3. Normaliza -> normalized.json
      4. Retorna dict final

    Parámetros:
      case_id: ID del caso
      question: pregunta al RAG
      mode: extract, extract-list, engineering, verify, mix, mix-v2, etc.
      top_k: número de chunks a recuperar
    """

    print("\n====================================================")
    print("        EJECUTANDO RAG (extract + normalize)")
    print("====================================================")
    print(f"Case ID: {case_id}")
    print(f"Mode: {mode}")
    print(f"Pregunta: {question}")
    print("====================================================\n")

    # 1. Ejecutar pipeline RAG
    try:
        raw = await RAG.extract_query(
            case_id=case_id,
            question=question,
            mode=mode,
            top_k=top_k,
            return_raw_dict=True
        )
    except Exception as e:
        print("\n[ERROR] Falló extract_query:")
        print(e)
        raise

    # 2. Guardar RAW
    _save_raw(case_id, raw)

    # 3. Normalizar usando Normalizer v4
    normalized = await NORM.normalize_result(raw)

    # 4. Guardar NORMALIZED
    NORM.save_normalized(normalized, case_id)

    # 5. Imprimir resultados
    print("\n===== RAW (Resumen) =====")
    print(json.dumps(raw, ensure_ascii=False, indent=2))

    print("\n===== NORMALIZED =====")
    print(json.dumps(normalized, ensure_ascii=False, indent=2))

    # 6. Retornar para la UI
    return {
        "case_id": case_id,
        "raw": raw,
        "normalized": normalized,
    }


# ================================================================
# CLI MANUAL
# ================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("\nUso:")
        print("  python -m raggrafo.pipelines.extract_and_normalize CASE_ID \"pregunta\" MODE")
        print("\nEjemplo:")
        print("  python -m raggrafo.pipelines.extract_and_normalize 2 \"¿Cuál es el flujo nominal?\" extract-list")
        sys.exit(1)

    case_id = int(sys.argv[1])
    question = sys.argv[2]
    mode = sys.argv[3]

    out = asyncio.run(extract_and_normalize(case_id, question, mode))
    print("\n=== FIN ===\n")
