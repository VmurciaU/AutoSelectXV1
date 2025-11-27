# -*- coding: utf-8 -*-
"""
EXTRACT & NORMALIZE – AutoSelect-X
==================================

Pipeline unificado:
1. Ejecuta RAG (extract / extract-list)
2. Guarda raw.json
3. Normaliza → normalizado.json
4. Retorna dict listo para UI

Depende de:
- rag_case_query_finetune.extract_query()
- normalizer.normalize_result()
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
# GUARDAR RAW.JSON
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

    # 3. Normalizar
    normalized = await NORM.normalize_result(raw)

    # 4. Guardar NORMALIZED
    NORM.save_normalized(normalized, case_id)

    # 5. Imprimir resultados
    print("\n===== RAW (Resumen) =====")
    print(json.dumps(raw, ensure_ascii=False, indent=2))

    print("\n===== NORMALIZED =====")
    print(json.dumps(normalized, ensure_ascii=False, indent=2))

    return {
        "case_id": case_id,
        "raw": raw,
        "normalized": normalized,
    }


# ================================================================
# CLI
# ================================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("\nUso:")
        print("  python -m raggrafo.pipelines.extract_and_normalize CASE_ID \"pregunta\" MODE")
        sys.exit(1)

    case_id = int(sys.argv[1])
    question = sys.argv[2]
    mode = sys.argv[3]

    out = asyncio.run(extract_and_normalize(case_id, question, mode))
    print("\n=== FIN ===\n")



# ================================================================
# FUNCIÓN SINCRÓNICA PARA FASTAPI
# ================================================================
def run_extract_and_normalize(case_id: int, question: str, mode="extract-list", top_k=6):
    """
    Wrapper SEGURO para FastAPI.
    Ejecuta extract_and_normalize() dentro del loop sin bloquearlo.
    """
    try:
        # Caso: CLI (no hay loop)
        return asyncio.run(
            extract_and_normalize(
                case_id=case_id,
                question=question,
                mode=mode,
                top_k=top_k,
            )
        )
    except RuntimeError:
        # Caso: FastAPI (loop ya corriendo)
        loop = asyncio.get_running_loop()
        return loop.create_task(
            extract_and_normalize(
                case_id=case_id,
                question=question,
                mode=mode,
                top_k=top_k,
            )
        )
