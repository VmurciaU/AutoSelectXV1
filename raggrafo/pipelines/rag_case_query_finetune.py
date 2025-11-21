# raggrafo/pipelines/rag_case_query_finetune.py
# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión final)
Compatible 100% con:
 - pc6_lightrag.py (local, sin servidor)
 - rag_config.py (modos, prompts, JSON schemas)
 - extract y extract-list (JSON obligatorios)
 - mix, combo, mix-v2
"""

from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from typing import Any, Dict, Union

from .pc6_lightrag import (
    RAG_STORAGE_DIR,
    _make_rag,
    _core_initialize,
)

# Cargar config
try:
    from . import rag_config as CFG
except Exception:
    CFG = None

CaseId = Union[int, str]


# ============================================================
# Helpers
# ============================================================

def _force_json(obj: Any) -> str:
    """
    Normaliza a JSON siempre.
    Si el modelo devolvió texto → lo empacamos como {"text": "..."}.
    """
    if obj is None:
        return json.dumps({}, ensure_ascii=False, indent=2)

    if isinstance(obj, dict) or isinstance(obj, list):
        return json.dumps(obj, ensure_ascii=False, indent=2)

    # texto → envolver
    return json.dumps({"text": str(obj).strip()}, ensure_ascii=False, indent=2)


async def _safe_aquery(rag, question: str, query_param: Any = None):
    """
    Wrapper universal que usa SIEMPRE rag.aquery(...)
    Firma compatible con tu PC6:
         async def aquery(self, query: str, query_param=None)
    """
    if hasattr(rag, "aquery") and inspect.iscoroutinefunction(rag.aquery):
        return await rag.aquery(question, query_param)

    raise RuntimeError("Tu LightRAG local no expone un método aquery compatible")


# ============================================================
# Inicializar RAG local
# ============================================================

async def _load_rag(case_id: CaseId):
    case_dir = Path(RAG_STORAGE_DIR) / f"case_{case_id}"
    if not case_dir.exists():
        raise RuntimeError(f"No existe el storage del caso {case_id}: {case_dir}")

    rag = _make_rag(case_dir)

    # Inicialización segura
    try:
        if inspect.iscoroutinefunction(_core_initialize):
            await _core_initialize(rag)
        else:
            _core_initialize(rag)
    except Exception as e:
        print(f"[WARN] _core_initialize falló: {e}")

    return rag


# ============================================================
# Ejecución por modo
# ============================================================

async def _run_simple(rag, question: str, mode: str):
    cfg = CFG.MODES.get(mode)
    if not cfg:
        return {"final": "", "raw": None, "error": f"Modo '{mode}' no existe"}

    prompt = cfg.get("prompt_text", "")
    q = f"{prompt}\n\nPregunta:\n{question}" if prompt else question

    resp = await _safe_aquery(rag, q, cfg.get("query_param"))

    final = resp["text"] if isinstance(resp, dict) and "text" in resp else str(resp)

    return {"final": final, "raw": resp, "error": None}


async def _run_extract(rag, question: str, list_mode: bool):
    cfg = CFG.MODES["extract"]

    prompt = cfg["prompt_json_list"] if list_mode else cfg["prompt_json_single"]
    q = f"{prompt}\n\nPregunta:\n{question}"

    resp = await _safe_aquery(rag, q, cfg["query_param"])

    # Garantizar JSON
    final = _force_json(resp)
    return {"final": final, "raw": resp, "error": None}


async def _run_extract_list(rag, question: str):
    cfg = CFG.MODES["extract-list"]

    prompt = cfg["prompt_json_list"]
    q = f"{prompt}\n\nPregunta:\n{question}"

    resp = await _safe_aquery(rag, q, cfg["query_param"])

    final = _force_json(resp)
    return {"final": final, "raw": resp, "error": None}


async def _run_mix(rag, question: str):
    r1 = await _run_simple(rag, question, "naive")
    r2 = await _run_simple(rag, question, "engineering")

    final = f"[NAIVE]\n{r1['final']}\n\n[ENGINEERING]\n{r2['final']}"
    return {"final": final, "raw": {"naive": r1, "engineering": r2}}


async def _run_combo(rag, question: str):
    r_naive = await _run_simple(rag, question, "naive")
    r_eng = await _run_simple(rag, question, "engineering")
    r_ext = await _run_extract(rag, question, list_mode=False)
    r_ver = await _run_simple(rag, question, "verify")

    final = (
        "=== NAIVE ===\n" + r_naive["final"] + "\n\n"
        "=== ENGINEERING ===\n" + r_eng["final"] + "\n\n"
        "=== EXTRACT ===\n" + r_ext["final"] + "\n\n"
        "=== VERIFY ===\n" + r_ver["final"]
    )

    return {
        "final": final,
        "raw": {"naive": r_naive, "engineering": r_eng, "extract": r_ext, "verify": r_ver},
        "error": None,
    }


async def _run_mix_v2(rag, question: str):
    weights = CFG.MIX_V2_WEIGHTS
    modes = ["engineering", "extract", "naive"]

    results = {}
    for m in modes:
        if m == "extract":
            results[m] = await _run_extract(rag, question, list_mode=False)
        else:
            results[m] = await _run_simple(rag, question, m)

    block = []
    for m in modes:
        block.append(f"### [{m.upper()} – peso {weights[m]}]\n{results[m]['final']}\n")

    return {
        "final": "\n".join(block),
        "raw": results,
        "weights": weights,
        "error": None,
    }


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

async def run_case_query_finetune(case_id: CaseId, question: str, mode: str, list_mode: bool = False):
    if CFG is None:
        raise RuntimeError("rag_config.py falló al cargar")

    rag = await _load_rag(case_id)

    if mode == "extract":
        return await _run_extract(rag, question, list_mode)

    if mode == "extract-list":
        return await _run_extract_list(rag, question)

    if mode == "mix":
        return await _run_mix(rag, question)

    if mode == "combo":
        return await _run_combo(rag, question)

    if mode == "mix-v2":
        return await _run_mix_v2(rag, question)

    return await _run_simple(rag, question, mode)


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--question", required=True)
    ap.add_argument("--mode", default="naive")
    ap.add_argument("--list-mode", action="store_true")

    args = ap.parse_args()

    out = asyncio.run(
        run_case_query_finetune(
            case_id=args.case_id,
            question=args.question,
            mode=args.mode,
            list_mode=args.list_mode,
        )
    )

    print(json.dumps(out, ensure_ascii=False, indent=2))
