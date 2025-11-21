# raggrafo/pipelines/rag_case_query_finetune.py
# -*- coding: utf-8 -*-
"""
RAG CASE QUERY – FINETUNED (versión estable)
============================================
Compatible 100% con:
 - pc6_lightrag.py (local, sin servidor)
 - rag_config.py (modos, prompts, JSON schemas)
 - extract / extract-list (JSON siempre válido)
 - mix, combo, mix-v2, naive, engineering, verify

NO TOCA PC6.
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

# JSON Repair
from .json_repair import repair_and_parse

# Config central
try:
    from raggrafo.pipelines import rag_config as CFG
except Exception:
    CFG = None

CaseId = Union[int, str]


# ============================================================
# Helpers
# ============================================================

def _force_json(obj: Any) -> str:
    """
    Normaliza SIEMPRE a JSON (str).
    - dict/list -> json.dumps(...)
    - texto -> {"text": "..."}
    - caso no-context -> JSON estándar con pumps vacías.
    """
    # Caso especial: respuestas tipo "no-context"
    if isinstance(obj, str) and "[no-context]" in obj:
        safe = {
            "pumps": [],
            "notes": "no-context: el RAG no encontró información suficiente para esta pregunta.",
        }
        return json.dumps(safe, ensure_ascii=False, indent=2)

    if obj is None:
        return json.dumps({}, ensure_ascii=False, indent=2)

    if isinstance(obj, (dict, list)):
        return json.dumps(obj, ensure_ascii=False, indent=2)

    # texto genérico → envolver
    return json.dumps({"text": str(obj).strip()}, ensure_ascii=False, indent=2)


async def _safe_aquery(rag, question: str, query_param: Any = None):
    """
    Wrapper universal que usa SIEMPRE rag.aquery(...).

    Firma esperada (compatible con tu PC6):
        async def aquery(self, query: str, query_param=None)
    """
    if rag is None:
        raise RuntimeError("RAG no inicializado (rag == None). Revisa _load_rag.")

    if hasattr(rag, "aquery") and inspect.iscoroutinefunction(rag.aquery):
        return await rag.aquery(question, query_param)

    raise RuntimeError("Tu LightRAG local no expone un método aquery async compatible")


# ============================================================
# Inicializar RAG local desde PC6
# ============================================================

async def _load_rag(case_id: CaseId):
    """
    Carga el RAG local para un case_id usando PC6.

    Usa:
      - RAG_STORAGE_DIR / f"case_{case_id}"
      - _make_rag(...)
      - _core_initialize(...)
    """
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

async def _run_simple(rag, question: str, mode: str) -> Dict[str, Any]:
    """
    Modos de texto puro: naive, engineering, verify.
    """
    cfg = CFG.MODES.get(mode) if CFG else None
    if not cfg:
        return {"final": "", "raw": None, "error": f"Modo '{mode}' no existe"}

    prompt = cfg.get("prompt_text", "")
    q = f"{prompt}\n\nPregunta:\n{question}" if prompt else question

    resp = await _safe_aquery(rag, q, cfg.get("query_param"))

    final = resp["text"] if isinstance(resp, dict) and "text" in resp else str(resp)
    return {"final": final, "raw": resp, "error": None}


async def _run_extract(rag, question: str, list_mode: bool) -> Dict[str, Any]:
    """
    Núcleo UNIFICADO de extract + selector de método A/B.

      MÉTODO A → Prompt JSON directo (actual)
      MÉTODO B → Usa modo engineering como motor semántico y construye JSON limpio

      - list_mode=False → bomba principal
      - list_mode=True  → lista de bombas
      - Siempre devuelve JSON string válido
    """
    if CFG is None:
        raise RuntimeError("rag_config.py no se cargó correctamente")

    cfg = CFG.MODES["extract"]

    # ============================
    # Método A → prompt JSON directo (actual)
    # ============================
    async def extract_method_a():
        prompt = cfg["prompt_json_list"] if list_mode else cfg["prompt_json_single"]
        q = f"{prompt}\n\nPregunta:\n{question}"

        resp = await _safe_aquery(rag, q, cfg["query_param"])

        # Reparación avanzada del JSON devuelto por LLM
        safe_json = repair_and_parse(resp)

        final = json.dumps(safe_json, ensure_ascii=False, indent=2)
        return {"final": final, "raw": resp, "error": None}

    # ============================
    # Método B → ingeniería + construcción JSON
    # ============================
    async def extract_method_b():
        eng = await _run_simple(rag, question, "engineering")
        text = eng["final"]

        import re
        tags = re.findall(r"P-\d{4}", text)
        caudales = re.findall(r"(\d+(\.\d+)?)\s*GPD", text)
        presiones = re.findall(r"(\d+)\s*psig", text)

        pumps = []

        for i, tag in enumerate(tags):
            pumps.append({
                "tag": tag,
                "service": None,
                "fluid": None,
                "location": None,
                "flow_nominal": caudales[i][0] if i < len(caudales) else None,
                "flow_min": None,
                "flow_max": None,
                "discharge_pressure": presiones[i][0] if i < len(presiones) else None,
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
            })

        # Resultado final según list_mode
        if list_mode:
            result = {"pumps": pumps, "notes": None}
        else:
            result = pumps[0] if pumps else {"pumps": [], "notes": "no-data"}

        return {
            "final": json.dumps(result, ensure_ascii=False, indent=2),
            "raw": eng,
            "error": None,
        }

    # ============================
    # SELECTOR A/B
    # ============================
    if CFG.EXTRACT_METHOD == "B":
        return await extract_method_b()
    else:
        return await extract_method_a()


async def _run_extract_list(rag, question: str) -> Dict[str, Any]:
    """Alias semántico de extract-list."""
    return await _run_extract(rag, question, list_mode=True)


async def _run_mix(rag, question: str) -> Dict[str, Any]:
    r1 = await _run_simple(rag, question, "naive")
    r2 = await _run_simple(rag, question, "engineering")

    final = f"[NAIVE]\n{r1['final']}\n\n[ENGINEERING]\n{r2['final']}"
    return {"final": final, "raw": {"naive": r1, "engineering": r2}, "error": None}


async def _run_combo(rag, question: str) -> Dict[str, Any]:
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
        "raw": {
            "naive": r_naive,
            "engineering": r_eng,
            "extract": r_ext,
            "verify": r_ver,
        },
        "error": None,
    }


async def _run_mix_v2(rag, question: str) -> Dict[str, Any]:
    weights = CFG.MIX_V2_WEIGHTS if CFG else {"engineering": 0.6, "extract": 0.3, "naive": 0.1}
    modes = ["engineering", "extract", "naive"]

    results: Dict[str, Any] = {}
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
# FUNCIÓN PRINCIPAL PÚBLICA
# ============================================================

async def run_case_query_finetune(
    case_id: CaseId,
    mode: str,
    question: str,
    list_mode: bool = False,
) -> Dict[str, Any]:
    """
    Punto de entrada único para todos los modos.
    """
    if CFG is None:
        raise RuntimeError("rag_config.py falló al cargar")

    rag = await _load_rag(case_id)

    if mode == "extract":
        return await _run_extract(rag, question, list_mode=list_mode)

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
            mode=args.mode,
            question=args.question,
            list_mode=args.list_mode,
        )
    )

    print(json.dumps(out, ensure_ascii=False, indent=2))
