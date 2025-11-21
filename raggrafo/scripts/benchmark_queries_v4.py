# raggrafo/scripts/benchmark_queries_v5.py
# -*- coding: utf-8 -*-
"""
Benchmark Queries v5 – AutoSelect-X
Versión final compatible con:
 - rag_case_query_finetune.py
 - rag_config.py (modes + extract-list)
 - pc6_lightrag.py (local)

Ejemplo:
    python -m raggrafo.scripts.benchmark_queries_v5 --case-id 18
"""

import os
import sys
import json
import time
from pathlib import Path
import argparse
import asyncio

from raggrafo.pipelines.rag_case_query_finetune import run_case_query_finetune

# -----------------------------------------------------------
# Utilidades
# -----------------------------------------------------------

def now_ts():
    return time.strftime("%Y%m%d_%H%M%S")


def ensure_project_root() -> Path:
    """
    Ubica el root del proyecto (carpeta de AutoSelectX).
    """
    p = Path(__file__).resolve()
    for _ in range(6):
        if (p / "raggrafo").exists():
            return p
        p = p.parent
    return Path.cwd()


# -----------------------------------------------------------
# Preguntas de benchmark
# -----------------------------------------------------------

QUESTIONS = [
    "¿Cuál es el caudal nominal, la presión de trabajo, la viscosidad de diseño y el turndown de las bombas dosificadoras del paquete, indicando sus TAG?",
    "¿Qué pruebas FAT, de desempeño e inspecciones de acuerdo con API 675 y la especificación técnica son solicitadas para las bombas dosificadoras y sus TAG?",
    "¿Cuántas bombas dosificadoras, tanques de almacenamiento tipo IBC y boquillas de inyección debe incluir el paquete, y cuál es la configuración duty/spare requerida?"
]

MODES = [
    "naive",
    "engineering",
    "verify",
    "extract",
    "extract-list",
    "combo",
    "mix",
    "mix-v2",
]


# -----------------------------------------------------------
# Ejecución del benchmark
# -----------------------------------------------------------

async def run_benchmark(case_id: int):
    results = []

    print(f"\n== RAG Local Benchmark v5 (FINETUNE) ==")
    print(f"case-id: {case_id}\n")

    for qi, q in enumerate(QUESTIONS, 1):
        print(f"Q{qi}: {q}")

        for mode in MODES:
            print(f" Ejecutando [{mode}]...")

            t0 = time.time()
            try:
                r = await run_case_query_finetune(case_id, q, mode, list_mode=False)
                dt = time.time() - t0
                print(f"   [{mode:<12}] {dt:.2f}s")

                results.append({
                    "question_idx": qi,
                    "question": q,
                    "mode": mode,
                    "time": dt,
                    "result": r,
                })

            except Exception as e:
                dt = time.time() - t0
                print(f"   [{mode:<12}] ERROR after {dt:.2f}s: {e}")
                results.append({
                    "question_idx": qi,
                    "question": q,
                    "mode": mode,
                    "time": dt,
                    "error": str(e),
                })

        print("-" * 60)

    return results


# -----------------------------------------------------------
# Guardado de outputs
# -----------------------------------------------------------

def save_results(root: Path, results):
    ts = now_ts()
    out_json = f"benchmark_v5_{ts}.json"
    out_md = f"benchmark_v5_{ts}.md"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Markdown
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("# Benchmark RAG – AutoSelect-X\n\n")
        for item in results:
            f.write(f"## Q{item['question_idx']} – {item['mode']}\n")
            if "error" in item:
                f.write(f"**ERROR:** {item['error']}\n\n")
            else:
                f.write(f"**Tiempo:** {item['time']:.2f}s\n\n")
                r = item["result"]
                f.write("```\n")
                f.write(json.dumps(r, ensure_ascii=False, indent=2))
                f.write("\n```\n\n")

    print(f"\n💾 JSON guardado: {out_json}")
    print(f"📝 Markdown guardado: {out_md}\n")
    print("Sugerencia: less -R", out_md)


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--case-id", type=int, required=True)
    args = ap.parse_args()

    project_root = ensure_project_root()
    print("Usando project_root:", project_root)

    results = asyncio.run(run_benchmark(args.case_id))
    save_results(project_root, results)
