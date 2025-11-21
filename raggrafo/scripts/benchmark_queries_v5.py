# raggrafo/scripts/benchmark_queries_v5.py
# -*- coding: utf-8 -*-
"""
Benchmark v5 – compatible con finetune + extract unificado.

Uso típico:

  python -m raggrafo.scripts.benchmark_queries_v5 --case-id 2
  python -m raggrafo.scripts.benchmark_queries_v5 --case-id 2 --modes extract engineering verify

Genera:
  - benchmark_v5_YYYYMMDD_HHMMSS.json
  - benchmark_v5_YYYYMMDD_HHMMSS.md
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from raggrafo.pipelines.rag_case_query_finetune import run_case_query_finetune


# Preguntas predefinidas (por ahora solo Q1, la que vienes usando)
QUESTIONS: Dict[str, str] = {
    "Q1": "¿Cuál es el caudal nominal, la presión de trabajo, la viscosidad de diseño y el turndown de las bombas dosificadoras del paquete, indicando sus TAG?"
}

DEFAULT_MODES: List[str] = [
    "naive",
    "verify",
    "engineering",
    "extract",
    "combo",
    "mix",
    "mix-v2",
]


async def run_modes(case_id: int, question: str, modes: List[str]) -> Dict[str, Dict]:
    results: Dict[str, Dict] = {}

    print("\n== RAG Local Benchmark v5 (FINETUNE) ==\n")
    print(f"Q1: {question}")

    for mode in modes:
        print(f" Ejecutando [{mode}]...")
        t0 = time.perf_counter()
        try:
            # list_mode=True solo tiene efecto real en modo "extract"
            list_mode = (mode == "extract-list")
            r = await run_case_query_finetune(
                case_id=case_id,
                mode=mode,
                question=question,
                list_mode=list_mode,
            )
            elapsed = time.perf_counter() - t0
            results[mode] = {
                "elapsed_s": round(elapsed, 2),
                "final": r.get("final"),
                "error": r.get("error"),
            }

            # Output inmediato en consola (similar a tus logs anteriores)
            final_txt = r.get("final") or ""
            print(f" [{mode:<11}] {round(elapsed, 2)}s")
            print(final_txt)
            print("-" * 60)

        except Exception as e:
            elapsed = time.perf_counter() - t0
            results[mode] = {
                "elapsed_s": round(elapsed, 2),
                "final": None,
                "error": str(e),
            }
            print(f"[ERROR en modo {mode}] {e}")
            print("-" * 60)

    return results


def save_outputs(case_id: int, question: str, modes: List[str], results: Dict[str, Dict]):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"benchmark_v5_{ts}"

    # JSON
    json_path = Path(f"{base}.json")
    payload = {
        "case_id": case_id,
        "question": question,
        "modes": modes,
        "results": results,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown
    md_path = Path(f"{base}.md")
    lines = []
    lines.append(f"# Benchmark v5 – case {case_id}\n")
    lines.append(f"**Pregunta:** {question}\n")
    lines.append(f"**Modos:** {', '.join(modes)}\n")

    for mode in modes:
        r = results.get(mode, {})
        lines.append("\n---\n")
        lines.append(f"## Modo: `{mode}`\n")
        lines.append(f"- Tiempo: {r.get('elapsed_s')} s\n")
        if r.get("error"):
            lines.append(f"- Error: `{r['error']}`\n")
        lines.append("\n```text\n")
        if r.get("final"):
            lines.append(str(r["final"]))
        else:
            lines.append("(sin salida)\n")
        lines.append("\n```\n")

    md_path.write_text("".join(lines), encoding="utf-8")

    print(f"\n💾 JSON guardado : {json_path.name}")
    print(f"📝 Markdown guardado: {md_path.name}\n")
    print(f"Sugerencia: less -R {md_path.name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-id", type=int, required=True)
    parser.add_argument(
        "--question",
        help="Texto de la pregunta. Si se omite, usa Q1 predefinida.",
    )
    parser.add_argument(
        "--modes",
        nargs="*",
        help="Lista de modos a ejecutar (si se omite, usa modos por defecto).",
    )

    args = parser.parse_args()

    case_id = args.case_id
    question = args.question or QUESTIONS["Q1"]
    modes = args.modes or DEFAULT_MODES

    print(f"Usando project_root: {Path('.').resolve()}")
    print(f"case-id: {case_id}\n")

    results = asyncio.run(run_modes(case_id, question, modes))
    save_outputs(case_id, question, modes, results)


if __name__ == "__main__":
    main()
