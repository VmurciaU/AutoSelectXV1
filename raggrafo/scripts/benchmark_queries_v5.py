#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# scripts/benchmark_queries_v5.py
"""
Benchmark RAG local avanzado – v5 (FINETUNE)
=============================================

Este benchmark:
- NO usa subprocess.
- Importa directamente run_case_query_finetune().
- Prueba TODOS los modos del motor avanzado:
    naive, engineering, verify, extract, extract-list,
    combo, mix, mix-v2.
- Compatible con la estructura Oil & Gas.

Genera:
    benchmark_v5_YYYYMMDD_HHMMSS.json
    benchmark_v5_YYYYMMDD_HHMMSS.md
"""

import argparse
import asyncio
import json
import textwrap
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

# Motor FINETUNE
from raggrafo.pipelines.rag_case_query_finetune import run_case_query_finetune


# ============================================================
# PREGUNTAS
# ============================================================

QUESTIONS: List[Tuple[str, str]] = [
    ("Q1", "¿Cuál es el caudal nominal, la presión de trabajo, la viscosidad de diseño y el turndown de las bombas dosificadoras del paquete, indicando sus TAG?"),
    ("Q2", "¿Qué pruebas FAT, de desempeño e inspecciones de acuerdo con API 675 y la especificación técnica son solicitadas para las bombas dosificadoras y sus TAG?"),
    ("Q3", "¿Cuántas bombas dosificadoras, tanques de almacenamiento tipo IBC y boquillas de inyección debe incluir el paquete de inyección de químicos, y cuál es la configuración duty/spare requerida?"),
]


# ============================================================
# RESULTADOS
# ============================================================

@dataclass
class ModeResult:
    mode: str
    elapsed: float
    final: str
    raw: Any
    error: Optional[str] = None


@dataclass
class QuestionResult:
    qid: str
    question: str
    modes: Dict[str, ModeResult]


# ============================================================
# UTILIDADES
# ============================================================

def detect_project_root() -> Path:
    """Detecta la raíz del proyecto, suponiendo que este script está en raggrafo/scripts."""
    return Path(__file__).resolve().parents[2]


def wrap_text(text: str, width: int) -> str:
    """Envuelve texto para impresión."""
    if width <= 0:
        return text
    out = []
    for line in text.splitlines():
        if not line.strip():
            out.append("")
            continue
        out.extend(
            textwrap.fill(line, width=width, subsequent_indent="  ").splitlines()
        )
    return "\n".join(out)


def save_results_json_md(results: List[QuestionResult], case_id: int, modes: List[str], project_root: Path):
    """Escribe benchmark a JSON y Markdown."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = project_root / f"benchmark_v5_{ts}.json"
    md_path   = project_root / f"benchmark_v5_{ts}.md"

    data = {
        "case_id": case_id,
        "timestamp": ts,
        "modes": modes,
        "questions": [],
    }

    for qr in results:
        qd = {"qid": qr.qid, "question": qr.question, "modes": {}}
        for mname, mr in qr.modes.items():
            qd["modes"][mname] = asdict(mr)
        data["questions"].append(qd)

    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown
    lines = [
        f"# Benchmark RAG v5 – case-id {case_id}\n",
        f"- Fecha/hora: {ts}",
        f"- Modos evaluados: {', '.join(modes)}\n",
    ]

    for qr in results:
        lines.append(f"## {qr.qid} – {qr.question}\n")
        for mname, mr in qr.modes.items():
            lines.append(f"### [{mname}]  ({mr.elapsed:.2f}s)\n")
            if mr.error:
                lines.append(f"> ⚠️ Error: `{mr.error}`\n")
            if mr.final:
                lines.append(wrap_text(mr.final, 120))
                lines.append("")
            lines.append("---\n")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    return json_path, md_path


# ============================================================
# EJECUTAR UN MODO
# ============================================================

async def run_mode(case_id: int, mode: str, question: str) -> ModeResult:
    """Ejecuta un modo del motor FINETUNE."""
    start = time.perf_counter()

    try:
        out = await run_case_query_finetune(
            case_id=case_id,
            question=question,
            mode=mode,
            list_mode=(mode == "extract-list"),
        )
        elapsed = time.perf_counter() - start

        return ModeResult(
            mode=mode,
            elapsed=elapsed,
            final=out.get("final", ""),
            raw=out,
            error=None,
        )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        return ModeResult(
            mode=mode,
            elapsed=elapsed,
            final="",
            raw=None,
            error=str(exc),
        )


# ============================================================
# MAIN
# ============================================================

def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="Benchmark RAG FINETUNE – v5")
    parser.add_argument("--case-id", type=int, required=True)
    parser.add_argument(
        "--modes",
        nargs="+",
        default=[
            "naive",
            #"engineering",
            "verify",
            #"extract",
            #"extract-list",
            #"combo",
            "mix",
            "mix-v2",
        ],
    )
    parser.add_argument("--wrap", type=int, default=120)
    args = parser.parse_args(argv)

    project_root = detect_project_root()

    print(f"Usando project_root: {project_root}")
    print(f"case-id: {args.case_id}\n")
    print("== RAG Local Benchmark v5 (FINETUNE) ==\n")

    results: List[QuestionResult] = []

    for qid, question in QUESTIONS:
        print(f"{qid}: {question}")
        q_modes: Dict[str, ModeResult] = {}

        for mode in args.modes:
            print(f" Ejecutando [{mode}]...")

            # CORRECCIÓN CRÍTICA: NO usamos run_until_complete
            mr = asyncio.run(run_mode(args.case_id, mode, question))

            q_modes[mode] = mr

            print(f" [{mode:<12}] {mr.elapsed:.2f}s")
            if mr.error:
                print(f"   ⚠️ Error: {mr.error}")
            if mr.final:
                print(wrap_text(mr.final, args.wrap))
            print("-" * 60 + "\n")

        results.append(QuestionResult(qid, question, q_modes))

    json_path, md_path = save_results_json_md(results, args.case_id, args.modes, project_root)

    print(f"💾 JSON guardado : {json_path.name}")
    print(f"📝 Markdown guardado: {md_path.name}")
    print("\nSugerencia: less -R", md_path.name)
    print()


if __name__ == "__main__":
    main()
