#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark RAG local por modos (v4 corregido), usando:

    python -m raggrafo.pipelines.rag_case_query --case-id N --mode M --question "..." --raw

Este script:
- NO usa servidor HTTP.
- Usa el pipeline local que ya tienes (PC1–PC6).
- Añade un modo lógico "combo" que fusiona naive + mix para cada pregunta.
- Eliminado completamente el argumento --style (tu pipeline no lo soporta).

Uso recomendado:

  python raggrafo/scripts/benchmark_queries_v4.py \
      --case-id 18 \
      --modes naive mix combo \
      --wrap 110

Genera:
- benchmark_v4_YYYYMMDD_HHMMSS.json
- benchmark_v4_YYYYMMDD_HHMMSS.md
"""

import argparse
import json
import subprocess
import sys
import time
import textwrap
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional


# === Preguntas del benchmark ===

QUESTIONS: List[Tuple[str, str]] = [
    ("Q1", "¿Cuál es el caudal nominal, la presión de trabajo, la viscosidad de diseño y el turndown de las bombas dosificadoras del paquete, indicando sus TAG?"),
    ("Q2", "¿Qué pruebas FAT, de desempeño e inspecciones de acuerdo con API 675 y la especificación técnica son solicitadas para las bombas dosificadoras y sus TAG?"),
    ("Q3", "¿Cuántas bombas dosificadoras, tanques de almacenamiento tipo IBC y boquillas de inyección debe incluir el paquete de inyección de químicos, y cuál es la configuración duty/spare requerida?"),
#    ("Q4", "¿Qué materiales se especifican para las partes mojadas de las bombas (cabezal hidráulico, válvulas, elemento dosificador, tuberías y skid) en el paquete de inyección de químicos?"),
#    ("Q5", "¿Cuáles son las condiciones ambientales de diseño (temperatura, humedad, altitud, velocidad del viento) y la clasificación de área eléctrica donde operará el paquete de inyección de químicos?"),
#    ("Q6", "¿Qué normas, estándares y códigos aplican al diseño, materiales, pruebas y seguridad del paquete de inyección de químicos y de las bombas dosificadoras (por ejemplo API 675, ASME, RETIE, NTC 2050)?"),
]


# === Dataclasses ===

@dataclass
class ModeResult:
    mode: str
    elapsed: float
    answer: str
    error: Optional[str] = None


@dataclass
class QuestionResult:
    qid: str
    question: str
    modes: Dict[str, ModeResult]


# === Utilidades ===

def detect_project_root() -> Path:
    """Detecta raíz del proyecto suponiendo que este script está en raggrafo/scripts."""
    return Path(__file__).resolve().parents[2]


def run_rag_query(
    case_id: int,
    mode: str,
    question: str,
    project_root: Optional[Path] = None,
) -> ModeResult:
    """
    Ejecuta:
        python -m raggrafo.pipelines.rag_case_query --case-id N --mode M --question "..." --raw
    """
    if project_root is None:
        project_root = detect_project_root()

    cmd = [
        sys.executable,
        "-m", "raggrafo.pipelines.rag_case_query",
        "--case-id", str(case_id),
        "--mode", mode,
        "--question", question,
        "--raw",
    ]

    start = time.perf_counter()

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False,
        )
        elapsed = time.perf_counter() - start

        if proc.returncode != 0:
            return ModeResult(
                mode=mode,
                elapsed=elapsed,
                answer=proc.stdout.strip(),
                error=f"Error {proc.returncode}: {proc.stderr.strip()}",
            )

        return ModeResult(
            mode=mode,
            elapsed=elapsed,
            answer=proc.stdout.strip(),
        )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        return ModeResult(
            mode=mode,
            elapsed=elapsed,
            answer="",
            error=f"Excepción: {exc}",
        )


def build_combo_answer(naive: ModeResult, mix: ModeResult) -> ModeResult:
    """
    Fusión textual simple naive + mix.
    (Sin LLM adicional aún; versión de prueba.)
    """
    text = [
        "**Respuesta base (modo naive)**",
        "",
        naive.answer or "(sin respuesta en modo naive)",
        "",
        "**Notas ampliadas (modo mix)**",
        "",
        mix.answer or "(sin respuesta en modo mix)",
    ]
    combined = "\n".join(text)

    error = None
    if naive.error or mix.error:
        parts = []
        if naive.error:
            parts.append(f"[naive] {naive.error}")
        if mix.error:
            parts.append(f"[mix] {mix.error}")
        error = " | ".join(parts)

    return ModeResult(
        mode="combo",
        elapsed=max(naive.elapsed, mix.elapsed),
        answer=combined,
        error=error,
    )


def wrap_text(text: str, width: int) -> str:
    """Envuelve texto para imprimir mejor en consola."""
    if width <= 0:
        return text

    out = []
    for line in text.splitlines():
        if not line.strip():
            out.append("")
            continue
        out.extend(
            textwrap.fill(
                line,
                width=width,
                subsequent_indent="  ",
            ).splitlines()
        )
    return "\n".join(out)


def save_results_json_md(results: List[QuestionResult], case_id: int, modes: List[str], project_root: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = project_root / f"benchmark_v4_{ts}.json"
    md_path   = project_root / f"benchmark_v4_{ts}.md"

    # JSON
    data = {
        "case_id": case_id,
        "timestamp": ts,
        "modes": modes,
        "questions": [],
    }

    for qr in results:
        qd = {
            "qid": qr.qid,
            "question": qr.question,
            "modes": {},
        }
        for mname, mr in qr.modes.items():
            qd["modes"][mname] = asdict(mr)
        data["questions"].append(qd)

    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown
    lines = [
        f"# Benchmark RAG v4 – case-id {case_id}\n",
        f"- Fecha/hora: {ts}",
        f"- Modos evaluados: {', '.join(modes)}\n",
    ]

    for qr in results:
        lines.append(f"## {qr.qid} – {qr.question}\n")
        for mname, mr in qr.modes.items():
            lines.append(f"### [{mname}]  ({mr.elapsed:.2f}s)\n")
            if mr.error:
                lines.append(f"> ⚠️ Error: `{mr.error}`\n")
            if mr.answer:
                lines.append(mr.answer)
                lines.append("")
            lines.append("---\n")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    return json_path, md_path


# === MAIN ===

def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="Benchmark RAG local por modos (v4 corregido).")
    parser.add_argument("--case-id", type=int, required=True)
    parser.add_argument("--modes", nargs="+", default=["naive", "mix", "combo"])
    parser.add_argument("--wrap", type=int, default=110)
    args = parser.parse_args(argv)

    project_root = detect_project_root()

    print(f"Usando project_root: {project_root}")
    print(f"case-id: {args.case_id}\n")
    print("== RAG Local Benchmark v4 (corregido) ==\n")

    results: List[QuestionResult] = []

    for qid, question in QUESTIONS:
        print(f"{qid}: {question}")
        q_modes: Dict[str, ModeResult] = {}

        # Ejecutar modos normales (naive / mix / etc.)
        simple_modes = [m for m in args.modes if m != "combo"]

        for mode in simple_modes:
            mr = run_rag_query(args.case_id, mode, question, project_root)
            q_modes[mode] = mr

            print(f"[{mode:<6}] {mr.elapsed:.2f}s")
            if mr.error:
                print(f"  ⚠️ Error: {mr.error}")
            if mr.answer:
                print(wrap_text(mr.answer, args.wrap))
            print("-" * 60 + "\n")

        # Construir combo si fue solicitado
        if "combo" in args.modes:
            naive_res = q_modes.get("naive")
            mix_res   = q_modes.get("mix")

            if naive_res and mix_res:
                combo_res = build_combo_answer(naive_res, mix_res)
            else:
                combo_res = ModeResult(
                    mode="combo",
                    elapsed=0,
                    answer="",
                    error="No se pudo construir combo: falta naive o mix.",
                )

            q_modes["combo"] = combo_res

            print(f"[combo ] {combo_res.elapsed:.2f}s")
            if combo_res.error:
                print(f"  ⚠️ Error: {combo_res.error}")
            if combo_res.answer:
                print(wrap_text(combo_res.answer, args.wrap))
            print("-" * 60 + "\n")

        results.append(QuestionResult(qid, question, q_modes))

    json_path, md_path = save_results_json_md(results, args.case_id, args.modes, project_root)

    print(f"💾 JSON guardado : {json_path.name}")
    print(f"📝 Markdown guardado: {md_path.name}\n")
    print("Sugerencia: less -R", md_path.name)
    print()


if __name__ == "__main__":
    main()
