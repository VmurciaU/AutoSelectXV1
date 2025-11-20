#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark RAG local por modos, usando el pipeline:

    python -m raggrafo.pipelines.rag_case_query --case-id N --mode M --question "..." --raw

No usa servidor HTTP ni la clase LightRAG directa; simplemente envuelve el comando
que ya vienes usando para consultar el RAG local.

Uso recomendado (desde CUALQUIER carpeta):

  python raggrafo/scripts/benchmark_queries_v3.py \
      --case-id 18 \
      --modes naive local global mix \
      --style concise \
      --wrap 110

- case-id: ID del caso (ej. 14, 18, etc.)
- modes: lista de modos a probar, como en rag_case_query (--mode)
- style: 'concise' o 'json' (solo afecta el prompt, no al pipeline)

Genera:
- benchmark_YYYYMMDD_HHMMSS.json
- benchmark_YYYYMMDD_HHMMSS.md
"""
import os
import sys
import json
import time
import argparse
import datetime
import textwrap
import subprocess
from pathlib import Path

# --------------------------- util de envoltura ---------------------------
def term_width(default=110):
    try:
        import shutil
        cols = shutil.get_terminal_size((default, 24)).columns
        return max(60, min(cols, 200))
    except Exception:
        return default

def wrap(text, width=None):
    width = width or term_width()
    out = []
    for line in (text or "").splitlines():
        if not line.strip():
            out.append("")
            continue
        # Si parece JSON (empieza con { o [) no lo envolvemos
        if line.lstrip().startswith(("{", "[")):
            out.append(line)
        else:
            out.append(
                textwrap.fill(
                    line,
                    width=width,
                    replace_whitespace=False,
                    break_long_words=False,
                )
            )
    return "\n".join(out)

# --------------------------- limpiar stdout de rag_case_query ---------------------------
def extract_pretty_answer(stdout: str) -> str:
    """
    Toma el stdout completo de `rag_case_query` y devuelve SOLO el bloque de la respuesta,
    recortando todo lo demás (encabezados, Storage dir, Respuesta RAW, etc.).

    Busca la línea 'Respuesta:' y toma lo que hay después hasta antes de:
      - 'Storage dir:'
      - 'Respuesta RAW'
    Si no encuentra marcadores, devuelve stdout completo.
    """
    lines = stdout.splitlines()
    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if line.strip().startswith("Respuesta:"):
            start_idx = i + 1  # saltamos la línea 'Respuesta:'
            break

    if start_idx is None:
        # No encontramos marcador, devolvemos todo
        return stdout.strip()

    for j in range(start_idx, len(lines)):
        t = lines[j].strip()
        if t.startswith("Storage dir:") or t.startswith("Respuesta RAW"):
            end_idx = j
            break

    if end_idx is None:
        end_idx = len(lines)

    chunk = "\n".join(lines[start_idx:end_idx]).strip()
    return chunk or stdout.strip()

# --------------------------- instrucciones de estilo ---------------------------
def build_instruction(style: str) -> str:
    style = (style or "concise").lower()
    if style == "json":
        return (
            "Devuelve SOLO un JSON válido y minimalista con los datos solicitados; "
            "usa claves cortas en español; incluye unidades y 'no especificado' si falta; "
            "usa únicamente la información de los documentos del caso; "
            "no añadas texto fuera del JSON."
        )
    return (
        "Usa exclusivamente la información contenida en los documentos del caso (HD, MR, ET, normas asociadas) "
        "y no inventes datos. Si un valor no aparece explícitamente, responde 'no especificado en los documentos "
        "del caso'. Responde en 5–8 viñetas técnicas, priorizando valores numéricos exactos con unidades. "
        "Incluye la norma o criterio cuando aplique (por ejemplo, API 675). "
        "Evita introducciones y conclusiones generales; solo la respuesta técnica."
    )

def make_query(user_q: str, style: str) -> str:
    instr = build_instruction(style)
    return f"{instr}\n\nPregunta: {user_q}".strip()

# --------------------------- llamada al pipeline rag_case_query ---------------------------
def ask_with_pipeline(
    project_root: Path,
    case_id: int,
    query: str,
    mode: str,
    timeout: int = 300,
):
    """
    Envuelve el comando:

      python -m raggrafo.pipelines.rag_case_query \
          --case-id CASE_ID \
          --mode MODE \
          --question "QUERY" \
          --raw

    Ejecutado con cwd=project_root, para que 'raggrafo' se resuelva bien como paquete.
    Devuelve (elapsed, {"response": texto, "references": []})

    OJO: ya no intentamos parsear JSON; tomamos el stdout y extraemos
    solo el bloque de 'Respuesta:' para que la salida sea limpia tipo RAG web.
    """
    cmd = [
        sys.executable,
        "-m",
        "raggrafo.pipelines.rag_case_query",
        "--case-id",
        str(case_id),
        "--mode",
        mode,
        "--question",
        query,
        "--raw",
    ]

    t0 = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    dt = time.time() - t0

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    if proc.returncode != 0:
        # Devolvemos error arriba, pero dejamos el stderr para depurar
        raise RuntimeError(f"rag_case_query fallo (rc={proc.returncode}): {stderr or stdout}")

    # Aquí “limpiamos” la respuesta:
    pretty = extract_pretty_answer(stdout)

    # Por ahora referencias vacías (rag_case_query no las expone estructuradas)
    return dt, {"response": pretty, "references": []}

# --------------------------- CLI principal ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--case-id",
        type=int,
        default=18,
        help="ID del caso para pasar a raggrafo.pipelines.rag_case_query (por defecto: 18)",
    )
    ap.add_argument(
        "--modes",
        nargs="*",
        default=["naive", "local", "global", "mix"],
        help="Modos a probar (valores para --mode de rag_case_query, ej. naive/local/global/mix/core)",
    )
    ap.add_argument(
        "--q",
        dest="queries",
        action="append",
        default=[],
        help="Agregar pregunta (repetible; si no se usa, se cargan las preguntas por defecto Q1–Q6)",
    )
    ap.add_argument(
        "--style",
        default="concise",
        choices=["concise", "json"],
        help="Formato de respuesta (solo afecta el prompt enviado al pipeline)",
    )
    ap.add_argument(
        "--no-md",
        action="store_true",
        help="No guardar Markdown",
    )
    ap.add_argument(
        "--no-json",
        action="store_true",
        help="No guardar JSON",
    )
    ap.add_argument(
        "--wrap",
        type=int,
        default=term_width(),
        help="Ancho de envoltura para impresión en consola",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout por consulta (segundos)",
    )
    args = ap.parse_args()

    # Detectar la raíz del proyecto (subimos desde raggrafo/scripts hasta la carpeta padre)
    script_path = Path(__file__).resolve()
    # .../AutoSelectX/raggrafo/scripts/benchmark_queries_v3.py -> padre de padre = AutoSelectX
    project_root = script_path.parents[2]
    print(f"Usando project_root: {project_root}")
    print(f"case-id: {args.case_id}")

    # Preguntas por defecto (alineadas con HD/MR/ET)
    if not args.queries:
        args.queries = [
            # Q1
            "¿Cuál es el caudal nominal, la presión de trabajo, la viscosidad de diseño y el turndown de las "
            "bombas dosificadoras del paquete, indicando sus TAG?",
            # Q2
            "¿Qué pruebas FAT, de desempeño e inspecciones de acuerdo con API 675 y la especificación técnica "
            "son solicitadas para las bombas dosificadoras y sus TAG?",
            # Q3
            "¿Cuántas bombas dosificadoras, tanques de almacenamiento tipo IBC y boquillas de inyección debe "
            "incluir el paquete de inyección de químicos, y cuál es la configuración duty/spare requerida?",
            # Q4
            "¿Qué materiales se especifican para las partes mojadas de las bombas (cabezal hidráulico, válvulas, "
            "elemento dosificador, tuberías y skid) en el paquete de inyección de químicos?",
            # Q5
            "¿Cuáles son las condiciones ambientales de diseño (temperatura, humedad, altitud, velocidad del viento) "
            "y la clasificación de área eléctrica donde operará el paquete de inyección de químicos?",
            # Q6
            "¿Qué normas, estándares y códigos aplican al diseño, materiales, pruebas y seguridad del paquete de "
            "inyección de químicos y de las bombas dosificadoras (por ejemplo API 675, ASME, RETIE, NTC 2050)?",
        ]

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_md = f"benchmark_{ts}.md"
    out_json = f"benchmark_{ts}.json"

    results = []

    print("\n== RAG Local Benchmark (via raggrafo.pipelines.rag_case_query) ==\n")
    for q_idx, raw_q in enumerate(args.queries, 1):
        q = make_query(raw_q, args.style)
        print(f"Q{q_idx}: {raw_q}")
        blk = {
            "question": raw_q,
            "modes": [],
            "style": args.style,
            "case_id": args.case_id,
        }

        for m in args.modes:
            try:
                dt, data = ask_with_pipeline(
                    project_root=project_root,
                    case_id=args.case_id,
                    query=q,
                    mode=m,
                    timeout=args.timeout,
                )
                resp = (data.get("response") or "").strip()
                refs = data.get("references") or []
                blk["modes"].append(
                    {
                        "mode": m,
                        "elapsed": dt,
                        "response": resp,
                        "references": refs,
                    }
                )

                print(f"[{m:6}] {dt:5.2f}s")
                print(wrap(resp, args.wrap))
                if refs:
                    print("↳ Referencias:")
                    for r in refs:
                        print(f"   - {r}")
                print()
            except Exception as e:
                blk["modes"].append({"mode": m, "error": str(e)})
                print(f"[{m:6}] ERROR: {e}\n")

        results.append(blk)

    # ====== Guardado ======
    if not args.no_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "project_root": str(project_root),
                    "case_id": args.case_id,
                    "style": args.style,
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"💾 JSON guardado: {out_json}")

    if not args.no_md:
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(f"# RAG Local Benchmark — {ts}\n\n")
            f.write(f"- project_root: `{project_root}`\n")
            f.write(f"- case_id: `{args.case_id}`\n")
            f.write(f"- Modos: {', '.join(args.modes)}\n")
            f.write(f"- Estilo: `{args.style}`\n\n---\n")

            for i, blk in enumerate(results, 1):
                f.write(f"## Q{i}. {blk['question']}\n\n")
                for m in blk["modes"]:
                    if "error" in m:
                        f.write(f"**[{m['mode']}]** ERROR: {m['error']}\n\n")
                        continue
                    f.write(f"**[{m['mode']}]** ({m['elapsed']:.2f}s)\n\n")
                    f.write(m["response"] + "\n\n")
                    refs = m.get("references") or []
                    if refs:
                        f.write("**Referencias**\n\n")
                        for r in refs:
                            f.write(f"- {r}\n")
                        f.write("\n")
                f.write("\n---\n")
        print(
            f"📝 Markdown guardado: {out_md}\n"
            f"Sugerencia: abre con `less -R {out_md}` o en VSCode."
        )

if __name__ == "__main__":
    main()
