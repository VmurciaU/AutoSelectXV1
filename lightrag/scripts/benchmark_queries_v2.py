#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark LightRAG por modos, con respuesta más concreta y control de salida.

Uso rápido:
  python benchmark_queries_v2.py
  python benchmark_queries_v2.py --modes mix global local
  python benchmark_queries_v2.py --q "pregunta 1" --q "pregunta 2"
  python benchmark_queries_v2.py --style concise --wrap 110
  python benchmark_queries_v2.py --style json     # (respuesta SOLO JSON)

Notas:
- --style concise: 5–8 bullets, datos y unidades, sin relleno.
- --style json    : SOLO JSON (útil para métricas/parsers).
"""
import os, sys, json, time, argparse, datetime, textwrap
import requests

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
    # fill conserva palabras y evita cortar registros JSON largos
    out = []
    for line in (text or "").splitlines():
        if not line.strip():
            out.append("")
            continue
        # Si parece JSON (empieza con { o [) no lo envolvemos
        if line.lstrip().startswith(("{", "[")):
            out.append(line)
        else:
            out.append(textwrap.fill(
                line,
                width=width,
                replace_whitespace=False,
                break_long_words=False,
            ))
    return "\n".join(out)

# --------------------------- llamada a la API ---------------------------
def build_instruction(style: str) -> str:
    style = (style or "concise").lower()
    if style == "json":
        # Instrucción genérica para devolver SOLO JSON (sin preámbulos)
        return (
            "Devuelve SOLO un JSON válido y minimalista con los datos solicitados; "
            "usa claves cortas en español; incluye unidades y 'no especificado' si falta; "
            "no añadas texto fuera del JSON."
        )
    # Por defecto: estilo conciso con bullets (concreto pero no escuálido)
    return (
        "Responde en 5–8 viñetas, concretas y técnicas. "
        "Prioriza valores numéricos exactos con unidades; si el dato no aparece, di 'no especificado'. "
        "Incluye la norma/criterio cuando aplique (p. ej., API 675). "
        "Evita introducciones, conclusiones y relleno; nada de promesas futuras."
    )

def make_query(user_q: str, style: str) -> str:
    instr = build_instruction(style)
    return f"{instr}\n\nPregunta: {user_q}".strip()

def ask(api, query, mode, timeout=180):
    head = {"Content-Type": "application/json"}
    payload = {"query": query, "mode": mode}
    t0 = time.time()
    r = requests.post(f"{api.rstrip('/')}/query", headers=head, data=json.dumps(payload), timeout=timeout)
    dt = time.time() - t0
    try:
        data = r.json()
    except Exception:
        data = {"response": r.text}
    return dt, data

# --------------------------- CLI principal ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8777", help="URL de LightRAG")
    ap.add_argument("--modes", nargs="*", default=["naive","local","global","mix"], help="Modos a probar")
    ap.add_argument("--q", dest="queries", action="append", default=[], help="Agregar pregunta (repetible)")
    ap.add_argument("--style", default="concise", choices=["concise", "json"], help="Formato de respuesta")
    ap.add_argument("--no-md", action="store_true", help="No guardar Markdown")
    ap.add_argument("--no-json", action="store_true", help="No guardar JSON")
    ap.add_argument("--wrap", type=int, default=term_width(), help="Ancho de envoltura para impresión")
    ap.add_argument("--timeout", type=int, default=180, help="Timeout por consulta (s)")
    args = ap.parse_args()

    # Preguntas por defecto
    if not args.queries:
        args.queries = [
    "¿Cuál es el caudal nominal, presion de trabajo, viscosidad y el turndown de la o las bomba dosificadora y sus Tag?",
    "¿Qué Test o pruebas, acorde al API-675 son solicitadas para las bombas y sus Tags?",
    "Dame los instrumentos o instrumentacion solicitada asociada a las bombas y a los sistemas de inyeccion",
    "Conecta Bomba Dosificadora -> Prueba FAT -> API 675 (camino y justificación).",
        ]
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_md  = f"benchmark_{ts}.md"
    out_json= f"benchmark_{ts}.json"

    results = []

    print("\n== LightRAG Benchmark por modos ==\n")
    for q_idx, raw_q in enumerate(args.queries, 1):
        q = make_query(raw_q, args.style)
        print(f"Q{q_idx}: {raw_q}")
        blk = {"question": raw_q, "modes": [], "style": args.style}
        for m in args.modes:
            try:
                dt, data = ask(args.api, q, m, timeout=args.timeout)
                resp = (data.get("response") or "").strip()
                refs = data.get("references") or []
                blk["modes"].append({"mode": m, "elapsed": dt, "response": resp, "references": refs})
                print(f"  [{m:6}] {dt:5.2f}s")
                print(wrap(resp, args.wrap))
                if refs:
                    print("  ↳ Referencias:")
                    for r in refs:
                        rid  = r.get("reference_id")
                        path = r.get("file_path")
                        print(f"     - [{rid}] {path}")
                print()
            except Exception as e:
                blk["modes"].append({"mode": m, "error": str(e)})
                print(f"  [{m:6}] ERROR: {e}\n")
        results.append(blk)

    # ====== Guardado ======
    if not args.no_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({"api": args.api, "style": args.style, "results": results}, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON guardado: {out_json}")

    if not args.no_md:
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(f"# LightRAG Benchmark — {ts}\n\n")
            f.write(f"- API: `{args.api}`\n- Modos: {', '.join(args.modes)}\n- Estilo: `{args.style}`\n\n---\n")
            for i, blk in enumerate(results, 1):
                f.write(f"## Q{i}. {blk['question']}\n\n")
                for m in blk["modes"]:
                    if "error" in m:
                        f.write(f"**[{m['mode']}]** ERROR: {m['error']}\n\n")
                        continue
                    f.write(f"**[{m['mode']}]** ({m['elapsed']:.2f}s)\n\n")
                    f.write(blk['modes'][[x['mode'] for x in blk['modes']].index(m['mode'])]['response'] + "\n\n")
                    refs = m.get("references") or []
                    if refs:
                        f.write("**Referencias**\n\n")
                        for r in refs:
                            f.write(f"- [{r.get('reference_id')}] {r.get('file_path')}\n")
                        f.write("\n")
                f.write("\n---\n")
        print(f"📝 Markdown guardado: {out_md}\nSugerencia: abre con `less -R {out_md}` o en VSCode.")

if __name__ == "__main__":
    main()
