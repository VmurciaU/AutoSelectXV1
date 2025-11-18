# raggrafo/scripts/run_pipeline_for_case.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import argparse
import subprocess
import sys
import json

from raggrafo.pipelines.raggrafo_case_runner import run_raggrafo_for_case
from raggrafo.pipelines import pc3_hd_pipeline


# Raíces
ROOT = Path(__file__).resolve().parents[1]      # .../raggrafo
PROJECT_ROOT = ROOT.parent                      # raíz del repo AutoSelectX


def _run(cmd: list[str]) -> None:
    """Wrapper pequeño para ejecutar comandos y mostrar lo que se corre."""
    print(f"[CMD] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def run_pc1_to_pc5_for_case(case_id: int):
    """
    Ejecuta PC1–PC5 para un caso específico usando los PDFs de:
        shared_data/inbox/<case_id>/original/

    Salidas a:
        raggrafo/outputs/cases/<case_id>/...
    """
    case_id_str = str(case_id)

    # PDFs reales del caso
    input_dir = PROJECT_ROOT / "shared_data" / "inbox" / case_id_str / "original"

    if not input_dir.exists():
        raise FileNotFoundError(
            f"[run_pc1_to_pc5_for_case] No existe la carpeta de PDFs para el caso {case_id}: {input_dir}"
        )

    # Directorios de salida por caso
    base_out = ROOT / "outputs" / "cases" / case_id_str
    pc1_dir = base_out / "pc1_raw_pages"
    pc2_dir = base_out / "pc2_clean_pages"
    pc3_dir = base_out / "pc3_blocks"
    pc4_dir = base_out / "pc4_consolidated"
    pc5_dir = base_out / "pc5_graph"

    # Crear estructura necesaria
    for d in [pc1_dir, pc2_dir, pc3_dir, pc4_dir, pc5_dir]:
        d.mkdir(parents=True, exist_ok=True)

    print("==============================================================")
    print(f"[CASE PIPELINE] Ejecutando PC1–PC5 para case_id={case_id_str}")
    print(f"[CASE PIPELINE] PDFs      : {input_dir}")
    print(f"[CASE PIPELINE] PC1 out   : {pc1_dir}")
    print(f"[CASE PIPELINE] PC2 out   : {pc2_dir}")
    print(f"[CASE PIPELINE] PC3 out   : {pc3_dir}")
    print(f"[CASE PIPELINE] PC4 out   : {pc4_dir}")
    print(f"[CASE PIPELINE] PC5 out   : {pc5_dir}")
    print("==============================================================")

    py = sys.executable  # python del venv actual

    # === PC1 ===
    #OJO: aquí dejamos los flags tal como ya te funciona (--input-dir / --out-dir)
    _run([
        py, "-m", "raggrafo.pipelines.pc1_read_pdfs",
        "--input-dir", str(input_dir),
        "--out-dir", str(pc1_dir),
    ])

    # === PC2 ===
    # pc2_clean_layout.py espera: --pc1 y --out (no --pc1-dir / --out-dir)
    _run([
        py, "-m", "raggrafo.pipelines.pc2_clean_layout",
        "--pc1", str(pc1_dir),
        "--out", str(pc2_dir),
    ])

    # === PC3 ===
    # En lugar de llamar por CLI a pc3_text_tables_pipeline (no tiene main),
    # usamos directamente pc3_hd_pipeline.run_pipeline() por cada documento
    index_path = pc1_dir / "index.json"
    if not index_path.exists():
        raise FileNotFoundError(
            f"[CASE PIPELINE] No se encontró el índice de PC-1 para el caso {case_id}: {index_path}"
        )

    index_data = json.loads(index_path.read_text(encoding="utf-8"))
    manifests = index_data.get("documents", [])
    if not manifests:
        raise RuntimeError(
            f"[CASE PIPELINE] El índice de PC-1 no contiene documentos para el caso {case_id}."
        )

    pc3_results = []
    print("[CASE PIPELINE] ▶️ PC3 — Bloques/Tablas HD + PID embebido por documento")
    for mf in manifests:
        doc_id = mf.get("doc_id")
        fname = mf.get("file_name")
        print(f"  [PC3] Procesando doc_id={doc_id} file={fname}")
        res = pc3_hd_pipeline.run_pipeline(
            manifest=mf,
            pc1_dir=pc1_dir,
            pc2_dir=pc2_dir,
            input_pdf_dir=input_dir,
            out_base_dir=pc3_dir,
        )
        pc3_results.append(res)

    # Índice global de PC-3 por caso
    (pc3_dir / "index.json").write_text(
        json.dumps({"documents": pc3_results}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("[CASE PIPELINE] ✅ PC3 completado.")

    # === PC4 ===
    # pc4_consolidate.py usa: --pc3-dir y --outdir (no --out-dir)
    _run([
        py, "-m", "raggrafo.pipelines.pc4_consolidate",
        "--pc3-dir", str(pc3_dir),
        "--outdir", str(pc4_dir),
    ])

    # === PC5 ===
    # pc5_graph_build.py usa: --pc4-dir y --outdir (no --out-dir)
    _run([
        py, "-m", "raggrafo.pipelines.pc5_graph_build",
        "--pc4-dir", str(pc4_dir),
        "--outdir", str(pc5_dir),
    ])

    print("[CASE PIPELINE] ✅ PC1–PC5 completados.")

    return pc2_dir, pc4_dir, pc5_dir


def main():
    parser = argparse.ArgumentParser(
        description="Ejecutar PC1–PC5 + PC7 para un caso (PDFs reales)."
    )
    parser.add_argument(
        "--case-id",
        type=int,
        required=True,
        help="ID del caso (ej. 14)."
    )
    parser.add_argument(
        "--use-core",
        action="store_true",
        help="Usar LightRAG Core en PC7."
    )

    args = parser.parse_args()
    case_id = args.case_id

    # Ejecutar PC1–PC5
    pc2_dir, pc4_dir, pc5_dir = run_pc1_to_pc5_for_case(case_id)

    print("[CASE PIPELINE] ▶️ Ejecutando PC7 (RAG + KG)...")

    corpus_jsonl, corpus_json, kg_jsonl, kg_json = run_raggrafo_for_case(
        case_id=case_id,
        pc2_dir=pc2_dir,
        pc4_dir=pc4_dir,
        pc5_dir=pc5_dir,
        use_core=args.use_core,
        api_url=None,
        api_key=None,
        push_kg_api=False,
    )

    print("==============================================================")
    print(f"[CASE PIPELINE] ✅ RAG listo para el caso {case_id}")
    print(f" corpus_jsonl : {corpus_jsonl}")
    print(f" corpus_json  : {corpus_json}")
    print(f" kg_jsonl     : {kg_jsonl}")
    print(f" kg_json      : {kg_json}")
    print("==============================================================")


if __name__ == "__main__":
    main()
