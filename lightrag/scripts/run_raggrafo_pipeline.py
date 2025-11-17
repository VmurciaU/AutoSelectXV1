#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_raggrafo_pipeline.py
------------------------
Orquestador de la cadena PC1–PC6 para 1..N PDFs.

- PC1: lectura PDF → pc1_raw_pages
- PC2: limpieza layout → pc2_clean_pages
- PC3: parse + dispatcher → pc3_blocks
- PC4: consolidación cross-doc → pc4_consolidated
- PC5: grafo mínimo → pc5_graph
- PC6 (opcional): corpus + export KG → pc6_export

Uso típico (desde la raíz del repo AutoSelectX):

  python lightrag/scripts/run_raggrafo_pipeline.py \
      --input-dir lightrag/tests/input \
      --out-root  lightrag/tests/output \
      --with-pc6

Si no pasas parámetros, toma esos mismos defaults.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

# --------------------------------------------------------------------
# Preparar sys.path para poder importar pipelines.* desde scripts/
# --------------------------------------------------------------------
THIS_FILE = Path(__file__).resolve()
ROOT = THIS_FILE.parent.parent  # carpeta lightrag/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines import (
    pc1_read_pdfs,
    pc2_clean_layout,
    pc3_parse_blocks,
    pc4_consolidate,
    pc5_graph_build,
    pc6_lightrag,
)


# --------------------------------------------------------------------
# Helpers de alto nivel
# --------------------------------------------------------------------
def run_pc1(input_dir: Path, pc1_dir: Path) -> List[Dict[str, Any]]:
    pc1_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[RUN] PC1 — input={input_dir}  out={pc1_dir}")
    manifests = pc1_read_pdfs.run_pc1(input_dir, out_dir=pc1_dir)
    return manifests


def run_pc2(pc1_dir: Path, pc2_dir: Path) -> None:
    pc2_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[RUN] PC2 — pc1_dir={pc1_dir}  out={pc2_dir}")
    pc2_clean_layout.run_pc2(pc1_dir=pc1_dir, out_dir=pc2_dir)


def run_pc3(pc1_dir: Path, pc2_dir: Path, pc3_dir: Path, data_dir: Path, doc_ids: List[str]) -> None:
    """
    Ajusta las rutas globales de pc3_parse_blocks para que apunten al
    caso que estamos procesando, y luego ejecuta run_pc3_for_doc(...)
    por cada doc_id.
    """
    pc3_dir.mkdir(parents=True, exist_ok=True)

    # Reconfigurar rutas globales de PC3 para este caso
    pc3_parse_blocks.PC1_DIR = pc1_dir
    pc3_parse_blocks.PC2_DIR = pc2_dir
    pc3_parse_blocks.PC3_BASE = pc3_dir
    pc3_parse_blocks.DATA_DIR = data_dir

    print(f"\n[RUN] PC3 — pc1_dir={pc1_dir}  pc2_dir={pc2_dir}  out={pc3_dir}")
    for doc_id in doc_ids:
        outdir_doc = pc3_dir / doc_id
        print(f"[PC3] Doc {doc_id} …", end="", flush=True)
        pc3_parse_blocks.run_pc3_for_doc(
            doc_id=doc_id,
            outdir=outdir_doc,
            pid_max_rows=5,
            pid_max_cols=4,
        )
        print(" listo.")


def run_pc4(pc3_dir: Path, pc4_dir: Path) -> None:
    """
    Replica la lógica de pc4_consolidate.main(), pero con rutas explícitas.
    """
    from pipelines.pc4_consolidate import (
        collect_doc_dirs,
        load_summary,
        build_master_sections,
        build_master_tables,
        build_pid_index,
        enrich_table_context,
    )

    pc4_dir.mkdir(parents=True, exist_ok=True)
    doc_dirs = collect_doc_dirs(pc3_dir)
    if not doc_dirs:
        print(f"[PC4] No hay documentos en {pc3_dir}.")
        return

    all_sections, all_tables, all_pids = [], [], []
    merged_summary: Dict[str, Any] = {"documents": []}

    print(f"\n[RUN] PC4 — pc3_dir={pc3_dir}  out={pc4_dir}")
    print(f"[PC4] Documentos detectados: {len(doc_dirs)}")
    for d in doc_dirs:
        try:
            summary = load_summary(d)
            doc_id = summary.get("doc_id", d.name)
            print(f"[PC4] Procesando {doc_id} …", end="", flush=True)

            df_sec = build_master_sections(d)
            df_tab = build_master_tables(d)
            df_pid = build_pid_index(d)

            if not df_tab.empty and not df_sec.empty:
                df_tab = enrich_table_context(df_tab, df_sec)

            if not df_sec.empty:
                all_sections.append(df_sec)
            if not df_tab.empty:
                all_tables.append(df_tab)
            if not df_pid.empty:
                all_pids.append(df_pid)

            summary["sections_count"] = int(df_sec.shape[0]) if not df_sec.empty else 0
            summary["tables_count"] = int(df_tab["table_uid"].nunique()) if not df_tab.empty else 0
            summary["pid_pages"] = (
                sorted(df_pid["page"].dropna().unique().tolist()) if not df_pid.empty else []
            )

            merged_summary["documents"].append(summary)
            print(" listo.")
        except Exception as e:
            print(f" ERROR: {e}")

    # Escritura de salidas
    (pc4_dir / "merged_summary.json").write_text(
        json.dumps(merged_summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    import pandas as pd

    if all_sections:
        pd.concat(all_sections, ignore_index=True).to_csv(
            pc4_dir / "master_sections.csv", index=False, encoding="utf-8"
        )
    else:
        (pc4_dir / "master_sections.csv").write_text("", encoding="utf-8")

    if all_tables:
        mt = pd.concat(all_tables, ignore_index=True)
        # Completar columnas c1..c50 si faltan
        for i in range(1, 51):
            c = f"c{i}"
            if c not in mt.columns:
                mt[c] = ""
        cols = [
            "doc_id",
            "doc_type",
            "file_name",
            "_page",
            "_table_idx",
            "_row",
            "table_uid",
            "section_number_near",
            "section_title_near",
            "caption_near",
            "table_order_in_page",
        ] + [f"c{i}" for i in range(1, 51)]
        mt = mt[[c for c in cols if c in mt.columns]]
        mt.to_csv(pc4_dir / "master_tables.csv", index=False, encoding="utf-8")
    else:
        (pc4_dir / "master_tables.csv").write_text("", encoding="utf-8")

    if all_pids:
        import pandas as pd

        pd.concat(all_pids, ignore_index=True).to_csv(
            pc4_dir / "pid_index.csv", index=False, encoding="utf-8"
        )
    else:
        (pc4_dir / "pid_index.csv").write_text("", encoding="utf-8")

    print("[PC4] ✅ Consolidación completada.")


def run_pc5(pc4_dir: Path, pc5_dir: Path) -> None:
    """
    Usa _load_pc4 + build_graph de pc5_graph_build, con rutas explícitas.
    """
    from pipelines.pc5_graph_build import _load_pc4, build_graph, _dump_jsonl

    pc5_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[RUN] PC5 — pc4_dir={pc4_dir}  out={pc5_dir}")

    data = _load_pc4(pc4_dir)
    df_sec, df_tab, df_pid = data["sections"], data["tables"], data["pid"]

    if df_sec.empty and df_tab.empty and df_pid.empty:
        print(f"[PC5] No hay datos en {pc4_dir}. Corre PC4 primero.")
        return

    nodes, edges = build_graph(df_sec, df_tab, df_pid)
    nodes.to_csv(pc5_dir / "graph_nodes.csv", index=False, encoding="utf-8")
    edges.to_csv(pc5_dir / "graph_edges.csv", index=False, encoding="utf-8")
    _dump_jsonl(nodes, edges, pc5_dir / "graph.jsonl")

    print("[PC5] ✅ Grafo generado.")
    print(f" - {pc5_dir/'graph_nodes.csv'}")
    print(f" - {pc5_dir/'graph_edges.csv'}")
    print(f" - {pc5_dir/'graph.jsonl'}")


def run_pc6(pc2_dir: Path, pc4_dir: Path, pc5_dir: Path, export_dir: Path, storage_dir: Path,
            use_core: bool, api_url: str | None, api_key: str | None,
            push_kg_api: bool = False) -> None:
    """
    Llama directamente a pipeline_ingest + pipeline_pushkg de pc6_lightrag,
    usando las rutas de este caso.
    """
    export_dir.mkdir(parents=True, exist_ok=True)
    storage_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[RUN] PC6 — ingest (pc2={pc2_dir}, pc4={pc4_dir}) → {export_dir}")
    pc6_lightrag.pipeline_ingest(
        pc2_dir=pc2_dir,
        pc4_dir=pc4_dir,
        export_dir=export_dir,
        storage_dir=storage_dir,
        use_core=use_core,
        api_url=api_url,
        api_key=api_key,
        batch_size=800,
    )

    print(f"\n[RUN] PC6 — pushkg (pc5={pc5_dir}) → {export_dir}")
    pc6_lightrag.pipeline_pushkg(
        pc5_dir=pc5_dir,
        export_dir=export_dir,
        storage_dir=storage_dir,
        use_core=use_core,
        api_url=api_url,
        api_key=api_key,
        push_kg_api=push_kg_api,
    )


# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Orquestador PC1–PC6 para RAGGrafo/LightRAG")
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(ROOT / "tests" / "input"),
        help="Carpeta con PDFs de entrada (default: lightrag/tests/input)",
    )
    parser.add_argument(
        "--out-root",
        type=str,
        default=str(ROOT / "tests" / "output"),
        help="Carpeta raíz de salidas (pc1_raw_pages, pc2_clean_pages, etc.)",
    )
    parser.add_argument(
        "--with-pc6",
        action="store_true",
        help="Incluye PC6 (corpus + export KG para LightRAG).",
    )
    parser.add_argument(
        "--use-core",
        action="store_true",
        help="Intentar también ingesta/push KG con LightRAG Core local.",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="",
        help="URL del LightRAG Server (ej. http://localhost:8777). Si vacío, modo solo export.",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default="",
        help="API key para el server (si aplica).",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    api_url = args.api_url.strip() or None
    api_key = args.api_key.strip() or None

    # Estructura por caso bajo out_root
    pc1_dir = out_root / "pc1_raw_pages"
    pc2_dir = out_root / "pc2_clean_pages"
    pc3_dir = out_root / "pc3_blocks"
    pc4_dir = out_root / "pc4_consolidated"
    pc5_dir = out_root / "pc5_graph"
    pc6_export_dir = out_root / "pc6_export"
    storage_dir = out_root / "rag_storage"

    out_root.mkdir(parents=True, exist_ok=True)

    print("================================================================")
    print("[PIPELINE] Iniciando cadena PC1–PC5" + (" + PC6" if args.with_pc6 else ""))
    print(f"  PDFs  : {input_dir}")
    print(f"  OUT   : {out_root}")
    print("================================================================")

    # --- PC1 ---
    manifests = run_pc1(input_dir, pc1_dir)
    doc_ids = [m.get("doc_id") for m in manifests if m.get("doc_id")]

    # --- PC2 ---
    run_pc2(pc1_dir, pc2_dir)

    # --- PC3 ---
    run_pc3(pc1_dir, pc2_dir, pc3_dir, data_dir=input_dir, doc_ids=doc_ids)

    # --- PC4 ---
    run_pc4(pc3_dir, pc4_dir)

    # --- PC5 ---
    run_pc5(pc4_dir, pc5_dir)

    # --- PC6 (opcional) ---
    if args.with_pc6:
        run_pc6(
            pc2_dir=pc2_dir,
            pc4_dir=pc4_dir,
            pc5_dir=pc5_dir,
            export_dir=pc6_export_dir,
            storage_dir=storage_dir,
            use_core=args.use_core,
            api_url=api_url,
            api_key=api_key,
            push_kg_api=False,
        )

    print("\n[PIPELINE] ✅ Proceso completado.")


if __name__ == "__main__":
    import json  # necesario para run_pc4
    main()
