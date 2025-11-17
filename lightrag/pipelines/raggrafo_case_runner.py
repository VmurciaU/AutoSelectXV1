# lightrag/pipelines/raggrafo_case_runner.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Union

from .pc6_lightrag import (
    pipeline_ingest,
    pipeline_pushkg,
    PC2_DIR,
    PC4_DIR,
    PC5_DIR,
    PC6_EXPORT_DIR,
    RAG_STORAGE_DIR,
)


def run_raggrafo_for_case(
    case_id: Union[int, str],
    pc2_dir: Union[str, Path],
    pc4_dir: Union[str, Path],
    pc5_dir: Union[str, Path],
    use_core: bool = True,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    push_kg_api: bool = False,
) -> Tuple[Path, Path, Path, Path]:
    """
    PC-7 — Orquestador por caso.
    Usa PC-6 (pipeline_ingest + pipeline_pushkg) pero aislando
    el storage y los exports en subcarpetas por case_id.

    Crea:
      outputs/pc6_export/case_<case_id>/
      rag_storage/case_<case_id>/

    Y ejecuta:
      - pipeline_ingest(...)
      - pipeline_pushkg(...)
    """

    case_id_str = str(case_id)

    pc2_dir = Path(pc2_dir).resolve()
    pc4_dir = Path(pc4_dir).resolve()
    pc5_dir = Path(pc5_dir).resolve()

    if not pc2_dir.exists():
        raise FileNotFoundError(f"[PC7] pc2_dir no existe: {pc2_dir}")
    if not pc4_dir.exists():
        raise FileNotFoundError(f"[PC7] pc4_dir no existe: {pc4_dir}")
    if not pc5_dir.exists():
        raise FileNotFoundError(f"[PC7] pc5_dir no existe: {pc5_dir}")

    # Subcarpetas por caso reutilizando las constantes globales de PC6
    case_export_dir = (PC6_EXPORT_DIR / f"case_{case_id_str}").resolve()
    case_storage_dir = (RAG_STORAGE_DIR / f"case_{case_id_str}").resolve()

    case_export_dir.mkdir(parents=True, exist_ok=True)
    case_storage_dir.mkdir(parents=True, exist_ok=True)

    print(f"[PC7] ▶️ Ejecutando Raggrafo para case_id={case_id_str}")
    print(f"[PC7]   pc2_dir       = {pc2_dir}")
    print(f"[PC7]   pc4_dir       = {pc4_dir}")
    print(f"[PC7]   pc5_dir       = {pc5_dir}")
    print(f"[PC7]   export_dir    = {case_export_dir}")
    print(f"[PC7]   storage_dir   = {case_storage_dir}")
    print(f"[PC7]   use_core      = {use_core}")
    print(f"[PC7]   api_url       = {api_url or '(no API)'}")
    print(f"[PC7]   push_kg_api   = {push_kg_api}")

    # 1) PC6 — INGEST (corpus + ingesta a RAG)
    corpus_jsonl, corpus_json = pipeline_ingest(
        pc2_dir=pc2_dir,
        pc4_dir=pc4_dir,
        export_dir=case_export_dir,
        storage_dir=case_storage_dir,
        use_core=use_core,
        api_url=api_url,
        api_key=api_key,
    )

    # 2) PC6 — PUSH KG (grafo a JSON/JSONL + opcional API)
    kg_jsonl, kg_json = pipeline_pushkg(
        pc5_dir=pc5_dir,
        export_dir=case_export_dir,
        storage_dir=case_storage_dir,
        use_core=use_core,
        api_url=api_url,
        api_key=api_key,
        push_kg_api=push_kg_api,
    )

    print(f"[PC7] ✅ Corpus y KG listos para el caso {case_id_str}")
    print(f"[PC7]   corpus_jsonl = {corpus_jsonl}")
    print(f"[PC7]   corpus_json  = {corpus_json}")
    print(f"[PC7]   kg_jsonl     = {kg_jsonl}")
    print(f"[PC7]   kg_json      = {kg_json}")

    return corpus_jsonl, corpus_json, kg_jsonl, kg_json


# Pequeño CLI para probar desde consola: python -m lightrag.pipelines.raggrafo_case_runner ...
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PC-7 - Raggrafo por caso (usa PC-6 por debajo)"
    )
    parser.add_argument("--case-id", required=True, help="ID del caso (ej. 101)")

    parser.add_argument(
        "--pc2-dir",
        default=str(PC2_DIR),
        help=f"Directorio PC2 (por defecto: {PC2_DIR})",
    )
    parser.add_argument(
        "--pc4-dir",
        default=str(PC4_DIR),
        help=f"Directorio PC4 (por defecto: {PC4_DIR})",
    )
    parser.add_argument(
        "--pc5-dir",
        default=str(PC5_DIR),
        help=f"Directorio PC5 (por defecto: {PC5_DIR})",
    )

    parser.add_argument(
        "--use-core",
        action="store_true",
        help="Usar LightRAG Core (modo local) si está disponible",
    )
    parser.add_argument(
        "--api-url",
        default="",
        help="URL del LightRAG Server (ej. http://localhost:8777). "
             "Si se omite, sólo Core/export.",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help="API Key para el server (si aplica)",
    )
    parser.add_argument(
        "--push-kg-api",
        action="store_true",
        help="Empujar KG por API (entity/create + relation/create)",
    )

    args = parser.parse_args()

    run_raggrafo_for_case(
        case_id=args.case_id,
        pc2_dir=args.pc2_dir,
        pc4_dir=args.pc4_dir,
        pc5_dir=args.pc5_dir,
        use_core=args.use_core,
        api_url=(args.api_url or None),
        api_key=(args.api_key or None),
        push_kg_api=args.push_kg_api,
    )
