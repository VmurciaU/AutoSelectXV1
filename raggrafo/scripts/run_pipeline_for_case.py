# raggrafo/scripts/run_pipeline_for_case.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import argparse
import subprocess
import sys
import json
import os

from raggrafo.pipelines.raggrafo_case_runner import run_raggrafo_for_case
from raggrafo.pipelines import pc3_hd_pipeline

# Intentamos importar el cliente nuevo de OpenAI
try:
    from openai import OpenAI
    from openai import AuthenticationError as OpenAIAuthError
except Exception:  # si no está instalado, dejamos marcadores nulos
    OpenAI = None
    OpenAIAuthError = Exception

# Raíces
ROOT = Path(__file__).resolve().parents[1]      # .../raggrafo
PROJECT_ROOT = ROOT.parent                      # raíz del repo AutoSelectXV1


def _run(cmd: list[str]) -> None:
    """
    Wrapper pequeño para ejecutar comandos y mostrar lo que se corre.
    Levanta CalledProcessError si el proceso hijo sale con código != 0.
    """
    print(f"[CMD] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def _check_embeddings_key() -> None:
    """
    Verifica que la API key de embeddings de OpenAI sea válida ANTES de PC7.
    Si hay error de autenticación (401) -> se lanza excepción y este script
    terminará con exit code 1, que es lo que necesita el backend.
    """
    binding = os.getenv("LIGHTRAG_EMBEDDING_BINDING", "openai").lower()
    if binding != "openai":
        # Para otros bindings (local, ollama, etc.) no hacemos chequeo.
        print(f"[PC7] LIGHTRAG_EMBEDDING_BINDING={binding}, no se valida OpenAI.", file=sys.stderr)
        return

    if OpenAI is None:
        # No está instalado el cliente nuevo; no podemos validar aquí.
        print("[PC7] Cliente OpenAI no disponible; no se pudo validar la API key.", file=sys.stderr)
        return

    # Modelo de embeddings a validar
    model = (
        os.getenv("EMBEDDING_MODEL")
        or os.getenv("LIGHTRAG_EMBEDDING_MODEL")
        or "text-embedding-3-large"
    )

    print(f"[PC7] Validando API key de OpenAI embeddings con modelo '{model}'...")

    client = OpenAI()

    try:
        # Llamada mínima para validar la key
        client.embeddings.create(
            model=model,
            input="ping-autoselectx-check",
        )
        print("[PC7] ✅ API key de OpenAI válida para embeddings.")
    except OpenAIAuthError as e:
        # Error típico 401: invalid_api_key
        print(
            "[PC7] ❌ Error de autenticación con OpenAI embeddings "
            "(revisa OPENAI_API_KEY / EMBEDDING_MODEL).",
            file=sys.stderr,
        )
        raise
    except Exception as e:
        # Cualquier otro error también lo consideramos fatal para el pipeline
        print(f"[PC7] ❌ Error al validar embeddings: {e}", file=sys.stderr)
        raise


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
    _run([
        py, "-m", "raggrafo.pipelines.pc1_read_pdfs",
        "--input-dir", str(input_dir),
        "--out-dir", str(pc1_dir),
    ])

    # === PC2 ===
    _run([
        py, "-m", "raggrafo.pipelines.pc2_clean_layout",
        "--pc1", str(pc1_dir),
        "--out", str(pc2_dir),
    ])

    # === PC3 ===
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
    _run([
        py, "-m", "raggrafo.pipelines.pc4_consolidate",
        "--pc3-dir", str(pc3_dir),
        "--outdir", str(pc4_dir),
    ])

    # === PC5 ===
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
        help="ID del caso (ej. 14).",
    )
    parser.add_argument(
        "--use-core",
        action="store_true",
        help="Usar LightRAG Core en PC7.",
    )

    args = parser.parse_args()
    case_id = args.case_id

    try:
        # 1) PC1–PC5
        pc2_dir, pc4_dir, pc5_dir = run_pc1_to_pc5_for_case(case_id)

        # 2) Validar API key de OpenAI antes de levantar LightRAG Core
        _check_embeddings_key()

        # 3) PC7 – RAG + KG
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

    except Exception as e:
        # Cualquier error (PC1–PC7 o _check_embeddings_key) hace que
        # este script termine con exit code 1.
        print(
            f"[CASE PIPELINE] ❌ Error general en case_id={case_id}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
