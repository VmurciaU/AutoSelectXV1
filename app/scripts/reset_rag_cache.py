"""
Reset de cache RAG + pipeline (+ shared_data opcional)
Funciona en WSL, Ubuntu, Render (Linux)

USO:

# Ver qué se borraría (TODO, incluyendo shared_data)
python -m app.scripts.reset_rag_cache --all --wipe-shared --dry-run

# Borrar TODO (incluye shared_data)
python -m app.scripts.reset_rag_cache --all --wipe-shared

# Solo RAG / pipeline (NO shared_data)
python -m app.scripts.reset_rag_cache --all

# Ver qué se borraría para un caso (incluye shared_data)
python -m app.scripts.reset_rag_cache --case-id 5 --wipe-shared --dry-run

# Borrar solo un caso (incluye shared_data)
python -m app.scripts.reset_rag_cache --case-id 5 --wipe-shared
"""

import argparse
import shutil
from pathlib import Path


def rm(path: Path, dry_run: bool):
    if not path.exists():
        print(f"[SKIP] not found: {path}")
        return
    if dry_run:
        print(f"[DRY-RUN] would delete: {path}")
    else:
        shutil.rmtree(path)
        print(f"[DELETED] {path}")


def safe_pycache_cleanup(root: Path, dry_run: bool):
    """
    Borra __pycache__ SOLO dentro del repo.
    Excluye venv / .venv explícitamente.
    """
    for pyc in root.rglob("__pycache__"):
        if any(p in ("venv", ".venv") for p in pyc.parts):
            continue
        rm(pyc, dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="Reset cache RAG / pipeline / shared_data"
    )
    parser.add_argument(
        "--case-id",
        type=str,
        help="ID del caso a borrar (ej: 4)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Borrar TODOS los casos",
    )
    parser.add_argument(
        "--wipe-shared",
        action="store_true",
        help="Borrar shared_data/inbox y shared_data/index",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular sin borrar",
    )
    args = parser.parse_args()

    if not args.all and not args.case_id:
        raise SystemExit("❌ Debes usar --all o --case-id")

    # Raíz del repo:
    # app/scripts/reset_rag_cache.py -> app -> repo root
    ROOT = Path(__file__).resolve().parents[2]

    targets = []

    # =====================================================
    # 1) RAG + PIPELINE
    # =====================================================
    if args.all:
        targets.extend([
            ROOT / "raggrafo" / "outputs",
            ROOT / "raggrafo" / "rag_storage",
        ])
    else:
        cid = args.case_id
        targets.extend([
            # outputs/cases/4
            ROOT / "raggrafo" / "outputs" / "cases" / cid,
            # outputs/cases/case_4 (por compatibilidad)
            ROOT / "raggrafo" / "outputs" / "cases" / f"case_{cid}",
            # pc6_export/case_4
            ROOT / "raggrafo" / "outputs" / "pc6_export" / f"case_{cid}",
            # rag_storage/case_4
            ROOT / "raggrafo" / "rag_storage" / f"case_{cid}",
        ])

    # =====================================================
    # 2) SHARED DATA (PDFs + índices)
    # =====================================================
    if args.wipe_shared:
        if args.all:
            targets.extend([
                ROOT / "shared_data" / "inbox",
                ROOT / "shared_data" / "index",
            ])
        else:
            cid = args.case_id
            targets.extend([
                ROOT / "shared_data" / "inbox" / str(cid),
                ROOT / "shared_data" / "index" / str(cid),
            ])

    print("\n=== RESET RAG CACHE / PIPELINE ===")
    for t in targets:
        rm(t, args.dry_run)

    print("\n=== CLEAN __pycache__ (safe, excluding venv) ===")
    safe_pycache_cleanup(ROOT, args.dry_run)

    if args.dry_run:
        print("\n🟡 DRY-RUN terminado (no se borró nada)")
    else:
        print("\n✅ RESET COMPLETADO CON ÉXITO")


if __name__ == "__main__":
    main()
