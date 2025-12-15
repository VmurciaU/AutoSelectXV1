"""
Reset de cache RAG + pipeline
Funciona en WSL, Ubuntu, Render (Linux)

Uso:
  # Ver qué se borraría (todo)
  python -m app.scripts.reset_rag_cache --all --dry-run

  # Borrar TODO
  python -m app.scripts.reset_rag_cache --all

  # Ver qué se borraría para un caso
  python -m app.scripts.reset_rag_cache --case-id 2 --dry-run

  # Borrar solo un caso
  python -m app.scripts.reset_rag_cache --case-id 2
"""

import argparse
import shutil
from pathlib import Path


def rm(path: Path, dry_run: bool):
    if not path.exists():
        return
    if dry_run:
        print(f"[DRY-RUN] would delete: {path}")
    else:
        shutil.rmtree(path)
        print(f"[DELETED] {path}")


def main():
    parser = argparse.ArgumentParser(description="Reset cache RAG / pipeline")
    parser.add_argument("--case-id", type=str, help="ID del caso a borrar")
    parser.add_argument("--all", action="store_true", help="Borrar TODOS los casos")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin borrar")

    args = parser.parse_args()

    if not args.all and not args.case_id:
        raise SystemExit("❌ Debes usar --all o --case-id")

    # 📍 raíz del repo (funciona igual en Render y WSL)
    ROOT = Path(__file__).resolve().parents[2]

    targets = []

    if args.all:
        targets.extend([
            ROOT / "raggrafo" / "outputs" / "cases",
            ROOT / "raggrafo" / "outputs" / "pc6_export",
            ROOT / "raggrafo" / "rag_storage",
        ])
    else:
        cid = args.case_id
        targets.extend([
            ROOT / "raggrafo" / "outputs" / "cases" / cid,
            ROOT / "raggrafo" / "outputs" / "pc6_export" / f"case_{cid}",
            ROOT / "raggrafo" / "rag_storage" / f"case_{cid}",
        ])

    print("\n=== RESET RAG CACHE ===")
    for t in targets:
        rm(t, args.dry_run)

    # Limpieza segura de pycache
    print("\n=== CLEAN __pycache__ ===")
    for pyc in ROOT.rglob("__pycache__"):
        rm(pyc, args.dry_run)

    if args.dry_run:
        print("\n🟡 DRY-RUN terminado (no se borró nada)")
    else:
        print("\n✅ RESET COMPLETADO")


if __name__ == "__main__":
    main()
