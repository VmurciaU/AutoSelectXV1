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

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable, List


def rm_tree(path: Path, dry_run: bool):
    """Borra directorios completos."""
    if not path.exists():
        print(f"[SKIP] not found: {path}")
        return
    if not path.is_dir():
        print(f"[SKIP] not a dir: {path}")
        return
    if dry_run:
        print(f"[DRY-RUN] would delete: {path}")
        return
    shutil.rmtree(path, ignore_errors=True)
    print(f"[DELETED] {path}")


def rm_file(path: Path, dry_run: bool):
    """Borra archivos sueltos si existen."""
    if not path.exists():
        return
    if not path.is_file():
        return
    if dry_run:
        print(f"[DRY-RUN] would delete file: {path}")
        return
    try:
        path.unlink()
        print(f"[DELETED] {path}")
    except Exception as e:
        print(f"[WARN] could not delete file {path}: {e}")


def safe_pycache_cleanup(root: Path, dry_run: bool):
    """
    Borra __pycache__ SOLO dentro del repo.
    Excluye venv / .venv explícitamente.
    """
    for pyc in root.rglob("__pycache__"):
        if any(p in ("venv", ".venv") for p in pyc.parts):
            continue
        rm_tree(pyc, dry_run)


def _case_variants(cid: str) -> List[str]:
    """
    Algunos historiales usaron case_17 vs case17. Soportamos ambos.
    """
    cid = str(cid).strip()
    return [f"case_{cid}", f"case{cid}"]


def _unique_existing_paths(paths: Iterable[Path]) -> List[Path]:
    """
    Dedup por string; mantenemos orden de aparición.
    """
    seen = set()
    out = []
    for p in paths:
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        out.append(p)
    return out


def main():
    parser = argparse.ArgumentParser(description="Reset cache RAG / pipeline / shared_data")
    parser.add_argument("--case-id", type=str, help="ID del caso a borrar (ej: 4)")
    parser.add_argument("--all", action="store_true", help="Borrar TODOS los casos")
    parser.add_argument("--wipe-shared", action="store_true", help="Borrar shared_data/inbox y shared_data/index")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin borrar")
    args = parser.parse_args()

    if not args.all and not args.case_id:
        raise SystemExit("❌ Debes usar --all o --case-id")

    # Raíz del repo:
    # app/scripts/reset_rag_cache.py -> app -> repo root
    ROOT = Path(__file__).resolve().parents[2]
    if not (ROOT / "raggrafo").exists():
        raise SystemExit(f"❌ ROOT inválido (no existe raggrafo): {ROOT}")

    targets_dirs: List[Path] = []
    targets_files: List[Path] = []

    # =====================================================
    # 1) RAG + PIPELINE
    # =====================================================
    if args.all:
        # OJO: aquí borramos TODO lo de outputs (incluye cases y pc6_export)
        targets_dirs.extend([
            ROOT / "raggrafo" / "outputs",
            ROOT / "raggrafo" / "rag_storage",
        ])
    else:
        cid = str(args.case_id).strip()

        # PC1–PC5
        targets_dirs.extend([
            ROOT / "raggrafo" / "outputs" / "cases" / cid,
            ROOT / "raggrafo" / "outputs" / "cases" / f"case_{cid}",  # compat
            ROOT / "raggrafo" / "outputs" / "cases" / f"case{cid}",   # compat
        ])

        # PC6 export (¡clave!)
        for v in _case_variants(cid):
            targets_dirs.append(ROOT / "raggrafo" / "outputs" / "pc6_export" / v)

        # RAG storage (vectores + KV + cache LLM) (¡clave!)
        for v in _case_variants(cid):
            targets_dirs.append(ROOT / "raggrafo" / "rag_storage" / v)

        # Si guardas raw/normalized en otros sitios, añádelos aquí (best-effort)
        # (no rompe si no existen)
        targets_files.extend([
            ROOT / "raggrafo" / "rag_storage" / f"case_{cid}" / "raw.json",
            ROOT / "raggrafo" / "rag_storage" / f"case_{cid}" / "normalized.json",
        ])

    # =====================================================
    # 2) SHARED DATA (PDFs + índices)
    # =====================================================
    if args.wipe_shared:
        if args.all:
            targets_dirs.extend([
                ROOT / "shared_data" / "inbox",
                ROOT / "shared_data" / "index",
            ])
        else:
            cid = str(args.case_id).strip()
            targets_dirs.extend([
                ROOT / "shared_data" / "inbox" / cid,
                ROOT / "shared_data" / "index" / cid,
            ])

    # Dedup
    targets_dirs = _unique_existing_paths(targets_dirs)
    targets_files = _unique_existing_paths(targets_files)

    print("\n=== RESET RAG CACHE / PIPELINE ===")
    for t in targets_dirs:
        rm_tree(t, args.dry_run)

    for f in targets_files:
        rm_file(f, args.dry_run)

    print("\n=== CLEAN __pycache__ (safe, excluding venv) ===")
    safe_pycache_cleanup(ROOT, args.dry_run)

    if args.dry_run:
        print("\n🟡 DRY-RUN terminado (no se borró nada)")
    else:
        print("\n✅ RESET COMPLETADO CON ÉXITO")


if __name__ == "__main__":
    main()
