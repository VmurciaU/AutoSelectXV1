#!/usr/bin/env python3
"""
PC-2 — Limpieza de layout (headers/footers repetidos)
----------------------------------------------------
Versión parametrizada (compatible con la arquitectura nueva):

Permite:
- Usar la antigua estructura:
    outputs/pc1_raw_pages/<doc_id>
    outputs/pc2_clean_pages/<doc_id>

- O una estructura parametrizada por caso:
    index/<case_id>/tmp/pc1_raw_pages/<doc_id>
    index/<case_id>/tmp/pc2_clean_pages/<doc_id>

NO cambia la lógica del PC-2, solo recibe rutas configurables
para el pipeline automático.
"""
import argparse
import json
import re
from pathlib import Path
from collections import Counter
from typing import List, Dict, Any

# --- Rutas por defecto (compatibilidad con uso clásico) ---
THIS_FILE = Path(__file__).resolve()
PARENT = THIS_FILE.parent
# Permite ejecutar desde raggrafo/scripts/, raggrafo/pipelines/ o desde el paquete raíz
if PARENT.name in {"scripts", "pipelines"}:
    ROOT = PARENT.parent
else:
    ROOT = PARENT

PC1_DIR_DEFAULT = ROOT / "outputs" / "pc1_raw_pages"
OUT_DIR_DEFAULT = ROOT / "outputs" / "pc2_clean_pages"
OUT_DIR_DEFAULT.mkdir(parents=True, exist_ok=True)

# --- Parámetros de detección ---
TOP_LINES = 6
FREQ_THRESHOLD = 0.6
MINLEN = 8
STRIP_CHARS = " \t\u200b\u200e\u200f"

# ============================================================
# UTILIDADES
# ============================================================

def normalize_line(line: str) -> str:
    s = line.replace("\xa0", " ").strip(STRIP_CHARS)
    s = re.sub(r"\s+", " ", s)
    return s


def detect_repeated_lines(pages: List[List[str]]) -> Dict[str, List[str]]:
    """Detecta headers/footers repetidos en ≥60% de las páginas."""
    n_pages = len(pages)
    top_counter = Counter()
    bot_counter = Counter()

    for lines in pages:
        if not lines:
            continue
        tops = lines[:TOP_LINES]
        bots = lines[-TOP_LINES:] if len(lines) >= TOP_LINES else lines[-len(lines):]

        top_counter.update([normalize_line(l) for l in tops if len(normalize_line(l)) >= MINLEN])
        bot_counter.update([normalize_line(l) for l in bots if len(normalize_line(l)) >= MINLEN])

    top_common = [ln for ln, c in top_counter.items() if c >= FREQ_THRESHOLD * n_pages]
    bot_common = [ln for ln, c in bot_counter.items() if c >= FREQ_THRESHOLD * n_pages]

    # Filtrado de líneas poco útiles
    def plausible(line: str) -> bool:
        if line.isupper() and len(line.split()) <= 2:
            return False
        return True

    return {
        "header": [ln for ln in top_common if plausible(ln)],
        "footer": [ln for ln in bot_common if plausible(ln)],
    }


def apply_cleanup(lines: List[str], patterns: Dict[str, List[str]]) -> List[str]:
    header = set(patterns.get("header", []))
    footer = set(patterns.get("footer", []))
    new_lines = []

    for idx, raw in enumerate(lines):
        norm = normalize_line(raw)
        if idx < TOP_LINES and norm in header:
            continue
        if idx >= len(lines) - TOP_LINES and norm in footer:
            continue
        new_lines.append(raw)
    return new_lines


# ============================================================
# PROCESAMIENTO DE DOCUMENTO
# ============================================================

def process_document(doc_dir: Path, out_dir: Path) -> Dict[str, Any]:
    """
    Procesa un documento desde PC-1 y genera su versión limpia en out_dir/<doc_id>.
    """
    manifest_path = doc_dir / "manifest.json"
    if not manifest_path.exists():
        return {"doc_dir": str(doc_dir), "skipped": True, "reason": "manifest.json missing"}

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pages_meta = manifest.get("pages", [])

    # Leer todas las páginas
    pages_texts: List[List[str]] = []
    for page_info in pages_meta:
        txt_file = ROOT / page_info["txt_file"]
        if not txt_file.exists():
            pages_texts.append([])
            continue
        lines = txt_file.read_text(encoding="utf-8", errors="ignore").splitlines()
        pages_texts.append(lines)

    # Detectar patrones globales del documento
    patterns = detect_repeated_lines(pages_texts)

    # Carpeta destino
    out_doc_dir = out_dir / doc_dir.name
    out_doc_dir.mkdir(parents=True, exist_ok=True)

    clean_index = []
    for page_info, lines in zip(pages_meta, pages_texts):
        page_num = page_info["page"]
        cleaned_lines = apply_cleanup(lines, patterns)

        out_txt = out_doc_dir / f"{doc_dir.name}_page_{page_num:03d}.txt"
        out_txt.write_text("\n".join(cleaned_lines), encoding="utf-8")

        clean_index.append({
            "page": page_num,
            "clean_txt_file": str(out_txt.relative_to(ROOT)),
            "orig_txt_file": page_info["txt_file"],
            "n_lines_before": len(lines),
            "n_lines_after": len(cleaned_lines),
        })

    # Guardar manifest
    clean_manifest = {
        "doc_id": manifest.get("doc_id"),
        "file_name": manifest.get("file_name"),
        "doc_type": manifest.get("doc_type"),
        "rev": manifest.get("rev"),
        "n_pages": manifest.get("n_pages"),
        "detected_patterns": patterns,
        "pages": clean_index,
    }

    (out_doc_dir / "clean_manifest.json").write_text(
        json.dumps(clean_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    return clean_manifest


# ============================================================
# API PARA run_case_pipeline()
# ============================================================

def run_pc2(pc1_dir: Path, out_dir: Path) -> List[Dict[str, Any]]:
    """
    Ejecuta PC-2 sobre todos los doc_id generados por PC-1.

    pc1_dir = carpeta donde están las carpetas <doc_id> con manifest.json
    out_dir = carpeta destino donde se crearán las carpetas limpias
    """
    if not pc1_dir.exists():
        raise SystemExit(f"[PC-2] No existe PC1_DIR: {pc1_dir}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Selección de documentos
    targets = [p for p in pc1_dir.iterdir() if p.is_dir() and (p / "manifest.json").exists()]
    if not targets:
        raise SystemExit(f"[PC-2] No se encontraron doc_id válidos en {pc1_dir}")

    results = []
    for doc_dir in targets:
        print(f"[PC-2] Limpiando: {doc_dir.name}")
        res = process_document(doc_dir, out_dir)
        results.append(res)

    # Índice global
    (out_dir / "index.json").write_text(
        json.dumps({"documents": results}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("\n✅ PC-2 completado.")
    print(f"• Salida en: {out_dir}/<doc_id>/clean_manifest.json")
    return results


# ============================================================
# CLI (compatibilidad)
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="PC-2 — Limpieza de layout (headers/footers)")
    parser.add_argument("--pc1", type=str, default=None, help="Carpeta con pc1_raw_pages")
    parser.add_argument("--out", type=str, default=None, help="Carpeta de salida")

    args = parser.parse_args()

    pc1_dir = Path(args.pc1) if args.pc1 else PC1_DIR_DEFAULT
    out_dir = Path(args.out) if args.out else OUT_DIR_DEFAULT

    run_pc2(pc1_dir, out_dir)


if __name__ == "__main__":
    main()
