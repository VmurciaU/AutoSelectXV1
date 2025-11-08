#!/usr/bin/env python3
"""
PC-1 — Lectura e ingesta de PDFs
--------------------------------
Lee todos los PDFs en ./data, extrae texto por página y metadatos mínimos,
y guarda resultados en ./outputs/pc1_raw_pages/<doc_id>/.

- Por ahora NO hacemos limpieza de headers/footers ni parsing semántico.
- Esta etapa deja todo listo para PC-2/PC-3.

Requisitos: pdfplumber, pypdf, python-dotenv (opcional)
"""

import os
import re
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

import pdfplumber

# ---------------------------
# Configuración de rutas
# ---------------------------
ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "outputs" / "pc1_raw_pages"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------
# Utilidades
# ---------------------------
DOC_TYPE_HINTS = {
    "ET": [" ET ", "Especificación Técnica", "ET "],
    "HD": [" HD ", "Hoja de Datos", "Hoja de Datos (HD)", "Hojas de Datos"],
    "MR": [" MR ", "Manual de Requisitos", "Requisición", "Requisición de Materiales", "MR "],
    "PID": ["P&ID", "P & ID", "DIAGRAMA DE TUBERÍAS", "Piping and Instrumentation Diagram"],
}

def detect_doc_type(filename: str, first_page_text: str) -> str:
    """Heurística sencilla basada en nombre y primera página."""
    name_upper = filename.upper()
    for dt in ["ET", "HD", "MR", "PID"]:
        if f" {dt} " in f" {name_upper} ":
            return dt
    # por contenido
    text_up = (first_page_text or "").upper()
    for dt, hints in DOC_TYPE_HINTS.items():
        for h in hints:
            if h.upper() in text_up:
                return dt
    # fallback
    return "OTRO"


def extract_revision_from_name(filename: str) -> Optional[str]:
    """
    Intenta extraer una 'rev' del nombre, ej: FFMFSME015.1 -> 015.1
    """
    # Busca patrones tipo 123.4 o 123,4 o 123-4 al final
    m = re.search(r'(\d{2,4}[\.\-_,]\d+)\D*$', filename)
    if m:
        return m.group(1).replace("_", ".").replace("-", ".").replace(",", ".")
    # alternativa: números sueltos
    m2 = re.search(r'(\d{2,4}\.\d+)', filename)
    return m2.group(1) if m2 else None


def slugify_doc_id(name: str) -> str:
    """Crea un doc_id amigable para carpetas/archivos."""
    base = re.sub(r'\s+', ' ', name).strip()
    base = base.replace("/", "-").replace("\\", "-")
    # corta muy largo
    if len(base) > 90:
        base = base[:90]
    return base


def page_text(page) -> str:
    """
    Extrae texto de la página con parámetros razonables.
    """
    try:
        return page.extract_text(x_tolerance=1.5, y_tolerance=1.5) or ""
    except Exception:
        # fallback simple
        return page.extract_text() or ""


def process_pdf(pdf_path: Path) -> Dict[str, Any]:
    """
    Procesa un PDF: extrae texto por página y escribe a disco.
    Devuelve un manifiesto con metadatos.
    """
    doc_id = slugify_doc_id(pdf_path.stem)
    target_dir = OUT_DIR / doc_id
    target_dir.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        "doc_id": doc_id,
        "file_name": pdf_path.name,
        "abs_path": str(pdf_path.resolve()),
        "n_pages": 0,
        "doc_type": None,
        "title": None,
        "rev": None,
        "pages": [],  # list[{"page": 1, "txt_file": "..."}]
    }

    with pdfplumber.open(pdf_path) as pdf:
        n_pages = len(pdf.pages)
        manifest["n_pages"] = n_pages

        # Primer página: intenta detectar doc_type y título
        fp_text = ""
        if n_pages > 0:
            fp = pdf.pages[0]
            fp_text = page_text(fp)
            # Título (heurística: primera línea no vacía y larga)
            lines = [ln.strip() for ln in fp_text.splitlines() if ln.strip()]
            manifest["title"] = lines[0][:180] if lines else pdf_path.stem

        manifest["doc_type"] = detect_doc_type(pdf_path.name, fp_text)
        manifest["rev"] = extract_revision_from_name(pdf_path.stem)

        for i, p in enumerate(pdf.pages, start=1):
            txt = page_text(p)
            txt_fname = f"{doc_id}_page_{i:03d}.txt"
            (target_dir / txt_fname).write_text(txt, encoding="utf-8")

            manifest["pages"].append({
                "page": i,
                "txt_file": str((target_dir / txt_fname).relative_to(ROOT)),
                "width": getattr(p, "width", None),
                "height": getattr(p, "height", None),
            })

    # Escribe manifest.json dentro de la carpeta del doc
    (target_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main(data_dir: str = None):
    base = Path(data_dir) if data_dir else DATA_DIR
    if not base.exists():
        raise SystemExit(f"No existe la carpeta de datos: {base}")

    pdfs = sorted([p for p in base.glob("*.pdf")])
    if not pdfs:
        raise SystemExit(f"No se encontraron PDFs en {base}")

    all_manifests: List[Dict[str, Any]] = []
    for pdf in pdfs:
        print(f"[PC-1] Procesando: {pdf.name}")
        manifest = process_pdf(pdf)
        all_manifests.append(manifest)

    # índice global
    index_path = OUT_DIR / "index.json"
    index = {"documents": all_manifests}
    index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n✅ PC-1 completado.")
    print(f"• Manifiestos por documento en: {OUT_DIR}\\<doc_id>\\manifest.json")
    print(f"• Índice global: {index_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PC-1 — Lectura e ingesta de PDFs")
    parser.add_argument("--data", type=str, default=None, help="Ruta alternativa de la carpeta data/")
    args = parser.parse_args()
    main(args.data)
