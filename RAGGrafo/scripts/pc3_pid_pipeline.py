# -*- coding: utf-8 -*-
"""
Pipeline PID (Planos P&ID)
------------------------------------------------------------
Versión integrada con pc3_parse_blocks y compatible con ejecución por lote.
- Modo página: process_page(input_pdf, outdir, page_idx1) -> {"tables_emitted": N}
- Modo lote:   run_pipeline(manifest, out_dir, ocr=False)  -> dict (summary PID)
  (mantiene tu firma original para no romper llamadas existentes)

Comportamiento:
- Extrae tablas de la(s) página(s) objetivo y guarda en:
    outdir/tables/page_XXX_tableYY.csv
    outdir/tables_clean/page_XXX_tableYY_clean.csv
- Emite placeholders para artefactos PID (titleblock/tags/linelist) en:
    outdir/pid_context/page_XXX_titleblock.csv
    outdir/pid_context/page_XXX_tags.csv
    outdir/pid_context/page_XXX_linelist.csv

Notas:
- No hace OCR aún (hook ocr=True reservado). Más adelante se integra Tesseract o DocAI.
- No altera estructura externa (outputs/pc3_blocks/...), el caller define outdir.
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import pdfplumber


# =========================
# Utilidades
# =========================

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _save_raw_table(rows: List[List[str]], path: Path) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for r in rows:
            writer.writerow(r)


def _clean_and_save(src_csv: Path, dst_csv: Path) -> Tuple[int, int]:
    """
    Limpieza ligera de tabla CSV: trim, drop filas y cols vacías.
    Devuelve (n_rows, n_cols) resultantes.
    """
    df = pd.read_csv(src_csv, header=None, dtype=str, keep_default_na=False)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # drop filas completamente vacías
    df = df[~(df.apply(lambda r: all((str(x) == "" for x in r)), axis=1))]
    # drop cols completamente vacías
    df = df.loc[:, ~(df.apply(lambda c: all((str(x) == "" for x in c)), axis=0))]
    _ensure_dir(dst_csv.parent)
    df.to_csv(dst_csv, index=False, header=False, encoding="utf-8")
    return df.shape[0], df.shape[1]


def _extract_tables_for_page(input_pdf: Path, page_idx1: int) -> List[List[List[str]]]:
    """
    Devuelve lista de tablas (lista de filas, cada fila = lista de celdas string)
    para la página 1-based indicada. Si no hay tablas, devuelve [].
    """
    tables: List[List[List[str]]] = []
    with pdfplumber.open(str(input_pdf)) as pdf:
        idx0 = page_idx1 - 1
        if idx0 < 0 or idx0 >= len(pdf.pages):
            return tables
        page = pdf.pages[idx0]
        extracted = page.extract_tables() or []
        for t in extracted:
            if not t:
                continue
            # normalizar celdas None -> ""
            t_norm = [[(c if c is not None else "") for c in (r or [])] for r in t]
            tables.append(t_norm)
    return tables


def _emit_pid_placeholders(pid_dir: Path, page_idx1: int) -> None:
    """
    Coloca CSVs vacíos de placeholder por página (titleblock/tags/linelist).
    Idempotente (no sobreescribe si ya existen).
    """
    _ensure_dir(pid_dir)
    for name in ["titleblock", "tags", "linelist"]:
        csv_path = pid_dir / f"page_{page_idx1:03d}_{name}.csv"
        if not csv_path.exists():
            csv_path.write_text("", encoding="utf-8")


# =========================
# Interfaz por página (dispatcher)
# =========================

def process_page(input_pdf: Path, outdir: Path, page_idx1: int) -> Dict[str, Any]:
    """
    Invocado por pc3_parse_blocks.dispatch_page(...) cuando la página fue marcada pid_like.
    Retorna {"tables_emitted": N, ...}
    """
    tables_dir = outdir / "tables"
    tables_clean_dir = outdir / "tables_clean"
    pid_dir = outdir / "pid_context"
    _ensure_dir(tables_dir)
    _ensure_dir(tables_clean_dir)
    _ensure_dir(pid_dir)

    emitted = 0
    tables = _extract_tables_for_page(input_pdf, page_idx1)

    tcount = 0
    for t in tables:
        tcount += 1
        raw_name = f"page_{page_idx1:03d}_table{tcount:02d}.csv"
        raw_path = tables_dir / raw_name
        _save_raw_table(t, raw_path)

        clean_name = f"page_{page_idx1:03d}_table{tcount:02d}_clean.csv"
        clean_path = tables_clean_dir / clean_name
        _clean_and_save(raw_path, clean_path)
        emitted += 1

    # Emitir placeholders específicos PID (por página)
    _emit_pid_placeholders(pid_dir, page_idx1)

    # Guardar un resumen mínimo de PID por página para trazabilidad (opcional)
    summary_pid = {
        "page": page_idx1,
        "pid_outputs": [
            str((pid_dir / f"page_{page_idx1:03d}_titleblock.csv").relative_to(outdir)),
            str((pid_dir / f"page_{page_idx1:03d}_tags.csv").relative_to(outdir)),
            str((pid_dir / f"page_{page_idx1:03d}_linelist.csv").relative_to(outdir)),
        ],
        "tables_emitted": emitted,
    }
    (pid_dir / f"page_{page_idx1:03d}_pid_summary.json").write_text(
        json.dumps(summary_pid, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return {"tables_emitted": emitted}


# =========================
# Interfaz por lote (compatibilidad)
# =========================

def run_pipeline(manifest: dict, out_dir: Path, ocr: bool = False) -> dict:
    """
    Mantiene compatibilidad con tu firma original (lote).
    Espera en manifest:
      - doc_id
      - file_name
      - n_pages (opcional; si no, infiere del PDF)
    Escribe un summary PID general + placeholders globales (si quieres).
    """
    doc_id = manifest["doc_id"]
    file_name = manifest["file_name"]
    n_pages = int(manifest.get("n_pages", 0))

    out_dir.mkdir(parents=True, exist_ok=True)
    pid_dir = out_dir / "pid_context"
    _ensure_dir(pid_dir)

    # Ubica PDF (mantén tu layout “data/” como fuente por lote)
    root = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
    pdf_path = (root / "data" / file_name)

    if not pdf_path.exists():
        # Summary mínimo informando que no hubo procesamiento por falta de PDF
        summary = {
            "doc_id": doc_id,
            "file_name": file_name,
            "doc_type_detected": "PID",
            "pid_outputs": [],
            "ocr_used": bool(ocr),
            "warning": f"PDF no encontrado en: {pdf_path}",
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary

    # Si n_pages no está en manifest, infiérelo
    if n_pages <= 0:
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                n_pages = len(pdf.pages)
        except Exception:
            n_pages = 0

    total_tables = 0

    # Recorre todas las páginas y extrae tablas + placeholders por página
    for page_idx1 in range(1, n_pages + 1):
        res = process_page(pdf_path, out_dir, page_idx1)
        try:
            total_tables += int(res.get("tables_emitted", 0))
        except Exception:
            pass

    # Summary global PID (por lote)
    summary = {
        "doc_id": doc_id,
        "file_name": file_name,
        "doc_type_detected": "PID",
        "pid_outputs": [  # a nivel global puedes apuntar a una convención fija
            str((pid_dir / "PID_titleblock.csv").relative_to(out_dir)),
            str((pid_dir / "PID_tags.csv").relative_to(out_dir)),
            str((pid_dir / "PID_linelist.csv").relative_to(out_dir)),
        ],
        "ocr_used": bool(ocr),
        "pages_processed": n_pages,
        "tables_total": total_tables,
    }

    # Placeholders “globales” opcionales (además de los por página)
    for name in ["titleblock", "tags", "linelist"]:
        g = pid_dir / f"PID_{name}.csv"
        if not g.exists():
            g.write_text("", encoding="utf-8")

    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


