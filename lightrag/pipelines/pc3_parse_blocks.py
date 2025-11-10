#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-3 — Parse blocks + Dispatcher P&ID (genérico, modular por doc_type)
----------------------------------------------------------------------
- Detecta páginas tipo P&ID ("pid_like") por heurística de texto/tablas.
- Aplica reglas configurables por tipo de documento (ej. HD, ET, MR).
- Despacha cada página al subpipeline correspondiente:
      pid_like  -> pc3_pid_pipeline.process_page
      normal    -> pc3_text_tables_pipeline.process_page
- Usa texto limpio de PC-2 (si existe) y PDF original (PC-1/data).
- Procesa todos los documentos de outputs/pc2_clean_pages/.
- Muestra solo una línea de estado por documento.

Requisitos: pdfplumber, pandas
Autor: Oz (para Víctor) — Octubre 2025
"""

from __future__ import annotations
import argparse
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber


# =========================
# Configuración de rutas
# =========================
ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
PC1_DIR = ROOT / "outputs" / "pc1_raw_pages"
PC2_DIR = ROOT / "outputs" / "pc2_clean_pages"
PC3_BASE = ROOT / "outputs" / "pc3_blocks"
DATA_DIR = ROOT / "data"


# =========================
# Reglas configurables
# =========================
PID_RULES = {
    # HD: las hojas 2-3 son hojas de datos, 4 es el esquema P&ID
    "HD": {"pid": [4], "no_pid": [2, 3]},
    # Ejemplo futuro:
    # "MR": {"pid": [7], "no_pid": []},
    # "ET": {"pid": [], "no_pid": []},
}


# =========================
# Heurística P&ID
# =========================
PID_TITLE_PATTERNS = [
    r"\bP&ID\b",
    r"\bP\s*&\s*ID\b",
    r"\bPIPING\s+AND\s+INSTRUMENTATION\b",
    r"\bPROCESS\s+AND\s+INSTRUMENTATION\b",
    r"\bDIAGRAMA\b",
    r"\bDIAGRAM\b",
    r"\bDIAGRAMA\s+DE\s+TUBER[ÍI]A\s+E\s+INSTRUMENTACI[ÓO]N\b",
]
PID_TITLE_RE = re.compile("|".join(PID_TITLE_PATTERNS), flags=re.IGNORECASE)
PID_CODE_RE = re.compile(r"[A-Z]{2,}(?:/[A-Z0-9\-]+){1,}")  # ej: FFM/F-S-ME-015
NOTE_RES = [
    re.compile(r"^\s*Nota:\s*(.+)$", flags=re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Note:\s*(.+)$", flags=re.IGNORECASE | re.MULTILINE),
]


# =========================
# Utilidades generales
# =========================
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def try_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None

def extract_tables_shapes(pdf_page) -> List[Tuple[int, int]]:
    shapes: List[Tuple[int, int]] = []
    try:
        tables = pdf_page.extract_tables() or []
        for t in tables:
            rows = [row or [] for row in t]
            n_rows = len(rows)
            n_cols = max(len(r) for r in rows) if rows else 0
            if n_rows > 0 and n_cols > 0:
                shapes.append((n_rows, n_cols))
    except Exception:
        pass
    return shapes

def is_pid_like_page(page_text: str, table_shapes: List[Tuple[int, int]],
                     max_rows: int = 5, max_cols: int = 4) -> Tuple[bool, List[str]]:
    """
    Marca pid_like si:
      - contiene términos P&ID / DIAGRAMA, y
      - no hay tablas grandes (≥10×6) salvo que haya 'DIAGRAMA' o 'Nota:'.
    """
    evidences: List[str] = []
    text = page_text or ""
    terms = PID_TITLE_RE.findall(text)
    if not terms:
        return False, evidences

    evidences.extend(sorted({t for t in terms if isinstance(t, str)}))

    has_large = any(r >= 10 and c >= 6 for r, c in (table_shapes or []))
    has_diagrama = bool(re.search(r"\bDIAGRAMA\b", text, re.I))
    has_note = bool(re.search(r"^\s*(Nota|Note)\s*:", text, re.I | re.M))
    if has_large and not (has_diagrama or has_note):
        return False, evidences

    if not table_shapes:
        return True, evidences
    for r, c in table_shapes:
        if r <= max_rows and c <= max_cols:
            return True, evidences
    return False, evidences

def extract_pid_reference(text: str) -> Optional[str]:
    m = PID_CODE_RE.search(text or "")
    return m.group(0) if m else None

def extract_pid_note(text: str) -> Optional[str]:
    for rx in NOTE_RES:
        m = rx.search(text or "")
        if m:
            return m.group(1).strip()
    return None

def read_pc2_clean_text(doc_id: str, page_idx1: int) -> str:
    p = PC2_DIR / doc_id / f"{doc_id}_page_{page_idx1:03d}.txt"
    if p.exists():
        return p.read_text(encoding="utf-8", errors="ignore")
    return ""

def get_pdf_for_doc(doc_id: str) -> Tuple[Path, int, str, str]:
    manifest = PC1_DIR / doc_id / "manifest.json"
    if not manifest.exists():
        raise FileNotFoundError(f"manifest.json no encontrado: {manifest}")
    meta = json.loads(manifest.read_text(encoding="utf-8"))
    file_name = meta.get("file_name")
    n_pages = int(meta.get("n_pages", 0))
    doc_type = (meta.get("doc_type") or "").strip().upper() or "OTRO"
    if not file_name:
        raise ValueError(f"manifest.json sin file_name para {doc_id}")
    pdf_path = DATA_DIR / file_name
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF no encontrado en data/: {pdf_path}")
    return pdf_path, n_pages, file_name, doc_type

def apply_pid_rules(doc_type: str, page_idx1: int,
                    current_pid_like: bool, evidence: List[str]) -> Tuple[bool, List[str]]:
    """
    Aplica reglas de PID_RULES (por tipo de documento).
    """
    rules = PID_RULES.get(doc_type.upper(), {})
    pid_pages = set(rules.get("pid", []))
    no_pid_pages = set(rules.get("no_pid", []))

    if page_idx1 in pid_pages:
        if "RULE_FORCE_PID" not in evidence:
            evidence.append("RULE_FORCE_PID")
        return True, evidence
    if page_idx1 in no_pid_pages:
        if "RULE_NO_PID" not in evidence:
            evidence.append("RULE_NO_PID")
        return False, evidence
    return current_pid_like, evidence


# =========================
# Dispatcher de subpipelines
# =========================
def dispatch_page(pid_mod, text_mod, input_pdf: Path, outdir: Path,
                  page_idx1: int, pid_like: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if pid_like and pid_mod and hasattr(pid_mod, "process_page"):
        try:
            result = pid_mod.process_page(input_pdf, outdir, page_idx1) or {}
            result["__dispatched_to__"] = "pc3_pid_pipeline"
        except Exception as e:
            result["__dispatch_error__"] = f"pc3_pid_pipeline: {e}"
    elif (not pid_like) and text_mod and hasattr(text_mod, "process_page"):
        try:
            result = text_mod.process_page(input_pdf, outdir, page_idx1) or {}
            result["__dispatched_to__"] = "pc3_text_tables_pipeline"
        except Exception as e:
            result["__dispatch_error__"] = f"pc3_text_tables_pipeline: {e}"
    else:
        result["__dispatched_to__"] = None
    return result


# =========================
# Flujo principal por documento
# =========================
def run_pc3_for_doc(doc_id: str, outdir: Path, pid_max_rows: int, pid_max_cols: int) -> None:
    ensure_dir(outdir)
    ensure_dir(outdir / "tables")
    ensure_dir(outdir / "tables_clean")
    ensure_dir(outdir / "pid_context")

    input_pdf, n_pages, file_name, doc_type = get_pdf_for_doc(doc_id)
    pid_module = try_import("pc3_pid_pipeline")
    text_module = try_import("pc3_text_tables_pipeline")

    blocks: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {
        "doc_id": doc_id,
        "file_name": file_name,
        "doc_type": doc_type,
        "page_count": n_pages,
        "embedded_pid_pages": [],
        "pid_context": [],
        "tables_total": 0,
    }

    with pdfplumber.open(str(input_pdf)) as pdf:
        n_pages = min(n_pages, len(pdf.pages)) if n_pages else len(pdf.pages)

        for idx0 in range(n_pages):
            idx1 = idx0 + 1
            page = pdf.pages[idx0]

            text_clean = read_pc2_clean_text(doc_id, idx1)
            page_text = text_clean if text_clean else (page.extract_text() or "")
            shapes = extract_tables_shapes(page)

            pid_like, evidence = is_pid_like_page(page_text, shapes, pid_max_rows, pid_max_cols)
            pid_like, evidence = apply_pid_rules(doc_type, idx1, pid_like, evidence)

            pid_ref = extract_pid_reference(page_text) if pid_like else None
            pid_note = extract_pid_note(page_text) if pid_like else None

            if pid_like:
                summary["embedded_pid_pages"].append(idx1)
                ctx_row = {
                    "page": idx1,
                    "pid_like": True,
                    "pid_reference": pid_ref or "",
                    "pid_note": pid_note or "",
                    "evidence": ";".join(evidence),
                }
                ctx_path = outdir / "pid_context" / f"page_{idx1:03d}_pid_context.csv"
                pd.DataFrame([ctx_row]).to_csv(ctx_path, index=False, encoding="utf-8")
                summary["pid_context"].append(ctx_row)

            dispatch_info = dispatch_page(pid_module, text_module, input_pdf, outdir, idx1, pid_like)
            if "tables_emitted" in dispatch_info:
                try:
                    summary["tables_total"] += int(dispatch_info["tables_emitted"])
                except Exception:
                    pass

            block = {
                "page": idx1,
                "has_text": bool(page_text.strip()),
                "tables_shapes": [{"rows": r, "cols": c} for (r, c) in shapes],
                "pid_like": pid_like,
                "pid_evidence": evidence,
                "pid_reference": pid_ref,
                "pid_note": pid_note,
            }
            for k, v in (dispatch_info or {}).items():
                if k.startswith("__"):
                    block[k] = v
            blocks.append(block)

    (outdir / "blocks.json").write_text(json.dumps(blocks, ensure_ascii=False, indent=2), encoding="utf-8")
    (outdir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # --- NUEVO: consolidación al final del documento (genera TEXT_sections.csv / TEXT_tables_all.csv) ---
    doc_meta = {
        "doc_id": doc_id,
        "doc_type": doc_type,
        "file_name": file_name,
        "n_pages": n_pages,
    }
    if text_module and hasattr(text_module, "finalize"):
        try:
            text_module.finalize(outdir, doc_meta)
        except Exception as e:
            print(f"[PC3][WARN] finalize text_tables: {e}")


# =========================
# Batch / CLI
# =========================
def list_docs_pc2() -> List[str]:
    if not PC2_DIR.exists():
        return []
    return sorted([d.name for d in PC2_DIR.iterdir() if d.is_dir() and any(d.glob(f"{d.name}_page_*.txt"))])

def main():
    ap = argparse.ArgumentParser(description="PC-3 — Parse blocks + Dispatcher P&ID (batch/single, genérico)")
    ap.add_argument("--doc-id", help="Procesa un solo documento (carpeta en outputs/pc2_clean_pages/<doc_id>)")
    ap.add_argument("--outdir", help="Salida si se procesa un solo doc (por defecto: outputs/pc3_blocks/<doc_id>)")
    ap.add_argument("--pid-max-rows", type=int, default=5)
    ap.add_argument("--pid-max-cols", type=int, default=4)
    args = ap.parse_args()

    if args.doc_id:
        doc_id = args.doc_id
        outdir = Path(args.outdir).expanduser().resolve() if args.outdir else (PC3_BASE / doc_id)
        print(f"[PC3] Procesando: {doc_id} ...", end="", flush=True)
        run_pc3_for_doc(doc_id, outdir, args.pid_max_rows, args.pid_max_cols)
        print(" listo.")
        return

    doc_ids = list_docs_pc2()
    if not doc_ids:
        print("[PC3] No hay carpetas en outputs/pc2_clean_pages/. Corre PC-1 y PC-2 primero.")
        sys.exit(0)

    total = 0
    for doc_id in doc_ids:
        try:
            outdir = PC3_BASE / doc_id
            print(f"[PC3] Procesando: {doc_id} ...", end="", flush=True)
            run_pc3_for_doc(doc_id, outdir, args.pid_max_rows, args.pid_max_cols)
            print(" listo.")
            total += 1
        except Exception as e:
            print(f" ERROR: {e}")
    print(f"[PC3] Completado. Documentos procesados: {total}")

if __name__ == "__main__":
    main()


