#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-3b — Docling Adapter (entrada desde PC-2/PC-1)
-------------------------------------------------
Genera salidas compatibles con PC-4:

  outputs/pc3_blocks/<doc_id>/
    - summary.json
    - TEXT_sections.csv
    - TEXT_tables_all.csv
    - tables/page_XXX_tableYY.csv
    - blocks.json
    - pid_context/page_XXX_pid_context.csv   (opcional por reglas)

Ejecución simple:
  python scripts/pc3b_docling_adapter.py

También soporta:
  python scripts/pc3b_docling_adapter.py --backend mock
  python scripts/pc3b_docling_adapter.py --pdf <ruta.pdf> --doc-id X --doc-type ET
  python scripts/pc3b_docling_adapter.py --pdf-dir <carpeta>

Requisitos:
  pip install docling  (ajusta imports si tu versión cambia el módulo)
"""

from __future__ import annotations
import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any

# =========================
# Rutas / Defaults
# =========================

ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
PC1_DIR = ROOT / "outputs" / "pc1_raw_pages"
PC2_DIR = ROOT / "outputs" / "pc2_clean_pages"
DEFAULT_OUTDIR = ROOT / "outputs" / "pc3_blocks"

# Búsqueda adicional de PDFs por si los movieron
PDF_HINT_DIRS = [
    ROOT / "outputs" / "pc2_clean_pages",
    ROOT / "outputs" / "pc1_raw_pages",
    ROOT / "outputs" / "pc2_docs",
    ROOT / "outputs" / "pc2",
    ROOT / "data" / "pdfs",
    ROOT / "inputs" / "pdfs",
    ROOT,
]

# =========================
# Utilitarios de IO
# =========================

def _mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def _safe_text(x) -> str:
    return "" if x is None else str(x)

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _infer_doc_type(name: str, fallback: str = "UNK") -> str:
    u = name.upper()
    if " HD " in f" {u} " or u.startswith("HD") or "HOJA DE DATOS" in u:
        return "HD"
    if u.startswith("ET") or " ESPECIFICACION TECNICA" in u or "ET " in u:
        return "ET"
    if u.startswith("MR") or " MEMORIA" in u or "MR " in u:
        return "MR"
    return fallback

def _resolve_pdf_by_name(file_name: str) -> Optional[Path]:
    """Busca el PDF por nombre en rutas conocidas y, si no, recorre todo el repo."""
    if not file_name:
        return None
    # 1) si es ruta absoluta/relativa válida
    p = Path(file_name)
    if p.suffix.lower() == ".pdf" and p.exists():
        return p.resolve()
    # 2) buscar por nombre exacto en directorios pista
    for base in PDF_HINT_DIRS:
        if base.exists():
            cand = list(base.rglob(file_name))
            if cand:
                return cand[0].resolve()
    # 3) buscar por stem (mismo nombre sin extensión)
    stem = Path(file_name).stem
    for base in PDF_HINT_DIRS:
        if base.exists():
            cand = [q for q in base.rglob("*.pdf") if q.stem == stem]
            if cand:
                return cand[0].resolve()
    return None

# =========================
# Normalización destino
# =========================

def _write_summary(outdir: Path, doc_id: str, doc_type: str, file_name: str, pages: int):
    _write_json(outdir / "summary.json", {
        "doc_id": doc_id,
        "doc_type": doc_type,
        "file_name": file_name,
        "pages": int(pages or 0),
        "meta": {"source": "docling", "version": "pc3b-2025.10"}
    })

def _write_text_sections(outdir: Path, doc_id: str, doc_type: str, file_name: str,
                         headings: List[Dict], paragraphs: List[Dict]):
    cols = ["doc_id","doc_type","file_name","page","type","section_number","section_title","text"]
    f = outdir / "TEXT_sections.csv"
    with f.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
        for h in headings:
            w.writerow({
                "doc_id": doc_id, "doc_type": doc_type, "file_name": file_name,
                "page": int(h.get("page") or 0),
                "type": "section_header",
                "section_number": _safe_text(h.get("number")),
                "section_title": _safe_text(h.get("title")),
                "text": _norm_spaces(f"{_safe_text(h.get('number'))} {_safe_text(h.get('title'))}")
            })
        for p in paragraphs:
            w.writerow({
                "doc_id": doc_id, "doc_type": doc_type, "file_name": file_name,
                "page": int(p.get("page") or 0),
                "type": "paragraph",
                "section_number": "", "section_title": "",
                "text": _norm_spaces(_safe_text(p.get("text")))
            })

def _write_tables_all(outdir: Path, doc_id: str, doc_type: str, file_name: str,
                      tables: List[Dict]):
    cols = ["doc_id","doc_type","file_name","_page","_table_idx","_row"] + [f"c{i}" for i in range(1,51)]
    f = outdir / "TEXT_tables_all.csv"
    with f.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
        for t in tables:
            page = int(t.get("page") or 0)
            idx  = int(t.get("index_in_page") or 0)
            rows = t.get("rows") or []
            for r_i, row in enumerate(rows, start=1):
                row = ["" if c is None else str(c) for c in row]
                rec = {"doc_id": doc_id, "doc_type": doc_type, "file_name": file_name,
                       "_page": page, "_table_idx": idx, "_row": r_i}
                for i in range(50):
                    rec[f"c{i+1}"] = row[i] if i < len(row) else ""
                w.writerow(rec)

def _write_tables_by_page(outdir: Path, tables: List[Dict]):
    tdir = outdir / "tables"; _mkdir(tdir)
    for t in tables:
        page = int(t.get("page") or 0)
        idx  = int(t.get("index_in_page") or 0)
        f = tdir / f"page_{page:03d}_table{idx:02d}.csv"
        with f.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            for row in (t.get("rows") or []):
                w.writerow([("" if c is None else str(c)) for c in row])

def _write_blocks(outdir: Path, headings: List[Dict], paragraphs: List[Dict]):
    blocks = []
    for h in headings:
        blocks.append({
            "type": "section_header",
            "page": int(h.get("page") or 0),
            "section_number": _safe_text(h.get("number")),
            "section_title": _safe_text(h.get("title")),
            "text": _norm_spaces(f"{_safe_text(h.get('number'))} {_safe_text(h.get('title'))}")
        })
    for p in paragraphs:
        blocks.append({
            "type": "paragraph",
            "page": int(p.get("page") or 0),
            "text": _norm_spaces(_safe_text(p.get("text")))
        })
    _write_json(outdir / "blocks.json", blocks)

def _write_pid_context(outdir: Path, pid_refs: Dict[int, List[Dict]]):
    if not pid_refs:
        return
    pdir = outdir / "pid_context"; _mkdir(pdir)
    cols = ["page","pid_like","pid_reference","pid_note","rule","evidence"]
    for page, items in pid_refs.items():
        f = pdir / f"page_{int(page):03d}_pid_context.csv"
        with f.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            for r in items:
                w.writerow({
                    "page": int(page),
                    "pid_like": r.get("pid_like", True),
                    "pid_reference": _safe_text(r.get("pid_reference")),
                    "pid_note": _safe_text(r.get("pid_note")),
                    "rule": _safe_text(r.get("rule")),
                    "evidence": _norm_spaces(_safe_text(r.get("evidence")))
                })

# =========================
# Backend Docling (adaptable)
# =========================

class DoclingBackend:
    def __init__(self):
        try:
            from docling.document_converter import DocumentConverter  # type: ignore
            self._converter_cls = DocumentConverter
        except Exception as e:
            raise RuntimeError(
                "No pude importar Docling. Instálalo con `pip install docling` "
                "o ajusta los imports según tu versión."
            ) from e

    def parse(self, pdf_path: Path) -> Dict:
        converter = self._converter_cls()
        result = converter.convert(pdf_path)

        pages_cnt = getattr(result, "pages_count", None) or getattr(result, "page_count", None)
        pages_cnt = int(pages_cnt or len(getattr(result, "pages", []) or []))

        headings, paragraphs = [], []
        blocks = getattr(result, "blocks", None) or []
        for b in blocks:
            btype = (getattr(b, "type", "") or "").lower()
            page  = int(getattr(b, "page", 0) or 0)
            if "heading" in btype or "section" in btype:
                num = _safe_text(getattr(b, "number", "") or getattr(b, "heading_number", ""))
                ttl = _safe_text(getattr(b, "title", "") or getattr(b, "text", ""))
                headings.append({"page": page, "number": _norm_spaces(num), "title": _norm_spaces(ttl)})
            else:
                txt = _safe_text(getattr(b, "text", ""))
                paragraphs.append({"page": page, "text": _norm_spaces(txt)})

        tables_norm: List[Dict] = []
        doc_tables = getattr(result, "tables", None) or []
        per_page_counter: Dict[int, int] = {}
        for tbl in doc_tables:
            page = int(getattr(tbl, "page", 0) or 0)
            per_page_counter[page] = per_page_counter.get(page, 0) + 1
            idx_in_page = per_page_counter[page]

            rows = []
            grid = getattr(tbl, "cells", None) or getattr(tbl, "grid", None) or getattr(tbl, "as_matrix", None)
            if callable(grid):
                grid = grid()
            if grid is None:
                grid = getattr(tbl, "rows", None) or getattr(tbl, "data", None) or []
            for row in grid:
                if isinstance(row, dict) and "cells" in row:
                    vals = [r.get("text","") for r in (row.get("cells") or [])]
                else:
                    vals = [str(c) if c is not None else "" for c in (list(row) if not isinstance(row, str) else [row])]
                rows.append(vals)

            header = getattr(tbl, "header", None)
            if header:
                hvals = [str(c) if c is not None else "" for c in (header if isinstance(header, (list, tuple)) else [header])]
                rows.insert(0, hvals)

            tables_norm.append({"page": page, "index_in_page": idx_in_page, "rows": rows})

        return {"meta": {"pages": pages_cnt}, "headings": headings, "paragraphs": paragraphs, "tables": tables_norm}

class MockBackend:
    def parse(self, pdf_path: Path) -> Dict:
        return {"meta": {"pages": 0}, "headings": [], "paragraphs": [], "tables": []}

# =========================
# P&ID heurístico
# =========================

def _detect_pid(paragraphs: List[Dict]) -> Dict[int, List[Dict]]:
    pid_pages: Dict[int, List[Dict]] = {}
    kw = [
        r"\bp&?i&?d\b", r"\bpiping\s*&\s*instrumentation\b", r"\bpiping\s+and\s+instrumentation\b",
        r"\bdiagrama\s+de\s+tuber(í|i)as\b", r"\bdiagrama\s+de\s+instrumentaci(ó|o)n\b"
    ]
    pat = re.compile("|".join(kw), flags=re.IGNORECASE)
    for p in paragraphs:
        text = _norm_spaces(_safe_text(p.get("text")))
        if not text:
            continue
        if pat.search(text):
            page = int(p.get("page") or 0)
            pid_pages.setdefault(page, []).append({
                "pid_like": True,
                "pid_reference": text,
                "pid_note": "docling-kw",
                "rule": " OR ".join(kw),
                "evidence": text[:160]
            })
    return pid_pages

# =========================
# Descubrimiento desde PC-2 / PC-1
# =========================

def _load_pc2_index() -> List[Dict]:
    """Lee outputs/pc2_clean_pages/index.json (formato de PC-2)."""
    idx_path = PC2_DIR / "index.json"
    if not idx_path.exists():
        return []
    data = json.loads(idx_path.read_text(encoding="utf-8"))
    docs = data.get("documents") or []
    out: List[Dict] = []
    for d in docs:
        if not isinstance(d, dict):
            continue
        # PC-2 guarda un objeto por documento con páginas y metadatos
        doc_id = d.get("doc_id") or d.get("file_name") or d.get("doc_dir") or ""
        # Si no vino doc_id, probamos con el nombre de carpeta en PC2
        if not doc_id and isinstance(d.get("doc_dir"), str):
            doc_id = Path(d["doc_dir"]).name
        # Fijar a carpeta real de PC2
        pc2_doc_dir = PC2_DIR / (doc_id if isinstance(doc_id, str) else str(doc_id))
        # doc_type preferentemente del manifest limpio; si no, lo inferimos
        doc_type = d.get("doc_type") or _infer_doc_type(str(doc_id))
        # file_name (nombre del PDF) si PC-2 lo preservó
        file_name = d.get("file_name") or ""
        out.append({"doc_id": str(doc_id), "doc_type": doc_type, "pc2_doc_dir": pc2_doc_dir, "file_name": file_name})
    return out

def _read_pc2_clean_manifest(pc2_doc_dir: Path) -> Dict:
    mf = pc2_doc_dir / "clean_manifest.json"
    if mf.exists():
        try:
            return json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _read_pc1_manifest(doc_id: str) -> Dict:
    mf = PC1_DIR / doc_id / "manifest.json"
    if mf.exists():
        try:
            return json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _resolve_pdf_for_doc(doc_id: str, file_name_hint: str) -> Optional[Path]:
    """
    Intenta resolver el PDF original:
      1) PC-1 manifest.json: campos comunes 'file_name', 'pdf', 'source_pdf', 'original_pdf'
      2) Buscar por 'file_name_hint'
      3) Buscar por stem 'doc_id'
    """
    # 1) PC-1 manifest
    pc1 = _read_pc1_manifest(doc_id)
    for key in ("file_name", "pdf", "source_pdf", "original_pdf"):
        if pc1.get(key):
            p = _resolve_pdf_by_name(pc1[key])
            if p:
                return p
    # 2) Hint desde PC-2
    if file_name_hint:
        p = _resolve_pdf_by_name(file_name_hint)
        if p:
            return p
    # 3) Por doc_id (stem)
    for base in PDF_HINT_DIRS:
        if base.exists():
            cand = [q for q in base.rglob("*.pdf") if q.stem == doc_id]
            if cand:
                return cand[0].resolve()
    return None

def _discover_inputs() -> List[Dict]:
    """
    Descubre docs desde PC-2 (index.json + clean_manifest.json) y resuelve el PDF
    apoyándose en PC-1 si es necesario.
    """
    candidates = _load_pc2_index()
    resolved: List[Dict] = []
    if not candidates:
        print("[PC3b] ⚠ No encontré outputs/pc2_clean_pages/index.json o está vacío.")
        return []

    for it in candidates:
        doc_id = it["doc_id"]
        doc_type = it["doc_type"]
        pc2_dir = it["pc2_doc_dir"]
        cm = _read_pc2_clean_manifest(pc2_dir)
        file_name_hint = cm.get("file_name") or it.get("file_name") or ""

        pdf_path = _resolve_pdf_for_doc(doc_id, file_name_hint)
        if not pdf_path:
            print(f"[PC3b] ⚠ No pude localizar el PDF para doc_id={doc_id}. "
                  f"Intenta colocar el PDF original en 'outputs/pc2_clean_pages/{doc_id}/' o en 'data/pdfs/'.")
            continue

        resolved.append({
            "pdf": pdf_path,
            "doc_id": doc_id,
            "doc_type": cm.get("doc_type") or doc_type or _infer_doc_type(doc_id)
        })

    print(f"[PC3b] Documentos detectados desde PC-2: {len(resolved)}")
    return resolved

# =========================
# Pipeline por documento
# =========================

def _process_pdf(pdf_path: Path, out_root: Path, doc_id: str, doc_type: str, backend: str):
    _mkdir(out_root)
    outdir = out_root / doc_id
    _mkdir(outdir)

    if backend == "docling":
        backend_impl = DoclingBackend()
    elif backend == "mock":
        backend_impl = MockBackend()
    else:
        raise ValueError("--backend debe ser 'docling' o 'mock'")

    parsed = backend_impl.parse(pdf_path)

    pages = int(parsed.get("meta", {}).get("pages") or 0)
    headings = parsed.get("headings", [])
    paragraphs = parsed.get("paragraphs", [])
    tables = parsed.get("tables", [])

    _write_summary(outdir, doc_id, doc_type, pdf_path.name, pages)
    _write_text_sections(outdir, doc_id, doc_type, pdf_path.name, headings, paragraphs)
    _write_tables_all(outdir, doc_id, doc_type, pdf_path.name, tables)
    _write_tables_by_page(outdir, tables)
    _write_blocks(outdir, headings, paragraphs)
    _write_pid_context(outdir, _detect_pid(paragraphs))

    print(f"[PC3b] OK: {pdf_path.name}  →  {outdir}")

# =========================
# CLI
# =========================

def main():
    ap = argparse.ArgumentParser(description="PC-3b Docling Adapter (entrada desde PC-2/PC-1)")
    # Todos los argumentos son opcionales; por defecto autodetecta desde PC-2
    ap.add_argument("--pdf", help="Ruta a un PDF (opcional)")
    ap.add_argument("--pdf-dir", help="Carpeta con PDFs (opcional)")
    ap.add_argument("--outdir", default=str(DEFAULT_OUTDIR), help="Salida base (por defecto outputs/pc3_blocks)")
    ap.add_argument("--doc-id", default=None, help="doc_id explícito (si --pdf)")
    ap.add_argument("--doc-type", default=None, help="HD/ET/MR/UNK (si --pdf)")
    ap.add_argument("--backend", default="docling", choices=["docling","mock"], help="docling | mock (pruebas)")
    args = ap.parse_args()

    out_root = Path(args.outdir)

    worklist: List[Dict] = []
    if args.pdf:
        pdf = Path(args.pdf)
        doc_id = args.doc_id or pdf.stem
        doc_type = args.doc_type or _infer_doc_type(pdf.stem)
        worklist = [{"pdf": pdf, "doc_id": doc_id, "doc_type": doc_type}]
    elif args.pdf_dir:
        pdf_dir = Path(args.pdf_dir)
        pdfs = sorted(list(pdf_dir.glob("*.pdf")) + [p for sub in pdf_dir.glob("*") if sub.is_dir() for p in sub.glob("*.pdf")])
        worklist = [{"pdf": p, "doc_id": p.stem, "doc_type": _infer_doc_type(p.stem)} for p in pdfs]
        print(f"[PC3b] PDFs detectados en {pdf_dir}: {len(worklist)}")
    else:
        # ← modo por defecto: leer PC-2/PC-1
        worklist = _discover_inputs()

    if not worklist:
        print("[PC3b] Nada que procesar. Asegúrate de haber corrido PC-2 (index.json y clean_manifest.json) "
              "o usa --pdf / --pdf-dir.")
        return

    for it in worklist:
        pdf = Path(it["pdf"])
        if not pdf.exists():
            print(f"[PC3b] ⚠ PDF no encontrado: {pdf}")
            continue
        _process_pdf(pdf, out_root, it["doc_id"], it["doc_type"], args.backend)

if __name__ == "__main__":
    main()
