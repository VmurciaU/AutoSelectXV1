# -*- coding: utf-8 -*-
"""
Pipeline genérico: Texto + Tablas (HD, ET, MR, Emails, Minutas, etc.)
---------------------------------------------------------------------
Versión integrada con pc3_parse_blocks (modo por página) y compatible
con tu modo por lote (run_pipeline).

- Modo página: process_page(input_pdf, outdir, page_idx1) -> {"tables_emitted": N}
- Modo lote:   run_pipeline(manifest, out_dir)            -> dict (summary TEXT)
- Hook final:  finalize(out_dir, doc_meta)                -> genera TEXT_sections.csv y TEXT_tables_all.csv

Salidas:
  outdir/tables/page_XXX_tableYY.csv
  outdir/tables_clean/page_XXX_tableYY_clean.csv
  outdir/blocks.jsonl (solo en modo lote)
  outdir/TEXT_sections.csv
  outdir/TEXT_tables_all.csv
  outdir/summary.json (solo en modo lote)
"""

from __future__ import annotations
import csv
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple, Iterable, Optional

import pdfplumber
import pandas as pd


# =========================
# Rutas base (para modo lote)
# =========================

ROOT = (
    Path(__file__).resolve().parent.parent
    if (Path(__file__).resolve().parent.name == "scripts")
    else Path(__file__).resolve().parent
)
PC1_DIR = ROOT / "outputs" / "pc1_raw_pages"
PC2_DIR = ROOT / "outputs" / "pc2_clean_pages"
OUT_DIR = ROOT / "outputs" / "pc3_blocks"
DATA_DIR = ROOT / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Utilidades de texto/secciones
# =========================

SEC_HEADER_RE = re.compile(r"^\s*(\d+(?:\.\d+)+\.?)\s+(.+)$")
SECTION_TITLE_STOPWORDS = {"de", "del", "la", "el", "los", "las", "y", "o", "u", "en", "por", "para", "con", "veces"}

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _fix_ocr_spaced_words(txt: str) -> str:
    patterns = [
        (r"\bM\s*a\s*x\b", "Max"),
        (r"\bM\s*i\s*n\b", "Min"),
        (r"\bN\s*/\s*A\b", "N/A"),
        (r"\bA\s*S\s*T\s*M\b", "ASTM"),
    ]
    for pat, rep in patterns:
        txt = re.sub(pat, rep, txt, flags=re.I)
    return txt

def normalize_cell(s: Any) -> str:
    if s is None:
        return ""
    txt = str(s)
    txt = txt.replace("\u2013", "-").replace("\u2014", "-").replace("\xa0", " ")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\s*\n\s*", " ", txt)
    txt = _fix_ocr_spaced_words(txt)
    return txt.strip()

def read_clean_page(doc_id: str, page_num: int) -> str:
    p = PC2_DIR / doc_id / f"{doc_id}_page_{page_num:03d}.txt"
    if p.exists():
        return p.read_text(encoding="utf-8", errors="ignore")
    p1 = PC1_DIR / doc_id / f"{doc_id}_page_{page_num:03d}.txt"
    if p1.exists():
        return p1.read_text(encoding="utf-8", errors="ignore")
    return ""

def looks_like_title(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if not re.match(r"[A-ZÁÉÍÓÚÑ0-9(]", s):
        return False
    first = s.split()[0].lower().strip("():;,.")
    if first in SECTION_TITLE_STOPWORDS:
        return False
    if len(s) > 180:
        return False
    return True

def segment_sections(lines: List[str]) -> List[Dict[str, Any]]:
    blocks = []
    current_section = None
    current_buffer: List[str] = []

    def flush_paragraph():
        nonlocal current_buffer, current_section, blocks
        if current_buffer:
            text = "\n".join(current_buffer).strip()
            if text:
                blocks.append(
                    {
                        "type": "paragraph",
                        "section_number": current_section["section_number"] if current_section else None,
                        "section_title": current_section["section_title"] if current_section else None,
                        "text": text,
                    }
                )
        current_buffer.clear()

    for ln in lines:
        m = SEC_HEADER_RE.match(ln)
        if m:
            title = normalize_ws(m.group(2))
            if looks_like_title(title):
                flush_paragraph()
                current_section = {
                    "type": "section_header",
                    "section_number": m.group(1).rstrip("."),
                    "section_title": title,
                }
                blocks.append(current_section)
                continue
        current_buffer.append(ln)

    flush_paragraph()
    return blocks


# =========================
# Filtro de tablas (permisivo)
# =========================

MIN_TOTAL_CELLS = 4
MAX_EMPTY_RATIO = 0.95

def _filter_table(t: List[List[str]]) -> bool:
    """
    Regla permisiva: descartar solo ruido extremo.
    """
    if not t:
        return False
    total_cells = sum(len(r) for r in t)
    if total_cells < MIN_TOTAL_CELLS:
        return False
    empty_cells = sum(1 for r in t for c in r if not c or not str(c).strip())
    if (empty_cells / max(total_cells, 1)) > MAX_EMPTY_RATIO:
        return False
    return True


# =========================
# Extracción de tablas
# =========================

def _extract_tables_for_page(input_pdf: Path, page_idx1: int) -> List[List[List[str]]]:
    tables: List[List[List[str]]] = []
    with pdfplumber.open(str(input_pdf)) as pdf:
        idx0 = page_idx1 - 1
        if idx0 < 0 or idx0 >= len(pdf.pages):
            return tables
        page = pdf.pages[idx0]
        text_page_up = (page.extract_text() or "").upper()
        if any(x in text_page_up for x in ["TABLA DE CONTENIDO", "CONTENTS", "INTRODUCCIÓN", "PORTADA"]):
            return tables
        extracted = page.extract_tables() or []
        for t in extracted:
            if not t:
                continue
            # normalizar None
            t_norm = [[(c if c is not None else "") for c in (r or [])] for r in t]
            if _filter_table(t_norm):
                tables.append(t_norm)
    return tables


def _write_csv_rows(path: Path, rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.writer(cf)
        for r in rows:
            writer.writerow(r)


def _clean_and_save(src_csv: Path, dst_csv: Path) -> Tuple[int, int]:
    df = pd.read_csv(src_csv, header=None, dtype=str, keep_default_na=False)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[~(df.apply(lambda r: all((str(x) == "" for x in r)), axis=1))]
    df = df.loc[:, ~(df.apply(lambda c: all((str(x) == "" for x in c)), axis=0))]
    dst_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst_csv, index=False, header=False, encoding="utf-8")
    return df.shape[0], df.shape[1]


# =========================
# Interfaz por página (dispatcher)
# =========================

def process_page(input_pdf: Path, outdir: Path, page_idx1: int) -> Dict[str, Any]:
    """
    Invocado por pc3_parse_blocks.dispatch_page(...)
    Extrae tablas solo de la página dada, guarda RAW y CLEAN.
    """
    tables_dir = outdir / "tables"
    tables_clean_dir = outdir / "tables_clean"
    tables_dir.mkdir(parents=True, exist_ok=True)
    tables_clean_dir.mkdir(parents=True, exist_ok=True)

    emitted = 0
    try:
        tables = _extract_tables_for_page(input_pdf, page_idx1)
        tcount = 0
        for t in tables:
            tcount += 1
            raw_csv = tables_dir / f"page_{page_idx1:03d}_table{tcount:02d}.csv"
            _write_csv_rows(raw_csv, t)
            clean_csv = tables_clean_dir / f"page_{page_idx1:03d}_table{tcount:02d}_clean.csv"
            _clean_and_save(raw_csv, clean_csv)
            emitted += 1
    except Exception as e:
        return {"tables_emitted": emitted, "__warning__": f"extract error p{page_idx1}: {e}"}

    return {"tables_emitted": emitted}


# =========================
# Interfaz por lote (compatibilidad)
# =========================

def run_pipeline(manifest: dict, out_dir: Path) -> dict:
    """
    Modo lote (mantiene tu firma original).
    - Lee texto de PC-2/PC-1 y segmenta a blocks.jsonl
    - Extrae tablas por página y guarda RAW CSV
    - Consolida:
        TEXT_sections.csv
        TEXT_tables_all.csv
    - Escribe summary.json
    """
    doc_id = manifest["doc_id"]
    file_name = manifest["file_name"]
    n_pages = int(manifest.get("n_pages", 0))

    out_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = DATA_DIR / file_name
    jsonl_path = out_dir / "blocks.jsonl"
    fout = jsonl_path.open("w", encoding="utf-8")

    total_blocks = 0
    total_tables = 0

    # 1) BLOQUES (secciones/párrafos)
    for page in range(1, n_pages + 1):
        text = read_clean_page(doc_id, page)
        if text.strip():
            lines = [ln for ln in text.splitlines() if ln.strip()]
            sec_blocks = segment_sections(lines)
            for b in sec_blocks:
                b["source"] = {"doc_id": doc_id, "file_name": file_name, "page": page}
                fout.write(json.dumps(b, ensure_ascii=False) + "\n")
            total_blocks += len(sec_blocks)

        # 2) TABLAS
        if pdf_path.exists():
            try:
                page_tables = _extract_tables_for_page(pdf_path, page)
            except Exception as e:
                page_tables = []
                print(f"[WARN] Error al extraer tablas {pdf_path.name} p.{page}: {e}")
            if page_tables:
                for idx, table in enumerate(page_tables, start=1):
                    raw_csv_path = tables_dir / f"page_{page:03d}_table{idx:02d}.csv"
                    _write_csv_rows(raw_csv_path, table)
                    total_tables += 1
                    fout.write(
                        json.dumps(
                            {
                                "type": "table",
                                "page": page,
                                "table_index": idx,
                                "csv_file": str(raw_csv_path.relative_to(ROOT)),
                                "source": {"doc_id": doc_id, "file_name": file_name, "page": page},
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    total_blocks += 1

    fout.close()

    # 3) CONSOLIDADOS
    # Secciones → TEXT_sections.csv
    sections = []
    with (out_dir / "blocks.jsonl").open("r", encoding="utf-8") as f:
        for line in f:
            try:
                b = json.loads(line)
                if b.get("type") in {"section_header", "paragraph"}:
                    src = b.get("source") or {}
                    sections.append({
                        "type": b.get("type"),
                        "section_number": b.get("section_number"),
                        "section_title": b.get("section_title"),
                        "text": b.get("text", ""),
                        "page": src.get("page"),
                    })
            except Exception:
                pass

    sec_csv = out_dir / "TEXT_sections.csv"
    with sec_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["type", "section_number", "section_title", "text", "page"])
        w.writeheader()
        for r in sections:
            w.writerow(r)

    # Tablas → TEXT_tables_all.csv
    text_tables_csv = out_dir / "TEXT_tables_all.csv"
    wrote_any = False
    with text_tables_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["_page", "_table_idx", "_row"] + [f"c{i}" for i in range(1, 51)])
        if tables_dir.exists():
            for csvf in sorted(tables_dir.glob("*.csv")):
                page, t_idx = None, None
                m = re.search(r"page_(\d{3})_table(\d{2})", csvf.stem)
                if m:
                    page = int(m.group(1))
                    t_idx = int(m.group(2))
                with csvf.open("r", encoding="utf-8", newline="") as cf:
                    rr = csv.reader(cf)
                    for r_i, row in enumerate(rr, start=1):
                        row = [(c or "").strip() for c in row]
                        w.writerow([page, t_idx, r_i] + row[:50])
                        wrote_any = True

    if not wrote_any:
        text_tables_csv.unlink(missing_ok=True)
        text_tables_csv = None

    # 4) Summary
    summary = {
        "doc_id": doc_id,
        "file_name": file_name,
        "doc_type_detected": "TEXT",
        "n_pages": n_pages,
        "total_blocks": total_blocks,
        "total_tables": total_tables,
        "blocks_jsonl": str((out_dir / "blocks.jsonl").relative_to(ROOT)),
        "text_sections_csv": str(sec_csv.relative_to(ROOT)),
        "text_tables_csv": str(text_tables_csv.relative_to(ROOT)) if text_tables_csv else None,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


# =========================
# Hook final para el modo dispatcher
# =========================

def finalize(out_dir: Path, doc_meta: Dict[str, Any]) -> None:
    """
    Genera consolidado por documento para el modo dispatcher:
      - TEXT_sections.csv    (segmentando desde PC-2 clean pages)
      - TEXT_tables_all.csv  (concatenando tables/*.csv)
    No re-extrae tablas; solo consolida lo ya emitido por process_page().
    """
    doc_id = doc_meta.get("doc_id")

    # ---- 1) Sections desde PC-2 ----
    sec_rows: List[Dict[str, Any]] = []
    doc_pc2 = PC2_DIR / doc_id
    pages = sorted(doc_pc2.glob(f"{doc_id}_page_*.txt"))
    for p in pages:
        try:
            page_num = int(p.stem.split("_")[-1])
        except Exception:
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        for b in segment_sections(lines):
            sec_rows.append({
                "type": b.get("type"),
                "section_number": b.get("section_number"),
                "section_title": b.get("section_title"),
                "text": b.get("text", ""),
                "page": page_num,
            })
    # siempre escribir (aunque quede vacío)
    pd.DataFrame(
        sec_rows, columns=["type", "section_number", "section_title", "text", "page"]
    ).to_csv(out_dir / "TEXT_sections.csv", index=False, encoding="utf-8")

    # ---- 2) Tablas consolidadas desde tables/*.csv ----
    tdir = out_dir / "tables"
    all_rows = []
    if tdir.exists():
        for csvf in sorted(tdir.glob("*.csv")):
            m = re.search(r"page_(\d{3})_table(\d{2})", csvf.stem)
            page, t_idx = (int(m.group(1)), int(m.group(2))) if m else (None, None)
            try:
                df = pd.read_csv(csvf, header=None, dtype=str, keep_default_na=False)
            except Exception:
                continue
            for r_i, row in df.iterrows():
                vals = [str(v).strip() if pd.notna(v) else "" for v in row.tolist()]
                all_rows.append([page, t_idx, r_i + 1] + vals[:50])

    if all_rows:
        pd.DataFrame(
            all_rows,
            columns=["_page", "_table_idx", "_row"] + [f"c{i}" for i in range(1, 51)]
        ).to_csv(out_dir / "TEXT_tables_all.csv", index=False, encoding="utf-8")
    else:
        (out_dir / "TEXT_tables_all.csv").write_text("", encoding="utf-8")

