#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-3 — Parsing estructural (secciones, numerales, tablas, notas/checkmarks) + Limpieza/Consolidación HD + Tidy
-------------------------------------------------------------------------------------------------------------
Actualizado: Octubre 2025

- Filtro de encabezados de sección más estricto (menos falsos positivos).
- Extracción de tablas con filtro permisivo (descarta solo ruido extremo).
- Clasificación de pruebas (Hydro, FAT, SAT, NDT, I/O) y bloque "test_req".
- Para doc_type == "HD": limpia y consolida tablas (HD_master_clean.csv).
- Tidy (HD_tidy.csv) con normalización de checkboxes y extracción de PRUEBAS/REQUISITOS/BANDERAS/PROCESO.
- NUEVO: Corrección de artefactos OCR tipo “M a x”, “M i n”, “A S T M”, etc., para evitar falsos checkboxes.
"""

import argparse
import csv
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Iterable, Tuple, Optional

import pdfplumber

# --- Paths ---
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

# --- Heurísticas/Umbrales para HD ---
HD_MIN_TOTAL_CELLS = 4
HD_MAX_EMPTY_RATIO = 0.90
HD_MIN_GOOD_ROWS = 2
HD_MIN_COLUMNS = 6

# --- Regex y patrones ---
SEC_HEADER_RE = re.compile(r"^\s*(\d+(?:\.\d+)+\.?)\s+(.+)$")

# SOLO acepta x/X si NO está pegada a letras (evita “Max”)
CHECK_PATTERN = re.compile(r"(^|\s)([☒☑✓✔●◼■•◻]|(?<![A-Za-z])[Xx](?![A-Za-z]))(\s|$)")
IGNORE_CHECK_PATTERNS = re.compile(
    r"(FOR\s+CONSTRUCTION|IFR|IFC|REV(ISION)?\s*:?|EMISI[ÓO]N|PROP[ÓO]SITO|ISSUED\s+FOR)",
    re.I,
)

TEST_TYPES = {
    r"\bhidrost": "hydrostatic_pressure_test",
    r"\bhidrostat": "hydrostatic_pressure_test",
    r"\bfat\b": "factory_acceptance_test",
    r"\bsat\b": "site_acceptance_test",
    r"\bradi(og|o)g": "ndt_rt",
    r"\bultrason": "ndt_ut",
    r"\bpart[ií]culas\s+magn": "ndt_mt",
    r"\bl[ií]quidos\s+penetr": "ndt_pt",
    r"\bi/o\b": "io_checks",
    r"\bentradas\b": "io_checks",
    r"\bsalidas\b": "io_checks",
    r"\balarmas\b": "io_checks",
}

SECTION_TITLE_STOPWORDS = {
    "de", "del", "la", "el", "los", "las", "y", "o", "u", "en", "por", "para", "con", "veces"
}

# --- Frases/llaves conocidas de la HD para tidy ---
PRUEBAS_HEADER_KEYS = ("PRUEBAS", "Requerida", "Presenciada")
PRUEBAS_NOMBRES = ["Hidrostática", "Precisión", "Repetibilidad", "Linealidad"]

REQ_FRASES = [
    "Inspección usando la lista de comprobación de API 675",
    "Certificados de materiales",
    "Ensayos no destructivos",
    "Radiográfico",
    "Ultrasonidos",
    "Partículas magnéticas",
    "Líquidos penetrantes",
    "Limpieza antes del montaje final",
    "Durezas en soldaduras y zonas térmicamente afectadas",
    "Suministrar procedimientos de ensayos presenciados",
    "Unidades en placa características",
    "Tuberías de proceso suministradas por vendedor",
    "Panel View para cada paquete de Inyección",
    "Quill de inyección con boquilla de dosificación",
    "El vendedor suministra válvula de alivio de presión",
    "Indicador de presión a la descarga de las bombas",
    "Válvulas dobles de antirretorno requeridas",
    "Tablero eléctrico (mínimo NEMA 4X)",
    "Sistema de Control",
    "Indicador de nivel",
    "Skid estructural para paquetizado",
    "El vendedor suministra válvula de contrapresión",
]

# --- Utilidades texto/tablas ---
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _fix_ocr_spaced_words(txt: str) -> str:
    # Corrige tokens comunes que salen “letra por letra”.
    patterns = [
        (r"\bM\s*a\s*x\b", "Max"),
        (r"\bM\s*i\s*n\b", "Min"),
        (r"\bN\s*/\s*A\b", "N/A"),
        (r"\bA\s*S\s*T\s*M\b", "ASTM"),
        (r"\bP\s*S\s*V\b", "PSV"),
        (r"\bD\s*C\s*S\b", "DCS"),
        (r"\bC\s*C\s*M\b", "CCM"),
        (r"\bH\s*D\b", "HD"),
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

def row_has_content(row: Iterable[str]) -> bool:
    for c in row:
        if re.search(r"[A-Za-z0-9]{2,}", c or ""):
            return True
    return False

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

def contains_checkmark(s: str) -> bool:
    # Normaliza antes para anular "M a x" -> "Max"
    s_norm = normalize_cell(s)
    if not CHECK_PATTERN.search(s_norm):
        return False
    if IGNORE_CHECK_PATTERNS.search(s_norm):
        return False
    return True

def detect_test_type(s: str) -> str:
    s_low = s.lower()
    for regex, ttype in TEST_TYPES.items():
        if re.search(regex, s_low):
            return ttype
    return ""

def read_clean_page(doc_id: str, page_num: int) -> str:
    p = PC2_DIR / doc_id / f"{doc_id}_page_{page_num:03d}.txt"
    if p.exists():
        return p.read_text(encoding="utf-8", errors="ignore")
    p1 = PC1_DIR / doc_id / f"{doc_id}_page_{page_num:03d}.txt"
    if p1.exists():
        return p1.read_text(encoding="utf-8", errors="ignore")
    return ""

# --- Segmentación de secciones/párrafos ---
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
        current_buffer = []

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

# --- Extracción de tablas PDF ---
def extract_tables_from_pdf(pdf_path: Path, page_num: int) -> List[List[List[str]]]:
    tables: List[List[List[str]]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not (1 <= page_num <= len(pdf.pages)):
                return tables
            page = pdf.pages[page_num - 1]
            text_page = (page.extract_text() or "").upper()

            if any(x in text_page for x in ["TABLA DE CONTENIDO", "CONTENTS", "INTRODUCCIÓN", "PORTADA"]):
                return tables

            extracted = page.extract_tables() or []
            for t in extracted:
                if not t:
                    continue
                total_cells = sum(len(r) for r in t)
                empty_cells = sum(1 for r in t for c in r if not c or not str(c).strip())

                if total_cells < HD_MIN_TOTAL_CELLS:
                    continue
                if (empty_cells / max(total_cells, 1)) > HD_MAX_EMPTY_RATIO:
                    continue

                tables.append(t)
    except Exception as e:
        print(f"[WARN] Error al extraer tablas {pdf_path.name} p.{page_num}: {e}")
    return tables

# --- Limpieza/validación de tablas (HD) ---
def clean_table(raw_table: List[List[Any]]) -> List[List[str]]:
    return [[normalize_cell(c) for c in (row or [])] for row in (raw_table or [])]

def table_is_valid_hd(table: List[List[str]]) -> bool:
    if not table or len(table) < HD_MIN_GOOD_ROWS:
        return False
    width = max((len(r) for r in table), default=0)
    if width < HD_MIN_COLUMNS:
        return False
    good_rows = sum(1 for r in table if row_has_content(r))
    return good_rows >= HD_MIN_GOOD_ROWS

def write_csv(path: Path, rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.writer(cf)
        for r in rows:
            writer.writerow(r)

# --- Normalización de checkboxes ---
def token_is_checked(tok: str) -> Optional[bool]:
    """
    'l' → True, 'x'/'X'/'✓'/'☑'/'☒' → True, 'm' → False.
    Retorna None si no aplica.
    """
    if not tok:
        return None
    t = tok.strip()
    if t.lower() == "l":
        return True
    if t.lower() == "m":
        return False
    if t in ("x", "X", "✓", "✔", "☑", "☒"):
        return True
    return None

def row_tokens(row: List[str]) -> List[str]:
    toks: List[str] = []
    for c in row:
        c2 = normalize_cell(c)
        if not c2:
            continue
        toks.extend(re.split(r"\s+", c2))
    return toks

# --- PRUEBAS (HD) ---
def find_pruebas_header(master_rows: List[List[str]]) -> Tuple[int, Dict[str, int]]:
    for i, row in enumerate(master_rows):
        cells = [normalize_cell(c) for c in row]
        joined = " ".join(cells).lower()
        if "pruebas" in joined and "requerida" in joined:
            name_idx = 0
            req_idx = None
            pres_idx = None
            for j, c in enumerate(cells):
                cl = c.lower()
                if "requerida" in cl and req_idx is None:
                    req_idx = j
                if "presenci" in cl and pres_idx is None:
                    pres_idx = j
            if req_idx is None and len(cells) > 1:
                req_idx = 1
            return i, {"nombre": name_idx, "req": req_idx, "pres": pres_idx}
    return -1, {}

def scan_pruebas(master_rows: List[List[str]], start_row: int, cols: Dict[str, int]) -> Dict[str, Dict[str, Optional[bool]]]:
    res: Dict[str, Dict[str, Optional[bool]]] = {}
    stop_words = ("NOTAS", "PREPARACIÓN", "PESOS", "FLUIDO DE LUBRICACIÓN", "ACCIONAMIENTO")
    for i in range(start_row + 1, len(master_rows)):
        row = [normalize_cell(c) for c in master_rows[i]]
        if not row_has_content(row):
            break
        head = row[0].upper() if row else ""
        if any(w in head for w in stop_words):
            break

        nombre = normalize_cell(row[cols.get("nombre", 0)] if len(row) > 0 else "")
        if not nombre:
            continue

        req_tok = normalize_cell(row[cols["req"]]) if cols.get("req") is not None and cols["req"] < len(row) else ""
        pres_tok = normalize_cell(row[cols["pres"]]) if cols.get("pres") is not None and cols["pres"] < len(row) else ""

        if not req_tok and not pres_tok:
            toks = row_tokens(row)
            marks = [token_is_checked(t) for t in toks if token_is_checked(t) is not None]
            req_val = marks[0] if len(marks) >= 1 else None
            pres_val = marks[1] if len(marks) >= 2 else None
        else:
            req_val = token_is_checked(req_tok)
            pres_val = token_is_checked(pres_tok)

        res[nombre] = {"requerida": req_val, "presenciada": pres_val}
    return res

# --- REQUISITOS (HD) ---
def scan_requisitos(master_rows: List[List[str]]) -> Dict[str, Optional[bool]]:
    found: Dict[str, Optional[bool]] = {f: None for f in REQ_FRASES}
    for row in master_rows:
        joined = " ".join(normalize_cell(c) for c in row)
        joined_nospace = re.sub(r"\s+", " ", joined).strip()
        if not joined_nospace:
            continue
        toks = row_tokens(row)
        checked_any = any(token_is_checked(t) is True for t in toks)
        unchecked_any = any(token_is_checked(t) is False for t in toks)
        for frase in REQ_FRASES:
            if frase in joined_nospace:
                if checked_any:
                    found[frase] = True
                elif unchecked_any and found[frase] is None:
                    found[frase] = False
    return found

# --- BANDERAS (HD) ---
def scan_banderas(master_rows: List[List[str]]) -> Dict[str, Optional[bool]]:
    flags = {
        "for_construction": None,
        "for_information": None,
        "for_approval_comments": None,
        "for_purchasing": None,
        "for_design": None,
        "as_built": None,
    }
    patterns = {
        "for_construction": re.compile(r"\bFOR\s+CONSTRUCTION\b", re.I),
        "for_information": re.compile(r"\bFOR\s+INFORMATION\b", re.I),
        "for_approval_comments": re.compile(r"\bFOR\s+APPROVAL/COMMENTS\b", re.I),
        "for_purchasing": re.compile(r"\bFOR\s+PURCHASING\b", re.I),
        "for_design": re.compile(r"\bFOR\s+DESIGN\b", re.I),
        "as_built": re.compile(r"\bAS\s+BUILT\b", re.I),
    }
    for row in master_rows:
        row_text = " ".join(normalize_cell(c) for c in row)
        toks = row_tokens(row)
        checked_any = any(token_is_checked(t) is True for t in toks)
        unchecked_any = any(token_is_checked(t) is False for t in toks)
        for key, pat in patterns.items():
            if pat.search(row_text):
                if checked_any:
                    flags[key] = True
                elif unchecked_any and flags[key] is None:
                    flags[key] = False
    return flags

# --- PROCESO (heurística ligera) ---
def scan_proceso(master_rows: List[List[str]]) -> Dict[str, str]:
    vals: Dict[str, str] = {}
    join_all = "\n".join(" ".join(normalize_cell(c) for c in row) for row in master_rows)

    m = re.search(r"Presión de descarga .*?Normal\s*([0-9]+)\b", join_all, flags=re.I)
    if m: vals["P_descarga_normal_psig"] = m.group(1)

    m = re.search(r"Gravedad Especifica .*?Normal\s*([0-9]+(?:\.[0-9]+)?)", join_all, flags=re.I)
    if m: vals["SG_normal"] = m.group(1)

    if "VISCOSIDAD (CP) TBD".lower() in join_all.lower():
        vals["viscosidad_cP"] = "TBD"

    m = re.search(r"Clase\s*1\s*Div\s*2\s*Grupo\s*D", join_all, flags=re.I)
    if m: vals["clasificacion_area"] = "Clase 1 Div 2 Grupo D"

    m = re.search(r"Voltaje\s*\(V\)\s*([0-9/ ]+)", join_all, flags=re.I)
    if m: vals["voltaje_V"] = m.group(1).strip()
    m = re.search(r"Frecuencia\s*\(Hz\)\s*([0-9]+)", join_all, flags=re.I)
    if m: vals["frecuencia_Hz"] = m.group(1)
    m = re.search(r"Fases\s*([0-9]+)", join_all, flags=re.I)
    if m: vals["fases"] = m.group(1)

    return vals

# --- Tidy builder ---
def build_hd_tidy(master_csv: Path, tidy_csv: Path) -> Dict[str, Any]:
    rows: List[List[str]] = []
    with master_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append([normalize_cell(c) for c in r])

    header_row_idx, colmap = find_pruebas_header(rows)
    pruebas: Dict[str, Dict[str, Optional[bool]]] = {}
    if header_row_idx >= 0:
        pruebas = scan_pruebas(rows, header_row_idx, colmap)

    requisitos = scan_requisitos(rows)
    banderas = scan_banderas(rows)
    proceso_vals = scan_proceso(rows)

    tidy_records: List[Dict[str, Any]] = []

    for nombre, estados in pruebas.items():
        tidy_records.append({
            "grupo": "PRUEBAS",
            "item": nombre,
            "requerida": estados.get("requerida"),
            "presenciada": estados.get("presenciada"),
        })

    for frase, val in requisitos.items():
        tidy_records.append({
            "grupo": "REQUISITOS",
            "item": frase,
            "seleccionado": val,
        })

    for k, v in banderas.items():
        tidy_records.append({
            "grupo": "BANDERAS",
            "item": k,
            "seleccionado": v,
        })

    for k, v in proceso_vals.items():
        tidy_records.append({
            "grupo": "PROCESO",
            "item": k,
            "valor": v,
        })

    cols = ["grupo", "item", "requerida", "presenciada", "seleccionado", "valor"]
    tidy_csv.parent.mkdir(parents=True, exist_ok=True)
    with tidy_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in tidy_records:
            w.writerow({k: r.get(k, "") for k in cols})

    return {
        "tidy_rows": len(tidy_records),
        "pruebas_detectadas": list(pruebas.keys()),
        "requisitos_true": [k for k, v in requisitos.items() if v is True],
        "banderas_true": [k for k, v in banderas.items() if v is True],
        "proceso_campos": list(proceso_vals.keys()),
    }

# --- Proceso por documento ---
def process_document(manifest: Dict[str, Any]) -> Dict[str, Any]:
    doc_id = manifest["doc_id"]
    file_name = manifest["file_name"]
    doc_type = manifest.get("doc_type")
    n_pages = int(manifest.get("n_pages", 0))

    out_dir = OUT_DIR / doc_id
    out_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    clean_tables_dir = out_dir / "tables_clean"
    clean_tables_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = DATA_DIR / file_name
    if not pdf_path.exists():
        print(f"[WARN] PDF no encontrado en data/: {file_name}. Se omite extracción de tablas.")

    jsonl_path = out_dir / "blocks.jsonl"
    fout = jsonl_path.open("w", encoding="utf-8")

    total_blocks = 0
    total_tables = 0
    tests_detected = []

    hd_clean_tables: List[List[List[str]]] = []
    hd_clean_meta: List[Dict[str, Any]] = []

    for page in range(1, n_pages + 1):
        # --- BLOQUES de texto ya limpiados (PC-2/PC-1) ---
        text = read_clean_page(doc_id, page)
        if text.strip():
            lines = [ln for ln in text.splitlines() if ln.strip()]

            sec_blocks = segment_sections(lines)
            for b in sec_blocks:
                b["source"] = {
                    "doc_id": doc_id,
                    "doc_type": doc_type,
                    "file_name": file_name,
                    "page": page,
                    "numeral": b.get("section_number"),
                }
                fout.write(json.dumps(b, ensure_ascii=False) + "\n")
            total_blocks += len(sec_blocks)

            for ln in lines:
                ttype = detect_test_type(ln)
                has_chk = contains_checkmark(ln)
                if ttype or has_chk:
                    b = {
                        "type": "test_req" if ttype else "check",
                        "text": normalize_cell(ln.strip()),
                        "test_type": ttype or None,
                        "has_checkmark": has_chk,
                        "source": {
                            "doc_id": doc_id,
                            "doc_type": doc_type,
                            "file_name": file_name,
                            "page": page,
                        },
                    }
                    tests_detected.append(b)
                    fout.write(json.dumps(b, ensure_ascii=False) + "\n")
                    total_blocks += 1

        # --- TABLAS desde PDF ---
        if pdf_path.exists():
            page_tables = extract_tables_from_pdf(pdf_path, page)
            if page_tables:
                for idx, table in enumerate(page_tables, start=1):
                    # RAW
                    raw_csv_path = tables_dir / f"page_{page:03d}_table{idx:02d}.csv"
                    with raw_csv_path.open("w", encoding="utf-8", newline="") as cf:
                        writer = csv.writer(cf)
                        for row in table:
                            # FIX 1: evitar NameError y valores None
                            writer.writerow([(cell if cell is not None else "") for cell in row])
                    total_tables += 1

                    fout.write(
                        json.dumps(
                            {
                                "type": "table",
                                "page": page,
                                "table_index": idx,
                                "csv_file": str(raw_csv_path.relative_to(ROOT)),
                                "source": {
                                    "doc_id": doc_id,
                                    "doc_type": doc_type,
                                    "file_name": file_name,
                                    "page": page,
                                },
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    total_blocks += 1

                    # CLEAN + pool HD
                    if doc_type and doc_type.upper() == "HD":
                        cleaned = clean_table(table)
                        clean_path = clean_tables_dir / f"page_{page:03d}_table{idx:02d}_clean.csv"
                        write_csv(clean_path, cleaned)

                        # FIX 2: siempre agregar al pool; meta solo si pasa validador
                        hd_clean_tables.append(cleaned)
                        if table_is_valid_hd(cleaned):
                            hd_clean_meta.append(
                                {"page": page, "table_index": idx, "clean_csv": str(clean_path.relative_to(ROOT))}
                            )

    fout.close()

    # --- Construcción de HD master + tidy ---
    hd_master_rel = None
    hd_tidy_rel = None
    tidy_summary = None

    if (manifest.get("doc_type") or "").upper() == "HD":
        # FIX 3: Fallback si el pool quedó vacío (p. ej. validador muy estricto)
        if not hd_clean_tables:
            for csv_path in sorted((OUT_DIR / doc_id / "tables_clean").glob("page_*_clean.csv")):
                try:
                    rows = []
                    with csv_path.open("r", encoding="utf-8", newline="") as cf:
                        rdr = csv.reader(cf)
                        for r in rdr:
                            rows.append([normalize_cell(c) for c in r])
                    if rows:
                        hd_clean_tables.append(rows)
                except Exception:
                    pass

        if hd_clean_tables:
            maxw = max((max(len(r) for r in t if t) for t in hd_clean_tables if t), default=0)
            master_rows: List[List[str]] = []
            for t in hd_clean_tables:
                for r in t:
                    row = list(r) + [""] * (maxw - len(r))
                    master_rows.append(row)

            master_path = out_dir / "HD_master_clean.csv"
            write_csv(master_path, master_rows)
            hd_master_rel = str(master_path.relative_to(ROOT))

            tidy_path = out_dir / "HD_tidy.csv"
            tidy_summary = build_hd_tidy(master_path, tidy_path)
            hd_tidy_rel = str(tidy_path.relative_to(ROOT))

    # --- Summary ---
    summary = {
        "doc_id": doc_id,
        "file_name": file_name,
        "doc_type": doc_type,
        "n_pages": n_pages,
        "total_blocks": total_blocks,
        "total_tables": total_tables,
        "notes_checks_found": len(tests_detected),
        "examples_checks": tests_detected[:10],
        "blocks_jsonl": str((OUT_DIR / doc_id / "blocks.jsonl").relative_to(ROOT)),
        "tables_dir": str((OUT_DIR / doc_id / "tables").relative_to(ROOT)),
    }
    if hd_master_rel:
        summary["hd_master_clean"] = hd_master_rel
        summary["hd_tables_clean"] = hd_clean_meta
    if hd_tidy_rel:
        summary["hd_tidy"] = hd_tidy_rel
        if tidy_summary:
            summary["hd_tidy_summary"] = tidy_summary

    (OUT_DIR / doc_id / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="PC-3 — Parsing estructural + HD cleaner + Tidy")
    parser.add_argument("--doc", type=str, default=None, help="Procesa un doc_id específico (carpeta de pc1_raw_pages)")
    args = parser.parse_args()

    if not PC1_DIR.exists():
        raise SystemExit(f"[ERROR] No existe la carpeta {PC1_DIR}. Ejecuta primero PC-1.")

    manifests: List[Dict[str, Any]] = []
    if args.doc:
        path = PC1_DIR / args.doc / "manifest.json"
        if not path.exists():
            raise SystemExit(f"[ERROR] No se encontró manifest: {path}")
        manifests = [json.loads(path.read_text(encoding="utf-8"))]
    else:
        for m in sorted(PC1_DIR.glob("*/manifest.json")):
            try:
                manifests.append(json.loads(m.read_text(encoding="utf-8")))
            except Exception as e:
                print(f"[WARN] No se pudo leer {m}: {e}")

    print(f"[INFO] Manifests detectados: {len(manifests)}")
    if not manifests:
        print(
            "[ERROR] No se encontraron manifests de PC-1.\n"
            f"       Revisa que existan carpetas dentro de {PC1_DIR} con su manifest.json.\n"
            "       Corre en orden:\n"
            "         python .\\scripts\\pc1_read_pdfs.py\n"
            "         python .\\scripts\\pc2_clean_layout.py\n"
            "         python .\\scripts\\pc3_parse_blocks.py\n"
        )
        (OUT_DIR / "index.json").write_text(json.dumps({"documents": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    results = []
    for man in manifests:
        print(f"[PC-3] Procesando: {man.get('file_name', man.get('doc_id', '<sin nombre>'))}")
        res = process_document(man)
        results.append(res)

    (OUT_DIR / "index.json").write_text(
        json.dumps({"documents": results}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("\n✅ PC-3 completado.")
    print("• Resultados por documento en: outputs/pc3_blocks/<doc_id>/")
    print("• Bloques JSONL: blocks.jsonl | Resumen: summary.json | Tablas RAW: tables/*.csv")
    print("• HD limpio: tables_clean/*.csv | Maestro: HD_master_clean.csv | Tidy: HD_tidy.csv")

if __name__ == "__main__":
    main()
