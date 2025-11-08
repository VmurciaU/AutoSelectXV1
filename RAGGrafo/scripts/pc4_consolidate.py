#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-4 — Consolidación cross-documento (versión extendida Octubre 2025)
---------------------------------------------------------------------
- Lee salidas de PC-3 en outputs/pc3_blocks/<doc_id>/.
- Construye:
    master_sections.csv
    master_tables.csv
    pid_index.csv
    merged_summary.json

Novedades clave
  • table_uid = f"{doc_id}::p{_page:03d}::t{_table_idx:02d}" (estable y único).
  • merged_summary.json incluye sections_count, tables_count, pid_pages.
  • PIDs etiquetados con rule/evidence (si existen).
  • Enlaza cada tabla con su contexto cercano:
        - section_number_near
        - section_title_near
        - caption_near   (detecta “Tabla X …” / “Table X …” en párrafos de la misma página)
        - table_order_in_page

Requisitos: pandas
Uso:
  python scripts/pc4_consolidate.py
  # o con rutas personalizadas:
  python scripts/pc4_consolidate.py --pc3-dir outputs/pc3_blocks --outdir outputs/pc4_consolidated
"""

from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# --- Paths base ---
ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
DEFAULT_PC3_DIR = ROOT / "outputs" / "pc3_blocks"
DEFAULT_OUTDIR  = ROOT / "outputs" / "pc4_consolidated"


# --- Utilitarios ---
def safe_read_csv(path: Path, **kwargs) -> Optional[pd.DataFrame]:
    """Lee CSV si existe (y no está vacío). Devuelve None si falla."""
    try:
        if path.exists() and path.stat().st_size > 0:
            return pd.read_csv(path, **kwargs)
    except Exception as e:
        print(f"[WARN] No se pudo leer CSV: {path} ({e})")
    return None


def collect_doc_dirs(pc3_dir: Path) -> List[Path]:
    """Lista los subdirectorios que contienen summary.json (una unidad por documento)."""
    if not pc3_dir.exists():
        return []
    return sorted([p for p in pc3_dir.iterdir() if p.is_dir() and (p / "summary.json").exists()])


def load_summary(doc_dir: Path) -> Dict:
    return json.loads((doc_dir / "summary.json").read_text(encoding="utf-8"))


# --- Secciones ---
def sections_from_blocks(doc_dir: Path) -> pd.DataFrame:
    """
    Backup si no existe TEXT_sections.csv: derivar secciones y párrafos desde blocks.json.
    Excluye bloques marcados como pid_like.
    """
    blocks_path = doc_dir / "blocks.json"
    if not blocks_path.exists():
        return pd.DataFrame(columns=["doc_id","doc_type","file_name","page","type","section_number","section_title","text"])

    meta = load_summary(doc_dir)
    doc_id  = meta.get("doc_id", doc_dir.name)
    doc_type= meta.get("doc_type")
    file_name = meta.get("file_name")

    rows = []
    blocks = json.loads(blocks_path.read_text(encoding="utf-8"))
    for b in blocks:
        if b.get("pid_like"):
            continue
        btype = b.get("type")
        if btype in ("section_header", "paragraph"):
            rows.append({
                "doc_id": doc_id,
                "doc_type": doc_type,
                "file_name": file_name,
                "page": b.get("page"),
                "type": btype,
                "section_number": b.get("section_number"),
                "section_title": b.get("section_title"),
                "text": b.get("text", "")
            })
    return pd.DataFrame(rows)


def build_master_sections(doc_dir: Path) -> pd.DataFrame:
    """Carga TEXT_sections.csv si existe; si no, fallback a blocks.json."""
    meta = load_summary(doc_dir)
    doc_id   = meta.get("doc_id", doc_dir.name)
    doc_type = meta.get("doc_type")
    file_name= meta.get("file_name")

    sec_csv = doc_dir / "TEXT_sections.csv"
    df = safe_read_csv(sec_csv)
    if df is None:
        df = sections_from_blocks(doc_dir)
    else:
        # Asegurar columnas mínimas
        for c in ["type","section_number","section_title","text","page"]:
            if c not in df.columns:
                df[c] = None
        df.insert(0, "file_name", file_name)
        df.insert(0, "doc_type",  doc_type)
        df.insert(0, "doc_id",    doc_id)
        df = df[["doc_id","doc_type","file_name","page","type","section_number","section_title","text"]]
    return df


# --- Tablas ---
def build_master_tables(doc_dir: Path) -> pd.DataFrame:
    """
    Carga TEXT_tables_all.csv si existe (preferido).
    Si no, fusiona tables/*.csv e infiere _page y _table_idx del nombre (page_XXX_tableYY.csv).
    Siempre genera table_uid estable.
    """
    meta = load_summary(doc_dir)
    doc_id   = meta.get("doc_id", doc_dir.name)
    doc_type = meta.get("doc_type")
    file_name= meta.get("file_name")

    tt = doc_dir / "TEXT_tables_all.csv"
    df = safe_read_csv(tt)
    if df is not None:
        # Asegurar columnas de página/índice
        if "_page" not in df.columns:
            df["_page"] = 1
        if "_table_idx" not in df.columns:
            df["_table_idx"] = df.groupby("_page").cumcount() + 1
        df.insert(0, "file_name", file_name)
        df.insert(0, "doc_type",  doc_type)
        df.insert(0, "doc_id",    doc_id)
        df["table_uid"] = df.apply(lambda r: f"{r['doc_id']}::p{int(r['_page']):03d}::t{int(r['_table_idx']):02d}", axis=1)
        return df

    # Fallback: leer tables individuales
    rows = []
    tdir = doc_dir / "tables"
    if not tdir.exists():
        return pd.DataFrame()

    pat = re.compile(r"page_(\d{3})_table(\d{2})")
    for csvf in sorted(tdir.glob("*.csv")):
        page, idx = None, None
        m = pat.search(csvf.stem)
        if m:
            page, idx = int(m.group(1)), int(m.group(2))
        dfp = safe_read_csv(csvf, header=None)
        if dfp is None:
            continue
        for r_i, row in dfp.iterrows():
            vals = [str(v) if pd.notna(v) else "" for v in row.tolist()]
            rows.append({
                "doc_id": doc_id,
                "doc_type": doc_type,
                "file_name": file_name,
                "_page": page,
                "_table_idx": idx,
                "_row": r_i + 1,
                **{f"c{i+1}": vals[i] if i < len(vals) else "" for i in range(max(50, len(vals)))}
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_page"] = df["_page"].fillna(0).astype(int)
        df["_table_idx"] = df["_table_idx"].fillna(0).astype(int)
        df["table_uid"] = df.apply(lambda r: f"{r['doc_id']}::p{int(r['_page']):03d}::t{int(r['_table_idx']):02d}", axis=1)
    return df


# --- Contexto de tabla (numeral/caption cercanos) ---
def enrich_table_context(df_tables: pd.DataFrame, df_sections: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega a cada fila de tabla:
      - section_number_near: último numeral en la misma página (o página previa), patrón ^\d+(\.\d+)*$.
      - section_title_near : título de esa sección.
      - caption_near       : primer párrafo/título en la página que coincida con r"^(tabla|table) \d+ ..."
      - table_order_in_page: ordinal 1..N por página.

    Nota: No modifica conteo de tablas; solo añade contexto.
    """
    if df_tables.empty or df_sections.empty:
        return df_tables

    df = df_tables.copy()
    df["_page"] = df["_page"].fillna(0).astype(int)

    sec = df_sections.dropna(subset=["page"]).copy()
    sec["page"] = sec["page"].fillna(0).astype(int)
    # Normalizar columnas de texto
    if "section_title" not in sec.columns: sec["section_title"] = ""
    if "text" not in sec.columns: sec["text"] = ""
    sec["section_title"] = sec["section_title"].fillna("").astype(str)
    sec["text"] = sec["text"].fillna("").astype(str)

    # Numeral tipo 4.4.2
    numeral_pat = re.compile(r"^\d+(?:\.\d+)*$")
    sec_num = sec[sec["section_number"].fillna("").astype(str).str.match(numeral_pat, na=False)].copy()

    # Para cada página, tomar la última sección numerada (más cercana “hacia atrás”)
    sec_map_df = (sec_num.sort_values(["page"])
                  .groupby("page")
                  .tail(1)[["page","section_number","section_title"]])
    page_to_sec = {int(r.page):(str(r.section_number), str(r.section_title)) for _, r in sec_map_df.iterrows()}

    # Captions tipo "Tabla X ..." o "Table X ..." (primer match por página)
    cap_pat = re.compile(r"(?i)^\s*(tabla|table)\s+\d+\.?\s+.+")
    cap_map: dict[int, str] = {}
    for _, r in sec.iterrows():
        p = int(r.page)
        if p not in cap_map:  # conservar PRIMER caption encontrado en esa página
            text_candidate = (r["section_title"] or r["text"]).strip()
            if cap_pat.match(text_candidate):
                cap_map[p] = text_candidate

    # Enriquecer
    enriched_rows = []
    for _, r in df.iterrows():
        p = int(r["_page"])
        near_num, near_title = "", ""
        if p in page_to_sec:
            near_num, near_title = page_to_sec[p]
        elif (p - 1) in page_to_sec:
            near_num, near_title = page_to_sec[p - 1]
        caption = cap_map.get(p, "")
        enriched_rows.append({
            **r.to_dict(),
            "section_number_near": near_num,
            "section_title_near": near_title,
            "caption_near": caption,
        })
    out = pd.DataFrame(enriched_rows)
    out["table_order_in_page"] = out.groupby("_page").cumcount() + 1
    return out


# --- P&ID ---
def build_pid_index(doc_dir: Path) -> pd.DataFrame:
    """Concatena page_*_pid_context.csv, asegurando columnas base y metadatos de documento."""
    meta = load_summary(doc_dir)
    doc_id   = meta.get("doc_id", doc_dir.name)
    doc_type = meta.get("doc_type")
    file_name= meta.get("file_name")

    pid_dir = doc_dir / "pid_context"
    if not pid_dir.exists():
        return pd.DataFrame()

    rows: List[pd.DataFrame] = []
    for f in sorted(pid_dir.glob("page_*_pid_context.csv")):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        for c in ["page","pid_like","pid_reference","pid_note","rule","evidence"]:
            if c not in df.columns:
                df[c] = None
        df.insert(0, "file_name", file_name)
        df.insert(0, "doc_type",  doc_type)
        df.insert(0, "doc_id",    doc_id)
        rows.append(df)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# --- MAIN ---
def main():
    ap = argparse.ArgumentParser(description="PC-4 — Consolidación cross-documento (extendida)")
    ap.add_argument("--pc3-dir", default=str(DEFAULT_PC3_DIR), help="Directorio base de salidas PC-3")
    ap.add_argument("--outdir",  default=str(DEFAULT_OUTDIR),  help="Salida de PC-4")
    args = ap.parse_args()

    pc3_dir = Path(args.pc3_dir)
    outdir  = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    doc_dirs = collect_doc_dirs(pc3_dir)
    if not doc_dirs:
        print(f"[PC4] No hay documentos en {pc3_dir}.")
        return

    all_sections, all_tables, all_pids = [], [], []
    merged_summary = {"documents": []}

    print(f"[PC4] Documentos detectados: {len(doc_dirs)}")
    for d in doc_dirs:
        try:
            summary = load_summary(d)
            doc_id = summary.get("doc_id", d.name)
            print(f"[PC4] Procesando {doc_id} ...", end="", flush=True)

            # 1) Build por doc
            df_sec = build_master_sections(d)
            df_tab = build_master_tables(d)
            df_pid = build_pid_index(d)

            # 2) Enriquecer contexto de tablas (numeral/caption cercanos)
            if not df_tab.empty and not df_sec.empty:
                df_tab = enrich_table_context(df_tab, df_sec)

            # 3) Acumular para maestros
            if not df_sec.empty:
                all_sections.append(df_sec)
            if not df_tab.empty:
                all_tables.append(df_tab)
            if not df_pid.empty:
                all_pids.append(df_pid)

            # 4) Métricas por documento
            summary["sections_count"] = int(df_sec.shape[0]) if not df_sec.empty else 0
            summary["tables_count"]   = int(df_tab["table_uid"].nunique()) if not df_tab.empty else 0
            summary["pid_pages"]      = sorted(df_pid["page"].dropna().unique().tolist()) if not df_pid.empty else []

            merged_summary["documents"].append(summary)
            print(" listo.")
        except Exception as e:
            print(f" ERROR: {e}")

    # --- Escritura de salidas ---
    (outdir / "merged_summary.json").write_text(json.dumps(merged_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if all_sections:
        pd.concat(all_sections, ignore_index=True).to_csv(outdir / "master_sections.csv", index=False, encoding="utf-8")
    else:
        (outdir / "master_sections.csv").write_text("", encoding="utf-8")

    if all_tables:
        mt = pd.concat(all_tables, ignore_index=True)
        # Completar columnas c1..c50 si faltan
        for i in range(1, 51):
            c = f"c{i}"
            if c not in mt.columns:
                mt[c] = ""
        cols = [
            "doc_id","doc_type","file_name",
            "_page","_table_idx","_row","table_uid",
            "section_number_near","section_title_near","caption_near","table_order_in_page",
        ] + [f"c{i}" for i in range(1, 51)]
        mt = mt[[c for c in cols if c in mt.columns]]
        mt.to_csv(outdir / "master_tables.csv", index=False, encoding="utf-8")
    else:
        (outdir / "master_tables.csv").write_text("", encoding="utf-8")

    if all_pids:
        pd.concat(all_pids, ignore_index=True).to_csv(outdir / "pid_index.csv", index=False, encoding="utf-8")
    else:
        (outdir / "pid_index.csv").write_text("", encoding="utf-8")

    print("[PC4] ✅ Consolidación completada con contexto de tabla incluido.")
    print(f" - {outdir/'merged_summary.json'}")
    print(f" - {outdir/'master_sections.csv'}")
    print(f" - {outdir/'master_tables.csv'}")
    print(f" - {outdir/'pid_index.csv'}")


if __name__ == "__main__":
    main()
