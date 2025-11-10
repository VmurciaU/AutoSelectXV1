#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-5 — Construcción de grafo mínimo (alineado con PC-4 extendido)
-----------------------------------------------------------------
Toma salidas de PC-4 y genera un grafo ligero en formato JSONL y CSV:
- graph_nodes.csv
- graph_edges.csv
- graph.jsonl  (mezcla de nodos y aristas, 1 registro por línea)

Nodos:
  - Doc(doc_id)
  - Section(doc_id,page,section_number,section_title)
  - Table(doc_id,page,table_idx|table_uid, section_number_near, section_title_near, caption_near, table_order_in_page)
  - Param(nombre de columna tabular no vacío)
  - PidRef(doc_id,page,reference,rule,evidence)

Aristas:
  - DOC_CONTAINS_SECTION    Doc -> Section
  - DOC_CONTAINS_TABLE      Doc -> Table
  - TABLE_HAS_PARAM         Table -> Param
  - DOC_HAS_PID_PAGE        Doc -> PidRef
  - SECTION_NEAR_TABLE      Section -> Table   (🆕 si hay match por página/numeral)

Compatibilidad:
  • Usa `table_uid` si existe; si no, lo reconstruye con doc_id/_page/_table_idx.
  • Propaga `section_number_near`, `section_title_near`, `caption_near`, `table_order_in_page` a los nodos Table.
  • Captura `rule`/`evidence` desde pid_index si existen.

Uso:
  python scripts/pc5_graph_build.py
  # o con rutas personalizadas:
  python scripts/pc5_graph_build.py --pc4-dir outputs/pc4_consolidated --outdir outputs/pc5_graph
"""

from __future__ import annotations
import argparse
import json
import math
import re
from pathlib import Path
from typing import Dict, Tuple, List

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
DEFAULT_PC4_DIR = ROOT / "outputs" / "pc4_consolidated"
DEFAULT_OUTDIR  = ROOT / "outputs" / "pc5_graph"


# -------------------------
# Utilitarios
# -------------------------
def _safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        if path.exists() and path.stat().st_size > 0:
            return pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] No se pudo leer CSV: {path} ({e})")
    return pd.DataFrame()

def _load_pc4(pc4_dir: Path) -> Dict[str, pd.DataFrame]:
    return {
        "sections": _safe_read_csv(pc4_dir / "master_sections.csv"),
        "tables":   _safe_read_csv(pc4_dir / "master_tables.csv"),
        "pid":      _safe_read_csv(pc4_dir / "pid_index.csv"),
    }

def _slug(s: str, maxlen: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s\-\.\#/\(\)]", "", s, flags=re.UNICODE)  # conserva símbolos útiles usuales
    s = s[:maxlen]
    return s or "NA"

def _node_id(label: str, *parts) -> str:
    safe = "_".join(str(p) for p in parts)
    return f"{label}:{safe}"


# -------------------------
# Normalizaciones (tablas)
# -------------------------
def _ensure_table_uid(df_tab: pd.DataFrame) -> pd.DataFrame:
    if df_tab.empty:
        return df_tab

    # Asegurar columnas base
    for c in ["doc_id", "_page", "_table_idx"]:
        if c not in df_tab.columns:
            if c == "doc_id":
                df_tab["doc_id"] = df_tab.get("file_name", "DOC")
            else:
                df_tab[c] = 0

    # Si ya viene table_uid desde PC-4, úsalo tal cual; si hay NaN, reconstruir
    if "table_uid" in df_tab.columns:
        mask_nan = df_tab["table_uid"].isna() | (df_tab["table_uid"].astype(str).str.len() == 0)
        if mask_nan.any():
            df_tab.loc[mask_nan, "table_uid"] = df_tab.loc[mask_nan].apply(
                lambda r: f"{r['doc_id']}::p{int(r['_page'] or 0):03d}::t{int(r['_table_idx'] or 0):02d}", axis=1
            )
        return df_tab

    # Fallback: construir table_uid con doc_id/_page/_table_idx
    df_tab["table_uid"] = df_tab.apply(
        lambda r: f"{r['doc_id']}::p{int(r['_page'] or 0):03d}::t{int(r['_table_idx'] or 0):02d}", axis=1
    )
    return df_tab


# -------------------------
# Construcción de grafo
# -------------------------
def build_graph(df_sections: pd.DataFrame, df_tables: pd.DataFrame, df_pid: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    nodes, edges = [], []

    # --- DOC nodes ---
    doc_ids = set()
    for df in (df_sections, df_tables, df_pid):
        if not df.empty and "doc_id" in df.columns:
            doc_ids.update(df["doc_id"].dropna().astype(str).unique().tolist())
    for d in sorted(doc_ids):
        nodes.append({"id": _node_id("Doc", d), "label": "Doc", "doc_id": d})

    # --- SECTION nodes + edges Doc->Section ---
    section_nodes_index = {}  # key: (doc_id, page, section_number_or_slug) -> node_id
    if not df_sections.empty:
        for _, r in df_sections.iterrows():
            d = str(r.get("doc_id", ""))
            page = r.get("page")
            page = int(page) if pd.notna(page) and page != "" else None
            s_num = str(r.get("section_number")) if pd.notna(r.get("section_number")) else ""
            s_title = str(r.get("section_title")) if pd.notna(r.get("section_title")) else ""
            key_text = s_num if s_num else _slug(s_title, 40)
            sid = _node_id("Section", d, page if page is not None else "NA", key_text)
            nodes.append({
                "id": sid, "label": "Section",
                "doc_id": d, "page": page,
                "section_number": s_num, "section_title": s_title
            })
            edges.append({"src": _node_id("Doc", d), "dst": sid, "type": "DOC_CONTAINS_SECTION"})
            section_nodes_index[(d, page, s_num or key_text)] = sid

    # --- TABLE nodes + edges Doc->Table (+ propiedades nuevas) ---
    if not df_tables.empty:
        df_tables = _ensure_table_uid(df_tables)

        # Mantener sólo una fila por tabla para el nodo (propagando contexto si está)
        cols_keep = ["table_uid", "doc_id", "_page", "_table_idx",
                     "section_number_near", "section_title_near", "caption_near", "table_order_in_page"]
        for c in cols_keep:
            if c not in df_tables.columns:
                df_tables[c] = "" if c not in ["_page","_table_idx","table_order_in_page"] else 0

        tbl_min = (
            df_tables[cols_keep]
            .drop_duplicates(subset=["table_uid"])
            .reset_index(drop=True)
        )

        # Crear Table nodes y aristas Doc->Table
        table_node_ids = {}  # map table_uid -> node_id
        for _, r in tbl_min.iterrows():
            d   = str(r["doc_id"])
            pg  = int(r["_page"]) if not (pd.isna(r["_page"]) or r["_page"] == "") else 0
            tix = int(r["_table_idx"]) if not (pd.isna(r["_table_idx"]) or r["_table_idx"] == "") else 0
            tu  = str(r["table_uid"])
            sidn = str(r.get("section_number_near") or "")
            sitt = str(r.get("section_title_near") or "")
            cap  = str(r.get("caption_near") or "")
            ordp = int(r.get("table_order_in_page") or 0)

            tid = _node_id("Table", tu)  # id estable (usa table_uid)
            nodes.append({
                "id": tid, "label": "Table",
                "doc_id": d, "page": pg, "table_idx": tix, "table_uid": tu,
                "section_number_near": sidn, "section_title_near": sitt,
                "caption_near": cap, "table_order_in_page": ordp
            })
            edges.append({"src": _node_id("Doc", d), "dst": tid, "type": "DOC_CONTAINS_TABLE"})
            table_node_ids[tu] = tid

        # PARAM nodes + edges Table->Param
        # Estrategia: tomar la primera fila de cada tabla como cabecera (si existe "_row": ==1; si no, head(1) por groupby).
        group_cols = ["table_uid"]
        value_cols = [c for c in df_tables.columns if c.startswith("c")]
        if value_cols:
            if "_row" in df_tables.columns:
                headers = df_tables[df_tables["_row"] == 1][group_cols + value_cols]
            else:
                headers = df_tables.groupby(group_cols).head(1)[group_cols + value_cols]

            for _, row in headers.iterrows():
                tu = str(row["table_uid"])
                tid = _node_id("Table", tu)
                for c in value_cols:
                    col_name = str(row.get(c, "")).strip()
                    if not col_name or col_name.lower() in ("nan", "none", "null"):
                        continue
                    pname = _slug(col_name, 120)
                    pid_  = _node_id("Param", pname)
                    nodes.append({"id": pid_, "label": "Param", "name": pname})
                    edges.append({"src": tid, "dst": pid_, "type": "TABLE_HAS_PARAM"})

        # 🆕 SECTION_NEAR_TABLE: enlaza sección cercana a la tabla cuando hay match por doc_id + page + section_number_near
        # (Si no hay numeral, no se crea esta arista — el Doc->Table ya garantiza navegabilidad)
        if not df_sections.empty:
            for _, r in tbl_min.iterrows():
                d   = str(r["doc_id"])
                pg  = int(r["_page"]) if not (pd.isna(r["_page"]) or r["_page"] == "") else None
                tu  = str(r["table_uid"])
                sidn = str(r.get("section_number_near") or "")
                if not sidn or pg is None:
                    continue
                sec_key = (d, pg, sidn)
                if sec_key in section_nodes_index:
                    sec_node_id = section_nodes_index[sec_key]
                    tbl_node_id = _node_id("Table", tu)
                    edges.append({"src": sec_node_id, "dst": tbl_node_id, "type": "SECTION_NEAR_TABLE"})

    # --- PID refs nodes + edges Doc->PidRef ---
    if not df_pid.empty:
        # columnas esperadas: doc_id, page, pid_reference, rule, evidence (opcionales)
        have_rule = "rule" in df_pid.columns
        have_evd  = "evidence" in df_pid.columns
        for _, r in df_pid.iterrows():
            d = str(r.get("doc_id", ""))
            page = r.get("page")
            page = int(page) if pd.notna(page) and page != "" else None
            ref = str(r.get("pid_reference", "")).strip()
            rule = str(r.get("rule", "")).strip() if have_rule else ""
            evd  = str(r.get("evidence", "")).strip() if have_evd else ""
            if not ref and page is None:
                continue
            nid = _node_id("PidRef", d, page if page is not None else "NA", _slug(ref or "ref", 64))
            nodes.append({
                "id": nid, "label": "PidRef",
                "doc_id": d, "page": page, "reference": ref,
                **({"rule": rule} if rule else {}),
                **({"evidence": evd} if evd else {}),
            })
            edges.append({"src": _node_id("Doc", d), "dst": nid, "type": "DOC_HAS_PID_PAGE"})

    # Deduplicar
    nodes_df = pd.DataFrame(nodes).drop_duplicates(subset=["id"])
    edges_df = pd.DataFrame(edges).drop_duplicates(subset=["src", "dst", "type"])

    return nodes_df, edges_df


# -------------------------
# Export
# -------------------------
def _dump_jsonl(nodes: pd.DataFrame, edges: pd.DataFrame, outpath: Path):
    with outpath.open("w", encoding="utf-8") as f:
        for _, r in nodes.iterrows():
            rec = {"type": "node", **{k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in r.to_dict().items()}}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        for _, r in edges.iterrows():
            rec = {"type": "edge", **{k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in r.to_dict().items()}}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="PC-5 — Construcción de grafo mínimo (PC-4 extendido)")
    ap.add_argument("--pc4-dir", default=str(DEFAULT_PC4_DIR), help="Directorio con salidas de PC-4")
    ap.add_argument("--outdir",  default=str(DEFAULT_OUTDIR),  help="Salida del grafo")
    args = ap.parse_args()

    pc4_dir = Path(args.pc4_dir)
    outdir  = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    data = _load_pc4(pc4_dir)
    df_sec, df_tab, df_pid = data["sections"], data["tables"], data["pid"]

    if df_sec.empty and df_tab.empty and df_pid.empty:
        print(f"[PC5] No hay datos en {pc4_dir}. Corre PC-4 primero.")
        return

    print("[PC5] Construyendo grafo ...", end="", flush=True)
    nodes, edges = build_graph(df_sec, df_tab, df_pid)
    nodes.to_csv(outdir / "graph_nodes.csv", index=False, encoding="utf-8")
    edges.to_csv(outdir / "graph_edges.csv", index=False, encoding="utf-8")
    _dump_jsonl(nodes, edges, outdir / "graph.jsonl")
    print(" listo.")

    print("[PC5] ✅ Grafo generado:")
    print(f" - {outdir/'graph_nodes.csv'}  (nodos={len(nodes)})")
    print(f" - {outdir/'graph_edges.csv'}  (aristas={len(edges)})")
    print(f" - {outdir/'graph.jsonl'}")


if __name__ == "__main__":
    main()

