#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC-6 — LightRAG (ingesta + custom KG + query)
---------------------------------------------
Subcomandos:
  - ingest  : Construye corpus desde PC-2 (+PC-4) y lo ingesta vía API
  - pushkg  : Exporta grafo (PC-5) a JSONL/JSON; opcional empuje por API
  - query   : Ejecuta consultas de prueba
  - all     : ingest + pushkg

Compatibilidad con tu servidor:
- Documentos: /documents/texts  (recomendado)  | /documents/text | /documents/upload + /documents/scan
- Query:     /query  (mode: naive|local|global|mix|hybrid según build)
- KG:        Import masivo por WebUI; API granular: /graph/entity/create, /graph/relation/create
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

# ---------- Dependencias opcionales ----------
try:
    import requests
except Exception:
    requests = None

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# ---------- LightRAG Core (best-effort; normalmente NO se usa con tu server) ----------
_LIGHTRAG_AVAILABLE = False
try:
    try:
        from lightrag import LightRAG  # noqa
        _LIGHTRAG_AVAILABLE = True
    except Exception:
        from lightrag.core import LightRAG  # noqa
        _LIGHTRAG_AVAILABLE = True
except Exception:
    _LIGHTRAG_AVAILABLE = False

# ---------- Rutas ----------
ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
PC2_DIR = ROOT / "outputs" / "pc2_clean_pages"
PC4_DIR = ROOT / "outputs" / "pc4_consolidated"
PC5_DIR = ROOT / "outputs" / "pc5_graph"
PC6_EXPORT_DIR = ROOT / "outputs" / "pc6_export"
RAG_STORAGE_DIR = ROOT / "rag_storage"
PC6_OUT_QUERIES = ROOT / "outputs" / "pc6_queries"

# =========================================================
# Utils
# =========================================================
def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _read_txt(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return p.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""

def _ensure_dirs(*paths: Path):
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)

def _safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        if path.exists() and path.stat().st_size > 0:
            return pd.read_csv(path)
    except Exception as e:
        print(f"[WARN] No se pudo leer CSV: {path} ({e})")
    return pd.DataFrame()

def _gather_pc2_docs(pc2_dir: Path) -> Dict[str, List[Tuple[int, Path]]]:
    """
    Devuelve: { doc_id: [ (page, path_txt), ... ] }
    Soporta:
      A) pc2_dir/<doc_id>/page_*.txt
      B) pc2_dir/<doc_id>/*.txt (enumera 1..N)
      C) pc2_dir/*.txt (doc único, page=1)
    """
    mapping: Dict[str, List[Tuple[int, Path]]] = {}
    if not pc2_dir.exists():
        return mapping

    loose = sorted(pc2_dir.glob("*.txt"))
    if loose:
        for p in loose:
            mapping[p.stem] = [(1, p)]
        return mapping

    for doc_dir in sorted(pc2_dir.iterdir()):
        if not doc_dir.is_dir():
            continue
        page_files = list(doc_dir.glob("page_*.txt"))
        if page_files:
            pages: List[Tuple[int, Path]] = []
            for p in page_files:
                m = re.search(r"page_(\d+)\.txt$", p.name)
                page = int(m.group(1)) if m else 0
                pages.append((page, p))
            pages.sort(key=lambda x: x[0])
            mapping[doc_dir.name] = pages
            continue

        any_txts = sorted(doc_dir.glob("*.txt"))
        if any_txts:
            mapping[doc_dir.name] = [(i + 1, p) for i, p in enumerate(any_txts)]

    return mapping

# =========================================================
# LightRAG Core helpers (best-effort; usualmente NO se usan)
# =========================================================
def _make_rag(storage_dir: Path):
    from lightrag import LightRAG as _LR  # import tardío
    for style in (
        lambda: _LR(storage_path=str(storage_dir)),
        lambda: _LR(storage_dir=str(storage_dir)),
        lambda: _LR(str(storage_dir)),
        ):
        try:
            return style()
        except Exception:
            pass
    rag = _LR()
    for attr in ("storage_dir", "storage_path", "data_dir"):
        if hasattr(rag, attr):
            try:
                setattr(rag, attr, str(storage_dir))
                break
            except Exception:
                pass
    return rag

async def _core_initialize(rag) -> None:
    if hasattr(rag, "initialize_storages") and callable(getattr(rag, "initialize_storages")):
        await rag.initialize_storages()
    if hasattr(rag, "initialize_pipeline_status") and callable(getattr(rag, "initialize_pipeline_status")):
        await rag.initialize_pipeline_status()

async def _core_ingest_documents(corpus: List[Dict], storage_dir: Path) -> None:
    rag = _make_rag(storage_dir)
    await _core_initialize(rag)
    for rec in (tqdm(corpus, desc="Ingestando (Core)") if tqdm else corpus):
        text = rec.get("text", "")
        metadata = rec.get("metadata", {})
        try:
            if hasattr(rag, "add_document"):
                await rag.add_document(text=text, metadata=metadata)
            elif hasattr(rag, "ingest"):
                await rag.ingest(text=text, metadata=metadata)
            elif hasattr(rag, "index"):
                await rag.index(text=text, metadata=metadata)
            else:
                print("[WARN] LightRAG Core no expone método de ingesta.")
        except Exception as e:
            print(f"[WARN] Falló ingesta de {metadata.get('doc_id')} (Core): {e}")

async def _core_push_graph(pc5_jsonl: List[Dict], storage_dir: Path) -> None:
    if not pc5_jsonl:
        print("[pushkg] No hay graph.jsonl, omitiendo.")
        return
    rag = _make_rag(storage_dir)
    await _core_initialize(rag)

    add_entity = getattr(rag, "add_entity", None)
    add_relation = getattr(rag, "add_relation", None)
    add_graph = getattr(rag, "add_graph", None)

    if add_graph:
        try:
            await add_graph(pc5_jsonl)
            print("[pushkg] add_graph() OK (Core).")
            return
        except Exception as e:
            print(f"[pushkg] add_graph falló, intento nodos/aristas: {e}")

    nodes = [r for r in pc5_jsonl if (r.get("type") or "").lower() in ("node", "entity")]
    edges = [r for r in pc5_jsonl if (r.get("type") or "").lower() in ("edge", "relation", "rel")]

    if add_entity:
        for n in (tqdm(nodes, desc="Nodos (Core)") if tqdm else nodes):
            try:
                await add_entity(n)
            except Exception as e:
                print(f"[WARN] Nodo no publicado {n.get('id') or n.get('name')}: {e}")

    if add_relation:
        for e in (tqdm(edges, desc="Aristas (Core)") if tqdm else edges):
            try:
                await add_relation(e)
            except Exception as ex:
                print(f"[WARN] Arista no publicada {e.get('src') or e.get('source')}->{e.get('dst') or e.get('target')}: {ex}")

# =========================================================
# API helpers (válidos para tu server)
# =========================================================
def _api_texts_batch(api_url: str, batch: List[Dict], api_key: Optional[str]) -> Tuple[bool, str]:
    """Envía un lote a /documents/texts probando ambos formatos (array plano y {"texts":[...]}).
       Devuelve (ok, mensaje). Sin excepciones hacia arriba.
    """
    if not requests:
        return False, "requests no disponible"

    url = api_url.rstrip("/") + "/documents/texts"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payloads = [batch, {"texts": batch}]
    errors: List[str] = []
    last_err = "desconocido"

    for payload in payloads:
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=600)
            if r.status_code < 300:
                return True, f"OK {url}"
            errors.append(f"{r.status_code}: {r.text[:300]}")
            continue
        except Exception as e:
            last_err = str(e)
            errors.append(f"excepción: {last_err}")

    joined = " | ".join(errors) if errors else f"último error: {last_err}"
    return False, f"{url} falló → {joined}"

def _api_text_single(api_url: str, item: Dict, api_key: Optional[str]) -> Tuple[bool, str]:
    """POST /documents/text para un solo item {text, metadata}."""
    if not requests:
        return False, "requests no disponible"
    url = api_url.rstrip("/") + "/documents/text"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        r = requests.post(url, headers=headers, data=json.dumps(item), timeout=120)
        if r.status_code < 300:
            return True, "OK /documents/text"
        return False, f"{r.status_code}: {r.text[:300]}"
    except Exception as e:
        return False, f"excepción: {e}"

def _api_upload_and_scan(api_url: str, file_path: Path, api_key: Optional[str]) -> Tuple[bool, str]:
    if not requests:
        return False, "requests no disponible"
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    # /documents/upload
    try:
        with file_path.open("rb") as fh:
            files = {"file": (file_path.name, fh)}
            r = requests.post(api_url.rstrip("/") + "/documents/upload", headers=headers, files=files, timeout=600)
        if r.status_code >= 300:
            return False, f"/documents/upload → {r.status_code}: {r.text[:300]}"
    except Exception as e:
        return False, f"/documents/upload excepción: {e}"
    # /documents/scan
    try:
        r2 = requests.post(api_url.rstrip("/") + "/documents/scan", headers=headers, timeout=120)
        if r2.status_code >= 300:
            return False, f"/documents/scan → {r2.status_code}: {r2.text[:300]}"
    except Exception as e:
        return False, f"/documents/scan excepción: {e}"
    return True, "OK upload+scan"

def _api_status_counts(api_url: str) -> Optional[Dict]:
    if not requests:
        return None
    try:
        r = requests.get(api_url.rstrip("/") + "/documents/status_counts", timeout=60)
        if r.status_code < 300:
            return r.json()
    except Exception:
        pass
    return None

def _api_query(api_url: str, question: str, mode: str = "mix", api_key: Optional[str] = None) -> Dict:
    if not requests:
        return {"error": "requests no disponible"}
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {"query": question, "mode": mode}
    for ep in ("/query", "/api/query"):
        try:
            r = requests.post(api_url.rstrip("/") + ep, headers=headers, data=json.dumps(payload), timeout=180)
            if r.status_code < 300:
                return r.json()
        except Exception:
            pass
    return {"error": "No se pudo consultar API /query"}

def _api_push_kg_records(api_url: str, records: List[Dict], api_key: Optional[str]) -> None:
    """Empuja entidades y relaciones por API granular. Idempotente-ish por nombre."""
    if not requests or not records:
        return
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    ents = []
    rels = []
    for rec in records:
        t = (rec.get("type") or rec.get("_type") or "").lower()
        if t in ("entity", "node"):
            name = rec.get("entity_name") or rec.get("name") or rec.get("label")
            data = rec.get("entity_data") or rec.get("data") or {}
            if name:
                ents.append({"entity_name": name, "entity_data": data})
        elif t in ("relation", "edge", "rel"):
            src = rec.get("source_entity") or rec.get("src") or rec.get("source") or rec.get("from")
            tgt = rec.get("target_entity") or rec.get("tgt") or rec.get("target") or rec.get("to")
            data = rec.get("relation_data") or rec.get("data") or {}
            if src and tgt:
                rels.append({"source_entity": src, "target_entity": tgt, "relation_data": data})

    # ENTIDADES
    it = tqdm(ents, desc="KG entidades") if tqdm else ents
    for e in it:
        try:
            r = requests.post(api_url.rstrip("/") + "/graph/entity/create", headers=headers, data=json.dumps(e), timeout=60)
            print(f"[kg] entity {e['entity_name'][:40]}… → {r.status_code}")
        except Exception as ex:
            print(f"[kg] entity error: {ex}")
        time.sleep(0.02)

    # RELACIONES
    it2 = tqdm(rels, desc="KG relaciones") if tqdm else rels
    for rj in it2:
        try:
            r = requests.post(api_url.rstrip("/") + "/graph/relation/create", headers=headers, data=json.dumps(rj), timeout=60)
            print(f"[kg] rel {rj['source_entity'][:20]}->{rj['target_entity'][:20]} → {r.status_code}")
        except Exception as ex:
            print(f"[kg] relation error: {ex}")
        time.sleep(0.02)

# =========================================================
# Corpus & pipelines
# =========================================================
def _augment_with_pc4_text(doc_id: str, page_texts: Dict[int, str],
                           pc4_sections: pd.DataFrame, pc4_tables: pd.DataFrame) -> Dict[int, str]:
    if pc4_sections is None:
        pc4_sections = pd.DataFrame()
    if pc4_tables is None:
        pc4_tables = pd.DataFrame()

    sec = pc4_sections[pc4_sections["doc_id"].astype(str) == str(doc_id)] if not pc4_sections.empty else pd.DataFrame()
    tab = pc4_tables[pc4_tables["doc_id"].astype(str) == str(doc_id)] if not pc4_tables.empty else pd.DataFrame()

    for c in ["page", "section_number", "section_title"]:
        if c not in sec.columns:
            sec[c] = ""
    for c in ["_page", "caption_near"]:
        if c not in tab.columns:
            tab[c] = ""

    headers_by_uid: Dict[str, List[str]] = {}
    if not tab.empty:
        if "table_uid" not in tab.columns:
            if "_table_idx" not in tab.columns:
                tab["_table_idx"] = 0
            tab["table_uid"] = tab.apply(
                lambda r: f"{doc_id}::p{int(r.get('_page',0)):03d}::t{int(r.get('_table_idx',0)):02d}", axis=1)

        value_cols = [c for c in tab.columns if c.startswith("c")]
        if value_cols:
            if "_row" in tab.columns:
                hdr = tab[tab["_row"] == 1][["table_uid"] + value_cols]
            else:
                hdr = tab.sort_values(by=["table_uid"]).groupby("table_uid").head(1)[["table_uid"] + value_cols]
            for _, r in hdr.iterrows():
                vals = []
                for c in value_cols:
                    v = str(r.get(c, "")).strip()
                    if v and v.lower() not in ("nan", "none", "null"):
                        vals.append(v)
                headers_by_uid[str(r["table_uid"])] = vals

    for pg in list(page_texts.keys()):
        pack = []
        near_sec = sec[sec["page"] == pg] if not sec.empty else pd.DataFrame()
        if not near_sec.empty:
            s_num = str(near_sec.iloc[0].get("section_number", "") or "")
            s_title = str(near_sec.iloc[0].get("section_title", "") or "")
            if s_num or s_title:
                pack.append(f"[NEAR_SECTION] {s_num} {s_title}".strip())

        if not tab.empty:
            tabs_pg = tab[tab["_page"] == pg]
            if not tabs_pg.empty:
                for _, tr in tabs_pg.drop_duplicates(subset=["table_uid"]).iterrows():
                    cap = str(tr.get("caption_near", "") or "")
                    tu = str(tr.get("table_uid", "") or "")
                    if cap:
                        pack.append(f"[TABLE_CAPTION] {cap}")
                    hdr_vals = headers_by_uid.get(tu, [])
                    if hdr_vals:
                        pack.append(f"[TABLE_HEADERS] " + " | ".join(hdr_vals))

        if pack:
            page_texts[pg] = (page_texts[pg] + "\n\n" + "\n".join(pack)).strip()
    return page_texts

def _concat_pages_to_corpus(doc_id: str, pages: List[Tuple[int, Path]],
                            pc4_sections: pd.DataFrame, pc4_tables: pd.DataFrame) -> Tuple[str, Dict[str, int]]:
    page_texts: Dict[int, str] = {}
    for pg, p in pages:
        page_texts[pg] = _read_txt(p)
    page_texts = _augment_with_pc4_text(doc_id, page_texts, pc4_sections, pc4_tables)

    chunks = []
    for pg in sorted(page_texts.keys()):
        t = page_texts[pg].strip()
        if t:
            chunks.append(f"[PAGE {pg}]\n{t}")
    full_text = ("\n\n" + "-"*80 + "\n\n").join(chunks)
    meta = {"doc_id": doc_id, "pages": len(page_texts), "chars": len(full_text)}
    return full_text, meta

def _export_corpus_jsonl_and_json(corpus: List[Dict], out_dir: Path) -> Tuple[Path, Path]:
    out_jsonl = out_dir / "corpus.jsonl"
    out_json = out_dir / "corpus.json"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for rec in corpus:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)
    return out_jsonl, out_json

def _load_pc5_graph(pc5_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict]]:
    nodes = _safe_read_csv(pc5_dir / "graph_nodes.csv")
    edges = _safe_read_csv(pc5_dir / "graph_edges.csv")
    jsonl_path = pc5_dir / "graph.jsonl"
    jsonl_recs: List[Dict] = []
    if jsonl_path.exists():
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    jsonl_recs.append(json.loads(line))
                except Exception:
                    pass
    return nodes, edges, jsonl_recs

def _export_graph_jsonl_and_json(records: List[Dict], out_dir: Path) -> Tuple[Path, Path]:
    out_jsonl = out_dir / "graph.jsonl"
    out_json = out_dir / "graph.json"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    return out_jsonl, out_json

# =========================================================
# Pipelines
# =========================================================
def pipeline_ingest(pc2_dir: Path, pc4_dir: Path, export_dir: Path,
                    storage_dir: Path, use_core: bool,
                    api_url: Optional[str], api_key: Optional[str],
                    batch_size: int = 800) -> Tuple[Path, Path]:
    _ensure_dirs(export_dir, storage_dir)

    sec = _safe_read_csv(pc4_dir / "master_sections.csv")
    tab = _safe_read_csv(pc4_dir / "master_tables.csv")

    doc_pages = _gather_pc2_docs(pc2_dir)
    if not doc_pages:
        print(f"[ingest] No encontré páginas en {pc2_dir}. ¿Ejecutaste PC-2?")
        return export_dir / "corpus.jsonl", export_dir / "corpus.json"

    corpus: List[Dict] = []
    for doc_id, pages in (tqdm(doc_pages.items(), desc="Preparando corpus") if tqdm else doc_pages.items()):
        full_text, meta = _concat_pages_to_corpus(doc_id, pages, sec, tab)
        if not full_text.strip():
            continue
        corpus.append({
            "text": full_text,
            "metadata": {"doc_id": doc_id, "pages": meta["pages"], "chars": meta["chars"]},
            "hash": _hash_bytes(full_text.encode("utf-8"))
        })

    out_jsonl, out_json = _export_corpus_jsonl_and_json(corpus, export_dir)
    print(f"[ingest] Export JSONL → {out_jsonl} (docs={len(corpus)})")
    print(f"[ingest] Export JSON  → {out_json}")

    pushed = False
    if api_url and requests:
        # 1) Intento preferente: /documents/texts (en lotes)
        print(f"[ingest] Subiendo {len(corpus)} docs vía API /documents/texts (batch={batch_size})…")
        for i in range(0, len(corpus), batch_size):
            batch_src = corpus[i:i+batch_size]
            batch = [{"text": r.get("text",""), "metadata": r.get("metadata",{})} for r in batch_src]
            ok, msg = _api_texts_batch(api_url, batch, api_key)
            print(f"[API] batch {i//batch_size+1} → {msg}")
            pushed = pushed or ok

        # 2) Si ningún lote entró, intentar por-doc con /documents/text
        if not pushed:
            print("[ingest] Intentando fallback individual: /documents/text …")
            for idx, rec in enumerate(corpus, 1):
                ok1, m1 = _api_text_single(api_url, {"text": rec["text"], "metadata": rec.get("metadata", {})}, api_key)
                if idx <= 5:
                    print(f"[API] doc {idx}/{len(corpus)} → {m1}")
                pushed = pushed or ok1

        # 3) Si todavía nada, respaldo: upload+scan con corpus.json
        if not pushed:
            print("[ingest] Intentando respaldo: /documents/upload + /documents/scan con corpus.json …")
            ok2, m2 = _api_upload_and_scan(api_url, out_json, api_key)
            print(f"[API] upload+scan → {m2}")
            pushed = pushed or ok2

        # 4) Mostrar status_counts si es posible
        sc = _api_status_counts(api_url)
        if sc:
            print(f"[ingest] status_counts: {json.dumps(sc, ensure_ascii=False)}")

    # Fallback Core local
    if (not pushed) and use_core and _LIGHTRAG_AVAILABLE:
        print("[ingest] Intentando ingesta con LightRAG Core…")
        try:
            asyncio.run(_core_ingest_documents(corpus, storage_dir))
            print("[ingest] ✅ Ingesta Core finalizada.")
        except Exception as e:
            print(f"[ingest] ⚠️ Core falló ({e}). Usa la WebUI del server con corpus.json.")

    if not api_url and not use_core:
        print("[ingest] (Modo export) Sube corpus.json en la WebUI (Documents → Upload) o usa /documents/texts.")

    return out_jsonl, out_json

def pipeline_pushkg(pc5_dir: Path, export_dir: Path, storage_dir: Path,
                    use_core: bool, api_url: Optional[str], api_key: Optional[str],
                    push_kg_api: bool = False) -> Tuple[Path, Path]:
    _ensure_dirs(export_dir, storage_dir)
    nodes, edges, jsonl = _load_pc5_graph(pc5_dir)

    if not jsonl:
        combined: List[Dict] = []
        if not nodes.empty:
            for _, r in nodes.iterrows():
                combined.append({"type": "node", **{k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}})
        if not edges.empty:
            for _, r in edges.iterrows():
                combined.append({"type": "edge", **{k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}})
        jsonl = combined

    out_jsonl, out_json = _export_graph_jsonl_and_json(jsonl, export_dir)
    print(f"[pushkg] Export JSONL → {out_jsonl} (registros={len(jsonl)})")
    print(f"[pushkg] Export JSON  → {out_json}")

    if push_kg_api and api_url and requests and jsonl:
        print("[pushkg] Empujando KG por API (entity/create + relation/create)…")
        _api_push_kg_records(api_url, jsonl, api_key)
    else:
        print("\n[pushkg] ℹ️ Importar por WebUI:")
        print("   - Knowledge Graph → Import → selecciona:")
        print(f"     {out_jsonl}")
        print("   - Luego verifica nodos/aristas.")

    if use_core and _LIGHTRAG_AVAILABLE and jsonl:
        print("[pushkg] Intentando publicar KG con LightRAG Core (best-effort)…")
        try:
            asyncio.run(_core_push_graph(jsonl, storage_dir))
            print("[pushkg] ✅ Custom KG publicado en Core.")
        except Exception as e:
            print(f"[pushkg] ⚠️ Core falló ({e}).")

    return out_jsonl, out_json

def pipeline_query(questions: List[str], api_url: Optional[str], mode: str,
                   out_dir: Path, api_key: Optional[str] = None):
    _ensure_dirs(out_dir)
    out_path = out_dir / "results.jsonl"
    if not questions:
        print("[query] No hay preguntas.")
        return out_path

    results = []
    if api_url:
        print(f"[query] Consultando vía API: {api_url} (mode={mode})")
        for q in (tqdm(questions, desc="Consultas") if tqdm else questions):
            resp = _api_query(api_url, q, mode=mode, api_key=api_key)
            results.append({"q": q, "resp": resp})
    else:
        print("[query] Sin API URL. Exporto preguntas para WebUI.")
        for q in questions:
            results.append({"q": q, "note": "Sin API; usar WebUI o indicar --api-url"})

    with out_path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[query] Output → {out_path} (n={len(results)})")
    return out_path

# =========================================================
# CLI
# =========================================================
def main():
    ap = argparse.ArgumentParser(description="PC-6 LightRAG — ingest/pushkg/query/all")
    ap.add_argument("--pc2-dir", default=str(PC2_DIR))
    ap.add_argument("--pc4-dir", default=str(PC4_DIR))
    ap.add_argument("--pc5-dir", default=str(PC5_DIR))
    ap.add_argument("--export-dir", default=str(PC6_EXPORT_DIR))
    ap.add_argument("--storage-dir", default=str(RAG_STORAGE_DIR))
    ap.add_argument("--use-core", action="store_true", help="Usa LightRAG Core si está disponible (best-effort)")
    ap.add_argument("--api-url", default=os.environ.get("LIGHTRAG_API_URL", ""), help="URL del LightRAG Server (ej. http://localhost:8777)")
    ap.add_argument("--api-key", default=os.environ.get("LIGHTRAG_API_KEY", ""), help="API Key (si aplica)")
    ap.add_argument("--mode", default="mix", help="Modo de consulta: naive|local|global|mix|hybrid")
    ap.add_argument("--batch-size", type=int, default=800, help="Tamaño de lote para /documents/texts")
    ap.add_argument("--push-kg-api", action="store_true", help="Empuja el KG por API (entity/create + relation/create)")

    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ingest", help="Ingesta corpus (PC-2/PC-4) → API")
    sub.add_parser("pushkg", help="Exporta KG (PC-5) a JSONL/JSON; opcional push por API")
    sub.add_parser("all", help="Ingest + PushKG")

    ap_q = sub.add_parser("query", help="Ejecuta preguntas de prueba")
    ap_q.add_argument("--questions", nargs="*", default=[
        "¿Cuál es el caudal nominal y el rango turndown de la bomba dosificadora especificada?",
        "Muéstrame la tabla de materiales (BOM) asociada a la sección de pruebas FAT.",
        "¿En qué página está el P&ID y qué referencia de línea o tag se menciona?",
        "Listado de parámetros extraídos como cabeceras de las tablas de HD.",
        "¿Qué ensayo NDT se pide y con qué norma de aceptación?",
        "Dame el setpoint y la clase del presostato, si aplica."
    ])

    args = ap.parse_args()

    pc2_dir = Path(args.pc2_dir)
    pc4_dir = Path(args.pc4_dir)
    pc5_dir = Path(args.pc5_dir)
    export_dir = Path(args.export_dir)
    storage_dir = Path(args.storage_dir)
    api_url = (args.api_url or "").strip() or None
    api_key = (args.api_key or "").strip() or None

    if args.cmd in ("ingest", "all"):
        pipeline_ingest(pc2_dir, pc4_dir, export_dir, storage_dir,
                        use_core=args.use_core, api_url=api_url, api_key=api_key,
                        batch_size=args.batch_size)

    if args.cmd in ("pushkg", "all"):
        pipeline_pushkg(pc5_dir, export_dir, storage_dir,
                        use_core=args.use_core, api_url=api_url, api_key=api_key,
                        push_kg_api=args.push_kg_api)

    if args.cmd == "query":
        pipeline_query(args.questions, api_url, args.mode, PC6_OUT_QUERIES, api_key=api_key)

if __name__ == "__main__":
    main()
