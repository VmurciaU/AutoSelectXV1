#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
upload_graph.py — Subir grafo de PC-6 a LightRAG Server
-------------------------------------------------------
Lee RAGGrafo/outputs/pc6_export/graph.jsonl (nodos y aristas) y los publica
en un servidor LightRAG vía:

  POST /graph/entity/create
  POST /graph/relation/create

Convención:
  - Usa el campo "id" del nodo como entity_name (estable y coincide con src/dst).
  - Para aristas, usa "src" y "dst" tal como vienen en graph.jsonl.

Config:
  - API_URL:  URL del server (default: http://localhost:8777)
  - API_KEY:  API key si aplica (opcional)

Uso:
  python upload_graph.py
"""

import json
import os
import time
from pathlib import Path

import requests

# =========================
# Config
# =========================
ROOT = Path(__file__).resolve().parent
GRAPH_PATH = ROOT / "outputs" / "pc6_export" / "graph.jsonl"

API_URL = os.environ.get("LIGHTRAG_API_URL", "http://localhost:8777").rstrip("/")
API_KEY = os.environ.get("LIGHTRAG_API_KEY", "").strip() or None

HEAD = {"Content-Type": "application/json"}
if API_KEY:
    HEAD["Authorization"] = f"Bearer {API_KEY}"


# =========================
# Helpers
# =========================
def _load_graph_lines(path: Path):
    if not path.exists():
        raise SystemExit(f"[upload_graph] No se encontró graph.jsonl en: {path}")
    lines = []
    with path.open("r", encoding="utf-8") as f:
        for l in f:
            l = l.strip()
            if not l:
                continue
            try:
                lines.append(json.loads(l))
            except Exception as e:
                print(f"[WARN] Línea inválida en {path}: {e}")
    return lines


def _post_json(endpoint: str, payload: dict) -> int:
    url = API_URL + endpoint
    try:
        r = requests.post(url, headers=HEAD, json=payload, timeout=60)
        return r.status_code
    except Exception as e:
        print(f"[ERR] POST {endpoint} falló: {e}")
        return 0


# =========================
# Main
# =========================
def main():
    print(f"[upload_graph] API_URL = {API_URL}")
    if API_KEY:
        print("[upload_graph] Usando API_KEY (oculta).")

    lines = _load_graph_lines(GRAPH_PATH)
    if not lines:
        print("[upload_graph] graph.jsonl está vacío, nada que subir.")
        return

    nodes = [o for o in lines if (o.get("type") or "").lower() in ("node", "entity")]
    edges = [o for o in lines if (o.get("type") or "").lower() in ("edge", "relation", "rel")]

    print(f"[upload_graph] Nodos: {len(nodes)}  |  Aristas: {len(edges)}")
    print("[upload_graph] Subiendo entidades...")

    # --- ENTIDADES ---
    ok_nodes = 0
    for idx, obj in enumerate(nodes, 1):
        # Usamos el id del nodo como nombre estable
        entity_name = obj.get("id") or obj.get("name")
        if not entity_name:
            # Si no hay id ni name, la saltamos
            continue

        payload = {
            "entity_name": entity_name,
            "entity_data": {
                # Puedes guardar label y otros campos útiles
                "label": obj.get("label", ""),
                "doc_id": obj.get("doc_id"),
                "page": obj.get("page"),
                "section_number": obj.get("section_number"),
                "section_title": obj.get("section_title"),
                "table_uid": obj.get("table_uid"),
                "entity_type": obj.get("label", "Concept"),
            },
        }

        status = _post_json("/graph/entity/create", payload)
        if status and status < 300:
            ok_nodes += 1
        if idx % 50 == 0:
            print(f"  - Entidades procesadas: {idx}/{len(nodes)} (OK={ok_nodes})")
        time.sleep(0.02)

    print(f"[upload_graph] Entidades OK: {ok_nodes}/{len(nodes)}")

    # --- RELACIONES ---
    print("[upload_graph] Subiendo relaciones...")

    ok_edges = 0
    for idx, obj in enumerate(edges, 1):
        src = obj.get("src") or obj.get("source_entity") or obj.get("from")
        dst = obj.get("dst") or obj.get("target_entity") or obj.get("to")
        if not src or not dst:
            continue

        payload = {
            "source_entity": src,
            "target_entity": dst,
            "relation_data": {
                "type": obj.get("type") or obj.get("relation_type") or "edge",
                "edge_type": obj.get("type"),
                "relation": obj.get("type"),
                "description": obj.get("description", ""),
                "weight": obj.get("weight", 1.0),
            },
        }

        status = _post_json("/graph/relation/create", payload)
        if status and status < 300:
            ok_edges += 1
        if idx % 50 == 0:
            print(f"  - Relaciones procesadas: {idx}/{len(edges)} (OK={ok_edges})")
        time.sleep(0.02)

    print(f"[upload_graph] Relaciones OK: {ok_edges}/{len(edges)}")
    print("✅ Graph upload completed.")


if __name__ == "__main__":
    main()
