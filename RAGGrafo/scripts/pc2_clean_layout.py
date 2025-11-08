#!/usr/bin/env python3
"""
PC-2 — Limpieza de layout (headers/footers repetidos)
----------------------------------------------------
Lee los resultados de PC-1 en ./outputs/pc1_raw_pages/<doc_id>/page_*.txt,
detecta encabezados y pies repetidos por documento y genera versiones limpias en:
./outputs/pc2_clean_pages/<doc_id>/page_*.txt

Además, escribe un manifest de limpieza con los patrones removidos.
Heurística:
- Considera las primeras y últimas N líneas de cada página.
- Cuenta frecuencia de líneas (normalizadas) en todo el documento.
- Si una línea aparece en >= THRESHOLD% de páginas, se considera header/footer.
- Remueve EXACT matches (después de normalizar espacios).
"""
import argparse
import json
import re
from pathlib import Path
from collections import Counter, defaultdict
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).resolve().parent.name == "scripts") else Path(__file__).resolve().parent
PC1_DIR = ROOT / "outputs" / "pc1_raw_pages"
OUT_DIR = ROOT / "outputs" / "pc2_clean_pages"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_LINES = 6          # cuántas líneas de la parte superior e inferior consideramos candidatas
FREQ_THRESHOLD = 0.6   # 60% de las páginas
MINLEN = 8             # longitud mínima de la línea candidata
STRIP_CHARS = " \t\u200b\u200e\u200f"  # espacios y caracteres invisibles

def normalize_line(line: str) -> str:
    # compactar espacios, quitar invisibles
    s = line.replace("\xa0", " ").strip(STRIP_CHARS)
    s = re.sub(r"\s+", " ", s)
    return s

def detect_repeated_lines(pages: List[List[str]]) -> Dict[str, List[str]]:
    """
    Devuelve candidatos para header/footer por documento.
    """
    n_pages = len(pages)
    top_counter = Counter()
    bot_counter = Counter()

    for lines in pages:
        if not lines: 
            continue
        tops = lines[:TOP_LINES]
        bots = lines[-TOP_LINES:] if len(lines) >= TOP_LINES else lines[-len(lines):]

        top_counter.update([normalize_line(l) for l in tops if len(normalize_line(l)) >= MINLEN])
        bot_counter.update([normalize_line(l) for l in bots if len(normalize_line(l)) >= MINLEN])

    top_common = [ln for ln, c in top_counter.items() if c >= FREQ_THRESHOLD * n_pages]
    bot_common = [ln for ln, c in bot_counter.items() if c >= FREQ_THRESHOLD * n_pages]

    # filtra cosas demasiado genéricas
    def plausible(line: str) -> bool:
        if line.isupper() and len(line.split()) <= 2:
            return False
        if re.fullmatch(r"page \d+|página \d+|pag \d+", line, flags=re.I):
            return True
        # evita líneas que son demasiado genéricas
        return True

    top_common = [ln for ln in top_common if plausible(ln)]
    bot_common = [ln for ln in bot_common if plausible(ln)]

    return {"header": top_common, "footer": bot_common}

def apply_cleanup(lines: List[str], patterns: Dict[str, List[str]]) -> List[str]:
    header = set(patterns.get("header", []))
    footer = set(patterns.get("footer", []))

    new = []
    for idx, raw in enumerate(lines):
        norm = normalize_line(raw)
        if idx < TOP_LINES and norm in header:
            continue
        if idx >= len(lines) - TOP_LINES and norm in footer:
            continue
        new.append(raw)
    return new

def process_document(doc_dir: Path) -> Dict[str, Any]:
    manifest_path = doc_dir / "manifest.json"
    if not manifest_path.exists():
        return {"doc_dir": str(doc_dir), "skipped": True, "reason": "manifest.json not found"}

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pages_meta = manifest.get("pages", [])
    pages_texts: List[List[str]] = []

    # carga texto por página
    for page_info in pages_meta:
        txt_file = ROOT / page_info["txt_file"]
        if not txt_file.exists():
            pages_texts.append([])
            continue
        content = txt_file.read_text(encoding="utf-8", errors="ignore")
        lines = content.splitlines()
        pages_texts.append(lines)

    # detecta patrones
    patterns = detect_repeated_lines(pages_texts)

    # salida
    out_doc_dir = OUT_DIR / doc_dir.name
    out_doc_dir.mkdir(parents=True, exist_ok=True)

    clean_index = []
    for page_info, lines in zip(pages_meta, pages_texts):
        page_num = page_info["page"]
        cleaned_lines = apply_cleanup(lines, patterns)
        out_txt = out_doc_dir / f"{doc_dir.name}_page_{page_num:03d}.txt"
        out_txt.write_text("\n".join(cleaned_lines), encoding="utf-8")

        clean_index.append({
            "page": page_num,
            "clean_txt_file": str(out_txt.relative_to(ROOT)),
            "orig_txt_file": page_info["txt_file"],
            "n_lines_before": len(lines),
            "n_lines_after": len(cleaned_lines),
        })

    # guarda manifest de limpieza
    clean_manifest = {
        "doc_id": manifest.get("doc_id"),
        "file_name": manifest.get("file_name"),
        "doc_type": manifest.get("doc_type"),
        "rev": manifest.get("rev"),
        "n_pages": manifest.get("n_pages"),
        "detected_patterns": patterns,
        "pages": clean_index,
    }
    (out_doc_dir / "clean_manifest.json").write_text(json.dumps(clean_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return clean_manifest

def main():
    parser = argparse.ArgumentParser(description="PC-2 — Limpieza de layout (headers/footers)")
    parser.add_argument("--doc", type=str, default=None, help="Procesa solo un doc_id (carpeta dentro de pc1_raw_pages)")
    args = parser.parse_args()

    targets = []
    if args.doc:
        targets = [PC1_DIR / args.doc]
    else:
        targets = [p for p in PC1_DIR.iterdir() if p.is_dir() and (p / "manifest.json").exists()]

    results = []
    for doc_dir in targets:
        print(f"[PC-2] Limpiando: {doc_dir.name}")
        res = process_document(doc_dir)
        results.append(res)

    # índice global
    (OUT_DIR / "index.json").write_text(json.dumps({"documents": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n✅ PC-2 completado.")
    print(f"• Archivos limpios en: {OUT_DIR}\\<doc_id>\\page_XXX.txt")
    print(f"• Patrones detectados por documento en: {OUT_DIR}\\<doc_id>\\clean_manifest.json")

if __name__ == "__main__":
    main()
