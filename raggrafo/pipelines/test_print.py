import json
from pathlib import Path
from .printPumpsJson import build_all_messages

# Carpeta base = raggrafo/
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_PATH = BASE_DIR / "rag_storage" / "case_2" / "raw.json"
NORM_PATH = BASE_DIR / "rag_storage" / "case_2" / "normalized.json"

# Cargar los JSON
with open(RAW_PATH, "r", encoding="utf-8") as f:
    raw_json = json.load(f)

with open(NORM_PATH, "r", encoding="utf-8") as f:
    norm_json = json.load(f)

# Ejecutar las funciones
messages = build_all_messages(raw_json, norm_json)

#print("\n========== MENSAJE 1 (SUMMARY) ==========\n")
#print(messages["summary"])

#print("\n========== MENSAJE 2 (RAW HTML) ==========\n")
#print(messages["raw"])

print("\n========== MENSAJE 3 (NORMALIZED HTML) ==========\n")
print(messages["normalized"])
