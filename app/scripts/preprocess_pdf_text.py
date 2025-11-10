import json
import os
import spacy
from tqdm import tqdm

INPUT_PATH = "outputs/texto_limpio_sin_encabezados.json"
OUTPUT_PATH = "outputs/texto_preprocesado.json"

def cargar_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_json(data, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def preprocesar_texto(texto: str, nlp) -> list:
    doc = nlp(texto)
    tokens_limpios = [
        token.lemma_.lower() for token in doc
        if not token.is_stop and not token.is_punct and not token.like_num and token.is_alpha
    ]
    return tokens_limpios

if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Archivo no encontrado: {INPUT_PATH}")
        exit(1)

    print(f"📥 Cargando texto limpio desde: {INPUT_PATH}")
    data = cargar_json(INPUT_PATH)

    print("⚙️ Cargando modelo de spaCy...")
    nlp = spacy.load("es_core_news_sm")

    print("🧠 Procesando texto por página...")
    resultado = {}
    for num_pagina, contenido in tqdm(data.items(), desc="🔬 Preprocesando texto"):
        texto = contenido["texto_limpio"]
        tokens = preprocesar_texto(texto, nlp)
        resultado[num_pagina] = {
            "tokens": tokens,
            "num_tokens": len(tokens)
        }

    guardar_json(resultado, OUTPUT_PATH)
    print(f"\n✅ Preprocesamiento completo. Resultado guardado en: {OUTPUT_PATH}")
