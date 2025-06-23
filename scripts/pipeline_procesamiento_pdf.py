import os
import json
import spacy
import pdfplumber
from tqdm import tqdm
from collections import Counter

# ------------------ CONFIGURACIÓN GENERAL ------------------

def construir_rutas(session_id, pdf_filename):
    base_name = os.path.splitext(pdf_filename)[0]
    base_dir = os.path.join("outputs", "session_files", str(session_id), base_name)
    os.makedirs(base_dir, exist_ok=True)
    return {
        "pdf_path": os.path.join("outputs", "session_files", str(session_id), pdf_filename),
        "extraido_json": os.path.join(base_dir, "texto_extraido_por_pagina.json"),
        "limpio_json": os.path.join(base_dir, "texto_limpio_sin_encabezados.json"),
        "preprocesado_json": os.path.join(base_dir, "texto_preprocesado.json")
    }

# ------------------ MÓDULO 1: Extracción PDF ------------------

def extraer_texto_por_pagina(pdf_path):
    resultados = {}
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(tqdm(pdf.pages, desc="📄 Extrayendo texto", unit="página"), start=1):
            texto = page.extract_text()
            resultados[i] = {
                "texto": texto.strip() if texto else "",
                "longitud": len(texto.strip()) if texto else 0
            }
    return resultados

# ------------------ MÓDULO 2: Limpieza de encabezados/pies ------------------

def detectar_lineas_repetidas(data, umbral=10):
    contador = Counter()
    for pagina in data.values():
        for linea in set(pagina["texto"].splitlines()):
            if linea.strip():
                contador[linea.strip()] += 1
    return {linea for linea, rep in contador.items() if rep >= umbral}

def limpiar_texto(data, lineas_repetidas):
    limpio = {}
    for num_pagina, contenido in tqdm(data.items(), desc="🧹 Limpiando encabezados/pies"):
        nuevas_lineas = [
            l for l in contenido["texto"].splitlines() if l.strip() not in lineas_repetidas
        ]
        texto_limpio = "\n".join(nuevas_lineas)
        limpio[num_pagina] = {
            "texto_limpio": texto_limpio,
            "longitud": len(texto_limpio)
        }
    return limpio

# ------------------ MÓDULO 3: Preprocesamiento NLP ------------------

def preprocesar_texto(texto, nlp):
    doc = nlp(texto)
    return [
        token.lemma_.lower() for token in doc
        if not token.is_stop and not token.is_punct and not token.like_num and token.is_alpha
    ]

def procesar_nlp(data, nlp):
    resultado = {}
    for num_pagina, contenido in tqdm(data.items(), desc="🔬 Tokenizando texto limpio"):
        tokens = preprocesar_texto(contenido["texto_limpio"], nlp)
        resultado[num_pagina] = {
            "tokens": tokens,
            "num_tokens": len(tokens)
        }
    return resultado

# ------------------ UTILIDADES ------------------

def guardar_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ------------------ PARA USO EN FASTAPI ------------------

def procesar_pdfs_por_usuario(session_id: int):
    session_dir = os.path.join("outputs", "session_files", str(session_id))

    if not os.path.exists(session_dir):
        print(f"❌ No existe la carpeta de sesión: {session_dir}")
        return

    pdf_files = [f for f in os.listdir(session_dir) if f.lower().endswith(".pdf")]
    if not pdf_files:
        print(f"⚠️ No se encontraron archivos PDF en la sesión {session_id}")
        return

    print(f"🔎 Archivos PDF encontrados en sesión {session_id}: {len(pdf_files)}")

    print("⚙️ Cargando modelo spaCy...")
    nlp = spacy.load("es_core_news_sm")

    for pdf_filename in pdf_files:
        rutas = construir_rutas(session_id, pdf_filename)
        print(f"\n📂 Procesando: {pdf_filename}")

        # Paso 1: Extraer texto
        paginas = extraer_texto_por_pagina(rutas["pdf_path"])
        guardar_json(paginas, rutas["extraido_json"])
        print(f"✅ Texto extraído guardado en: {rutas['extraido_json']}")

        # Paso 2: Limpiar encabezados/pies
        lineas_repetidas = detectar_lineas_repetidas(paginas)
        limpio = limpiar_texto(paginas, lineas_repetidas)
        guardar_json(limpio, rutas["limpio_json"])
        print(f"✅ Texto limpio guardado en: {rutas['limpio_json']}")

        # Paso 3: NLP
        preprocesado = procesar_nlp(limpio, nlp)
        guardar_json(preprocesado, rutas["preprocesado_json"])
        print(f"✅ Preprocesamiento guardado en: {rutas['preprocesado_json']}")

# ------------------ USO LOCAL OPCIONAL ------------------

if __name__ == "__main__":
    session_id = 3
    procesar_pdfs_por_usuario(session_id)
