import os
import json
import spacy
import pdfplumber
from tqdm import tqdm
from collections import Counter

from scripts.progress_tracker import set_progress, reset_progress

# ---------- CONFIGURACIÓN RUTAS ----------
def construir_rutas(user_id, pdf_filename):
    base_name = os.path.splitext(pdf_filename)[0]
    base_dir = os.path.join("outputs", "session_files", str(user_id), base_name)
    os.makedirs(base_dir, exist_ok=True)
    return {
        "pdf_path": os.path.join("outputs", "session_files", str(user_id), pdf_filename),
        "extraido_json": os.path.join(base_dir, "texto_extraido_por_pagina.json"),
        "limpio_json": os.path.join(base_dir, "texto_limpio_sin_encabezados.json"),
        "preprocesado_json": os.path.join(base_dir, "texto_preprocesado.json")
    }

# ---------- MÓDULO 1: Extracción ----------
def extraer_texto_por_pagina(pdf_path, user_id=None, total_pasos=None, paso_offset=0):
    resultados = {}
    with pdfplumber.open(pdf_path) as pdf:
        total_paginas = len(pdf.pages)
        for i, page in enumerate(tqdm(pdf.pages, desc="📄 Extrayendo texto", unit="página"), start=1):
            texto = page.extract_text()
            resultados[i] = {
                "texto": texto.strip() if texto else "",
                "longitud": len(texto.strip()) if texto else 0
            }

            if user_id and total_pasos:
                progreso = (paso_offset + (i / total_paginas)) / total_pasos * 100
                set_progress(user_id, round(progreso))
    return resultados

# ---------- MÓDULO 2: Limpieza ----------
def limpiar_texto(data, lineas_repetidas, user_id=None, total_pasos=None, paso_offset=0):
    limpio = {}
    total_paginas = len(data)
    for j, (num_pagina, contenido) in enumerate(tqdm(data.items(), desc="🧹 Limpiando encabezados/pies")):
        nuevas_lineas = [
            l for l in contenido["texto"].splitlines() if l.strip() not in lineas_repetidas
        ]
        texto_limpio = "\n".join(nuevas_lineas)
        limpio[num_pagina] = {
            "texto_limpio": texto_limpio,
            "longitud": len(texto_limpio)
        }

        if user_id and total_pasos:
            progreso = (paso_offset + ((j + 1) / total_paginas)) / total_pasos * 100
            set_progress(user_id, round(progreso))
    return limpio

# ---------- MÓDULO 3: Preprocesamiento NLP ----------
def procesar_nlp(data, nlp, user_id=None, total_pasos=None, paso_offset=0):
    resultado = {}
    total_paginas = len(data)
    for k, (num_pagina, contenido) in enumerate(tqdm(data.items(), desc="🔬 Tokenizando texto limpio")):
        tokens = preprocesar_texto(contenido["texto_limpio"], nlp)
        resultado[num_pagina] = {
            "tokens": tokens,
            "num_tokens": len(tokens)
        }

        if user_id and total_pasos:
            progreso = (paso_offset + ((k + 1) / total_paginas)) / total_pasos * 100
            set_progress(user_id, round(progreso))
    return resultado

def preprocesar_texto(texto, nlp):
    doc = nlp(texto)
    return [
        token.lemma_.lower() for token in doc
        if not token.is_stop and not token.is_punct and not token.like_num and token.is_alpha
    ]

def detectar_lineas_repetidas(data, umbral=10):
    contador = Counter()
    for pagina in data.values():
        for linea in set(pagina["texto"].splitlines()):
            if linea.strip():
                contador[linea.strip()] += 1
    return {linea for linea, rep in contador.items() if rep >= umbral}

# ---------- UTILIDAD ----------
def guardar_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------- PROCESAMIENTO PRINCIPAL ----------
def procesar_pdfs_por_usuario(user_id: int):
    session_dir = os.path.join("outputs", "session_files", str(user_id))

    if not os.path.exists(session_dir):
        print(f"❌ No existe carpeta: {session_dir}")
        return

    pdf_files = [f for f in os.listdir(session_dir) if f.lower().endswith(".pdf")]
    if not pdf_files:
        print(f"⚠️ No hay PDFs en carpeta de usuario {user_id}")
        return

    total_pdfs = len(pdf_files)
    total_pasos = total_pdfs * 3
    print(f"🔎 Archivos PDF encontrados para usuario {user_id}: {total_pdfs}")
    print("⚙️ Cargando modelo spaCy...")
    nlp = spacy.load("es_core_news_sm")
    reset_progress(user_id)

    for index, pdf_filename in enumerate(pdf_files):
        rutas = construir_rutas(user_id, pdf_filename)
        print(f"\n📂 Procesando archivo: {pdf_filename}")

        paso_offset_extract = index * 3
        paso_offset_clean = paso_offset_extract + 1
        paso_offset_nlp = paso_offset_extract + 2

        # Paso 1: Extracción
        paginas = extraer_texto_por_pagina(
            rutas["pdf_path"],
            user_id=user_id,
            total_pasos=total_pasos,
            paso_offset=paso_offset_extract
        )
        guardar_json(paginas, rutas["extraido_json"])

        # Paso 2: Limpieza
        repetidas = detectar_lineas_repetidas(paginas)
        limpio = limpiar_texto(
            paginas,
            repetidas,
            user_id=user_id,
            total_pasos=total_pasos,
            paso_offset=paso_offset_clean
        )
        guardar_json(limpio, rutas["limpio_json"])

            # Paso 3: Preprocesamiento NLP
        preprocesado = procesar_nlp(
            limpio,
            nlp,
            user_id=user_id,
            total_pasos=total_pasos,
            paso_offset=paso_offset_nlp
        )
        guardar_json(preprocesado, rutas["preprocesado_json"])

# ---------- MODO LOCAL ----------
if __name__ == "__main__":
    user_id = 3
    procesar_pdfs_por_usuario(user_id)
