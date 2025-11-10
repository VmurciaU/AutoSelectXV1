import pdfplumber
import os
import json
from tqdm import tqdm  # ✅ NUEVO: Barra de progreso

# Ruta al archivo PDF
PDF_DIR = "outputs/session_files/3"
PDF_FILENAME = "CAS09991MERMR000003_InyeccionQxSTAPEC3_4_250429_200355.pdf"
PDF_PATH = os.path.join(PDF_DIR, PDF_FILENAME)

def extraer_texto_por_pagina(pdf_path: str):
    resultados = {}
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(tqdm(pdf.pages, desc="📄 Extrayendo páginas", unit="página"), start=1):
            texto = page.extract_text()
            resultados[i] = {
                "texto": texto.strip() if texto else "",
                "longitud": len(texto.strip()) if texto else 0
            }
    return resultados

def guardar_como_json(data, output_path="outputs/texto_extraido_por_pagina.json"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    print(f"📄 Procesando: {PDF_FILENAME}")

    # Verificación de existencia
    if not os.path.exists(PDF_PATH):
        print(f"❌ Archivo no encontrado en: {PDF_PATH}")
        exit(1)

    paginas = extraer_texto_por_pagina(PDF_PATH)
    print(f"\n✅ Total páginas extraídas: {len(paginas)}")

    guardar_como_json(paginas)
    print(f"📝 Texto exportado a: outputs/texto_extraido_por_pagina.json")
