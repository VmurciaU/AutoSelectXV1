import json
import os
from collections import Counter
from tqdm import tqdm

INPUT_PATH = "outputs/texto_extraido_por_pagina.json"
OUTPUT_PATH = "outputs/texto_limpio_sin_encabezados.json"
REPETICION_MINIMA = 10  # 🔧 Se puede ajustar según el documento

def cargar_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_json(data, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def detectar_lineas_repetidas(data: dict, umbral: int = REPETICION_MINIMA):
    contador = Counter()
    for pagina in data.values():
        lineas = pagina["texto"].splitlines()
        for linea in set(lineas):  # Usamos set para contar solo una vez por página
            if linea.strip():
                contador[linea.strip()] += 1
    # Filtrar líneas que aparecen al menos en X páginas
    lineas_repetidas = {linea for linea, rep in contador.items() if rep >= umbral}
    print(f"🔍 Encabezados/pies detectados (≥{umbral} páginas): {len(lineas_repetidas)}")
    for l in sorted(lineas_repetidas):
        print(f" - {l}")
    return lineas_repetidas

def limpiar_texto(data: dict, lineas_repetidas: set):
    data_limpia = {}
    for num_pagina, contenido in tqdm(data.items(), desc="🧹 Limpiando encabezados/pies"):
        texto = contenido["texto"]
        lineas = texto.splitlines()
        nuevas_lineas = [l for l in lineas if l.strip() not in lineas_repetidas]
        texto_limpio = "\n".join(nuevas_lineas)
        data_limpia[num_pagina] = {
            "texto_limpio": texto_limpio,
            "longitud": len(texto_limpio)
        }
    return data_limpia

if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Archivo no encontrado: {INPUT_PATH}")
        exit(1)

    print(f"📥 Cargando texto desde: {INPUT_PATH}")
    data = cargar_json(INPUT_PATH)

    lineas_comunes = detectar_lineas_repetidas(data)

    print(f"\n✂️ Eliminando encabezados/pies...")
    limpio = limpiar_texto(data, lineas_comunes)

    guardar_json(limpio, OUTPUT_PATH)
    print(f"\n✅ Archivo limpio exportado a: {OUTPUT_PATH}")
