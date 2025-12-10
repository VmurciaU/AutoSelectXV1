import pdfplumber
import os
import re
import pandas as pd

# Ruta del PDF y configuraciones
PDF_PATH = "/home/user/Desktop/Tesis2025/AutoSelectX/data/MiltonRoy_PriceList.pdf"
OUTPUT_DIR = "/home/user/Desktop/Tesis2025/AutoSelectX/outputs/MRA11"
PAGES = [32, 33, 34]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Palabras clave para segmentar subtablas
SUBTABLE_HEADERS = {
    "Liquid End": r"(?i)liquid end",
    "Plunger Diameter": r"(?i)plunger diameter",
    "Gear Ratio": r"(?i)gear ratio",
    "Standard Motor": r"(?i)standard motor",
    "Motor Mount": r"(?i)motor mount",
    "Pipe Connection": r"(?i)pipe connection",
    "O-ring Material": r"(?i)oring|o-ring material",
    "Capacity Control": r"(?i)capacity control",
    "Diaphragm Rupture": r"(?i)diaphragm rupture",
    "Base Option": r"(?i)base option",
    "Code Complete Identifier": r"(?i)code complete identifier",
    "Liquid End Extended Option": r"(?i)liquid end extended option",
    "Temperature": r"(?i)temperature",
    "Drive Extender": r"(?i)drive extender",
    "Motor Extender": r"(?i)motor extender",
    "Lubrication Option": r"(?i)lubrication option",
    "Coating System": r"(?i)coating system",
    "Component Test": r"(?i)component test",
    "Rust Test": r"(?i)rust test",
}

# Limpiar texto
clean_text = lambda s: re.sub(r'\s+', ' ', s.strip()) if s else ""

# Extraer texto y segmentar subtablas
subtables_data = {key: [] for key in SUBTABLE_HEADERS}

with pdfplumber.open(PDF_PATH) as pdf:
    for page_num in PAGES:
        page = pdf.pages[page_num - 1]
        tables = page.extract_tables()

        print(f"\n📘 Página {page_num} — Tablas encontradas: {len(tables)}")

        for table_idx, table in enumerate(tables, 1):
            print(f"  📄 Procesando Tabla {table_idx}...")
            current_subtable = None
            for row in table:
                row_text = " ".join(clean_text(cell) for cell in row if cell)
                for subtable_name, pattern in SUBTABLE_HEADERS.items():
                    if re.search(pattern, row_text):
                        current_subtable = subtable_name
                        print(f"    ✅ Subtabla detectada: '{subtable_name}'")
                        break

                if current_subtable:
                    subtables_data[current_subtable].append([clean_text(cell) for cell in row])

# Mostrar resumen por consola
for name, data in subtables_data.items():
    if data:
        print(f"\n📊 Subtabla: {name} — Filas: {len(data)}")
        for row in data[:5]:
            print("   ", row)

# Guardar como CSV (descomentar al aprobar resultados)
# for name, data in subtables_data.items():
#     if data:
#         df = pd.DataFrame(data)
#         output_path = os.path.join(OUTPUT_DIR, f"{name.replace(' ', '_').lower()}.csv")
#         df.to_csv(output_path, index=False)
#         print(f"💾 Guardado: {output_path}")
