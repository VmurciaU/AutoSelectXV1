import os
import pandas as pd

# Ruta donde están los archivos CSV
carpeta_csv = "/home/user/Desktop/Tesis2025/AutoSelectX/outputs/MRA11"

# Obtener la lista de archivos CSV, ordenada por nombre (asume numeración como 1_, 2_, etc.)
archivos_csv = sorted(
    [f for f in os.listdir(carpeta_csv) if f.endswith(".csv")],
    key=lambda x: int(x.split("_")[0])
)

# Leer e imprimir cada archivo
for archivo in archivos_csv:
    ruta_completa = os.path.join(carpeta_csv, archivo)
    print(f"\n📄 Tabla: {archivo}")
    print("=" * 80)
    try:
        df = pd.read_csv(ruta_completa)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ Error leyendo {archivo}: {e}")
