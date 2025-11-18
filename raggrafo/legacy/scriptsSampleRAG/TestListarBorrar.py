# scriptsSampleRAG/TestListar.py
import chromadb
import os

# ==========================
# Configuración
# ==========================
# Usa ruta absoluta para evitar confusiones con Jupyter
PERSIST_DIR = "/home/user/Desktop/Tesis2025/AutoSelectX/vectordb"

# ==========================
# Función principal
# ==========================
def listar_colecciones():
    if not os.path.exists(PERSIST_DIR):
        print(f"❌ No existe la carpeta {PERSIST_DIR}")
        return

    client = chromadb.PersistentClient(path=PERSIST_DIR)
    colecciones = client.list_collections()

    if not colecciones:
        print("⚠️ No hay colecciones en esta ruta.")
        return

    print(f"📂 Colecciones en {PERSIST_DIR}:\n")
    for c in colecciones:
        try:
            col = client.get_collection(c.name)
            count = col.count()
            print(f" - {c.name} → {count} documentos")
        except Exception as e:
            print(f" - {c.name} → error al contar ({e})")

# ==========================
# Ejecutar script
# ==========================
if __name__ == "__main__":
    listar_colecciones()
