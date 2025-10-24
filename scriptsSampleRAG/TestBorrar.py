import chromadb
import os

# ==========================
# Configuración
# ==========================
PERSIST_DIR = "/home/user/Desktop/Tesis2025/AutoSelectX/scriptsSampleRAG/vectordb"

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
        col = client.get_collection(c.name)
        count = col.count()
        print(f" - {c.name} → {count} documentos")

        # Si hay documentos, mostramos el primer metadata
        if count > 0:
            try:
                sample = col.get(limit=1)
                print("   Ejemplo de metadatos:", sample["metadatas"][0])
            except Exception as e:
                print("   ⚠️ No se pudo leer ejemplo:", e)

# ==========================
# Ejecutar script
# ==========================
if __name__ == "__main__":
    listar_colecciones()
