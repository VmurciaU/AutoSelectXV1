import os
import json

# Ruta al archivo de progreso (por usuario)
def _ruta_progress_file(user_id):
    return os.path.join("outputs", "session_files", str(user_id), "progress.json")

# Establece el progreso actual en % (0-100)
def set_progress(user_id, progress_value):
    ruta = _ruta_progress_file(user_id)
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    try:
        print(f"📝 [set_progress] Escribiendo progreso {progress_value}% en {ruta}")
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump({"progress": progress_value}, f)
            f.flush()
            os.fsync(f.fileno())  # 🔧 fuerza la escritura a disco
        print(f"✅ [set_progress] Usuario {user_id} -> {progress_value}%")
    except Exception as e:
        print(f"❌ [set_progress] Error al escribir progreso para usuario {user_id}: {e}")

# Obtiene el progreso actual desde archivo
def get_progress(user_id):
    ruta = _ruta_progress_file(user_id)
    print(f"🔍 [get_progress] Leyendo progreso desde archivo: {ruta}")
    
    if not os.path.exists(ruta):
        print(f"⚠️ [get_progress] progress.json no existe para usuario {user_id}, devolviendo 0%")
        return 0

    try:
        with open(ruta, "r", encoding="utf-8") as f:
            data = json.load(f)
            progreso = data.get("progress", 0)
            print(f"📊 [get_progress] Usuario {user_id} -> {progreso}% leído desde archivo")
            return progreso
    except Exception as e:
        print(f"❌ [get_progress] Error al leer progreso para usuario {user_id}: {e}")
        return 0

# Reinicia el progreso a 0%
def reset_progress(user_id):
    print(f"🔄 [reset_progress] Reiniciando progreso para usuario {user_id}")
    set_progress(user_id, 0)

# Prueba local manual
if __name__ == "__main__":
    user_id = 3
    set_progress(user_id, 33)
    progreso_actual = get_progress(user_id)
    print(f"🧪 Progreso actual leído desde archivo: {progreso_actual}%")
