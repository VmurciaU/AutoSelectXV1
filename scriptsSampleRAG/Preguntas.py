# scriptsSampleRAG/Inicio.py
import sys
import os

# Asegúrate de importar la función `respuesta` de tu rag_pago.py
# Ajusta la ruta si rag_pago.py no está en el mismo nivel
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.rag_pago import respuesta

# ==========================
# Preguntas organizadas en bloques
# ==========================
bloques = {
    "🧪 Bloque 1: Alcance del suministro": [
        "¿Cuál es el alcance del suministro del paquete de inyección de químicos STAP EC3?",
        "¿Qué responsabilidades específicas tiene el PROVEEDOR en este contrato?",
        "¿El alcance incluye montaje, pruebas y asistencia en campo?",
        "¿Cuáles son los servicios adicionales que debe garantizar el proveedor (ej. garantías, confiabilidad)?",
    ],
    "🧪 Bloque 2: Bombas dosificadoras (datos de diseño API 675)": [
        "¿Cuál es el caudal en GPH de la bomba dosificadora especificada?",
        "¿Qué presión de descarga en psig se indica para la bomba principal?",
        "¿Cuántos caballos de potencia (HP) debe tener el motor eléctrico de la bomba?",
        "¿Qué materiales se especifican para el tanque y partes en contacto con el fluido?",
        "¿Se menciona que la bomba cumple con API 675?",
    ],
    "🧪 Bloque 3: Redundancia y configuración": [
        "¿Cuántas bombas operativas y cuántas de respaldo se requieren en el paquete?",
        "¿Las bombas de respaldo son idénticas a las operativas o se diferencian en caudal/presión?",
        "¿Qué configuración de cámaras de preparación y dosificación se menciona en el paquete?",
    ],
    "🧪 Bloque 4: Instrumentación y control": [
        "¿Qué instrumentos de presión o caudal se requieren en la descarga de la bomba?",
        "¿Se especifica algún sistema de control local o PLC en el paquete?",
        "¿Se menciona comunicación con el sistema de control del STAP EC3 (ej. Modbus TCP/IP)?",
    ],
    "🧪 Bloque 5: Tablas y TAGs": [
        "¿Cuáles son los TAGs de los sistemas de dosificación y sus cantidades?",
        "¿Qué información tabular de catálogos de ECOPETROL se incluye en el documento?",
        "¿Puedes listar los ítems de repuestos recomendados por el fabricante?",
    ],
    "🧪 Bloque 6: Pruebas cruzadas": [
        "¿Hay diferencias entre el caudal de la bomba indicado en texto y el indicado en tablas?",
        "¿Cuáles páginas mencionan explícitamente la bomba dosificadora y sus características técnicas?",
    ],
}

# ==========================
# Ejecución de las pruebas
# ==========================
def main():
    for bloque, preguntas in bloques.items():
        print("\n" + "=" * 80)
        print(bloque)
        print("=" * 80)
        for q in preguntas:
            print(f"\n❓ {q}")
            try:
                a = respuesta(q)
                print(f"👉 {a}")
            except Exception as e:
                print(f"⚠️ Error procesando la pregunta: {e}")

if __name__ == "__main__":
    main()
