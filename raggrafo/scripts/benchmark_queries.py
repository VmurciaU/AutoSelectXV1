# benchmark_queries.py
import time, json, requests

API = "http://localhost:8777"
HEAD = {"Content-Type": "application/json"}

QUERIES = [
    "¿Cuál es el caudal nominal, presion de trabajo, viscosidad y el turndown de la o las bomba dosificadora?",
    "¿Qué normas aplican a FAT y cómo se vinculan con el paquete de inyección?",
    "Dame setpoint y clase del presostato asociados al equipo de dosificación.",
    "Conecta Bomba Dosificadora -> Prueba FAT -> API 675 (camino y justificación).",
]

MODES = ["naive", "local", "global", "mix"]  # usa los que soporte tu build

def ask(q, mode):
    t0 = time.time()
    r = requests.post(f"{API}/query", headers=HEAD,
                      data=json.dumps({"query": q, "mode": mode}), timeout=180)
    dt = time.time() - t0
    try:
        data = r.json()
    except Exception:
        data = {"response": r.text}
    return dt, data

print("\n== LightRAG Benchmark por modos ==\n")
for q in QUERIES:
    print(f"Q: {q}")
    for m in MODES:
        try:
            dt, data = ask(q, m)
            resp = (data.get("response") or "").strip().split("\n")[0]
            print(f"  [{m:6}] {dt:5.2f}s  →  {resp[:110]}{'…' if len(resp)>110 else ''}")
        except Exception as e:
            print(f"  [{m:6}] ERROR: {e}")
    print()
