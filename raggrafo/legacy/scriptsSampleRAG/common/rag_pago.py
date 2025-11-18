# -----------------------------------------------------------------------------
# RAG afinado para paquetes de inyección de químicos (STAP EC3)
# -----------------------------------------------------------------------------

import os
import json
import re
from typing import List, Dict, Any

from dotenv import load_dotenv
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain.chains.combine_documents import create_stuff_documents_chain
import chromadb

# ==========================
# Configuración API
# ==========================
from dotenv import load_dotenv
import os

# Cargar variables del archivo .env
load_dotenv()

# Leer la API key del entorno
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

# Validar que exista
if not OPENAI_KEY:
    raise ValueError("❌ Falta configurar la variable OPENAI_API_KEY en el archivo .env")

# Establecerla en el entorno para librerías que la requieran
os.environ["OPENAI_API_KEY"] = OPENAI_KEY


# ==========================
# Parámetros
# ==========================
# Ruta absoluta correcta a la carpeta vectordb (donde están tus 4904 docs)
PERSIST_DIR = "/home/user/Desktop/Tesis2025/AutoSelectX/scriptsSampleRAG/vectordb"
DEFAULT_COLLECTION = "autoselx_docs"
MODEL_NAME = "gpt-4o-mini"
DEBUG = True

# ==========================
# Carga de VDB (con autodescubrimiento de colección)
# ==========================
def _pick_collection(path: str, desired: str) -> str:
    client = chromadb.PersistentClient(path=path)
    cols = client.list_collections()
    names = [c.name for c in cols]
    if DEBUG:
        print(f"📁 VDB path: {path}")
        print(f"📚 Colecciones disponibles: {names or '—'}")

    def count_docs(name: str) -> int:
        try:
            coll = client.get_collection(name)
            meta = coll.get(include=["metadatas"])
            return len(meta.get("metadatas", []) or [])
        except Exception:
            return 0

    if desired in names:
        n = count_docs(desired)
        if DEBUG:
            print(f"🔎 '{desired}': {n} documentos")
        if n > 0:
            return desired

    for nm in names:
        n = count_docs(nm)
        if DEBUG:
            print(f"🔎 '{nm}': {n} documentos")
        if n > 0:
            if DEBUG and nm != desired:
                print(f"➡️ Usando colección '{nm}' (con docs) en lugar de '{desired}'")
            return nm

    if DEBUG:
        print("⚠️ No se encontraron colecciones con documentos.")
    return desired

def load_vectordb(persist_dir: str = PERSIST_DIR, collection_name: str = DEFAULT_COLLECTION) -> Chroma:
    chosen = _pick_collection(persist_dir, collection_name)
    vs = Chroma(
        persist_directory=persist_dir,
        collection_name=chosen,
        embedding_function=OpenAIEmbeddings()
    )
    return vs

vectorstore = load_vectordb()

# ==========================
# Prompt estricto
# ==========================
SYSTEM_PROMPT = """Eres un asistente técnico especializado en paquetes de inyección de químicos.
Usa EXCLUSIVAMENTE el contenido dentro de {context} para responder.
Reglas:
- Si hay valores en tablas o KV (ej: caudal_gph, presion_psig, hp_motor, materiales), repórtalos literalmente con unidades.
- Si hay conflicto entre valores, prioriza 'hoja_datos' y explícitalo (ej: 'según p.42 vs p.43').
- Si no hay información en {context}, responde: "No se encontró en el documento."
- Nunca inventes datos ni uses conocimiento general.
- Máximo 3 oraciones y entrega números con unidades (GPH, psig, HP, SS 304/316, etc.).
- Si conoces la página, cítala en el texto como p.X.
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{input}")
])

llm = ChatOpenAI(model=MODEL_NAME, temperature=0)
stuff_chain = create_stuff_documents_chain(llm, prompt, document_variable_name="context")

# ==========================
# Intención + refuerzos
# ==========================
ALCANCE_KEYS = [
    "alcance", "suministro", "responsab", "garant", "servicio", "montaje",
    "asistencia", "pruebas", "itp", "kom", "documentos de referencia"
]
HOJADATOS_KEYS = [
    "bomba", "dosificadora", "api 675", "gph", "psig", "psi", "hp", "caballos",
    "material", "tanque", "316ss", "304ss", "viscosidad", "cps", "modbus", "plc",
    "instrument", "quill", "inyector"
]
KV_KEYS = {
    "caudal_gph", "presion_psig", "hp_motor", "kw_motor",
    "material_tanque", "material_mojado", "rango_cps_min", "rango_cps_max",
    "bombas_operativas", "bombas_respaldo", "api_675"
}

# ==========================
# Utils
# ==========================
def _clean(q: str) -> str:
    q = (q or "").strip().lower()
    q = re.sub(r"\s+", " ", q)
    return q

def _has_any(text: str, keys: List[str]) -> bool:
    t = text.lower()
    return any(k in t for k in keys)

def _parse_kv(md: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(md, dict):
        return {}
    kv = md.get("kv")
    if isinstance(kv, dict):
        return kv
    if isinstance(kv, str):
        try:
            return json.loads(kv)
        except Exception:
            return {}
    return {}

def _smart_intent(query: str) -> str:
    q = _clean(query)
    if _has_any(q, HOJADATOS_KEYS):
        return "hoja_datos"
    if _has_any(q, ALCANCE_KEYS):
        return "alcance"
    if re.search(r"\b(gph|psig|psi|hp|kw|cps)\b", q):
        return "hoja_datos"
    return "general"

def _dedupe_docs(docs: List[Document]) -> List[Document]:
    seen = set()
    out = []
    for d in docs:
        key = (d.metadata.get("page"), d.metadata.get("type"), (d.page_content or "")[:160])
        if key not in seen:
            seen.add(key)
            out.append(d)
    return out

def _reinforce_with_kv(intent: str, limit_extra: int = 12) -> List[Document]:
    extra_docs: List[Document] = []
    try:
        coll = vectorstore._collection
        meta = coll.get(
            where={"prio": "hoja_datos"} if intent == "hoja_datos" else {},
            include=["metadatas", "documents"]
        )
        for md, doc in zip(meta.get("metadatas", []), meta.get("documents", [])):
            if not isinstance(md, dict):
                continue
            kv = _parse_kv(md)
            if (kv and any(k in kv for k in KV_KEYS)) or (
                isinstance(doc, str) and re.search(r"\b(GPH|psig|API\s*675)\b", doc, re.I)
            ):
                extra_docs.append(Document(page_content=doc, metadata=md))
                if len(extra_docs) >= limit_extra:
                    break
    except Exception:
        pass
    return extra_docs

def _score_for_sort(d: Document) -> tuple:
    md = d.metadata or {}
    kv = _parse_kv(md)
    has_kv = 1 if kv else 0
    is_hd = 1 if md.get("prio") == "hoja_datos" else 0
    return (has_kv, is_hd)

def _fallback_docs(intent: str, limit_n: int = 14) -> List[Document]:
    """
    Si la similitud devuelve 0, traemos docs directos por prioridad desde Chroma.
    """
    docs: List[Document] = []
    try:
        coll = vectorstore._collection
        where = {}
        if intent == "alcance":
            where = {"prio": "alcance"}
        elif intent == "hoja_datos":
            where = {"prio": "hoja_datos"}

        meta = coll.get(where=where, include=["metadatas", "documents"], limit=limit_n)
        for md, doc in zip(meta.get("metadatas", []), meta.get("documents", [])):
            docs.append(Document(page_content=doc, metadata=md))
    except Exception as e:
        if DEBUG:
            print("⚠️ Fallback falló:", e)
    return docs

# ==========================
# Recuperación principal
# ==========================
def smart_retrieve(query: str, k_alc: int = 10, k_hd: int = 14) -> List[Document]:
    intent = _smart_intent(query)
    docs: List[Document] = []

    # 1) Similaridad filtrada por prioridad
    if intent == "alcance":
        retr = vectorstore.as_retriever(
            search_kwargs={"k": k_alc, "filter": {"prio": "alcance"}},
            search_type="similarity"
        )
        docs = retr.invoke(query)

    elif intent == "hoja_datos":
        retr = vectorstore.as_retriever(
            search_kwargs={"k": k_hd, "filter": {"prio": "hoja_datos"}},
            search_type="similarity"
        )
        docs = retr.invoke(query)

    else:
        retr = vectorstore.as_retriever(search_kwargs={"k": 12}, search_type="similarity")
        docs = retr.invoke(query)

    # 2) Si quedó vacío, probamos MMR (más diverso)
    if not docs:
        retr_mmr = vectorstore.as_retriever(
            search_kwargs={"k": 12, "filter": {"prio": "hoja_datos" if intent == "hoja_datos" else None}},
            search_type="mmr"
        )
        # gradio podría pasar None; protegemos:
        try:
            docs = retr_mmr.invoke(query)
        except Exception:
            docs = []

    # 3) Refuerzo con KV cuando aplica
    if intent in ("hoja_datos", "general"):
        docs.extend(_reinforce_with_kv("hoja_datos", limit_extra=12))

    # 4) Si AÚN no hay docs, traemos fallback directo por prioridad
    if not docs:
        docs = _fallback_docs(intent=intent, limit_n=14)

    # 5) Limpieza, orden, tope
    docs = _dedupe_docs(docs)
    docs.sort(key=_score_for_sort, reverse=True)
    return docs[:16]

# ==========================
# Función principal
# ==========================
def respuesta(pregunta: str, history=None):
    docs = smart_retrieve(pregunta)

    if DEBUG:
        print(f"\n=== DEBUG | Contexto FINAL al LLM ({len(docs)}) ===")
        for i, d in enumerate(docs[:8]):
            md = d.metadata or {}
            kv = _parse_kv(md)
            kv_flag = "✅KV" if kv else "—"
            print(f"[{i+1}] p.{md.get('page')} | {md.get('type')} | prio={md.get('prio')} | {kv_flag}")
            print((d.page_content or "")[:200].replace("\n", " "), "\n")

    if not docs:
        return "👉 No se encontró en el documento."

    out = stuff_chain.invoke({"input": pregunta, "context": docs})
    if isinstance(out, dict):
        for k in ("answer", "output_text", "text"):
            if k in out:
                return out[k]
        return json.dumps(out, ensure_ascii=False)
    return str(out)

# ==========================
# Pruebas rápidas
# ==========================
if __name__ == "__main__":
    pruebas = [
        "¿Cuál es el alcance del suministro del paquete STAP EC3?",
        "¿Cuál es el caudal en GPH de la bomba dosificadora?",
        "¿Qué presión de descarga en psig se indica para la bomba?",
        "¿Cuántas bombas operativas y cuántas de respaldo se requieren?",
        "¿La bomba cumple con API 675?",
        "Materiales en contacto (liquid end / tuberías) y del tanque",
        "¿Se requiere PLC y comunicación Modbus TCP/IP?"
    ]
    for q in pruebas:
        print("\n❓", q)
        print("➡️", respuesta(q))

