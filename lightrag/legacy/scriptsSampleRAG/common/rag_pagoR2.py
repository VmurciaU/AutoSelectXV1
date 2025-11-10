# -----------------------------------------------------------------------------
# RAG afinado para paquetes de inyección de químicos (STAP EC3) – v2
# - Recuperación robusta (MMR + fallback)
# - Fusión de KV y citas por página
# - Anti-alucinación (prompt estricto + menos contexto + dedupe por página)
# - Compatible con Gradio: función respuesta(mensaje, history=None) -> str
# -----------------------------------------------------------------------------

import os
import re
import json
import pathlib
from typing import List, Dict, Any, Tuple

from dotenv import load_dotenv
import chromadb
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import Document
from langchain.prompts import ChatPromptTemplate

# ==========================
# Configuración
# ==========================
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

# Obtener la clave de OpenAI desde las variables de entorno
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

# Validar que exista la variable
if not OPENAI_KEY:
    raise ValueError("❌ Falta configurar OPENAI_API_KEY en tu entorno o en el archivo .env")

# Establecerla en el entorno (por compatibilidad con librerías)
os.environ["OPENAI_API_KEY"] = OPENAI_KEY


# ⚠️ RUTA CORRECTA A TU VDB (con fallback a ENV)
#   Preferimos la VDB donde viste 626 documentos:
#   /home/user/Desktop/Tesis2025/AutoSelectX/scriptsSampleRAG/vectordb
PERSIST_DIR = (
    os.getenv("VDB_PATH")
    or "/home/user/Desktop/Tesis2025/AutoSelectX/scriptsSampleRAG/vectordb"
)
DEFAULT_COLLECTION = os.getenv("VDB_COLLECTION") or "autoselx_docs"

MODEL_NAME = os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
DEBUG = bool(int(os.getenv("RAG_DEBUG", "1")))

# ==========================
# Carga de VDB (autodescubrimiento)
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

    # Preferida
    if desired in names:
        n = count_docs(desired)
        if DEBUG:
            print(f"🔎 '{desired}': {n} documentos")
        if n > 0:
            return desired

    # Primera con docs > 0
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
    if DEBUG:
        print(f"✅ VectorDB cargada: colección='{chosen}'")
    return vs

vectorstore = load_vectordb()

# ==========================
# LLM & Prompt anti-alucinación
# ==========================
llm = ChatOpenAI(model=MODEL_NAME, temperature=0)

SYSTEM_PROMPT = """
Eres un asistente técnico especializado en paquetes de inyección de químicos.
Usa EXCLUSIVAMENTE el contenido del CONTEXTO para responder. Si algo no está en el
contexto, responde literalmente: "No se encuentra en los documentos".

Reglas estrictas:
- Copia literalmente cifras y expresiones siempre que sea posible (GPH, psig, HP, materiales).
- Si hay conflicto entre valores, indícalo comparando páginas (ej.: "p.42 vs p.43").
- Prioriza contenido con prioridad 'hoja_datos' para números técnicos.
- Máximo 3 oraciones, en español, sin adornos.
- Cierra SIEMPRE con: "Fuentes: {citations}"
"""

QA_PROMPT = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("user", "Pregunta: {question}\n\n[CONTEXTO]\n{context}\n\n[KV]\n{kv_json}")
])

# ==========================
# Detección de intención (para filtrar por prioridad)
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

def _clean(q: str) -> str:
    q = (q or "").strip().lower()
    q = re.sub(r"\s+", " ", q)
    return q

def _has_any(text: str, keys: List[str]) -> bool:
    t = text.lower()
    return any(k in t for k in keys)

def _smart_intent(query: str) -> str:
    q = _clean(query)
    if _has_any(q, HOJADATOS_KEYS) or re.search(r"\b(gph|psig|psi|hp|kw|cps)\b", q):
        return "hoja_datos"
    if _has_any(q, ALCANCE_KEYS):
        return "alcance"
    return "general"

# ==========================
# Recuperación robusta
# ==========================
def _priority_fallback(prio: str | None) -> List[str] | None:
    if not prio:
        return None
    m = {
        "alcance":   ["alcance", "general"],
        "requisito": ["requisito", "hoja_datos", "general"],
        "norma":     ["norma", "general"],
        "hoja_datos":["hoja_datos", "requisito", "general"],
        "general":   ["general"],
    }
    return m.get(prio, [prio, "general"])

def _mmr_or_similarity_retrieve(q: str, search_kwargs: dict, try_mmr=True) -> List[Document]:
    try:
        retr = vectorstore.as_retriever(search_type="mmr" if try_mmr else "similarity",
                                        search_kwargs=search_kwargs)
        return retr.get_relevant_documents(q)
    except Exception:
        try:
            retr = vectorstore.as_retriever(search_type="similarity" if try_mmr else "mmr",
                                            search_kwargs=search_kwargs)
            return retr.get_relevant_documents(q)
        except Exception:
            return []

def _retrieve_docs(query: str, prio: str | None = None, k:int=10) -> List[Document]:
    base_kwargs = {"k": max(6, k), "fetch_k": 40, "lambda_mult": 0.45}
    allowed = _priority_fallback(prio)
    if allowed:
        base_kwargs["filter"] = {"prio": {"$in": allowed}}
    docs = _mmr_or_similarity_retrieve(query, base_kwargs, try_mmr=True)

    if not docs:
        fb_kwargs = {"k": max(6, k)}
        if allowed:
            fb_kwargs["filter"] = {"prio": {"$in": allowed}}
        docs = _mmr_or_similarity_retrieve(query, fb_kwargs, try_mmr=False)
        if not docs and allowed:
            docs = _mmr_or_similarity_retrieve(query, {"k": max(6, k)}, try_mmr=False)

    # De-dup por (page, type, 140 chars)
    seen = set()
    uniq: List[Document] = []
    for d in docs:
        key = (d.metadata.get("page"), d.metadata.get("type"), (d.page_content or "")[:140])
        if key not in seen:
            uniq.append(d); seen.add(key)
    return uniq

# ==========================
# KV + Citas + Contexto (con dedupe por página)
# ==========================
def _json_loads_safe(x: str) -> Dict[str, Any]:
    try:
        return json.loads(x)
    except Exception:
        return {}

def _collect_kv(docs: List[Document]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for d in docs:
        md = d.metadata or {}
        kv = _json_loads_safe(md.get("kv", "")) if md else {}
        for k, v in kv.items():
            out[k] = v
    return out

def _basename(p: str) -> str:
    try:
        return pathlib.Path(p).name
    except Exception:
        return str(p)

def _make_citation(d: Document, idx: int) -> str:
    meta = d.metadata or {}
    src = _basename(meta.get("source", ""))
    page = meta.get("page", "?")
    typ = meta.get("type", "")
    return f"[S{idx} p.{page} {typ} · {src}]"

def _limit_by_page(docs: List[Document], max_per_page: int = 1, max_docs: int = 8) -> List[Document]:
    per_page: Dict[Any, int] = {}
    out: List[Document] = []
    for d in docs:
        p = d.metadata.get("page")
        if per_page.get(p, 0) < max_per_page:
            out.append(d)
            per_page[p] = per_page.get(p, 0) + 1
        if len(out) >= max_docs:
            break
    return out

def _score_for_sort(d: Document, intent: str) -> tuple:
    md = d.metadata or {}
    prio = 1 if md.get("prio") == intent else 0
    kv = 1 if md.get("kv") else 0
    typ_txt = 1 if md.get("type") == "text" else 0  # ligero sesgo a texto para alcance
    return (kv, prio, typ_txt)

def _build_context_and_citations(docs: List[Document]) -> Tuple[str, str]:
    ctx_parts = []
    cits = []
    for i, d in enumerate(docs, 1):
        snippet = (d.page_content or "").strip()
        snippet = re.sub(r"\s+", " ", snippet)[:700]
        ctx_parts.append(f"— {snippet}")
        cits.append(_make_citation(d, i))
    context = "\n".join(ctx_parts)
    citations = " | ".join(cits) if cits else ""
    return context, citations

# ==========================
# RAG end-to-end
# ==========================
def rag_answer(question: str, prio: str | None = None, k:int=10) -> dict:
    intent = prio or _smart_intent(question)
    docs = _retrieve_docs(question, prio=intent, k=k)
    if not docs:
        return {"answer": "No se encuentra en los documentos.", "citations": "", "docs": []}

    # Ordenar por score y limitar
    docs.sort(key=lambda d: _score_for_sort(d, intent), reverse=True)
    docs = _limit_by_page(docs, max_per_page=1, max_docs=8)

    kv = _collect_kv(docs)
    context, citations = _build_context_and_citations(docs)

    messages = QA_PROMPT.format_messages(
        question=question,
        context=context,
        kv_json=json.dumps(kv, ensure_ascii=False, indent=2),
        citations=citations
    )
    resp = llm.invoke(messages)
    return {
        "answer": resp.content.strip(),
        "citations": citations,
        "docs": docs,
        "kv": kv
    }

# ==========================
# Interfaz pública (Gradio)
# ==========================
def respuesta(pregunta: str, history=None) -> str:
    """Función compatible con gradio.ChatInterface."""
    intent = _smart_intent(pregunta)
    out = rag_answer(pregunta, prio=intent, k=10)
    if DEBUG:
        print(f"\n=== DEBUG | intent={intent} | citas ===\n{out.get('citations','')}\n")
    return out["answer"]

# ==========================
# Pruebas rápidas (opcional)
# ==========================
if __name__ == "__main__":
    tests = [
        "¿Cuál es el alcance del suministro del paquete STAP EC3?",
        "Dame caudal (GPH), presión (psig), HP y materiales de la bomba API 675.",
        "¿Cuántas bombas operativas y de respaldo se requieren?",
        "¿Se requiere PLC y comunicación Modbus TCP/IP?",
        "RETIE y normas aplicables"
    ]
    for q in tests:
        print("\n❓", q)
        print("➡️", respuesta(q))
