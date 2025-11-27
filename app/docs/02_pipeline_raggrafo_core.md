📘 Pipeline PC1–PC7 + LightRAG Core (RAG Local por Caso)

Documentación técnica – Proyecto AutoSelect-X (Tesis 2025)
Última actualización: 2025-11-26

1. Resumen Ejecutivo

Este documento describe la arquitectura, funcionamiento y estado totalmente actualizado del pipeline técnico Raggrafo Core, componente central de AutoSelect-X / AI EngiQuote.
El sistema procesa PDFs de ingeniería del sector Oil & Gas (Ecopetrol, Cupiagua, Caño Sur), construye un RAG local por caso, ejecuta consultas técnicas y extrae automáticamente los parámetros para selección y cotización de bombas dosificadoras API-675.

El pipeline integra:

🔹 PC1–PC5

Procesamiento profundo de PDFs: lectura, limpieza de layout, parsing de texto, tablas, P&ID y consolidación semántica contextual.

🔹 PC6

Construcción automática del RAG local por case_id usando LightRAG Core (grafo + embeddings por caso).

🔹 PC7

Query inteligente multi-modo usando rag_case_query_finetune (naive, engineering, mix, mix-v2, verify, extract, extract-list).

🔹 Otras capas del sistema

Normalizador v3 (GPH / PSI / °C / cP)

Multipaso industrial 80/20 (TAGS → tablas → fallback)

Benchmarks técnicos v5 (RAG local por caso)

Aislamiento completo por caso (carpeta rag_storage/case_<id>)

Resultado:
Un pipeline industrial, reproducible y escalable para extracción automática de parámetros técnicos desde PDFs reales de ingeniería.

2. Arquitectura General del Pipeline
PDFs (HD, MR, ET, P&ID)
        ↓
 PC1 – Lectura
 PC2 – Limpieza de Layout
 PC3 – Parsing (tablas, texto, P&ID)
 PC4 – Consolidación semántica
 PC5 – Construcción del grafo (entities / relations / chunks)
        ↓
 PC6 – Inicialización de LightRAG local (por caso)
        ↓
 PC7 – LightRAG Core (RAG local)
        ↓
 rag_case_query_finetune (multi-modo)
        ↓
 extract_and_normalize (normalización unificada)
        ↓
 benchmark_queries_v5 (validación industrial)

Componentes adicionales

Normalizador v3 (min/nom/max + unidades)

Multipaso inteligente (tags → tablas → fallback)

Caches por caso

Grafo técnico (GraphML) por caso

# 3. Variables de Entorno Requeridas
# 3.1. Variables para Desarrollo (LOCAL)
# Archivo sugerido: .env.local

# === OpenAI / Modelos ===
OPENAI_API_KEY="sk-proj-XXXX"
LLM_MODEL="gpt-4o-mini"
EMBEDDING_MODEL="text-embedding-3-small"
EMBEDDING_DIM="1536"

# === LightRAG / VectorDB ===
LIGHTRAG_LLM_BINDING="openai"
LIGHTRAG_EMBEDDING_BINDING="openai"
LIGHTRAG_EMBEDDING_MODEL="text-embedding-3-small"
NANO_VECTORDB_DIM="1536"

# === Base de Datos Local ===
DATABASE_URL="postgresql://postgres:root@localhost:5433/autoselectx"


# 3.2. Variables para Producción (Render)
# Configurar en: Render → Environment → Environment Variables

# === OpenAI / Modelos ===
OPENAI_API_KEY=sk-proj-XXXX
LLM_MODEL=gpt-4o-mini
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIM=1536

# === LightRAG / VectorDB ===
LIGHTRAG_LLM_BINDING=openai
LIGHTRAG_EMBEDDING_BINDING=openai
LIGHTRAG_EMBEDDING_MODEL=text-embedding-3-small
NANO_VECTORDB_DIM=1536

# === Base de Datos (Render) ===
DATABASE_URL=postgres://<USER>:<PASSWORD>@<HOST>.internal:5432/<DBNAME>

# === Seguridad / Autenticación ===
SECRET_KEY=sBk3K8ef13AxU2aFAKEKEY123
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30


4. Ejecución Completa del Pipeline (PC1–PC6)
python -m raggrafo.scripts.run_pipeline_for_case \
    --case-id 14 \
    --with-pc6


Esto genera la estructura:

rag_storage/case_14/
    corpus, chunks, entities, relations
    vdb_entities.json
    vdb_relationships.json
    vdb_chunks.json
    graph_chunk_entity_relation.graphml
    full_docs.jsonl
    text_chunks.jsonl
    llm_response_cache.kv
    raw.json
    normalized.json
    doc_status.json

5. Consultas al RAG Local
🚀 Modo directo
python -m raggrafo.pipelines.rag_case_query_finetune \
    --case-id 14 \
    --mode engineering \
    --question "¿Cuál es el caudal nominal?"

Raw
--raw

6. Ejemplos Reales (Case 2 – Cupiagua)
Caudales nominales extraídos (extract-list + Normalizador v3)

P-5540 → 0.1–2.0 GPD

P-5541 → 0.1–1.0 GPD

P-5542 → 0.01–0.51 GPD

P-5543 → 0.1–1.0 GPD

P-5544 → 0.3–3.0 GPD

P-5545 → 0.1–2.0 GPD

Todos convertidos automáticamente a GPH.

Turndown

≈ 10:1 (según API-675)

Ensayos NDT

Detectados vía modo engineering.

7. Modos Disponibles del RAG Local (AutoSelect-X)
Modo	Estado	Descripción
naive	✔ OK	Respuesta directa desde chunks
verify	✔ OK	Chequeo cruzado / consistencia
engineering	⭐ TOP	Explicación técnica contextual
mix	✔ OK	naive + engineering
mix-v2	✔ OK	Peso ponderado (según rag_config)
combo	✔ OK	naive + engineering + verify
extract	✔ OK	Extrae una bomba
extract-list	⭐ Industrial	Extrae TODAS las bombas (PC7 80/20)
Modos recomendados:

engineering → Texto técnico

extract-list → Extracción industrial

8. Estructura Generada por Caso
rag_storage/
 └── case_<id>/
      full_docs.jsonl
      text_chunks.jsonl
      entity_chunks.jsonl
      relation_chunks.jsonl
      full_entities.jsonl
      full_relations.jsonl
      graph_chunk_entity_relation.graphml
      vdb_entities.json
      vdb_relationships.json
      vdb_chunks.json
      llm_response_cache.kv
      raw.json
      normalized.json


Sistema totalmente reproducible por caso.

9. Buenas Prácticas Operativas

No mezclar PDFs entre casos

Mantener número de caso estable

Respaldar PDFs originales

Borrar caché solo si es necesario

Registrar versiones de LLM/embeddings

Mantener estructura HD/MR/ET

10. Avances Recientes (2025-11-26)

✔ Corrección total de UI (print JSON, tablas, colores)
✔ extract-list industrial estable (PC7 80/20)
✔ Nueva versión rag_service.py integrada
✔ chat_routes.py refactorizado (70% limpio)
✔ Normalizador v3 ajustado (flujo min/nom/max, GPH/PSI/°C/cP)
✔ LightRAG Core estable por caso
✔ RAG local → 100% aislado por carpeta
✔ Benchmarks v5 funcionando
✔ Eliminado rag_case_query.py (obsoleto)
✔ Multipaso completo (tags → tablas → fallback)
✔ Corrección del doble color en tablas HTML
✔ Interoperación total con extract_and_normalize.py
✔ Integración de comandos manuales (@buscar_bombas_items)
✔ Preparado para integración UI/selector de bomba

11. Trabajo Futuro Pendiente (Sprint Final)

Integrar selector automático de bomba → AI EngiQuote

Generar JSON schema rígido para extract-list

Integración con Cloud SQL (listas de precios)

Generación automática de PDF técnico

Métricas: exactitud / chunks / tiempo por modo

Optimización PC4 para RETIE 2025 y API-675

UI final: navbar + timer + comandos manuales

Autenticación y permisos en UI por caso

12. Autor

Victor Murcia
Proyecto AutoSelect-X – AI EngiQuote
Maestría en Ingeniería de Sistemas y Computación
Universidad del Valle — 2025