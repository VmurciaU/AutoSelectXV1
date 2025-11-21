Pipeline PC1–PC7 + LightRAG Core (RAG Local por Caso)
Documentación técnica – Proyecto AutoSelectX (Tesis 2025)
1. Resumen Ejecutivo

Este documento describe la arquitectura, funcionamiento y operación del pipeline técnico “Raggrafo Core”, que integra de manera completa:

Procesamiento de PDFs de ingeniería (HD, MR, ET, P&ID).

Normalización, limpieza y estructuración (PC1–PC5).

Construcción del grafo técnico por caso (entidades, relaciones, chunks).

Creación automática del RAG local por case_id mediante LightRAG Core (sin servidor).

Consultas inteligentes usando diferentes modos del motor RAG.

Integración con modelos OpenAI (GPT-4o-mini) usando QueryParam optimizado.

Benchmark técnico por caso (benchmark_queries_v5).

Cada caso (case_id) mantiene su propio corpus independiente, su grafo, embeddings, caches y estructura.
Esto garantiza aislamiento, reproducibilidad, trazabilidad y escalabilidad del proyecto AutoSelectX.

2. Arquitectura General (Pipeline completo)
PDFs (HD, MR, ET, P&ID)
        ↓
 PC1 – Lectura
 PC2 – Limpieza de Layout
 PC3 – Parsing (tablas, bloques, P&ID, texto)
 PC4 – Consolidación semántica
 PC5 – Construcción de grafo (entities / relations / chunks)
        ↓
 PC6 – Inicialización LightRAG (almacenamiento por caso)
        ↓
 PC7 – LightRAG Core (RAG local)
        ↓
 rag_case_query (múltiples modos RAG)
        ↓
 Benchmark v5 (validación)

3. Variables de Entorno Requeridas
export OPENAI_API_KEY="sk-proj-XXXXXXX"
export LLM_MODEL=gpt-4o-mini
export EMBEDDING_MODEL=text-embedding-3-small
export EMBEDDING_DIM=1536


Opcionales:

export LIGHTRAG_LLM_BINDING=openai
export LIGHTRAG_EMBEDDING_BINDING=openai
export LIGHTRAG_EMBEDDING_MODEL=text-embedding-3-small
export NANO_VECTORDB_DIM=1536

4. Ejecución completa del pipeline
Procesamiento PC1–PC6 (por caso)
python -m raggrafo.scripts.run_pipeline_for_case \
    --case-id 14 \
    --with-pc6


Esto genera el rag_storage/case_<id> completo.

5. Consultas al RAG local

Consulta directa por modo:

python -m raggrafo.pipelines.rag_case_query \
    --case-id 14 \
    --mode engineering \
    --question "PREGUNTA"


Para ver la salida cruda:

--raw

6. Ejemplos (Case 14)
Caudal nominal
P-5540 → 2.0 gpd
P-5541 → 1.0 gpd
P-5542 → 0.51 gpd
P-5543 → 1.0 gpd
P-5544 → 3.0 gpd
P-5545 → 2.0 gpd

Turndown
10:1 ±1% según API 675

Ensayo NDT

Inspección No Destructiva – normas, ITP, aceptación.

7. Modos reales del RAG Local

Estos son los modos implementados actualmente en AutoSelectX:

Modo	Estado	Descripción
naive	✔ Funcional	Respuesta directa basada en chunks
verify	✔ Funcional	Chequeo cruzado + coherencia
engineering	⭐ Mejor respuesta	Explicación técnica completa
mix	✔ Funcional	Fusión naive+engineering
mix-v2	✔ Funcional	Fusión ponderada (pesos en rag_config)
combo	✔ Funcional	Orquesta naive+engineering+verify
extract	❌ Pendiente	Requiere template JSON
extract-list	❌ Pendiente	Igual que extract, formato lista

ENGINEERING es el modo principal recomendado.

8. Estructura generada por caso
rag_storage/
 └── case_14/
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
      doc_status.json
      llm_response_cache.kv


Cada directorio es autosuficiente.

9. Buenas prácticas

No mezclar PDFs entre casos.

No reutilizar IDs de casos procesados previamente.

Mantener trazabilidad del PDF original.

Mantener consistencia en nombres (HD/MR/ET).

Borrar caché solo si el pipeline lo requiere.

Documentar versiones de modelos y embedding.

10. Avances recientes añadidos (2025)

✔ Pipeline PC1-PC6 revisado y estable
✔ PC6 inicializa LightRAG local sin servidor
✔ QueryParam extendido, ahora 100% compatible
✔ Fusion mix-v2 estable
✔ benchmark_queries_v5 funcionando
✔ engineering = modo recomendado
✔ extract y extract-list ya integrados en código
❗ Pendiente: activar plantilla JSON para extract y extract-list

11. Trabajo Futuro

Implementar JSON schema definitivo para extract-list.

Conectar rag_case_query al chat web del caso.

Parametrizar lógica de selección automática de bombas.

Integración total AI EngiQuote (selector, precios, reporte técnico).

Métricas: tiempo por modo, aciertos, calidad de chunking.

Ampliación para RETIE 2025, API 675 y ANSI/HI.

12. Autor

Victor Murcia – Proyecto AutoSelectX
Maestría en Ingeniería de Sistemas y Computación – Universidad del Valle