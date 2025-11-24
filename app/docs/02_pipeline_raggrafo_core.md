📘 Pipeline PC1–PC7 + LightRAG Core (RAG Local por Caso)
Documentación técnica – Proyecto AutoSelect-X (Tesis 2025)
1. Resumen Ejecutivo

Este documento describe la arquitectura, funcionamiento y estado actualizado (2025-11-24) del pipeline técnico Raggrafo Core, componente central del proyecto AutoSelectX / AI EngiQuote, diseñado para procesar PDFs de ingeniería del sector Oil & Gas (Ecopetrol, Cupiagua, Caño Sur), construir un RAG por caso y extraer requisitos técnicos para selección y cotización de bombas dosificadoras.

El pipeline integra:

PC1–PC5 → Procesamiento profundo de PDFs (texto, tablas, P&ID, secciones, contexto).

PC6 → Construcción automática de RAG local por case_id usando LightRAG Core.

PC7 → Query inteligente multi-modo (naive, engineering, mix, mix-v2, verify).

Pipeline industrial 80/20 para extract/extract-list.

Normalizador v3 (unidades → GPH/PSI/°C/cP).

Benchmark técnico v5.

Aislamiento completo por caso (cada case_id tiene su grafo, embeddings, caches).

Resultado:
Un RAG estable, reproducible, industrial y escalable para extracción automática de parámetros técnicos.

2. Arquitectura General (Pipeline Completo)
PDFs (HD, MR, ET, P&ID)
        ↓
 PC1 – Lectura
 PC2 – Limpieza de Layout
 PC3 – Parsing (tablas, bloques, P&ID, texto)
 PC4 – Consolidación semántica
 PC5 – Construcción de grafo (entities / relations / chunks)
        ↓
 PC6 – Inicialización LightRAG local (por caso)
        ↓
 PC7 – LightRAG Core (RAG local independiente)
        ↓
 rag_case_query_finetune (múltiples modos)
        ↓
 extract_and_normalize (normalización unificada)
        ↓
 benchmark_queries_v5 (validación industrial)

Componentes adicionales:

Normalizador v3 (corrige flujos min/nom/max y unidades).

Multipaso 80/20 (DESCUBRE bombas → TAGS → TABLAS → FALLBACK JSON).

RAG por caso: cada caso se procesa de forma aislada en rag_storage/case_<id>.

3. Variables de Entorno Requeridas
Obligatorias
export OPENAI_API_KEY="sk-proj-XXXX"
export LLM_MODEL=gpt-4o-mini
export EMBEDDING_MODEL=text-embedding-3-small
export EMBEDDING_DIM=1536

Opcionales
export LIGHTRAG_LLM_BINDING=openai
export LIGHTRAG_EMBEDDING_BINDING=openai
export LIGHTRAG_EMBEDDING_MODEL=text-embedding-3-small
export NANO_VECTORDB_DIM=1536

4. Ejecución Completa del Pipeline (PC1–PC6)
python -m raggrafo.scripts.run_pipeline_for_case \
    --case-id 14 \
    --with-pc6


Esto genera:

rag_storage/case_14/
    corpus, chunks, entidades, relaciones
    vdb_entities.json
    vdb_relationships.json
    vdb_chunks.json
    graph_chunk_entity_relation.graphml
    llm_response_cache.kv
    doc_status.json

5. Consultas al RAG Local
Modo directo:
python -m raggrafo.pipelines.rag_case_query_finetune \
    --case-id 14 \
    --mode engineering \
    --question "¿Cuál es el caudal nominal?"

Ver salida cruda:
--raw

6. Ejemplos Reales (Case 2 / Cupiagua)
Caudales nominales (extraídos con extract-list + normalización)

P-5540 → 0.1-2.0 GPD

P-5541 → 0.1-1.0 GPD

P-5542 → 0.01-0.51 GPD

P-5543 → 0.1-1.0 GPD

P-5544 → 0.3-3.0 GPD

P-5545 → 0.1-2.0 GPD

Convertidos a GPH automáticamente por el Normalizador v3.

Turndown

≈ 10:1 (según API-675)

Ensayos NDT

Inspección según normas API/ASME, extraíble por modo engineering.

7. Modos Reales del RAG Local (AutoSelectX)
Modo	Estado	Descripción
naive	✔ OK	Respuesta directa basada en chunks
verify	✔ OK	Chequeo cruzado / consistencia
engineering	⭐ TOP	Explicación técnica detallada
mix	✔ OK	naive + engineering
mix-v2	✔ OK	Ponderado (pesos desde rag_config)
combo	✔ OK	naive + engineering + verify
extract	✔ OK	Modo estructurado, una bomba
extract-list	✔ OK	Modo industrial 80/20 (todas las bombas)
El modo recomendado:
ENGINEERING para texto
extract-list para extracción técnica industrial
8. Estructura Generada por Caso
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
      raw.json
      normalized.json


Todos los archivos son autosuficientes y reproducibles.

9. Buenas Prácticas

No mezclar PDFs entre casos.

Case IDs inmutables.

Respetar nombres HD/MR/ET.

Mantener backups de PDFs originales.

Borrar caché solo si es estrictamente necesario.

Registrar versión del modelo LLM y embeddings.

10. Avances Recientes (2025-11-24)
✔ Gran avance del proyecto:

✔ Pipeline PC1–PC6 estable y validado.

✔ LightRAG Core funcionando sin servidor por caso.

✔ query_param y binding OpenAI integrados.

✔ Fusión mix-v2 optimizada.

✔ extract-list industrial 80/20 funcionando.

✔ Normalizador v3 estable (GPH/PSI/°C/cP).

✔ Corrección automática del flujo nominal = null cuando corresponde.

✔ Multipaso inteligente (tags → tablas → fallback).

✔ benchmark_queries_v5 funcionando correctamente.

✔ Grafo técnico estable (305 nodos, 272 edges en Case 2).

✔ Error interno de LightRAG "mode NoneType" aislado e inofensivo.

✔ Interoperabilidad total con extract_and_normalize.py.

11. Trabajo Futuro

Implementar JSON schema rígido para extract-list.

Conectar extracción al chat web del caso.

Integrar selector automático de bomba (AI EngiQuote).

Integración con Cloud SQL / precios.

Métricas por modo: precisión, chunks, tiempo.

Optimizar PC4 para RETIE 2025 y API-675.

Formalizar reportes PDF automáticos.

12. Autor

Victor Murcia
Proyecto AutoSelectX – AI EngiQuote
Maestría en Ingeniería de Sistemas y Computación
Universidad del Valle, 2025