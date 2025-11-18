# Pipeline PC1–PC7 + LightRAG Core (RAG Local por Caso)

## Documentación técnica – Proyecto AutoSelectX (Tesis 2025)

### 1. Resumen Ejecutivo

Este documento describe la arquitectura, funcionamiento y operación del pipeline “Raggrafo Core”, que integra:

- Procesamiento estructurado de PDFs de ingeniería (HD, MR, ET).
- Normalización y consolidación de información técnica por etapas PC1–PC5.
- Construcción de un grafo técnico (entidades, relaciones, chunks).
- Creación de un RAG local por caso, usando LightRAG Core (sin servidor web).
- Consulta inteligente usando modos híbridos (Local + Global + Re-ranking) con modelos OpenAI.

Este pipeline permite que cada `case_id` tenga su propio corpus, grafo, embeddings y almacenamiento independiente. Esto habilita escalabilidad por usuario/caso y permite auditoría técnica del conocimiento procesado.

### 2. Arquitectura General

```
PDFs (HD, MR, ET)
        ↓
 PC1 – Lectura
 PC2 – Limpieza de Layout
 PC3 – Parsing (tablas, bloques, P&ID)
 PC4 – Consolidación semántica
 PC5 – Grafo (nodes/edges/chunks)
        ↓
PC7 Core – LightRAG local por caso
        ↓
Consulta inteligente (rag_case_query)
```

### 3. Variables de Entorno Necesarias

```
export OPENAI_API_KEY="sk-proj-XXXXXXXX..."
export LLM_MODEL=gpt-4o-mini
export EMBEDDING_MODEL=text-embedding-3-small
```

Opcionales:

```
export LIGHTRAG_LLM_BINDING=openai
export LIGHTRAG_EMBEDDING_BINDING=openai
export EMBEDDING_DIM=1536
```

### 4. Flujo Completo Paso a Paso

```
python -m raggrafo.scripts.run_pipeline_for_case --case-id 14 --use-core
```

### 5. Consultas al RAG

```
python -m raggrafo.pipelines.rag_case_query --case-id 14 --mode core --question "PREGUNTA" --raw
```

### 6. Ejemplos (Case 14)

#### Caudal nominal
```
P‑5540 → 2.0 gpd
P‑5541 → 1.0 gpd
P‑5542 → 0.51 gpd
P‑5543 → 1.0 gpd
P‑5544 → 3.0 gpd
P‑5545 → 2.0 gpd
```

#### Turndown
```
10:1 ±1% según API 675
```

#### Ensayo NDT
Inspección No Destructiva — normas aplicables + ITP.

### 7. Modos del RAG

| Modo | Origen | Descripción |
|------|--------|-------------|
| Naive | Texto | Respuestas simples |
| Local | Grafo local | Vecindad inmediata |
| Global | Grafo completo | Expansión total |
| Hybrid | Local + Global | Modo más potente |

AutoSelectX usa:
```
QueryParam(mode="hybrid")
```

### 8. Estructura generada por caso

```
rag_storage/case_14/
    full_docs.jsonl
    graph_chunk_entity_relation.graphml
    vdb_entities.json
    vdb_relationships.json
    vdb_chunks.json
    llm_response_cache.kv
```

### 9. Buenas Prácticas

- No mezclar casos
- Documentar cada ejecución
- Mantener consistencia en nombres de PDF

### 10. Trabajo Futuro

- Integración Chat Web AutoSelectX
- Métricas de consulta
- Expansión a normas API/ASME/EN

### 11. Autor

Victor Murcia – Proyecto AutoSelectX
