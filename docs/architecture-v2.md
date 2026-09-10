# Arquitectura v2 — plataforma de inteligencia de contratación pública

## 1. Objetivo

El TFM implementa una plataforma de datos reproducible e incremental para integrar fuentes heterogéneas de contratación pública y transformarlas en productos de datos útiles para detectar y priorizar oportunidades comerciales.

El producto final es un feed de oportunidades: una empresa define qué vende, geografía, tamaño de contrato y otros criterios; el sistema publica diariamente nuevas oportunidades y señales de mercado compatibles con ese perfil.

La clasificación semántica es una etapa de enriquecimiento del pipeline, no el objeto principal del TFM.

## 2. Alcance P0 para la entrega

El sistema debe demostrar de extremo a extremo:

- backfill histórico;
- ingesta incremental diaria y reejecutable;
- idempotencia;
- almacenamiento raw inmutable;
- integración de al menos tres fuentes de contratación;
- normalización a un modelo canónico;
- reconciliación de revisiones, anulaciones y duplicados;
- enriquecimiento con datos de referencia;
- controles de calidad y observabilidad;
- generación de productos Gold para explotación y feed;
- orquestación con Airflow;
- procesamiento tabular con Polars y almacenamiento Parquet.

Fuera de P0: two-tower recommender, entrenamiento de modelos propios, Spark por obligación, dbt por obligación y arquitectura distribuida compleja.

## 3. Fuentes

### Fuentes transaccionales

1. **TED** — avisos europeos mediante API oficial.
2. **OpenPLACSP — licitaciones** — sindicación oficial española.
3. **OpenPLACSP — contratos menores** — histórico y actualizaciones para inteligencia de compradores y oportunidades de menor ticket cuando proceda.

Fuentes P1 si su integración resulta directa:

- licitaciones procedentes de plataformas autonómicas agregadas;
- consultas preliminares de mercado como early signals.

### Fuentes de enriquecimiento

- **CPV 2008** — taxonomía jerárquica de productos y servicios.
- **DIR3** — identificación y jerarquía de organismos compradores.
- **NUTS / Eurostat / INE** — contexto territorial y económico; P1 salvo que la integración sea trivial.

## 4. Flujo de datos

```text
TED API ───────────────────────┐
PLACSP licitaciones ───────────┤
PLACSP contratos menores ─────┤
CPV / DIR3 / NUTS ─────────────┘
              │
              ▼
        RAW IMMUTABLE
   payload original + metadata
              │
              ▼
       BRONZE PARQUET
   parsing source-specific
              │
              ▼
            SILVER
   entidades canónicas y limpias
              │
              ▼
          ENRICHMENT
 CPV + buyer + geo + semantic tags
              │
              ▼
             GOLD
 opportunities / signals / buyer intelligence
              │
              ▼
        API / dashboard / feed
```

La separación entre ingesta y procesamiento se mantiene: una ejecución de transformación nunca necesita red y puede reproducirse a partir de raw.

## 5. Grano y modelo Silver

Silver no es una mega tabla enriquecida. Mantiene entidades reconciliadas con claves estables.

### `silver.procurement_events`

Grano: un evento o estado canónico asociado a un procedimiento de contratación.

Campos mínimos:

```text
event_id
procedure_id
source
source_event_type
buyer_id
buyer_name
title
description
cpv_codes
estimated_value
awarded_value
currency
publication_date
source_updated_at
deadline
status
nuts_code
country
source_url
ingested_at
```

`publication_date` y `source_updated_at` son conceptos distintos. Ningún timestamp de actualización debe reutilizarse silenciosamente como fecha original de publicación.

### Dimensiones Silver

```text
silver.buyers
silver.cpv
silver.regions
silver.suppliers   # cuando la fuente lo permita
```

Estas dimensiones conservan claves oficiales (`DIR3`, `CPV`, `NUTS`) para evitar joins por texto.

## 6. Incrementalidad e idempotencia

El sistema soporta dos modos:

### Backfill

Carga histórica parametrizable por rango temporal. Sirve para reconstruir el lake desde raw y para ampliar el corpus sin cambiar la lógica del pipeline.

Objetivo inicial: intentar cubrir 2021–2026 para TED y las fuentes PLACSP que permitan histórico equivalente.

### Daily incremental

Airflow ejecuta diariamente una ventana móvil por fuente.

Principios:

- reejecutar un día no crea duplicados;
- cada payload raw conserva origen, fecha de descarga y checksum;
- los registros se identifican con claves naturales de la fuente;
- las revisiones se resuelven mediante `latest-wins` usando el timestamp correcto de actualización;
- tombstones/anulaciones se aplican explícitamente;
- una ventana incompleta de una fuente hace fallar su quality gate;
- las descargas idénticas pueden saltarse mediante checksum;
- se usa un pequeño lookback para tolerar retrasos y correcciones de origen.

Estado mínimo por fuente:

```text
source
window_start
window_end
expected_partitions
downloaded_partitions
checksum
status
started_at
finished_at
```

## 7. Capas y almacenamiento

### Raw

Payload original inmutable. No se transforma ni sobrescribe silenciosamente.

### Bronze

Parquet con parsing source-specific, provenance y campos técnicos de ingesta. Los registros rechazados se contabilizan con `rejection_reason`.

### Silver

Parquet tipado y normalizado. Polars se utiliza para transformaciones, deduplicación, joins, controles y agregaciones tabulares.

### Gold

Productos de datos orientados a casos de uso. CSV puede existir únicamente como export interoperable; Parquet es el formato analítico principal.

## 8. Reconciliación y linkage

### Revisiones dentro de una fuente

Se conserva un estado canónico por clave natural mediante `latest-wins`; las anulaciones se propagan explícitamente.

### Linkage entre fuentes

El linkage de TED y PLACSP solo compara registros de fuentes distintas.

Candidate generation:

```text
buyer identifier/name
+ CPV
+ time window
```

Ranking del candidato:

```text
title similarity
+ structured agreement
```

Los bloques que excedan un límite no pueden descartarse silenciosamente: deben subdividirse o contabilizarse como no evaluados.

El output conserva evidencia de linkage, score, método y registro canónico.

## 9. Enriquecimiento

El enrichment se aplica después de construir entidades Silver estables.

### Determinista

- jerarquía CPV;
- buyer/DIR3;
- territorio/NUTS;
- tipo de evento;
- rangos de importe;
- historial del comprador.

### Semántico

Se utilizará un modelo preentrenado ya existente para producir una taxonomía multilabel de negocio, por ejemplo:

```text
Cloud
Data Engineering
Business Intelligence
AI / ML
Cybersecurity
Software Development
Infrastructure
Managed Services
```

Cada enriquecimiento conserva al menos:

```text
labels
confidence/model score
model_name
model_version
classified_at
```

El clasificador actual de keywords + CPV se mantiene como baseline interpretable, no como solución semántica final.

La evaluación se realiza sobre una muestra etiquetada manualmente y no debe computar clases sin soporte dentro del macro-F1.

## 10. Gold y producto

Gold se define por casos de uso, no por fuente.

### `gold.open_opportunities`

Procedimientos abiertos y accionables, con enrichment y campos necesarios para filtrar por sector, ticket, geografía, comprador y deadline.

### `gold.minor_contract_signals`

Contratos menores y señales de compra de bajo ticket. Debe distinguir claramente oportunidades activas de inteligencia histórica.

### `gold.early_market_signals`

Consultas preliminares u otras señales previas a una licitación, si se incorpora esa fuente.

### `gold.buyer_profiles`

Perfil agregado del organismo: categorías compradas, importes, frecuencia, proveedores cuando estén disponibles y tendencias temporales.

### `gold.daily_candidate_feed`

Candidate set diario para el frontend. La primera versión usa scoring explicable:

```text
semantic relevance
+ CPV match
+ geography fit
+ budget fit
+ buyer/history signal
```

Opcional: similitud entre embedding del perfil de empresa y embedding del contrato.

No se implementará un modelo two-tower en el TFM. Queda como evolución futura cuando exista suficiente feedback empresa–oportunidad para entrenar retrieval personalizado.

## 11. Orquestación y observabilidad

Airflow es el plano de control del pipeline.

DAG lógico:

```text
extract_ted ──────────────┐
extract_placsp_tenders ───┤
extract_minor_contracts ──┤
refresh_reference_data ───┘
            │
            ▼
          bronze
            │
            ▼
          silver
            │
            ▼
          quality
            │
            ▼
          linkage
            │
            ▼
        enrichment
            │
            ▼
           gold
```

Debe soportar retries, backfills, logs por tarea y reruns.

El manifest de ejecución incluirá como mínimo:

- inputs y checksums;
- ventanas solicitadas y realmente ingeridas;
- registros leídos, aceptados y rechazados;
- registros tras latest-wins y tombstones;
- métricas de linkage;
- métricas de calidad;
- duración por etapa;
- versión del código/config/modelos de enrichment.

## 12. Quality gates P0

Como mínimo:

- completitud de ventanas de ingesta;
- claves naturales no vacías y únicas tras reconciliación;
- conteos de `parsed / accepted / rejected`;
- completitud de campos críticos;
- validez de fechas e importes;
- moneda coherente;
- integridad referencial de CPV/DIR3/NUTS cuando aplique;
- duplicación/linkage dentro de umbrales observables;
- freshness del incremental diario.

Los thresholds deben tener justificación; no se rebajan únicamente para obtener un `passed=true`.

## 13. Decisiones tecnológicas

### P0

- Python para adaptadores y parsing CODICE/XML;
- Polars para transformación tabular;
- Parquet como formato analítico;
- Airflow para orquestación;
- modelo semántico preentrenado para enrichment;
- aplicación web ligera únicamente como demostración de explotación.

### No obligatorias

**Spark:** solo se incorpora si el backfill permite demostrar una necesidad o comparación de escalabilidad. No se usa para justificar artificialmente el término Big Data.

**dbt:** se incorpora únicamente si aparece un serving layer SQL que lo haga útil.

## 14. Evolución futura

Con más tiempo y datos de interacción:

- company embeddings más sofisticados;
- feedback implícito y explícito del usuario;
- learning-to-rank;
- two-tower retrieval entrenado con pares empresa–oportunidad;
- streaming/event-driven ingestion donde la fuente lo permita;
- lakehouse Iceberg/Delta y procesamiento distribuido;
- alertas multicanal y perfiles organizativos colaborativos.

## 15. Criterio de éxito del TFM

La entrega es satisfactoria si puede demostrarse que, partiendo de fuentes oficiales heterogéneas, el sistema puede reconstruir un histórico y ejecutar una carga incremental idempotente, reconciliar y enriquecer los datos, detectar fallos de calidad, producir productos analíticos reproducibles y alimentar un feed diario de oportunidades sin lógica específica del frontend.
