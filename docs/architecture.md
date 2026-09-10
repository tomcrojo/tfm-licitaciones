# Arquitectura del pipeline

## Objetivo técnico

El sistema convierte avisos heterogéneos de contratación pública en un contrato
común, deduplicado y auditable. La unidad de ingesta es un aviso identificado
por `(source, tender_id)`; la unidad analítica de gold es el procedimiento
único (grupo de linkage), con su aviso canónico y los duplicados enlazados.

## Flujo medallion con capa de enlace

```text
                          ┌──────────────────────────┐
                          │ Fuentes oficiales        │
                          │ TED · BOE · OpenPLACSP  │
                          └────────────┬─────────────┘
                                       │ ingest explícita (CA FNMT para PLACSP)
                                       ▼
                          ┌──────────────────────────┐
                          │ raw/                     │
                          │ payload original JSONL  │
                          │ + ZIPs mensuales PLACSP │
                          └────────────┬─────────────┘
                                       │ adaptadores + plegado de updates
                                       ▼
                          ┌──────────────────────────┐
                          │ bronze/records.jsonl     │
                          │ payload + procedencia    │
                          └────────────┬─────────────┘
                                       │ normalización + latest-wins
                                       ▼
                          ┌──────────────────────────┐
                          │ silver/tenders.jsonl     │
                          │ TenderRecord común       │
                          │ (cpv, dir3, nuts, eur)   │
                          └────────────┬─────────────┘
                                       │ linkage: blocking + TF-IDF
                                       ▼
                          ┌──────────────────────────┐
                          │ gold/                    │
                          │ opportunities + marts   │
                          │ + calidad + evaluación   │
                          └──────────────────────────┘
```

## Decisiones principales

### Ingesta y fuentes

| Fuente | Función | Estado |
| --- | --- | --- |
| TED | Fuente primaria europea, avisos españoles con query tecnológica | Adaptador POST implementado; corpus ene-jun 2026 |
| OpenPLACSP | Licitaciones españolas completas (sindicación 643, excluye menores) | En producción: 6 ZIPs mensuales ene-jun 2026 |
| BOE | Contexto normativo español | Verificado: el sumario no publica licitaciones; fuente opcional |

La sindicación 643 de OpenPLACSP sirve ZIPs mensuales con Atom encadenados que
embedden fragmentos CODICE: CPV, DIR3 del comprador, importes con moneda,
NUTS y estado del expediente. El endpoint usa una cadena TLS de la FNMT-RCM
que no está en el almacén de confianza de Python, por lo que el adaptador ancla
la raíz publicada (`config/certs/fnmt-root-servidores-seguros.pem`,
SHA-256 `55:41:53:B1:...:7B:CB`) sin desactivar la verificación.

La sindicación republica el mismo id en cada actualización del aviso (hasta
96 veces en el corpus). El plegado `latest-wins` por `(source, tender_id)`
usa el timestamp `updated` del entry —no el orden de fichero, porque el Atom
base contiene el estado más reciente pero ordena primero dentro del zip—.
Los tombstones (`at:deleted-entry`) retiran los avisos anulados.

### Normalización y contrato

La normalización es source-specific hasta el límite del contrato silver. La v2
añade `cpv_main` (TED `PC` y PLACSP CODICE), `buyer_id` (DIR3), `region`
(NUTS) y `status`. La moneda se informa siempre que hay importe: PLACSP la
publica (`currencyID`) y en TED la Search API normaliza los valores a EUR,
supuesto documentado en el propio registro.

### Clasificación híbrida y evaluación

La clasificación sigue siendo explicable por reglas. El primer señal es el
diccionario de keywords; cuando no hay coincidencia y el CPV es inequívoco,
el mapa CPV (`config/pipeline.json`, verificado contra
`config/reference/cpv2008_es.csv`) asigna la categoría. `category_source`
registra qué señal decidió (`keywords`, `cpv`, `none`).

La evaluación jerárquica (`gold/classifier_evaluation.json`) mide el señal de
keywords contra las etiquetas CPV inequívocas con precisión, recall y F1 por
categoría y macro-F1. Los registros decididos por CPV se excluyen de la
evaluación para evitar circularidad. Limitación documentada: CPV 2008 no
tiene códigos propios de IA ni ciberseguridad, por lo que esas categorías no
tienen proxy CPV.

### Linkage entre fuentes

La capa de enlace agrupa avisos que describen el mismo procedimiento. El
blocking usa comprador normalizado + división CPV; los pares candidatos deben
caer en una ventana de ±7 días y se puntúan con coseno TF-IDF sobre los
títulos (los resúmenes son estructuralmente distintos entre fuentes y se
excluyen del score). Los pares por encima del umbral se fusionan con
union-find; cada grupo conserva un canónico (fecha más antigua, título más
largo) y el resto apunta a él con `duplicate_of`. Los marts de gold se
calculan solo sobre canónicos para contar procedimientos, no avisos.

### Almacenamiento

Las salidas son archivos JSONL/CSV sin dependencia de un motor de base de
datos. Esto mantiene el MVP ejecutable en local y deja abierta una evolución
a DuckDB, Spark u otro serving layer sin cambiar el contrato silver.

## Calidad

Las puertas se calculan antes de cerrar la ejecución y se guardan en
`gold/quality_report.json`: volumen mínimo, completitud de título y fecha,
validez de importes, relevancia tecnológica (recalibrada a 5% en v2: PLACSP
aporta la contratación completa española, no solo tecnología), unicidad de
clave y cuota de duplicados del linkage. Un fallo no borra las capas
generadas; permite inspeccionar los datos y corregir la causa en una nueva
ejecución.
