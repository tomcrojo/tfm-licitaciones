# tfm-licitaciones

Pipeline DataOps reproducible para integrar, normalizar, enlazar, enriquecer y explotar contratación pública de España y la Unión Europea.

Trabajo Fin de Máster (UCM, Máster en Big Data, Data Science e IA — opción 2: pipeline de preparación/disponibilización de datos). La propuesta y el índice del trabajo están en [`docs/propuesta/propuesta-enviada.md`](docs/propuesta/propuesta-enviada.md).

## Qué hace

Transforma avisos heterogéneos de contratación pública en entidades canónicas, deduplicadas y auditables, y genera productos de datos orientados a detectar oportunidades comerciales para empresas de cualquier sector.

La arquitectura objetivo es generalista: el pipeline no filtra el universo de contratación a tecnología. CPV actúa como taxonomía oficial primaria y el enriquecimiento semántico permite describir dominios como obras, catering, sanidad, logística, energía, servicios profesionales o tecnología. El caso tecnológico se mantiene como caso de estudio del TFM, no como restricción de ingesta o modelado.

```text
TED / OpenPLACSP / contratos menores / reference data
                         │
                         ▼
raw → bronze → silver → enrichment/linkage → gold → API/feed
```

El producto final es un feed de oportunidades: una empresa puede definir qué vende, geografía, tamaño de contrato y otros criterios, y el sistema prioriza las oportunidades y señales de mercado que mejor encajan con ese perfil.

### Estado actual y dirección v2

El corpus inicial se construyó con 9.905 avisos TED de España obtenidos mediante una query tecnológica y 129.090 avisos PLACSP de seis meses de sindicación 643. Esa query tecnológica describe el corpus inicial, no el alcance objetivo de la plataforma. La arquitectura v2 elimina ese filtro como requisito: los backfills e incrementales deben poder ingerir contratación general y aplicar segmentación, clasificación y matching downstream.

La implementación actual ya incluye:

- **Plegado de actualizaciones**: la sindicación republica cada aviso en cada revisión; se conserva la última versión por `(source, tender_id)` y se retiran los avisos anulados (tombstones).
- **Linkage cross-source**: blocking por comprador+CPV, ventana temporal y similitud de títulos para detectar avisos del mismo procedimiento.
- **Clasificación explicable de baseline**: keywords + mapa CPV 2008, mantenida como referencia frente al enrichment semántico posterior.
- **Evaluación y calidad**: artefactos machine-readable con métricas de clasificación, calidad y manifest de ejecución.

La v2, descrita en [`docs/architecture-v2.md`](docs/architecture-v2.md), añade como dirección P0:

- backfill histórico e ingesta incremental diaria idempotente;
- TED + OpenPLACSP licitaciones + contratos menores;
- raw inmutable, Bronze/Silver/Gold en Parquet;
- Polars para transformación tabular;
- Airflow para orquestación;
- dimensiones oficiales CPV/DIR3 y enriquecimiento semántico generalista;
- productos Gold para oportunidades, señales de mercado, buyer intelligence y candidate feed.

Las pruebas usan fixtures locales en `tests/fixtures/` y no dependen de red.

## Ejecución reproducible

Requisitos actuales: Python ≥ 3.10 y [`uv`](https://docs.astral.sh/uv/). Sin credenciales; la única dependencia de red es la ingesta explícita.

```bash
uv run --with-editable . python -m unittest discover -s tests -v
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp
uv run --with-editable . python -m tfm_licitaciones.cli run
uv run --with-editable . python -m tfm_licitaciones.cli report
```

`ingest` conserva respuestas raw, respeta las APIs con reintentos/throttle y valida cada zip; `run` no hace llamadas externas y deja los artefactos en `data/bronze/`, `data/silver/` y `data/gold/`. El corpus raw no se versiona: se descarga mediante `ingest`. En `data/gold/` se versionan únicamente artefactos ligeros de resultados y auditoría.

## Estructura

```text
config/pipeline.json          # configuración de fuentes, linkage y enrichment
config/certs/                 # raíz FNMT anclada para OpenPLACSP
config/reference/             # taxonomías/datos de referencia oficiales
data/raw/                     # descargas inmutables (fuera de git salvo README)
data/bronze/                  # parsing source-specific + provenance
data/silver/                  # entidades canónicas normalizadas
data/gold/                    # productos analíticos, calidad y manifest
docs/                         # arquitectura, contrato, ejecución y propuesta
src/tfm_licitaciones/         # implementación
tests/                        # unitarios y smoke tests end-to-end
```

Documentación principal: [`docs/architecture-v2.md`](docs/architecture-v2.md) (dirección objetivo), [`docs/data_contract.md`](docs/data_contract.md) (contrato silver/gold), [`docs/run.md`](docs/run.md) (guía de ejecución) y [`docs/propuesta/propuesta-enviada.md`](docs/propuesta/propuesta-enviada.md) (propuesta académica aprobada).

## Origen

Este repositorio se extrajo como proyecto standalone desde un monorepo privado de máster. Las decisiones heredadas de los prototipos previos (`tfm-codex-1`, `tfm-glm-1`) y sus diferencias están documentadas en [`docs/prototype_migration.md`](docs/prototype_migration.md).

## Licencia

MIT — ver [`LICENSE`](LICENSE).
