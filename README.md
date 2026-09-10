# tfm-licitaciones

Pipeline DataOps reproducible para integrar, normalizar, enlazar y explotar
licitaciones públicas tecnológicas de España y la Unión Europea.

Trabajo Fin de Máster (UCM, Máster en Big Data, Data Science e IA — opción 2:
pipeline de preparación/disponibilización de datos). La propuesta y el índice
del trabajo están en [`docs/propuesta/propuesta-enviada.md`](docs/propuesta/propuesta-enviada.md).

## Qué hace

Transforma avisos heterogéneos de contratación pública en un contrato común,
deduplicado y auditable:

```text
TED / OpenPLACSP (sindicación 643)
              │
              ▼
raw → bronze → silver → linkage → gold → CSV/JSONL para análisis
```

- **Fuentes reales**: 9.905 avisos TED de España (query tecnológica) y 129.090
  avisos PLACSP (6 meses de sindicación 643: CPV, DIR3, importes EUR, NUTS y
  estado). La ingesta PLACSP ancla la raíz TLS de la FNMT sin desactivar la
  verificación. BOE queda como fuente opcional (verificado: su sumario no
  publica licitaciones).
- **Plegado de actualizaciones**: la sindicación republica cada aviso en cada
  revisión; se conserva la última versión por `(source, tender_id)` y se
  retiran los avisos anulados (tombstones).
- **Linkage cross-source**: blocking por comprador+CPV, ventana de ±7 días y
  coseno TF-IDF de títulos agrupan avisos del mismo procedimiento; gold marca
  canónicos y duplicados, y los marts cuentan procedimientos únicos.
- **Clasificación híbrida explicable**: keywords primero; si no hay
  coincidencia, mapa CPV 2008 inequívoco (`category_source` audita la señal).
- **Evaluación y calidad**: P/R/F1 por categoría y macro-F1 contra proxy CPV
  en `data/gold/classifier_evaluation.json`; 7 puertas machine-readable en
  `data/gold/quality_report.json`.

Las pruebas usan fixtures locales en `tests/fixtures/` y no dependen de red.

## Ejecución reproducible

Requisitos: Python ≥ 3.10 y [`uv`](https://docs.astral.sh/uv/). Sin
credenciales; la única dependencia de red es la ingesta explícita.

```bash
uv run --with-editable . python -m unittest discover -s tests -v
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted      # corpus TED (~4 min)
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp   # corpus PLACSP (~1,1 GB)
uv run --with-editable . python -m tfm_licitaciones.cli run
uv run --with-editable . python -m tfm_licitaciones.cli report
```

`ingest` conserva respuestas raw, respeta las APIs con reintentos/throttle y
valida cada zip; `run` no hace llamadas externas (~15-17 min sobre el corpus
completo) y deja los artefactos en `data/bronze/`, `data/silver/` y
`data/gold/`. El corpus raw (~1,2 GB) **no se versiona**: se descarga con
`ingest`. En `data/gold/` sí se versionan los artefactos ligeros (marts,
calidad, evaluación, manifest).

## Estructura

```text
config/pipeline.json          # configuración única (fuentes, linkage, clasificador)
config/certs/                 # raíz FNMT anclada para OpenPLACSP
config/reference/             # taxonomía CPV 2008 oficial (9.454 códigos)
data/raw/                     # descargas inmutables (fuera de git salvo README)
data/bronze/                  # payloads semiestructurados con procedencia
data/silver/                  # TenderRecord normalizado y plegado
data/gold/                    # oportunidades, marts, calidad y evaluación
docs/                         # arquitectura, contrato, ejecución y propuesta
src/tfm_licitaciones/         # implementación
tests/                        # unitarios y smoke test end-to-end
```

Documentación: [`docs/architecture.md`](docs/architecture.md) (diseño y
decisiones), [`docs/data_contract.md`](docs/data_contract.md) (contrato silver/gold
y matriz de trazabilidad), [`docs/run.md`](docs/run.md) (guía de ejecución),
[`docs/evaluation_review.md`](docs/evaluation_review.md) (revisión por criterios).

## Origen

Este repositorio se extrajo como proyecto standalone desde un monorepo privado
de máster. Las decisiones heredadas de los prototipos previos (`tfm-codex-1`,
`tfm-glm-1`) y sus diferencias están documentadas en
[`docs/prototype_migration.md`](docs/prototype_migration.md).

## Licencia

MIT — ver [`LICENSE`](LICENSE).
