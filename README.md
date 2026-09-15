# tfm-licitaciones

Pipeline reproducible para integrar y analizar datos de contratación pública.
Es el repositorio del Trabajo Fin de Máster de la opción de Data Engineering de
la UCM.

La plataforma objetivo es generalista: debe poder procesar contratación de
cualquier sector representado por CPV. La tecnología se mantiene como caso de
estudio de la memoria, de acuerdo con la
[propuesta enviada](docs/propuesta/propuesta-enviada.md).

## Estado del proyecto

La versión actual es un punto de partida funcional, no la arquitectura final.
Incluye:

- descarga explícita desde TED y OpenPLACSP;
- conservación local de los payloads descargados;
- parsing de JSON, ZIP, Atom y CODICE/XML;
- Bronze Parquet con procedencia, rechazos localizados y conteos de ingesta;
- normalización a un `TenderRecord` común;
- plegado de revisiones y aplicación de tombstones de OpenPLACSP;
- clasificación tecnológica por reglas y CPV como baseline;
- un enlace heurístico de avisos y controles de calidad básicos;
- artefactos de ejecución con conteos y checksums.

El [manifest versionado](data/gold/run_manifest.json) corresponde a una
ejecución del 2 de septiembre de 2026 sobre 9.905 avisos TED y 129.090 avisos
OpenPLACSP tras el plegado. Estos datos describen ese corpus concreto; no son
una estimación del histórico completo.

Bronze utiliza Parquet; Silver y Gold todavía usan JSONL/CSV y ejecución manual.
La migración hacia ingesta incremental idempotente, Polars en las
transformaciones, Parquet en las demás capas, nuevas fuentes y Airflow está
descrita en la [arquitectura](docs/architecture.md). Las limitaciones conocidas
se documentan en el [contrato de datos](docs/data_contract.md).

## Ejecución local

Requisitos: Python 3.10 o posterior y
[`uv`](https://docs.astral.sh/uv/).

```bash
uv run --with-editable . python -m unittest discover -s tests -v

uv run --with-editable . python -m tfm_licitaciones.cli run \
  --raw-dir tests/fixtures/raw \
  --output-root /tmp/tfm-licitaciones-fixture
```

Las pruebas y el ejemplo anterior trabajan con fixtures locales. La descarga
desde fuentes oficiales es una operación separada y requiere red:

```bash
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp
```

La [guía de ejecución](docs/run.md) explica las salidas y las limitaciones del
flujo actual.

## Estructura

```text
config/                       configuración y referencias
data/raw/                     descargas originales no versionadas
data/bronze/                  representación source-specific
data/silver/                  registros normalizados
data/gold/                    resultados y evidencia ligera
src/tfm_licitaciones/         implementación Python
tests/                        pruebas y fixtures offline
docs/                         arquitectura, contrato y operación
```

Documentación principal:

- [arquitectura técnica](docs/architecture.md);
- [contrato de datos actual](docs/data_contract.md);
- [guía de ejecución](docs/run.md);
- [evolución desde los prototipos](docs/prototype_migration.md);
- [propuesta académica enviada](docs/propuesta/propuesta-enviada.md).

## Licencia

MIT. Véase [LICENSE](LICENSE).
