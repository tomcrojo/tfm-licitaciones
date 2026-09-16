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
- dimensión de referencia DIR3 de unidades orgánicas por ámbito (AGE, CCAA,
  EELL, Universidades, Otras Instituciones y Justicia);
- conservación local de los payloads descargados;
- parsing de JSON, ZIP, Atom y CODICE/XML por lotes acotados (Python);
- Bronze Parquet con procedencia, columnas tipadas del adaptador, rechazos
  localizados y conteos de ingesta (ensamblado y métricas en Polars);
- eventos canónicos `silver/procurement_events.parquet` append-only
  (PySpark, extra `spark`; revisiones, tombstones, Decimal y UTC);
- normalización a un `TenderRecord` común con plegado nativo en Polars;
- clasificación tecnológica por reglas con matching vectorizado y CPV como baseline;
- un enlace heurístico de avisos (bloqueo nativo, presupuesto de pares con
  fallo explícito) y controles de calidad básicos;
- artefactos de ejecución con conteos, checksums y duración por etapa.

El [manifest versionado](data/gold/run_manifest.json) corresponde a una
ejecución del 2 de septiembre de 2026 sobre 9.905 avisos TED y 129.090 avisos
OpenPLACSP tras el plegado. Estos datos describen ese corpus concreto; no son
una estimación del histórico completo.

Bronze utiliza Parquet; la Silver canónica también (`procurement_events.parquet`
vía PySpark) mientras la vista legada y Gold todavía usan JSONL/CSV y
ejecución manual. Los motores están asignados por etapa (Python en los
límites, Polars en transformaciones locales, Spark en la consolidación
canónica); la ingesta incremental idempotente, nuevas fuentes y Airflow están
descritos en la [arquitectura](docs/architecture.md). Las limitaciones conocidas
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

La Silver canónica requiere el extra `spark` (PySpark + JVM); sin él, el
pipeline registra el motor como no disponible y continúa con las demás capas:

```bash
uv run --locked --extra spark --with-editable . python -m unittest discover -s tests -v
```

Las pruebas y el ejemplo anterior trabajan con fixtures locales. La descarga
desde fuentes oficiales es una operación separada y requiere red:

```bash
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-09-15 --end 2026-09-15 --source dir3

# dimensión DIR3 offline a partir de data/raw/dir3 (ver docs/dir3-reference.md)
uv run --with-editable . python -m tfm_licitaciones.cli dir3
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
