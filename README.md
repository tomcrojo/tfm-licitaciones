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
- parsing de JSON, ZIP, Atom y CODICE/XML;
- Bronze Parquet con procedencia, rechazos localizados y conteos de ingesta;
- Silver canónico `procurement_events.parquet` que preserva el histórico de
  avisos, revisiones y tombstones como eventos con identidad determinista;
- una vista legada de estado vigente (`TenderRecord`, en memoria) que Gold 0.1
  sigue consumiendo con plegado de revisiones y tombstones;
- clasificación tecnológica por reglas y CPV como baseline;
- un enlace heurístico de avisos y controles de calidad básicos;
- artefactos de ejecución con conteos y checksums.

El [manifest versionado](data/gold/run_manifest.json) corresponde a una
ejecución del 2 de septiembre de 2026 sobre 9.905 avisos TED y 129.090 avisos
OpenPLACSP tras el plegado. Estos datos describen ese corpus concreto; no son
una estimación del histórico completo. Este artefacto versionado conserva los
nombres de campos históricos del manifest 0.1 y no es un ejemplo del nuevo
contrato de manifest en ejecución; los nombres vigentes están documentados en
el [contrato de datos](docs/data_contract.md).

Bronze y Silver canónico utilizan Parquet. Silver canónico (`procurement_events`)
se ejecuta con una ruta híbrida: una guarda de elegibilidad inspecciona el
batch completo y decide antes de ejecutar entre el kernel nativo de Polars
para su dominio admitido y la referencia python-row congelada (mismos
frames) para cualquier batch fuera de él, con la semántica histórica
como oráculo de paridad y la ruta registrada por batch. Se conserva un candidato
PySpark como ruta de scale-out evaluada fuera del pipeline
productivo, con paridad acreditada solo sobre el contrato sintético
TED/PLACSP del experimento (sin BOE ni instantes subsegundo); Gold todavía usa JSONL/CSV y
ejecución manual. El objetivo asigna Python al
parsing y control, Polars a lotes acotados, dimensiones locales y Silver
canónico dentro del envelope medido, y PySpark a joins grandes, generación de
candidatos de linkage y Gold de alta cardinalidad, con Parquet entre etapas y
Airflow como plano de control. La migración hacia ingesta incremental idempotente,
Parquet en Gold, nuevas fuentes y Airflow está descrita en la
[arquitectura](docs/architecture.md). La frontera contractual preparada para la
siguiente etapa Silver→Spark→Gold se documenta en
[Gold/Spark contract foundation](docs/gold_spark_contract.md). El
[benchmark](docs/benchmarks.md) fija el protocolo de comparación y recoge los
resultados medidos, incluida la decisión del motor de Silver. Las limitaciones
conocidas se documentan en el [contrato de datos](docs/data_contract.md).

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
- [contrato Gold/Spark](docs/gold_spark_contract.md);
- [capa analítica DuckDB](docs/analytics.md);
- [enriquecimiento Gold CPV/DIR3](docs/gold_enrichment.md);
- [benchmark Bronze→Silver y paridad](docs/benchmarks.md);
- [contrato de datos actual](docs/data_contract.md);
- [guía de ejecución](docs/run.md);
- [evolución desde los prototipos](docs/prototype_migration.md);
- [propuesta académica enviada](docs/propuesta/propuesta-enviada.md).

## Licencia

MIT. Véase [LICENSE](LICENSE).
