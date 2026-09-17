# tfm-licitaciones

Prototipo del Trabajo Fin de Máster de Data Engineering de la UCM para
integrar datos de contratación pública, conservar su procedencia y preparar
análisis de oportunidades en Tableau. El modelo es generalista; la tecnología
es el caso de estudio de la [propuesta académica](docs/propuesta/propuesta-enviada.md).

## Fuentes y arquitectura

TED aporta avisos europeos y OpenPLACSP aporta licitaciones españolas mediante
ZIP mensuales con Atom/CODICE. CPV 2008 permite analizar categorías y DIR3
aporta referencias de organismos. BOE tiene un adaptador de compatibilidad,
deshabilitado por defecto. Los contratos menores no están integrados.

```text
Raw → Bronze Parquet → Silver Parquet → Gold Parquet → DuckDB → CSV → Tableau
      parsing/rechazos  histórico       oportunidades  vistas   exportación
```

Python descarga y parsea los datos. Polars procesa Bronze, referencias y Silver;
Silver usa una referencia Python cuando el lote queda fuera del dominio del
kernel nativo. PySpark resuelve el estado vigente y construye Gold. DuckDB
consulta Gold con SQL versionado; Tableau consume los CSV exportados.
Airflow y los modelos semánticos preentrenados son trabajo futuro.

`run` genera Bronze y Silver y conserva salidas JSONL/CSV de un baseline
anterior (clasificación tecnológica y linkage). La cadena analítica actual
continúa explícitamente con `build-gold`, `build-analytics` y `export-tableau`.

## Ejecución local

Desde la raíz del repositorio, con Python 3.10+, `uv` y Java 17+ para Gold.
La instalación inicial necesita red; este ejemplo usa únicamente fixtures
locales. `uv.lock` fija las dependencias base; PySpark se solicita con versión
explícita porque es una dependencia opcional.

```bash
bash examples/demo/run.sh
```

La [demo reproducible](examples/demo/README.md) usa una fixture **sintética**:
2 avisos y 1 tombstone producen 3 eventos Silver y **1 oportunidad abierta**,
con categoría CPV oficial y cuatro CSV con datos. El CSV mensual conserva su
cabecera porque PLACSP no aporta `publication_date` al contrato canónico.
Los resultados permanecen en `data/demo/`; no representan una muestra de mercado.
El script encadena los cuatro comandos productivos y prepara CPV sin descargar
datos de contratación. DIR3 ausente se registra como tal.

Para datos reales, ejecutar `ingest` con fuente y fechas explícitas según la
[guía de reproducción](docs/reproducibility.md). La configuración está en
[config/pipeline.json](config/pipeline.json); la construcción de dimensiones
se explica en [CPV y DIR3](docs/references.md).

Salidas del ejemplo (los directorios por defecto no incluyen `demo/`):

| Ruta bajo `data/demo/` | Contenido |
| --- | --- |
| `bronze/` | Registros, rechazos, tombstones e informe de ingesta |
| `silver/procurement_events.parquet` | Histórico canónico |
| `gold/open_opportunities/` | Oportunidades abiertas en Parquet |
| `gold/gold_manifest.json` | Versiones, conteos y métricas Gold |
| `analytics/licitaciones.duckdb` | Vistas SQL sobre Gold |
| `exports/tableau/` | Cinco CSV y manifest de exportación |

El montaje del dashboard en Tableau es manual; el repositorio genera sus datos.
La [guía analítica](docs/analytics.md) explica los granos y cómo evitar doble
conteo al analizar varios CPV por oportunidad.

## Evidencia con datos reales

La [ejecución documentada del 17 de septiembre de 2026](docs/real-run-evidence.md)
procesó **359.304 avisos Bronze**, obtuvo **359.180 eventos Silver** y exportó
**14.763 oportunidades abiertas** para la ventana solicitada enero–junio.
Se conservan manifests, sidecars, logs, configuración del commit registrado y
un inventario verificado de checksums y conteos. El documento distingue avisos,
eventos, estado vigente y productos analíticos, e indica las limitaciones de
cobertura, fechas y referencias. Los datasets grandes permanecen fuera de Git.

## Pruebas

La suite utiliza **unittest**, fixtures locales y HTTP simulado:

```bash
uv run --locked --with-editable . python -m unittest discover -s tests -v
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest discover -s tests -v
```

La primera ejecución omite las pruebas Spark si PySpark no está instalado.
La segunda incluye esas pruebas. Los experimentos tienen una suite separada,
documentada junto con los [smoke tests y benchmarks](docs/reproducibility.md).

## Estructura y documentación

```text
config/       configuración, certificado público FNMT y vocabulario CPV
src/          adaptadores, transformaciones y CLI
sql/duckdb/   vistas analíticas versionadas
tests/        pruebas de regresión y fixtures pequeños
experiments/  comparación reproducible de motores Silver
docs/         contratos, operación y evidencia seleccionada
data/         datos y outputs locales, excluidos de Git
```

- [Arquitectura](docs/architecture.md): componentes actuales y límites.
- [Contrato de datos](docs/data_contract.md): Raw, Bronze, Silver y Gold.
- [Analítica](docs/analytics.md): DuckDB y exportación Tableau.
- [Reproducibilidad](docs/reproducibility.md): ingesta, ejecución y validación.
- [Referencias](docs/references.md): CPV y DIR3.
- [Experimento de motores](experiments/silver_engine_comparison/README.md): método y evidencia.

## Limitaciones del prototipo

- TED conserva un filtro tecnológico en la configuración de descarga.
- La CLI no integra todavía un estado persistente de completitud de ventanas;
  un fallo parcial de descarga exige revisar los logs. Los ZIP cacheados no se
  refrescan automáticamente.
- Silver no mapea actualmente `deadline`; TED tampoco aporta `status` al
  contrato. Gold aplica una política conservadora y no publica como abiertas
  las filas sin evidencia suficiente. PLACSP usa el estado `PUB`.
- `publication_date` de PLACSP es nula: su fecha de actualización no se utiliza
  como fecha de publicación. El agregado mensual puede quedar vacío.
- La calidad del baseline y los rechazos Bronze se informan por separado.
  No existe una certificación global de completitud o calidad del corpus.
- Las pruebas locales y el benchmark sintético no acreditan operación en
  producción ni escalabilidad distribuida del pipeline completo.

Licencia [MIT](LICENSE).
