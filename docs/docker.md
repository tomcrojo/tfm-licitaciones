# Runtime Docker

Imagen mínima y reproducible del stack técnico actual: **Python 3.11,
dependencias locked (`uv.lock`), Java 17, PySpark 4.0.1 y el código del
proyecto**. No contiene lógica nueva ni datasets: la imagen solo prepara el
entorno para ejecutar los entrypoints existentes
(`licitaciones-pipeline` / `python -m tfm_licitaciones.cli`).

## Qué contiene la imagen

- `python:3.11-slim-bookworm` + `openjdk-17-jre-headless` (Spark 4.0 exige
  Java >= 17, el mismo major que usa CI);
- dependencias del proyecto instaladas con `uv sync --locked` desde
  `uv.lock` (idénticas a las de `uv run --locked`);
- `pyspark==4.0.1` como extra de ejecución, igual que en CI y que la vara de
  `spark_foundation.PYSPARK_VERSION`;
- `src/` instalado editable (para que `config.project_root()` resuelva
  `/app`), `config/`, `sql/duckdb/` y `tests/`;
- usuario no-root `tfm` (uid/gid 1000);
- directorios de datos vacíos bajo `/app/data`.

Los datasets **quedan fuera de la imagen** (`.dockerignore` excluye `data/`):
se montan en ejecución. Sin secretos: la configuración solo contiene URLs
públicas y el bundle CA de la FNMT ya versionado en `config/certs/`.

## Build

```bash
docker build -t tfm-licitaciones:local .
```

La caché está ordenada para rebuilds baratos: las dependencias locked y
PySpark se instalan en una capa que solo cambia al modificar `pyproject.toml`
o `uv.lock`; los cambios de código solo reinstalan el paquete.

## Smoke

```bash
# Smoke completo del proyecto: import del paquete, creación/cierre de
# SparkSession y contrato Spark (es el CMD por defecto de la imagen).
docker run --rm tfm-licitaciones:local

# Solo import + SparkSession create/stop:
docker run --rm tfm-licitaciones:local python -c "
from tfm_licitaciones.spark_foundation import create_spark_session
spark = create_spark_session(master='local[1]', shuffle_partitions=2)
print(spark.version)
spark.stop()
"

# Suite offline completa del proyecto:
docker run --rm tfm-licitaciones:local python -m unittest discover -s tests -v
```

## Datos: mounts y paths

| Path en el contenedor | Uso |
| --- | --- |
| `/app/data/raw` | descargas originales (`storage.raw_dir`) |
| `/app/data/bronze` | capa Bronze Parquet |
| `/app/data/silver` | Silver canónico `procurement_events.parquet` |
| `/app/data/gold` | salidas Gold y evidencia |
| `/app/data/reference` | dimensiones de referencia (DIR3, …) |
| `/app/data/analytics` | base DuckDB y manifest analítico |
| `/app/data/exports` | CSV Tableau y manifest de exportación |

Los paths relativos de `config/pipeline.json` resuelven a `/app/data/...`, de
modo que un único mount cubre todo:

```bash
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline run
```

### Ejecución con fixtures (sin tocar `data/`)

```bash
docker run --rm tfm-licitaciones:local \
  licitaciones-pipeline run --raw-dir tests/fixtures/raw --output-root /tmp/fixture
```

### Cadena completa Raw → Tableau

Con Raw y sus sidecars ya disponibles en `data/raw/`:

```bash
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline run
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline build-gold --as-of 2026-01-01
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline build-analytics
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline export-tableau
```

Fijar `--as-of` a la fecha de evaluación deseada. Las referencias CPV/DIR3
se preparan según la [guía de referencias](references.md); si faltan, las
etapas registran esa ausencia. Los CSV se escriben en `data/exports/tableau/`.

También se puede usar Podman sustituyendo `docker` por `podman` en los
comandos de build y ejecución. La prueba integrada con fixtures locales es:

```bash
docker run --rm tfm-licitaciones:local \
  python -m unittest tests.test_e2e_raw_to_tableau -v
```

### Ingesta (requiere red)

```bash
docker run --rm -v "$PWD/data:/app/data" tfm-licitaciones:local \
  licitaciones-pipeline ingest --start 2026-01-01 --end 2026-01-31 --source placsp
```

## Usuario y permisos

El contenedor ejecuta como `tfm` (uid 1000). Si tu usuario host no es 1000,
exporta tu uid/gid para que los mounts sean escribibles:

```bash
docker run --rm -u "$(id -u):$(id -g)" -v "$PWD/data:/app/data" ...
```

## docker compose (opcional)

`docker-compose.yml` cablea el build y el mount de `./data`. Con uid host
distinto de 1000, usa `DOCKER_UID=$(id -u) DOCKER_GID=$(id -g)`.

```bash
docker compose run --rm pipeline                          # smoke
docker compose run --rm pipeline licitaciones-pipeline run
```

## Límites

- Spark `local[*]`/`local[1]` exclusivamente: sin cluster, Airflow,
  Kubernetes ni cloud (decisión del alcance actual).
- La imagen no sustituye a `uv run`: es el mismo entorno, empaquetado para
  reproducibilidad y ejecución de Gold/DuckDB/Tableau por los entrypoints
  del proyecto.
