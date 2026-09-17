# syntax=docker/dockerfile:1

# Runtime reproducible del stack actual: Python 3.11 + dependencias locked
# (uv.lock) + Java 17 + PySpark 4.0.1 + codigo del proyecto.
#
# Los datasets quedan FUERA de la imagen: se montan en /app/data (ver
# docs/docker.md). Aqui no hay logica de negocio, solo el entorno de ejecucion.

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/app/.venv/bin:${PATH}"

# Java 17: Spark 4.0 exige >= 17 (mismo major que CI). El symlink fija un
# JAVA_HOME estable independiente de la arquitectura.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

COPY --from=ghcr.io/astral-sh/uv:0.12.7 /uv /uvx /usr/local/bin/

WORKDIR /app

# 1) Dependencias locked: esta capa solo se invalida al cambiar pyproject/lock.
#    pyspark==4.0.1 se instala como extra de ejecucion (igual que en CI, fuera
#    del lock base) y comparte capa para no re-descargarse al cambiar codigo.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project \
    && uv pip install --python "${UV_PROJECT_ENVIRONMENT}/bin/python" 'pyspark==4.0.1'

# 2) Codigo, config y tests. Instalacion editable: config.project_root()
#    resuelve /app y config/pipeline.json queda en su sitio.
COPY src/ src/
COPY config/ config/
COPY tests/ tests/
# --inexact conserva pyspark, que no forma parte del lock base.
RUN uv sync --locked --no-dev --inexact

# 3) Usuario no-root y layout de datos vacio: los mounts externos lo pueblan.
#    exports/ reserva la salida de exports futuros (Gold/DuckDB) por los mismos
#    entrypoints, sin cambios de imagen.
RUN useradd --uid 1000 --user-group --create-home tfm \
    && mkdir -p /app/data/raw /app/data/bronze /app/data/silver \
    /app/data/gold /app/data/reference /app/data/exports \
    && chown -R tfm:tfm /app
USER tfm:tfm

# Smoke por defecto: importa el paquete, crea y cierra SparkSession con el
# contrato Spark del proyecto (tests/test_spark_foundation.py).
CMD ["python", "-m", "unittest", "tests.test_spark_foundation", "-v"]
