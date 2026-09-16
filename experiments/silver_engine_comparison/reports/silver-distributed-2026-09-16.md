# Benchmark distribuido Silver 2026-09-16 (standalone vs local vs Polars)

Fecha: 2026-09-16. Estado: **evidencia medida, n=1 por perfil**.
Pregunta: ¿gana Spark en canonical Silver cuando ejecuta distribuido
con workers reales, en vez de `local[16]`?

## Topología y comandos (reproducible en este host)

Cluster Spark 4.0.1 standalone sobre el mismo host (loopback): 1 master +
3 workers × 4 cores / 5g (`SPARK_HOME` del pin `pyspark==4.0.1`,
master `spark://127.0.0.1:7077`, UI en 8081). No es multi-host: el shuffle
viaja por loopback y CPU/RAM/disco se comparten; se declara como
limitación, no como victoria.

```bash
export SPARK_HOME=<site-packages>/pyspark SPARK_LOG_DIR=/tmp/opencode/spark-cluster/logs
$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.master.Master 1 \
  --host 127.0.0.1 --port 7077 --webui-port 8081
for i in 1 2 3; do
  $SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker $i \
    spark://127.0.0.1:7077 -c 4 -m 5G --webui-port $((8081 + i))
done
```

Medición (frontera idéntica read→transform→write, hijo `spawn` frío,
paridad contra `python-row`):

```bash
uv run --with 'pyspark==4.0.1' --with-editable . python -m tfm_licitaciones.bench_engines \
  --profile large --seed 7 --spark-master spark://127.0.0.1:7077 \
  --spark-driver-memory 3g --spark-executor-memory 4g --spark-executor-cores 4 \
  --spark-executor-instances 3 --spark-eventlog-dir <dir> \
  --work-dir <dir> --output <json>
```

El harness registra topología real (`SparkListenerExecutorAdded`),
contadores por stage del event log (shuffle read/write, spill, GC,
tareas) y heap del driver. Sin event log no hay contadores: el resumen
queda en ceros en vez de inventarse.

## Resultados

Medium (253.577 entradas → 225.004 eventos):

| Motor | `transform_s` | Pico RSS árbol | Salida |
| --- | --- | --- | --- |
| python-row | 10,24 | 0,99 GiB | 1,07 MB ×1 |
| polars-native | 1,60 | 1,08 GiB | 1,07 MB ×1 |
| spark local[16] (1g) | 16,62 | 1,88 GiB | 1,61 MB zstd ×16 |
| spark cluster 3×(4c/3g) | 21,91–22,18 | 0,87 GiB driver **solo** | 1,51 MB zstd ×12 |

Large (2.028.577 → 1.800.004, paridad ok en todo):

| Motor | `transform_s` | `init_s` | Salida |
| --- | --- | --- | --- |
| python-row | 85,69 | ~0 | 8,76 MB ×1 |
| polars-native | 12,94 | ~0 | 8,76 MB ×1 |
| spark local[16] 8g | 33,30 | 3,45 | 8,97 MB zstd ×17 |
| spark cluster 3×(4c/4g), driver 3g | 42,95 | 4,48 | 8,80 MB zstd ×13 |

Contadores del cluster en large (event log, 22 jobs / 22 stages):
186 tareas, **shuffle read 712 MB / write 516 MB** por loopback,
**spill 0**, GC 9,7 s acumulados, 3 executors × 4 cores confirmados.

## Lectura crítica

- **Distribuir no ayudó: Spark cluster (42,95 s) es más lento que Spark
  local (33,30 s) y queda a ~3,3× de Polars (12,94 s).** A 2 M de filas
  en un host, el overhead de scheduling + shuffle por red domina sobre
  cualquier ganancia de paralelismo. En medium ocurre igual
  (22 frente a 16,6 s).
- El único beneficio honesto es de memoria del driver: 0,97 GiB en
  cluster frente a 7–8 GiB en local, porque los datos viven en los
  executors. Pero el RSS del padre **no incluye los executors**
  (JVMs fuera del árbol del hijo): la cifra del cluster no es comparable
  con las demás; el envelope real es driver 3g + 3×4g de executors.
- Conclusión para la decisión de arquitectura: ni siquiera con workers
  reales hay caso para Spark en canonical Silver a esta escala y en un
  host. El umbral donde Spark puede ganar sigue estando donde siempre:
  multi-host real, datos que no caben en un nodo, o workloads con joins
  cruzados/ventanas pesadas (linkage, ranking), que requieren sus propios
  benchmarks.

## Artefactos

- `/tmp/opencode/bench-after/medium-07-cluster3.json` (con `eventlog_summary`).
- `/tmp/opencode/bench-after/large-07-cluster.json` (con `eventlog_summary`).
- Event logs: subdirectorio de `--spark-eventlog-dir` por `application_id`.
