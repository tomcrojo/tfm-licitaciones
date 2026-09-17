# Auditoría adversarial del benchmark Spark 2026-09-16

Fecha: 2026-09-16. Estado: **evidencia medida, n=1 por perfil en el rerun**.
Objetivo: intentar demostrar que la comparación
[Silver 2026-09-16](silver-engine-comparison-2026-09-16.md)
(polars-native ~13,2 s vs spark-native ~33,5 s en large, 2,03 M entradas)
era injusta con Spark. Método: inspeccionar planes físicos, particiones,
AQE, acciones redundantes, parsing JSON, shuffles, sorts, caching, UDFs,
collections al driver, codec Parquet y JVM/cores; corregir solo problemas
confirmados sin cambiar semántica; reejecutar y comparar antes/después.

## Respuestas a las preguntas de auditoría

- **¿Shuffle innecesario?** No. El plan ejecutado (`tiny`, idéntico en
  estructura al resto de perfiles) contiene exactamente dos exchanges,
  ambos requeridos por la semántica: `hashpartitioning(event_id, 200)`
  para la ventana `row_number` por evento (selección del mínimo de
  procedencia) y `rangepartitioning(event_id, 200)` para el `orderBy`
  global por `event_id` antes de escribir. Polars paga el sort
  equivalente en un solo nodo; Spark paga shuffle distribuido en modo
  local sin obtener su ventaja (distribuir entre máquinas).
- **¿Particiones por stage?** `shuffle.partitions=200` inicial en ambos
  shuffles; AQE activado (`adaptive.enabled=true`) las fusiona en
  ejecución: particiones finales antes de escritura tiny=1, small=1,
  medium=16 ficheros, large=17 ficheros. Las 200 iniciales solo cuestan
  overhead de shuffle-write, que es parte honesta del coste Spark local.
  No se retocó: fijar menos particiones sería tunear a medida del dataset.
- **¿AQE activo?** Sí, `true` en todas las pasadas (snapshot de sesión).
- **¿Acciones repetidas?** Camino de éxito: 5 `count()` escalares (alcance,
  1 validación combinada por rama ×3, agregación de colisión) + el job de
  ventana/escritura. Cero recomputación del DAG: las 3 ramas y la unión
  están persistidas (`MEMORY_AND_DISK`, `unpersist` tras escribir). Sin
  cambios.
- **¿Python UDFs?** No: ni UDF ni pandas UDF (guardia estática en tests +
  plan físico con solo funciones built-in). Benchmark válido en esto.
- **¿`from_json` múltiple por payload?** No: un `from_json` con esquema
  cerrado por rama; el resto son proyecciones del struct. Quedan dos
  operaciones escalares simétricas a Polars: `get_json_object(...$.PC/cpv)`
  (puerta de forma array, en Polars `str.contains`) y `rlike` de presencia
  de claves de importe (en Polars `str.contains`). Sin cambios.
- **¿`collect()` grande en colisiones?** No: `limit(1).count()` y, solo en
  fallo, `distinct().limit(5).head(5)` para nombrar ids (test
  `test_success_path_never_collects_to_the_driver`). Sin cambios.
- **¿Global sort?** Sí, `orderBy(event_id)` (requerido: salida canónica
  determinista; Polars también ordena). Conteo como coste simétrico.
- **¿`coalesce(1)`?** No, nunca (guardia estática). 17 ficheros en large.
- **¿Compresión Parquet igual?** **No: problema confirmado y corregido.**
  Spark escribía snappy (defecto) y Polars zstd (defecto): mismo contenido
  lógico ocupaba 698 kB vs 145 kB (small) y 51,0 MB vs 8,8 MB (large).
  Fijado a `zstd` en ambos motores (`PARQUET_CODEC` en `silver_spark.py` y
  `bench_engines.py`, con test que bloquea la divergencia). Tras el fix:
  small 189 kB vs 145 kB, medium 1,61 MB vs 1,07 MB, large 8,97 MB vs
  8,76 MB (resto: footers × N ficheros y matices de encoding).
- **¿Heap/cores comparables?** Parcialmente: problema de reproducibilidad
  confirmado y corregido. El heap por defecto medido es **1 GiB**
  (`maxMemory=1073741824`), insuficiente para large (OOM observado). La
  repasada 8g se lanzó con `SPARK_DRIVER_MEMORY` sin registrar el
  mecanismo; ahora existe `--spark-driver-memory` (aplicado en el hijo
  fresco antes del arranque JVM: fijarlo después no redimensiona el heap)
  y las métricas guardan valor pedido + heap real. Cores: `local[16]` en
  host de 16 CPU, `defaultParallelism=16`; `executor.memory` n/a en local
  (una sola JVM).
- **¿Modo local?** Aceptado como limitación, no como defecto: la
  comparación es single-node vs single-node. La comparación distribuida
  (2–4 workers) sobre workload con shuffle pesado (linkage, ranking
  company×opportunity) queda como trabajo futuro explícito.

## Cambios aplicados (sin cambiar semántica)

- `src/tfm_licitaciones/silver_spark.py`: `PARQUET_CODEC="zstd"` y
  `.option("compression", ...)` en `write_silver_spark`.
- `src/tfm_licitaciones/bench_engines.py`: codec `zstd` explícito en los
  tres motores; flag `--spark-driver-memory`; heap JVM real y codec en el
  JSON de métricas.
- `tests/test_silver_engines.py`: pin de codec, flag CLI, nombres
  `*.zstd.parquet`, codec+heap en `run_comparison` tiny con Spark.
- `docs/benchmarks.md`: protocolo (codec, memoria driver) documentado.

No se tocó: particionado, persistencia, orden global, validaciones,
ni lógica de transformación (paridad `parity_ok` en todo).

## Antes / después (seed 7; antes = medianas v2 salvo large n=1)

| Perfil | Motor | Antes `transform_s` | Después `transform_s` | Salida antes | Salida después |
| --- | --- | --- | --- | --- | --- |
| small (25.363→22.504) | python-row | 0,970 | 1,010 | 144.718 B zstd | 144.718 B zstd |
| | polars-native | 0,255 | 0,252 | 144.718 B zstd | 144.718 B zstd |
| | spark-native | 10,002 | 10,300 | 698.184 B snappy ×1 | 189.536 B zstd ×1 |
| medium (253.577→225.004) | python-row | 10,444 | 10,519 | 1.067.623 B | 1.067.623 B |
| | polars-native | 1,590 | 1,665 | 1.067.623 B | 1.067.623 B |
| | spark-native | 16,695 | 16,617 | 6.781.021 B snappy ×16 | 1.605.903 B zstd ×16 |
| large (2.028.577→1.800.004) | python-row | 85,08 | 84,56 | 8.763.311 B | 8.763.311 B |
| | polars-native | 13,21 | 13,05 | 8.763.311 B | 8.763.311 B |
| | spark-native (8g, heap 8589934592) | 33,53 | 33,30 | 51.051.413 B snappy ×17 | 8.970.916 B zstd ×17 |

RSS y paridad sin cambios relevantes (Spark large 7,67 GiB vs 7,33;
Polars 6,09 vs 6,05; `parity_ok: true` en todo).

Reruns: `/tmp/opencode/bench-after/small-07.json`,
`medium-07.json`, `large-07.json` (comando:
`uv run --with 'pyspark==4.0.1' --with-editable . python -m
tfm_licitaciones.bench_engines --profile <p> --seed 7 --spark-driver-memory 8g
--work-dir <dir> --output <json>`; en small/medium el flag es inocuo salvo
para Spark con heap por defecto).

## Conclusión

Tras la auditoría adversarial y los fixes, los tiempos no se mueven:
**el ~2,5× de Polars sobre Spark en canonical Silver large es real para
este workload en un solo nodo**, no un artefacto del benchmark. Se acepta
el resultado sin forzar conclusión pro-Spark. Polars queda como opción
medida para Silver hasta 2,03 M entradas; Spark sigue candidato donde este
benchmark no mide (joins distribuidos, ventanas/agregaciones Gold de alta
cardinalidad), con benchmarks propios pendientes.
