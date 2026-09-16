# Benchmark Bronze-Parquet→Silver-Parquet y paridad semántica

Este documento describe el harness reproducible que fija la línea base de la
implementación canónica Bronze→Silver actual y el protocolo con el que
deberá compararse cualquier futura implementación (PySpark). No contiene
ningún resultado de rendimiento: **no existe ningún resultado hasta que el
harness se ejecute y sus métricas JSON se conserven**. Los datos y
resultados generados no se versionan en el repositorio.

## Baseline bajo medición

La implementación actual es un **baseline python-row sobre una frontera
Polars/Parquet**: recibe frames Polars pero ejecuta Python por filas
(`iter_rows`, un `json.loads` por fila, asignación de objetos
`ProcurementEvent`, agrupación en dict con comparación de
dedup/colisión y ordenación Python). No es una ejecución vectorizada
Polars. El JSON de métricas lo registra como
`"implementation": "python-row"` con el detalle correspondiente; llamar a
esta baseline "implementación Polars" sería inexacto.

## Comandos

```bash
# Perfil mínimo (cientos de filas): comprobación rápida local
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile tiny --seed 7

# Comparación local de segundos (~25k filas de entrada), reteniendo el dataset
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile small --seed 7 \
  --work-dir /tmp/bench-silver-small --output /tmp/bench-silver-small.json

# Escala de la heurística de enrutado (~250k filas). Solo manual, nunca en CI
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile medium --seed 7 \
  --work-dir /tmp/bench-silver-medium --output /tmp/bench-silver-medium.json

# Escala de planificación (~2M filas) y backfill completo (~5M filas).
# Solo manual, nunca en CI. No ejecutar sin necesidad: son perfiles definidos, sin resultados medidos.
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile large --seed 7 \
  --work-dir /tmp/bench-silver-large --output /tmp/bench-silver-large.json
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile backfill --seed 7 \
  --work-dir /tmp/bench-silver-backfill --output /tmp/bench-silver-backfill.json

# Chequeo de colisión material: la transformación debe fallar explícitamente (sin métricas)
uv run --with-editable . python -m tfm_licitaciones.bench_silver --profile tiny --seed 7 --with-collision
```

El harness es offline: no accede a la red, no lee el corpus del repositorio
y no escribe nada dentro del repositorio. Sin `--work-dir` el dataset vive
en un directorio temporal que se descarta; con `--work-dir` los part files
Bronze se conservan para su reutilización sin cambios por un futuro
benchmark Spark. Un `--work-dir` que ya contenga artefactos gestionados del
dataset (part files, `dataset.json` o el Silver medido) se rechaza con
`FileExistsError` en lugar de sobrescribirse o mezclarse con otro perfil:
use un directorio vacío nuevo. Nunca se borra contenido existente. Solo se
persisten métricas si se pasa `--output` explícito (recomendado: `/tmp`).

## Generación del dataset (excluida de la medición)

La generación emite especificaciones lógicas en orden determinista por
fases (primarias, revisiones, repeticiones, casos borde) y las escribe por
un buffer acotado (`rows_per_part`): cada buffer lleno se vuelca como un
part file Parquet y se libera. El proceso nunca mantiene el corpus en
memoria. El layout generado es:

```text
<work-dir>/dataset.json
<work-dir>/records/part-*.parquet      # esquema BRONZE_SCHEMA
<work-dir>/tombstones/part-*.parquet   # esquema TOMBSTONE_SCHEMA
<work-dir>/procurement_events.parquet  # salida de la etapa medida
```

`dataset.json` registra semilla, parámetros, conteos, número de partes y
esquemas; es el manifiesto que permite reutilizar la entrada sin cambios.

## Perfiles de dataset

| Perfil | `n_ted` | `n_placsp` | Filas de entrada aprox. | Uso |
| --- | --- | --- | --- | --- |
| `tiny` | 120 | 120 | ~310 | Pruebas unitarias y CI (milisegundos); único perfil en CI |
| `small` | 10.000 | 10.000 | ~25.000 | Comparaciones locales (segundos) |
| `medium` | 100.000 | 100.000 | ~250.000 | Escala de la heurística (~200k); solo manual |
| `large` | 800.000 | 800.000 | ~2.000.000 | Escala de planificación; solo manual |
| `backfill` | 2.000.000 | 2.000.000 | ~5.100.000 | Backfill completo; solo manual |

Ver `PROFILES` en `src/tfm_licitaciones/bench_silver.py` como referencia
autoritativa de los parámetros. Los perfiles `large` y `backfill` están
definidos pero **no ejecutados**: no existe ninguna cifra asociada a ellos.

## Cobertura del dataset sintético

- TED y OpenPLACSP, con observaciones repetidas del mismo evento fuente
  (deben deduplicarse a una fila por selección del tuple mínimo de
  procedencia) y revisiones PLACSP (mismo `atom_id`, instantes `updated`
  válidos distintos: eventos distintos con el mismo `procedure_id`);
- tombstones PLACSP como filas, incluido un control repetido que debe
  colapsar a un único evento `tombstone`;
- casos borde exactos de `Decimal(20,2)` (`0.01`, `100.00`, `1.10`,
  `123456789012.34`, límite `999999999999999999.99` con texto fuente
  retenido, importe `0`, importe malformado que permanece nulo con moneda
  nula) y de timestamps (`+01:00`, `Z`, `+00:00`, naive/inválido/ausente →
  marcador `@undated`);
- CPV con duplicados en orden fuente (deben conservar el orden estable sin
  duplicados; un orden distinto bajo la misma identidad es colisión
  material);
- con `--with-collision`, una observación TED deliberada que reutiliza un
  `ND` publicado con distinto título: la transformación debe fallar con
  `ValueError` nombrando el `event_id` en todos los motores. Es un chequeo
  de corrección, no un perfil de rendimiento (no genera métricas).

El texto fuente exacto de los importes viaja en los payloads y nunca se
altera aguas abajo: la comparación semántica exige los mismos decimales.

## Etapa medida y métricas requeridas (JSON)

La etapa medida es **Bronze-Parquet-read → `build_procurement_events` →
Silver-Parquet-write** y corre en un hijo `spawn` aislado: el `transform_s`
reportado lo cronometra el hijo (lectura, transformación y escritura
incluidas). El `peak_rss_bytes` del hijo es la marca máxima de su vida
completa (`ru_maxrss` al salir): excluye al padre y a la generación del
dataset, pero incluye el arranque del intérprete, los imports y el runtime
previos al temporizador; no es el pico de solo-transformación. El padre
observa además `wall_s` (incluye el arranque del hijo). La generación del
dataset se cronometra aparte como `generation_s` y queda **excluida** de la
comparación.

| Campo | Semántica |
| --- | --- |
| `implementation`, `implementation_detail` | `python-row` y descripción de la frontera Polars/Parquet |
| `python_version`, `polars_version` | Versiones del entorno de la baseline |
| `profile`, `profile_params`, `seed` | Dataset generado |
| `inputs.*` | `dataset_dir`, conteos (`bronze_records`, `bronze_tombstones`, `input_rows`), número de partes y `layout` de globs |
| `outputs.*` | Ruta del Silver escrito y `silver_events` |
| `stage` | Frontera de la etapa medida (lectura y escritura Parquet incluidas; generación excluida) |
| `timing_s.generation_s` | Generación del dataset (informativa, no comparable) |
| `timing_s.transform_s` | Etapa medida en el hijo aislado; es la cifra comparable |
| `timing_s.wall_s` | Tiempo de pared observado por el padre (incluye arranque del hijo) |
| `throughput_rows_per_s`, `throughput_events_per_s` | Rendimiento derivado de `transform_s` |
| `peak_rss_bytes` | Marca máxima de RSS del proceso hijo durante toda su vida (incluye arranque previo al temporizador); `null` si la plataforma no lo expone |
| `peak_rss_scope` | Alcance exacto de `peak_rss_bytes`, registrado en el propio JSON |
| `cpu_count` | Contexto de la máquina |
| `measured_at` | Timestamp operativo; nunca se compara |

## Protocolo de comparación python-row/Spark

1. Reutilizar el mismo `--work-dir` (mismo perfil y `seed`): la entrada
   Bronze es idéntica, sin regenerar.
2. Comparar `transform_s` (nunca `generation_s`) junto a conteos de
   entrada/salida, throughput y `peak_rss_bytes` del hijo (marca de vida
   completa con el alcance de `peak_rss_scope`, no pico de
   solo-transformación), sobre hardware documentado (CPU, RAM, `cpu_count`).
3. Exigir paridad semántica de las salidas con
   `src/tfm_licitaciones/silver_parity.py` (`assert_silver_parity` /
   `compare_silver_parquet`): mismo esquema canónico
   (`PROCUREMENT_EVENT_SCHEMA`), unicidad de `event_id`, mismo contenido
   alineado por `event_id` con comparaciones nativas (se ignoran el orden
   físico de filas y ficheros), sin comparar tiempos operativos.
   `ingested_at` **sí** se compara: es procedencia heredada de
   `raw_retrieved_at`, no tiempo operativo. Solo una muestra acotada de
   diagnóstico se recoge; el veredicto cubre siempre los frames completos.
4. Ejecutar el dataset `--with-collision` en ambos motores: los dos deben
   fallar con `ValueError` nombrando el mismo `event_id`.
5. Conservar cada JSON de métricas junto a la descripción de la máquina;
   citar ambos al interpretar cualquier cifra. Las estimaciones sin harness
   ejecutado no son resultados.

## Pruebas

```bash
uv run --with-editable . python -m unittest tests.test_bench_silver -v
```

Cubren a escala `tiny` el determinismo del generador, la cobertura anterior,
el writer acotado por partes, la medición aislada, el fallo de colisión
(también a través del hijo), las métricas JSON y la paridad (incluido el
orden físico irrelevante y los fallos ante contenido o esquema divergente).
No usan red y solo `tiny` corre en CI; `small`, `medium`, `large` y
`backfill` son manuales.

## Experimento de motores Silver (python-row / polars-native / spark-native)

Comparación controlada offline de tres candidatos
Bronze-Parquet→Silver-Parquet sobre el mismo dataset sintético retenido.
**Ámbito ruidoso: los candidatos nativos solo soportan el contrato Bronze
sintético TED/PLACSP del generador** (`scalar` JSON, CPV como arrays,
`updated` en segundos enteros); fallan explícitamente fuera de ese
contrato (BOE, CPV escalares, instantes con submilisegundos). No son un
reemplazo productivo de `silver.py` y PySpark sigue siendo dependencia
opcional solo del experimento (pin `pyspark==4.0.1`; no está en las
dependencias base).

- `python-row`: baseline actual (`build_procurement_events`).
- `polars-native` (`src/tfm_licitaciones/silver_polars.py`): expresiones,
  joins y agrupación Polars; cero Python por filas en el camino de éxito.
- `spark-native` (`src/tfm_licitaciones/silver_spark.py`): DataFrame Spark
  con funciones SQL built-in y ventanas; sin UDF, sin `coalesce(1)`; el
  camino de éxito no trae filas al driver (solo conteos escalares) y los
  fallos traen como máximo 5 ids para nombrarlos en el `ValueError`.
  Una sola pasada `from_json` por rama con esquema cerrado (el rollout
  inicial con ~20 `get_json_object` por fila superaba la capacidad del
  codegen whole-stage de janino).

El runner (`src/tfm_licitaciones/bench_engines.py`) genera el dataset una
vez (excluido de la medición) y mide cada motor en un hijo `spawn` fresco:
`engine_init_s` cubre imports y, solo en Spark, la creación de la sesión
(el baseline no necesita init); `transform_s` arranca justo antes de leer
el Bronze Parquet y cubre la misma frontera
lectura→transformación→escritura en todos los motores (en Spark incluye
validación, colisión y ventana sobre frames persistidos; `session.stop` y
`unpersist` son limpieza posterior al temporizador pero dentro del `wall_s`
frío). Los conteos de salida NO se miden en el hijo: el padre cuenta los
Parquet escritos (reutilizando los frames ya cargados para paridad) y
cronometra esa carga/conteo/paridad aparte como `validation_s`,
explícitamente fuera de la etapa medida. `peak_tree_rss_bytes` se muestrea
en el padre sobre todo el árbol del hijo (incluye la JVM Spark; método
`psutil` o fallback `/proc`, registrado). Spark persiste ramas y unión en
`MEMORY_AND_DISK` (ver `CACHE_STRATEGY` en `silver_spark.py`): el camino
válido es un count de alcance sobre lectura podada, una
materialización+validación por rama (cada fila Bronze se parsea una vez),
una agregación de colisión y el job de ventana/escritura; tras escribir se
hace `unpersist`. Se registran filas entrada/salida, `parquet_bytes`/
`parquet_files` (solo `*.parquet`, comparable) junto a `artifact_bytes`/
`artifact_files` (artefacto completo: en Spark añade `_SUCCESS`/CRC),
versiones, configuración Spark, hardware y paridad contra `python-row` con
`silver_parity.py`. Los timestamps Spark se leen re-etiquetando UTC sin
desplazar valores (normalización de interop documentada en el runner).
El orden de `array_distinct` se asume SOLO en el pin `pyspark==4.0.1`,
fijado por prueba unitaria de orden más paridad completa en cada
comparación.

```bash
# Smoke test tiny del harness de los tres motores
uv run --with 'pyspark==4.0.1' --with-editable . \
  python -m tfm_licitaciones.bench_engines --profile tiny --seed 7 \
  --work-dir /tmp/silver-engines-tiny --output /tmp/silver-engines-tiny.json

# Chequeo de colisión: los tres motores deben fallar con el mismo event_id
uv run --with 'pyspark==4.0.1' --with-editable . \
  python -m tfm_licitaciones.bench_engines --profile tiny --seed 7 \
  --work-dir /tmp/silver-collision --with-collision

# Pruebas (sin PySpark corren todas menos las 3 de Spark; con pin corren todas)
uv run --with-editable . python -m unittest tests.test_silver_engines -v
uv run --with 'pyspark==4.0.1' --with-editable . python -m unittest tests.test_silver_engines -v
```

Resultados autorizados: ver
[docs/experiments/silver-engine-comparison-2026-09-16.md](experiments/silver-engine-comparison-2026-09-16.md),
que contiene los resultados corregidos v2 (small/medium/large del
2026-09-16) y explica los conteos de repetición y las limitaciones. La
tabla tiny pre-optimización se ha retirado para no conservar números v1
como evidencia final.
