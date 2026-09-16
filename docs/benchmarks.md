# Benchmark Bronze-Parquet→Silver-Parquet y paridad semántica

Este documento describe el harness reproducible que fija la línea base de la
implementación canónica Bronze→Silver actual y el protocolo con el que
deberá compararse cualquier implementación candidata (Polars nativo,
PySpark). No contiene
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
benchmark de motores candidatos. Un `--work-dir` que ya contenga artefactos gestionados del
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

## Protocolo de comparación python-row/candidatos

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

## Motor de producción: Polars nativo (2026-09-16)

El motor productivo de Silver canónico es la implementación Polars nativa
(`src/tfm_licitaciones/silver_native.py`), que cubre el contrato real
completo: BOE, CPV escalar o lista, campos TED escalares o por idioma,
instantes con subsegundos y la selección PLACSP por presencia de clave. La
semántica del baseline python-row está congelada en
`src/tfm_licitaciones/silver_reference.py` como oráculo de paridad, y
`tests/test_silver_native.py` exige paridad exacta de frames y de mensajes
de fallo contra la referencia. La comparación controlada entre motores
(python-row, Polars nativo, Spark nativo) y su auditoría adversarial viven
como evidencia experimental en `experiments/silver_engine_comparison/`;
concluyen que Polars gana Silver canónico en el envelope medido de un solo
nodo y que Spark queda reservado a scale-out y joins de alta cardinalidad.

Medición de producción referencia vs motor nativo sobre el dataset
sintético retenido del generador (`bench_silver`, semilla 7, misma frontera
lectura→transformación, generación excluida del cronómetro, paridad
verificada con `silver_parity` en cada perfil):

| Perfil | Observaciones Bronze | Eventos Silver | python-row (ref) | polars-native (prod) | Aceleración |
| --- | --- | --- | --- | --- | --- |
| medium | 248.576 + 5.001 tombstones | 225.004 | 10,30 s | 3,01 s | ×3,4 |
| large | 1.988.576 + 40.001 tombstones | 1.800.004 | 85,18 s | 24,16 s | ×3,5 |

Comando reproducible (por perfil; `n_ted`/`n_placsp`/`rows_per_part` según
la tabla de perfiles del protocolo):

```bash
uv run --with-editable . python - <<'PY'
import time
from pathlib import Path
import polars as pl
from tfm_licitaciones.bench_silver import write_bronze_parts
from tfm_licitaciones.silver import build_procurement_events
from tfm_licitaciones.silver_reference import build_procurement_events_reference
from tfm_licitaciones.silver_parity import assert_silver_parity
root = Path("/tmp/silver-prod-bench")
manifest = write_bronze_parts(root, seed=7, n_ted=100_000, n_placsp=100_000, rows_per_part=10_000)
records = pl.read_parquet(str(root / manifest["layout"]["records"]))
tombstones = pl.read_parquet(str(root / manifest["layout"]["tombstones"]))
t0 = time.perf_counter(); reference = build_procurement_events_reference(records, tombstones); t_ref = time.perf_counter() - t0
t0 = time.perf_counter(); native = build_procurement_events(records, tombstones); t_nat = time.perf_counter() - t0
assert_silver_parity(native, reference)
print(f"reference {t_ref:.2f}s  native {t_nat:.2f}s  events {native.height}  parity ok")
PY
```

El coste fijo de plan del motor nativo (~0,3 s por llamada) es irrelevante
a esta escala y solo penaliza ejecuciones con muchos lotes diminutos
(documentado como seguimiento para ventanas diarias pequeñas).
