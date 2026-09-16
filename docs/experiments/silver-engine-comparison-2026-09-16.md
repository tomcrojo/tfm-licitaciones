# Comparación de motores Silver 2026-09-16 (small / medium / large)

Fecha: 2026-09-16. Estado: **exploratorio**. Los candidatos nativos solo
cubren el **contrato sintético TED/PLACSP** del generador y **no son
reemplazos productivos** de `silver.py`. La pasada `large` es
exploratoria **n=1** por motor. Este informe solo usa los artefactos
`v2-*` listados abajo; no incluye números v1 anteriores.

## Ámbito y frontera medida

Comparación offline Bronze-Parquet → Silver canónico → Parquet entre:

- `python-row`: baseline actual (`build_procurement_events`, Python por filas);
- `polars-native`: expresiones/joins/agrupación Polars, una única
  decodificación JSON tipada por rama fuente;
- `spark-native`: DataFrame Spark con funciones built-in y ventanas, sin
  UDF, sin `coalesce(1)`, con persist `MEMORY_AND_DISK` y `unpersist` tras
  escribir.

Frontera idéntica por motor en un hijo `spawn` aislado: lectura del Bronze
Parquet → transformación canónica → escritura del Silver Parquet.
`transform_s` excluye la inicialización (`engine_init_s`: imports y, solo
en Spark, creación de sesión) y la limpieza posterior. Generación del
dataset (`generation_s`) y conteo/paridad del padre (`validation_s`)
están excluidos de la comparación.

## Artefactos leídos

`v2-small-0{1,2,3}.json` (seeds 7/11/19), `v2-medium-0{1,2,3}.json`
(seeds 7/11/19), `v2-large-local-01.json` (solo motores locales),
`v2-large-spark-8g-01.json` (baseline + Spark con `driver.memory=8g`),
bajo `/mnt/pop-work/tfm-engine-experiment-results/2026-09-16/`.
Conteos idénticos entre repeticiones: small 25.363 entradas / 22.504
eventos; medium 253.577 / 225.004; large 2.028.577 / 1.800.004.
Paridad `parity_ok: true` en todos los motores y todas las pasadas
medidas contra la referencia `python-row`.

## Hardware / software / configuración

Host único Linux x86_64, 16 CPU, 32 GiB RAM. Python 3.11.16, Polars
1.44.2, PySpark/Spark 4.0.1, OpenJDK 17.0.19. Spark `local[16]`,
`session.timeZone=UTC`, `shuffle.partitions=200`, adaptativo activado,
`ansi=false`, resto por defecto (heap de driver por defecto salvo en la
repasada `8g`, que fija `spark.driver.memory=8g`). RSS por `procfs` cada
20 ms.

## Small (25.363 entradas, 3 repeticiones; mediana y min–max)

| Motor | `transform_s` mediana (min–max) | `init_s` mediana | Pico RSS árbol mediana (min–max) |
| --- | --- | --- | --- |
| `python-row` | 0,970 (0,937–0,999) | ~0,000 | 215.363.584 B / 205,4 MiB (215.142.400–215.486.464) |
| `polars-native` | 0,255 (0,246–0,257) | 0,0002 | 299.524.096 B / 285,7 MiB (296.693.760–304.979.968) |
| `spark-native` | 10,002 (9,858–10,018) | 3,569 | 1.340.338.176 B / 1,25 GiB (1.235.918.848–1.376.182.272) |

Salida: 144.718 B (1 fichero) en motores locales; Spark 698.184 B
(1 fichero Parquet) + artefacto 703.656 B (4 ficheros con `_SUCCESS`).

## Medium (253.577 entradas, 3 repeticiones; mediana y min–max)

| Motor | `transform_s` mediana (min–max) | `init_s` mediana | Pico RSS árbol mediana (min–max) |
| --- | --- | --- | --- |
| `python-row` | 10,444 (10,357–10,659) | ~0,000 | 1.075.216.384 B / 1,00 GiB (1.060.708.352–1.076.924.416) |
| `polars-native` | 1,590 (1,588–1,682) | 0,0002 | 1.137.516.544 B / 1,06 GiB (1.119.838.208–1.143.193.600) |
| `spark-native` | 16,695 (16,573–16,895) | 3,490 | 1.988.710.400 B / 1,85 GiB (1.983.381.504–2.039.742.464) |

Salida: 1.067.623 B (1 fichero) en motores locales; Spark 6.781.021 B
(16 ficheros) + artefacto 6.834.165 B (34 ficheros).

## Large (2.028.577 entradas, exploratorio n=1)

| Pasada / motor | `init_s` | `transform_s` | `wall_s` | Pico RSS árbol | Salida Parquet | Paridad |
| --- | --- | --- | --- | --- | --- | --- |
| local / `python-row` | ~0,000 | 85,08 | 85,42 | 7.434.747.904 B / 6,93 GiB | 8.763.311 B (1) | referencia |
| local / `polars-native` | 0,0002 | 13,21 | 13,95 | 6.499.405.824 B / 6,05 GiB | 8.763.311 B (1) | ok |
| 8g / `python-row` | ~0,000 | 84,98 | 85,33 | 7.413.710.848 B / 6,90 GiB | 8.763.311 B (1) | referencia |
| 8g / `spark-native` | 3,505 | 33,53 | 37,82 | 7.867.715.584 B / 7,33 GiB | 51.051.413 B (17) + artefacto 51.450.425 B (36) | ok |

Fallo observado con heap por defecto: el Spark `large` con heap de
driver por defecto murió por OOM antes de persistir métricas — no hay
JSON ni directorio de salida Spark en `v2-large-01-data/` (sí están el
dataset y las salidas de los dos motores locales). **No se registra
duración ni pico RSS de esa pasada fallida porque el runner no los
persistió; inventarlos sería fabricar evidencia.** La repasada con
`driver.memory=8g` (`v2-large-spark-8g-01.json`) sí completó con paridad
ok.

## Advertencia de medición (RSS)

`peak_tree_rss_bytes` es el pico muestreado por el padre de la suma de
RSS de todo el árbol del hijo (incluye la JVM Spark), con sondeo cada
20 ms: incluye arranque del intérprete/motor, y los picos breves entre
sondeos pueden perderse. A la vez, sumar RSS puede contar páginas
compartidas más de una vez. Por tanto es un indicador consistente para
estas pasadas, no una medida exacta de memoria física única; cubre el
árbol completo, a diferencia de `ru_maxrss` del hijo solo.

## Lectura crítica

- En este host único y para esta forma de canonicalización (parseo JSON
  por aviso, deduplicación por identidad determinista, ordenación por
  `event_id`), **Polars es la opción medida hasta 2,03 M de entradas**:
  ~3,8× el baseline en small, ~6,6× en medium, ~6,4× en large, con
  paridad exacta y el menor pico RSS en large.
- El umbral de filas por sí solo no justifica Spark aquí: con heap por
  defecto ni siquiera completa large, y con 8g queda 2,5× por detrás de
  Polars en `transform_s` (33,53 frente a 13,21) con mayor RSS (7,33
  frente a 6,05 GiB).
- Spark sigue siendo candidato para formas que este benchmark no mide:
  joins distribuidos entre fuentes, ventanas y agregaciones Gold de alta
  cardinalidad. Esas formas requieren benchmarks propios y ajuste de
  memoria/particionado en producción; nada de este informe las acredita.

## Nota metodológica

El primer borrador Polars llamaba a `str.json_path_match` por campo
(reparseando cada payload muchas veces) y resultó una comparación
injusta en la primera pasada medium. Se diagnosticó, se convirtió el
motor a **una única decodificación JSON tipada por rama fuente** y ese
borrador quedó **excluido de los resultados finales**: todas las cifras
de este informe corresponden al motor de decodificación única.
