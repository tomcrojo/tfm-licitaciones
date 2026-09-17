# Comparación de motores Silver

Experimento offline de Bronze Parquet → transformación canónica → Silver
Parquet. Compara una referencia Python por filas congelada y candidatos
nativos Polars/PySpark sobre datos sintéticos TED/PLACSP. Ningún módulo
productivo importa los candidatos de este directorio.

## Alcance y método

Los candidatos admiten payloads escalares, CPV como listas de cadenas e
instantes con segundos enteros. La paridad medida no cubre BOE ni fracciones
de segundo y no acredita el contrato completo de producción.

Cada motor ejecuta en un proceso hijo `spawn` nuevo sobre el mismo dataset.
`transform_s` incluye lectura, transformación y escritura; excluye generación,
inicialización del motor y validación del padre. `wall_s` y `engine_init_s`
se registran aparte. Todos escriben Parquet zstd. Spark usa funciones nativas,
sin UDF ni `coalesce(1)`, y persistencia `MEMORY_AND_DISK` durante la medición.

El RSS se muestrea cada 20 ms sobre el árbol de procesos, incluida la JVM.
Puede perder picos breves y contar páginas compartidas varias veces. En un
cluster externo, los ejecutores ajenos al árbol no quedan incluidos.

## Evidencia conservada

El [resultado small, seed 7, del 16 de septiembre de 2026](results/engines-small-seed7-2026-09-16.json)
registra 25.363 entradas Bronze y 22.504 eventos Silver, con paridad en los
tres motores. Es una ejecución, no una mediana ni una estimación del pipeline:

| Motor | `transform_s` | Inicialización | Pico RSS del árbol |
| --- | --- | --- | --- |
| Python por filas | 0,617 s | <0,001 s | 360.960.000 B |
| Polars nativo | 0,254 s | <0,001 s | 302.309.376 B |
| Spark nativo | 10,010 s | 3,473 s | 1.335.799.808 B |

Entorno registrado: Linux x86_64, 16 CPU, unos 32 GiB de RAM, Python 3.11.16,
Polars 1.44.2, Spark 4.0.1 y Java 17.0.19; Spark `local[16]`, heap 1 GiB.
El JSON conserva sus rutas temporales originales como metadatos históricos,
no como requisitos para reproducirlo. No contiene el SHA exacto del runner de
esa ejecución: es una limitación de su procedencia.

Polars fue más rápido en esta carga. El resultado no mide la guarda híbrida
productiva, Gold, joins entre fuentes ni escalabilidad multi-host. Los antiguos
informes medium/large y las auditorías dependían de artefactos externos no
incluidos; se pueden consultar en Git, pero sus cifras no se presentan como
evidencia reproducible de esta entrega.

## Referencia y carga congeladas

`engines/python_row_reference.py` conserva las transformaciones y contenedores
de datos del commit `e69016f62fb985639e17153e24b3a570f2f39c20`, sin importar
la fachada Silver productiva. Así, la etiqueta `python-row` sigue midiendo el
mismo algoritmo aunque cambie el motor del pipeline. Es distinta de la
referencia que producción usa como fallback.

El harness comparte los constructores Bronze y el comparador de paridad.
`workload.py` fija una huella de los valores lógicos Bronze para `tiny` y
`small`, seed 7, frente al generador histórico. Comprueba payload y procedencia,
no los bytes físicos Parquet. Otros perfiles y semillas son exploratorios:
se registran como `workload_pinned=false`, sin atribuirles el commit histórico.
Las pruebas comprueban aislamiento, huella, esquema, paridad y colisiones.

## Reproducción

Desde la raíz del repositorio, con Java 17+ para Spark:

```bash
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest discover -s experiments/silver_engine_comparison/tests -t . -v
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m experiments.silver_engine_comparison.bench_engines \
  --profile small --seed 7 --spark-master 'local[16]' \
  --work-dir /tmp/silver-engines-small --output /tmp/silver-engines-small.json
```

Los resultados nuevos deben conservarse aparte del JSON histórico. La suite
normal de `tests/` no descubre este directorio. Sin PySpark, las pruebas del
candidato Spark se omiten.

El harness `bench_silver`, distinto de esta comparación, mide la referencia
Python por filas de producción (`silver_reference`), sin la guarda ni el kernel
híbrido. También sirve como comprobación rápida de generación y persistencia:

```bash
uv run --locked --with-editable . python -m tfm_licitaciones.bench_silver \
  --profile tiny --seed 7
```

Perfiles disponibles: `tiny`, `small`, `medium`, `large`, `backfill`; los grandes
son ejecuciones manuales con necesidades de memoria crecientes. `--with-collision`
introduce un conflicto material y debe hacer fallar la transformación.
