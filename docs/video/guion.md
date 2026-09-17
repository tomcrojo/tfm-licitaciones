# Guion del vídeo de cinco minutos

Duración objetivo: **5:00**. Ocho diapositivas. Los intervalos incluyen las
transiciones; la duración efectiva debe comprobarse al ensayar. Las notas del
HTML reproducen exactamente los ocho bloques de narración de este documento.

Cifras del corpus de referencia con **fecha de corte 30 de junio de 2026**,
según la memoria v10 de la PR #30. No describen oportunidades abiertas hoy.

## 1. Apertura — 0:00–0:20

La información sobre contratación pública está repartida entre portales y formatos distintos. Mi TFM construye un pipeline reproducible que la transforma en datos preparados para analizar. Voy a explicar el problema, la arquitectura y los resultados obtenidos con el corpus real.

## 2. El problema — 0:20–0:45

Las fuentes principales son TED y OpenPLACSP. Sus publicaciones incluyen formatos diferentes, revisiones y anulaciones. Una misma licitación puede aparecer varias veces y cambiar de estado. Por eso, descargar y juntar archivos no es suficiente: hay que distinguir cada publicación del procedimiento al que pertenece y conservar su historia.

## 3. Objetivo y aportación — 0:45–1:10

El objetivo es integrar esas fuentes en una estructura común, conservar su procedencia y obtener una vista de oportunidades abiertas. El proyecto es generalista: utiliza la clasificación CPV para organizar la contratación por categorías, sin limitarse al sector tecnológico. La aportación central es conectar el dato original con un resultado analítico verificable.

## 4. Arquitectura implementada — 1:10–2:00

El flujo se organiza por capas. Raw conserva los archivos originales y sus metadatos de procedencia. Bronze interpreta cada formato, separa los registros aceptados y registra los rechazos y controles de borrado. Silver mantiene un histórico de eventos, incluidas las revisiones. Polars ofrece una ruta rápida para los lotes compatibles, con una implementación Python de respaldo. Después, PySpark resuelve el estado vigente de cada procedimiento y genera Gold con las oportunidades abiertas. DuckDB expone vistas SQL sobre Gold y exporta cinco CSV deterministas. La ejecución es local y por etapas: una vez disponibles los datos originales, la transformación no necesita descargar de nuevo las fuentes.

## 5. Resultados y reproducibilidad — 2:00–2:55

La ejecución final partió de siete archivos originales verificados. Produjo más de 359.000 registros aceptados en Bronze y unos 359.000 eventos en Silver. Tras resolver las revisiones, quedaron 130.118 estados vigentes y 14.763 oportunidades abiertas según la política del proyecto, con fecha de corte del 30 de junio de 2026. Son cifras del corpus analizado, no oportunidades disponibles hoy. Repetí la cadena completa sobre los mismos datos: los conteos coincidieron y los cinco CSV finales fueron idénticos byte a byte, comprobados mediante SHA-256. Esta comparación demuestra la reconstrucción de esos productos; no demuestra que se haya descargado toda la contratación publicada.

## 6. Decisiones técnicas con evidencia — 2:55–3:30

El experimento de motores es distinto de esa ejecución real. Con unas 25.000 entradas sintéticas, Polars, Python y Spark produjeron resultados equivalentes dentro del contrato probado. Polars registró 0,254 segundos de transformación; Python, 0,617; y Spark, 10,01. Son tiempos de esa transformación, no del pipeline completo. La implementación conserva una ruta Python de respaldo; Spark se emplea después para resolver el estado vigente y construir Gold.

## 7. Consumo en Tableau — 3:30–4:20

Para la demostración en Tableau se ha seleccionado un único fichero: el CSV de oportunidades abiertas, con una fila por procedimiento. Sus 14.763 filas permiten analizar el volumen, los importes estimados y los principales compradores, sin repetir la lógica de apertura dentro del dashboard. Los otros cuatro CSV quedan disponibles como productos analíticos, pero no forman parte de esta demostración. Hay una limitación importante: estas oportunidades no tienen fecha de publicación, por lo que el CSV mensual solo contiene la cabecera. No se representa una evolución temporal ni se sustituyen las fechas ausentes por fechas de actualización.

## 8. Límites y cierre — 4:20–5:00

TED llega a Silver, pero sus eventos no se incorporan a Gold porque falta una identidad de procedimiento válida. Tampoco se han recuperado las fechas de publicación de estas oportunidades. La evaluación utiliza un corpus concreto y una fecha de corte; no demuestra cobertura completa ni operación diaria. Docker, Airflow y el despliegue cloud quedan fuera de esta entrega. La aportación es una cadena funcional que conserva el histórico y genera productos analíticos cuya reconstrucción se ha comprobado.

## Indicaciones de grabación (no se narran)

- Abrir `index.html` en el navegador. Avanzar con `→` o espacio; `F` activa
  pantalla completa. `N` muestra las notas sobre la misma pantalla: mantenerlas
  ocultas durante la grabación y consultar este guion en una segunda pantalla.
- En la diapositiva 5, dejar visibles los conteos exactos mientras se narran las
  cifras redondeadas. Las 359.304 filas Bronze son registros aceptados; los
  1.046 controles de borrado se contabilizan aparte. Cada etapa tiene una
  granularidad diferente: los descensos no equivalen a rechazos.
- En la diapositiva 6, distinguir el experimento sintético de la ejecución real.
  Los tiempos son `transform_s` de la ejecución conservada, no medianas de
  varias repeticiones ni tiempos E2E; tampoco certifican el rendimiento del
  guard o de la ruta de respaldo actuales.
- En el bloque 7, mostrar el dashboard real solo cuando esté conectado al CSV
  definitivo y se haya comprobado que abre correctamente. Mantener el mismo
  intervalo de 50 segundos, sin añadir una demostración adicional. Si no está
  disponible, usar la diapositiva: el HTML no incluye una captura ficticia ni
  afirma que Tableau Public esté publicado o que el extract esté validado.
- Ensayar una vez con cronómetro. Las cifras, los decimales y «SHA-256» ocupan
  más palabras al pronunciarlos; ajustar pausas o abreviar la lectura de cifras
  manteniendo los valores exactos en pantalla. No añadir contenido al guion.

## Evidencia y alcance (no se narran)

- **Resultados E2E:** memoria v10, PR #30, commit
  `63bb5b3a39679419a5d8f134afa7dca25b0cf8fb`, sección «Resultados finales».
  [Fuente LaTeX fijada a ese commit](https://github.com/tomcrojo/tfm-licitaciones/blob/63bb5b3a39679419a5d8f134afa7dca25b0cf8fb/memoria/draft/main-v10.tex).
  Se documentan siete archivos Raw, 359.304 registros aceptados en Bronze,
  0 rechazos Bronze, 1.046 controles de borrado, 359.180 eventos Silver,
  130.118 estados vigentes y 14.763 oportunidades abiertas. Se incorporan
  resultados ya documentados; esta actualización del vídeo no vuelve a
  ejecutar el pipeline ni añade una medición nueva.
- **Reproducibilidad:** las dos ejecuciones documentadas coinciden en conteos
  y SHA-256 de los cinco CSV. La identidad byte a byte se refiere a esos CSV,
  no a todos los Parquet ni a todos los manifiestos: los manifiestos de export
  difieren en la ruta de la base DuckDB de origen.
- **Benchmark sintético:**
  [JSON conservado](../../experiments/silver_engine_comparison/results/engines-small-seed7-2026-09-16.json),
  perfil `small`, seed 7, 25.363 entradas y 22.504 eventos. Una ejecución
  conservada; `transform_s` = 0,2543495963 s (Polars), 0,6166623142 s
  (Python) y 10,0100125452 s (Spark). No es una medición del corpus real.
- **Productos de exportación:** `open_opportunities.csv` (14.763 filas),
  `opportunity_cpv.csv` (28.730), `buyer_summary.csv` (4.027 identidades
  analíticas), `cpv_summary.csv` (3.751) y `opportunities_monthly.csv`
  (0 filas de datos; conserva la cabecera). El dashboard se limita al primero.
  Las identidades de `buyer_summary` no deben confundirse con un recuento de
  nombres distintos en `open_opportunities`. Contrato: [analítica](../analytics.md).
- **Limitaciones:** 9.905 eventos TED conservados en Silver, sin identidad
  de procedimiento resoluble para Gold; `publication_date` ausente en las
  14.763 oportunidades. El corpus y el corte son acotados; no se declara
  completitud del mercado, operación diaria ni despliegue en producción.
