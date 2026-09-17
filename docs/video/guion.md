# Guion del vídeo de cinco minutos

Duración objetivo: **5:00**. Ocho diapositivas. Los intervalos incluyen las
transiciones; la duración efectiva debe comprobarse al ensayar. Las notas del
HTML reproducen exactamente los ocho bloques de narración de este documento.

Cifras del corpus de referencia con **fecha de corte 30 de junio de 2026**,
según la memoria final. No describen oportunidades abiertas hoy.

## 1. Apertura — 0:00–0:20

La información sobre contratación pública está repartida entre portales y
formatos distintos. Mi TFM construye un pipeline reproducible que la transforma
en datos preparados para analizar. Voy a explicar el problema, la arquitectura
y los resultados obtenidos con el corpus real.

## 2. El problema — 0:20–0:45

Las fuentes principales son TED y OpenPLACSP. Sus publicaciones incluyen
formatos diferentes, revisiones y anulaciones. Una misma licitación puede
aparecer varias veces y cambiar de estado. Por eso, descargar y juntar archivos
no es suficiente: hay que distinguir cada publicación del procedimiento al que
pertenece y conservar su historia.

## 3. Objetivo y aportación — 0:45–1:10

El objetivo es integrar esas fuentes en una estructura común, conservar su
procedencia y obtener una vista de oportunidades abiertas. El proyecto es
generalista: utiliza la clasificación CPV para organizar la contratación por
categorías, sin limitarse al sector tecnológico. La aportación central es
conectar el dato original con un resultado analítico verificable.

## 4. Arquitectura implementada — 1:10–2:00

El flujo se organiza por capas. Raw conserva los archivos originales y sus
metadatos de procedencia. Bronze interpreta cada formato, separa los registros
aceptados y registra rechazos y controles de borrado. Silver mantiene un
histórico de eventos, incluidas las revisiones. Polars ofrece una ruta rápida
para los lotes compatibles, con una implementación Python de respaldo. Después,
PySpark resuelve el estado vigente y genera Gold con las oportunidades abiertas.
DuckDB expone vistas SQL sobre Gold y exporta cinco CSV deterministas. La
ejecución final es local y por etapas; Airflow, Docker y el despliegue cloud
quedan fuera de esta entrega.

## 5. Resultados y reproducibilidad — 2:00–2:55

La ejecución final partió de siete archivos originales verificados. Produjo
359.304 registros aceptados en Bronze y 359.180 eventos en Silver. Tras resolver
las revisiones, quedaron 130.118 estados vigentes y 14.763 oportunidades abiertas
según la política del proyecto, con fecha de corte del 30 de junio de 2026. Son
cifras del corpus analizado, no oportunidades disponibles hoy. Repetí la cadena
completa sobre los mismos datos: los conteos coincidieron y los cinco CSV finales
fueron idénticos byte a byte, comprobados mediante SHA-256. Esto demuestra la
reconstrucción de esos productos; no demuestra cobertura completa del mercado.

## 6. Decisiones técnicas con evidencia — 2:55–3:30

El experimento de motores es distinto de esa ejecución real. Con 25.363 entradas
sintéticas, Polars, Python y Spark produjeron resultados equivalentes dentro del
contrato probado. En la ejecución conservada, Polars registró 0,254 segundos de
transformación; Python, 0,617; y Spark, 10,01. Son tiempos de esa transformación,
no del pipeline completo. Spark se utiliza después para resolver el estado
vigente y construir Gold.

## 7. Consumo en Tableau — 3:30–4:20

Para la demostración en Tableau se usa `open_opportunities.csv`, con una fila por
procedimiento y 14.763 oportunidades. Permite analizar volumen, importes y
compradores sin repetir la lógica de apertura dentro del dashboard. Hay dos
limitaciones importantes. Primero, las oportunidades no tienen fecha de
publicación, así que no se inventa una serie temporal. Segundo, la identidad de
compradores todavía puede fragmentarse por variantes nominales: por ejemplo,
«Comité Central de Compras de Navantia» y «Comité de Compras de Navantia»
aparecen como nombres distintos aunque representen al mismo comprador. El
pipeline evita fusionarlos mediante similitud sin evidencia suficiente.

## 8. Límites y cierre — 4:20–5:00

TED llega a Silver, pero sus 9.905 eventos no se incorporan a Gold porque falta
una identidad de procedimiento válida. También queda pendiente recuperar fechas
de publicación y consolidar aliases de compradores mediante identificadores
oficiales o reglas auditables. La evaluación utiliza un corpus concreto y una
fecha de corte; no demuestra operación diaria ni cobertura completa. La
aportación es una cadena funcional que conserva el histórico y genera productos
analíticos cuya reconstrucción se ha comprobado.

## Indicaciones de grabación (no se narran)

- Abrir `index.html` en el navegador. Avanzar con `→` o espacio; `F` activa
  pantalla completa. `N` muestra las notas. Mantenerlas ocultas durante la
  grabación y consultar este guion en una segunda pantalla.
- En la diapositiva 5, dejar visibles los conteos exactos mientras se narran las
  cifras. Las 359.304 filas Bronze son registros aceptados; los 1.046 controles
  de borrado se contabilizan aparte. Cada etapa tiene una granularidad distinta.
- En la diapositiva 6, distinguir el experimento sintético de la ejecución real.
  Los tiempos son `transform_s` de una ejecución conservada, no una afirmación
  universal sobre los motores ni tiempos E2E.
- En la diapositiva 7, mostrar el dashboard real solo cuando esté conectado al
  CSV definitivo. Al hablar de Navantia, señalar que el problema es de
  resolución de identidad del comprador, no de duplicación de procedimientos.
- Ensayar una vez con cronómetro y ajustar pausas antes de añadir contenido.

## Evidencia y alcance (no se narran)

- **Resultados E2E:** siete archivos Raw; 359.304 registros aceptados en Bronze,
  0 rechazos Bronze, 1.046 controles de borrado, 359.180 eventos Silver,
  130.118 estados vigentes y 14.763 oportunidades abiertas.
- **Reproducibilidad:** dos ejecuciones con los mismos conteos y el mismo SHA-256
  para los cinco CSV finales. La identidad byte a byte se refiere a esos CSV.
- **Benchmark sintético:** perfil `small`, seed 7, 25.363 entradas y 22.504
  eventos; `transform_s` = 0,2543495963 s (Polars), 0,6166623142 s (Python) y
  10,0100125452 s (Spark). Una ejecución conservada; no es el corpus real.
- **Productos:** `open_opportunities.csv` (14.763 filas), `opportunity_cpv.csv`
  (28.730), `buyer_summary.csv` (4.027 identidades analíticas),
  `cpv_summary.csv` (3.751) y `opportunities_monthly.csv` (0 filas de datos).
- **Identidad de comprador:** las identidades analíticas y los nombres distintos
  no equivalen necesariamente a entidades jurídicas distintas. Variantes
  nominales pueden fragmentar agregados. El caso observado de Navantia se
  conserva como limitación en vez de aplicar fuzzy matching agresivo.
- **Limitaciones adicionales:** 9.905 eventos TED conservados en Silver sin
  identidad de procedimiento resoluble para Gold; `publication_date` ausente en
  las 14.763 oportunidades; corpus y fecha de corte acotados.
