# Guion del vídeo de cinco minutos

Duración objetivo: **5:00**. Ritmo recomendado: tranquilo, unas 125–135
palabras por minuto. Las pausas breves ya están contempladas.

## 1. Apertura — 0:00–0:20

La contratación pública publica una enorme cantidad de información, pero está
repartida entre fuentes y formatos distintos. Mi TFM aborda ese problema desde
la ingeniería de datos: he construido un prototipo reproducible que transforma
datos oficiales de licitaciones en productos analíticos trazables. En cinco
minutos explicaré el problema, la arquitectura, las decisiones técnicas y qué
he podido demostrar realmente.

## 2. El problema — 0:20–0:55

Las dos fuentes principales son TED, para avisos europeos, y OpenPLACSP, para
licitaciones españolas. No ofrecen el mismo formato ni la misma semántica: hay
JSON, ZIP, Atom y XML CODICE; también revisiones, anulaciones y campos
incompletos. Descargar datos no basta. Para analizarlos con rigor hay que
conservar el origen, contabilizar rechazos, separar fechas que significan cosas
distintas y evitar que una nueva versión destruya la evidencia anterior.

## 3. Objetivo y aportación — 0:55–1:30

El objetivo ha sido diseñar una plataforma DataOps local, reproducible y
generalista. Aunque el caso de estudio de la memoria son las licitaciones
tecnológicas, el modelo se apoya en CPV, la taxonomía oficial, y no está limitado
a un único sector. La aportación principal no es un clasificador sofisticado:
es una cadena de datos auditable, con contratos estables, procedencia y
decisiones que pueden volver a ejecutarse y revisarse.

## 4. Arquitectura implementada — 1:30–2:20

El flujo comienza en Raw, donde se conserva el payload oficial junto con su
checksum y el instante de recuperación. Bronze parsea cada formato y separa
registros aceptados, rechazados y tombstones. Silver construye un histórico
canónico de eventos de contratación, sin reducirlo prematuramente a una única
fila vigente. Después, PySpark resuelve el estado actual y genera Gold con las
oportunidades abiertas. DuckDB publica vistas SQL y finalmente se exportan CSV
deterministas para Tableau. Esta separación permite repetir una etapa sin
reinterpretar las anteriores y mantiene la lógica fuera de la herramienta de
visualización.

## 5. Trazabilidad y calidad — 2:20–3:00

Una decisión clave es no ocultar los datos problemáticos. Cada rechazo queda
contabilizado con su motivo y localización. Los identificadores se derivan de
claves oficiales; las revisiones y tombstones permanecen en Silver; y Gold solo
publica una oportunidad como abierta cuando existe evidencia suficiente. Por
eso, obtener cero oportunidades con los fixtures de prueba no es un fallo: es
el comportamiento esperado de una política conservadora. Un informe verde no
debe fabricarse filtrando silenciosamente lo que no encaja.

## 6. Decisiones técnicas con evidencia — 3:00–3:45

También comparé motores para la transformación Silver. En el experimento
versionado, con una carga sintética de 25.363 entradas Bronze y 22.504 eventos
Silver, las tres implementaciones produjeron resultados equivalentes dentro
del contrato medido. Polars nativo tardó 0,254 segundos; la referencia Python,
0,617; y el candidato Spark, 10,01 segundos. Esto no demuestra que Polars sea
siempre mejor ni que Spark no sea útil. Demuestra algo más defendible: para
esta transformación local y este volumen, Polars fue la opción medida más
rápida; Spark se reserva para ventanas, joins y Gold, donde su modelo sí aporta.

## 7. Resultado demostrable — 3:45–4:30

El resultado es una cadena ejecutable desde CLI y probada con fixtures locales,
sin red: Raw, Bronze y Silver; estado vigente y Gold en Parquet; vistas DuckDB;
y cinco exportaciones CSV preparadas para Tableau. Además, el proyecto incluye
la referencia CPV completa, con 9.454 códigos medidos, y una dimensión DIR3
construida desde seis ámbitos oficiales. Los manifests registran versiones,
conteos y métricas para poder relacionar cada producto con la ejecución que lo
generó.

## 8. Límites y cierre — 4:30–5:00

El prototipo no es una plataforma en producción. La completitud incremental de
ventanas, Airflow, los contratos menores, el enriquecimiento semántico y el
despliegue cloud quedan como trabajo futuro. La conclusión es que la utilidad
analítica depende primero de una base reproducible y trazable. Este TFM aporta
esa base: integra fuentes heterogéneas, conserva la historia y convierte datos
oficiales en productos analíticos sin perder la evidencia necesaria para
defender cada resultado.

## Ensayo

- Ensayar una vez con cronómetro y ajustar solo pausas, no añadir contenido.
- Mostrar las diapositivas a pantalla completa; avanzar con `→` o espacio.
- Pulsar `N` si se necesitan las notas y `F` para entrar o salir de pantalla
  completa.
- Mantener la diapositiva 6 visible mientras se dicen las tres cifras del
  benchmark y remarcar que es una carga sintética acotada.
