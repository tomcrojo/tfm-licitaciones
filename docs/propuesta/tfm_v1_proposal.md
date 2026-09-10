# Propuesta TFM v1

## Titulo provisional

Diseno e implementacion de una plataforma DataOps para la integracion, clasificacion y explotacion de licitaciones publicas tecnologicas en Espana y la Union Europea.

## Motivacion

Las licitaciones publicas contienen informacion valiosa sobre la demanda institucional de servicios tecnologicos, pero se publican en fuentes heterogeneas, con formatos variables y distintos niveles de detalle. Para una empresa de servicios de datos, cloud, inteligencia artificial, business intelligence o automatizacion, el problema practico no es solo consultar una licitacion aislada, sino disponer de un sistema reproducible que integre fuentes, normalice campos, controle calidad y genere una vista final orientada a oportunidades comerciales.

Este TFM se plantea como un proyecto de Data Engineering/DataOps. La contribucion principal no sera entrenar un modelo aislado, sino construir un pipeline extremo a extremo que deje datos publicos listos para analisis, monitorizacion y consumo posterior.

## Objetivos

El objetivo general es disenar e implementar una plataforma reproducible para integrar licitaciones publicas tecnologicas y disponibilizarlas como datos analiticos.

Objetivos especificos:

- Identificar fuentes abiertas oficiales y documentar sus formatos, cobertura y limitaciones.
- Implementar una capa de ingesta para conservar datos originales sin alterar.
- Normalizar licitaciones en un modelo comun con campos comparables.
- Aplicar controles de calidad sobre campos obligatorios, duplicados y coherencia basica.
- Construir un scoring inicial de relevancia tecnologica mediante reglas interpretables.
- Generar tablas finales listas para analisis por organismo, importe, fecha, categoria y oportunidad.
- Documentar arquitectura, reproducibilidad, errores encontrados y decisiones tecnicas.

## Alcance de la primera version

La v1 crea la base del proyecto: estructura de carpetas, documentacion, inventario de fuentes y nucleo Python para normalizacion, scoring y calidad. No descarga todavia datasets completos ni incorpora orquestacion, almacenamiento analitico o dashboard. Es una version de arranque pensada para validar el diseno antes de aumentar complejidad.

## Entregables previstos del TFM completo

El TFM final deberia incluir un repositorio reproducible, una memoria academica, una arquitectura explicada, ejecuciones registradas, datos procesados de ejemplo, tablas finales, tests de calidad y una demostracion analitica de oportunidades tecnologicas.
