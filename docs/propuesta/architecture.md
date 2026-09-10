# Arquitectura v1

## Vision general

La arquitectura se organiza en capas para separar ingesta, normalizacion, calidad y explotacion. Esta separacion facilita reproducibilidad, auditoria y evolucion hacia cloud.

```text
Fuentes oficiales
  -> raw
  -> bronze
  -> silver
  -> gold
  -> consumo analitico
```

## Capas

### Raw

Conserva los ficheros originales descargados desde OpenPLACSP, TED y BOE. En esta capa no se transforma contenido; solo se guardan bytes, metadatos de descarga y fecha de captura.

### Bronze

Convierte cada fuente a registros semiestructurados. La prioridad es parsear sin perder informacion, aunque los nombres de campos sigan dependiendo de la fuente.

### Silver

Normaliza licitaciones a un contrato comun (`TenderRecord`). En esta capa se estandarizan identificadores, titulo, resumen, organismo, fecha, importe, moneda, pais, fuente y URL.

### Gold

Genera tablas orientadas a consumo. La primera tabla objetivo sera `gold_opportunities`, con licitaciones normalizadas, score tecnologico, palabras clave detectadas y banderas de calidad.

## Componentes implementados en v1

- `schemas.py`: modelo comun de licitacion.
- `placsp_atom.py`: parser inicial de feeds Atom/XML compatibles con OpenPLACSP.
- `keyword_scoring.py`: scoring interpretable por palabras clave tecnologicas.
- `quality.py`: validaciones minimas de completitud y duplicados.
- `pipeline.py`: orquestacion local en memoria para transformar una muestra Atom en registros enriquecidos.

## Componentes previstos

- Persistencia con DuckDB.
- Orquestacion con Prefect o Dagster.
- Validaciones ampliadas con dbt tests, Pandera o Great Expectations.
- Dashboard o notebook final para explotacion.
- Clasificador supervisado si se consigue un conjunto etiquetado razonable.
