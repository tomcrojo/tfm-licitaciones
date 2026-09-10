# Inventario de fuentes v1

## OpenPLACSP

Fuente oficial de datos abiertos de la Plataforma de Contratacion del Sector Publico. Sera la fuente principal para licitaciones espanolas. La documentacion publica indica el uso de conjuntos de datos abiertos y especificaciones de sindicacion en formatos reutilizables.

Uso previsto:

- ingesta de licitaciones espanolas;
- normalizacion de organos contratantes, fechas, importes y enlaces;
- identificacion de licitaciones con contenido tecnologico.

Riesgos:

- XML/Atom con campos anidados;
- cambios de estructura entre conjuntos;
- necesidad de controlar duplicados entre descargas.

## TED EU

Fuente europea para public procurement notices. La documentacion oficial de TED describe APIs y recursos para trabajar con anuncios eForms y archivos TED.

Uso previsto:

- ampliar cobertura fuera de Espana;
- comparar demanda tecnologica espanola con demanda europea;
- enriquecer categorias y vocabularios de contratacion.

Riesgos:

- mayor complejidad del modelo de datos;
- vocabulario multilingue;
- volumen superior al necesario para una primera version.

## BOE Datos Abiertos

API oficial de datos abiertos del BOE para acceder, descargar y reutilizar informacion juridica de su sede electronica.

Uso previsto:

- fuente complementaria para contexto normativo;
- posible trazabilidad de anuncios o disposiciones relacionadas;
- no debe bloquear el MVP si no aporta valor analitico claro.

Riesgos:

- podria desviar el alcance hacia analisis legal;
- conviene incorporarla solo si mejora la explicabilidad del caso de uso.
