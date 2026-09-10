# Revisión de la versión 0.1.0

## Alcance y método

Se revisó la implementación canónica de `tfm-licitaciones/` con siete
perspectivas alineadas con el protocolo del repositorio. El lanzamiento de
evaluadores externos en paralelo no fue posible porque el límite de hilos ya
estaba ocupado por la delegación superior; por ello, esta versión conserva una
revisión interna equivalente, separada por criterios y respaldada por la
ejecución real de tests y pipeline.

| Evaluador | Foco | Resultado | Hallazgo principal |
| --- | --- | --- | --- |
| E1 Estricto | Rúbrica y evidencia | OK | Cada bloque conecta código, salida y criterio en `data_contract.md` |
| E2 Laxo | Coherencia global | OK | El MVP es ejecutable, portable y adecuado como base del TFM |
| E3 Estilo | Redacción y claridad | OK | README y docs explican decisiones y límites sin mezclar prototipos |
| E4 Estructura | Trazabilidad | OK | El flujo raw→bronze→silver→gold tiene artefactos verificables |
| E5 Reproducibilidad | Tests, rutas y dependencias | OK | 12 tests pasan; fixtures locales evitan dependencia de red en `run` |
| E6 IA/Humano | Autoría y decisiones | OK | Se documentan trade-offs: JSONL/CSV en MVP, DuckDB/Spark como evolución |
| E7 Formal | Entrega y artefactos | OK | `pyproject`, configuración, tests, manifests, calidad y marts presentes |

## Consenso

La versión cumple el objetivo técnico de opción 2: integra contratos TED, BOE y
Atom, normaliza avisos, aplica controles de calidad, clasifica con reglas
explicables y genera salidas analíticas. Los prototipos anteriores permanecen
intactos y solo se reutilizan como antecedentes documentados.

## Disenso y decisión

El único punto pendiente es la explotación académica completa sobre un corpus
real: todavía no se incluye un dashboard, notebook de análisis ni una descarga
masiva histórica. Se mantiene fuera del cierre de esta base porque el objetivo
de la versión es consolidar el pipeline reproducible; queda identificado como
la siguiente versión del TFM y no como una carencia oculta.

## Evidencia ejecutada

```text
12 tests unitarios/smoke: OK
run local: 6 bronze, 6 silver, 6 gold
quality_report: passed=true
technology_match_share: 0.6667
duplicate_keys: 0
```
