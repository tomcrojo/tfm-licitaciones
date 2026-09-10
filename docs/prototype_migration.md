# Evolución desde los prototipos

Los prototipos anteriores (`tfm-codex-1`, `tfm-glm-1`) permanecen en el
repositorio privado original y no se copian aquí para no crear versiones
divergentes. Lo que aportó cada uno:

- `tfm-codex-1`: contrato `TenderRecord`, parser Atom, scoring por keywords
  y validaciones unitarias.
- `tfm-glm-1`: adaptadores TED/BOE, arquitectura medallion, configuración de
  calidad y marts analíticos.

La carpeta actual no importa módulos de ninguno de los dos prototipos. Se han
reexpresado las decisiones en el paquete `tfm_licitaciones` para que exista un
único punto de entrada del TFM. Las diferencias intencionadas son:

1. El contrato y el nombre del paquete ya no dependen del prototipo que los
   originó.
2. La persistencia MVP usa JSONL/CSV para eliminar la dependencia inicial de
   DuckDB y hacer el run portable; DuckDB queda como evolución de serving.
3. Los adaptadores live están aislados en `fetch.py`, mientras `run` procesa
   siempre ficheros raw ya capturados.
4. La calidad se evalúa sobre todas las fuentes integradas y sus resultados se
   guardan como artefacto de entrega.
