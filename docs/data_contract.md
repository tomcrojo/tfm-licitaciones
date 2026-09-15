# Contratos de datos

Este documento separa el contrato canónico ya definido de la persistencia 0.1
que todavía utiliza el pipeline. El modelo y el esquema tipado de
`silver.procurement_events` existen en código, pero TED y OpenPLACSP aún
escriben `TenderRecord` en JSONL. Esa migración se hará por límites en cambios
posteriores; no se presenta aquí como terminada.

## Raw: evidencia de recuperación por artefacto

Cada fichero que entra en Bronze requiere un sidecar `<fichero>.provenance.json`.
`RawArtifact` en `src/tfm_licitaciones/raw_provenance.py` define esta evidencia
inmutable, independiente del estado operativo de ventanas y del manifest Gold:

| Campo | Semántica |
| --- | --- |
| `version` | Versión del sidecar, actualmente `1` |
| `source` | Fuente que recuperó el artefacto (`ted`, `placsp`, `boe`) |
| `raw_path` | Ruta relativa al directorio Raw, con separadores `/` |
| `sha256` | SHA-256 hexadecimal de los bytes del fichero completo |
| `retrieved_at` | Finalización de la recuperación del artefacto; ISO 8601 con zona, escrito en UTC |
| `partition` | Identidad lógica del lote/partición, o null si no aplica |
| `window_start`, `window_end` | Límites inclusivos ISO de la ventana, ambos null si no aplica |

TED conserva su lote JSONL por ventana solicitada: `partition=YYYYMMDD-YYYYMMDD`
y `retrieved_at` corresponde al final de la recuperación del lote agregado,
antes de serializarlo. No representa la publicación del aviso ni el instante
individual de cada página HTTP. OpenPLACSP conserva el ZIP mensual, con
`partition=YYYYMM` y los límites del mes completo, incluso cuando se solicita
solo parte del mes; el timestamp se captura al terminar la transferencia.

Una descarga idéntica reutiliza el fichero y su primera evidencia. Una descarga
distinta bajo la misma identidad conserva la versión anterior y publica la
nueva como `<nombre>-<sha256>.<extensión>`, con su propio sidecar y timestamp.
Bronze comprueba ruta y checksum antes de parsear. Evidencia ausente, inválida
o que no coincide con los bytes detiene la transformación; no se inventan
rechazos de registros cuyo origen no puede verificarse. Ni la transformación,
ni el manifest del run, ni el mtime proporcionan `retrieved_at`.

Payload y sidecar se publican mediante enlaces locales sin sobrescritura; no
forman una transacción de dos ficheros. Bronze detecta ambos huérfanos, incluso
un directorio que solo contiene sidecars, y detiene el procesamiento. Una
nueva descarga puede completar una publicación interrumpida:

- Si existe solo el payload, los bytes recuperados deben tener el mismo SHA.
  El nuevo sidecar registra el timestamp real de **ese reintento**, sin atribuir
  una fecha histórica al payload huérfano. Bytes distintos fallan sin sobrescribir.
- Si existe solo el sidecar, el reintento debe coincidir en bytes, fuente,
  partición y ventana. Se repone el payload y se conserva el sidecar, incluido
  su primer timestamp; cualquier diferencia falla.

El sidecar es la raíz de confianza local. Se valida su estructura, ruta e
integridad respecto al payload, pero no puede detectarse una edición externa
semánticamente válida de su timestamp o identidad sin otra autoridad externa.

`ingestion_state` sigue siendo estado de ejecución. No se duplica allí
una segunda autoridad de recuperación: sus timestamps operativos y de
aceptación de cambios no sustituyen al sidecar. La integración de completitud
de ventanas sigue pendiente. Para datos históricos, solo una evidencia de
descarga vinculada al mismo fichero y checksum puede justificar un sidecar;
sin ella se necesita una nueva recuperación en un directorio Raw separado.

## Bronze: parsing source-specific en Parquet

`bronze/records.parquet` es la salida primaria de registros aceptados antes del
plegado de revisiones y de aplicar tombstones. `BRONZE_SCHEMA` en
`src/tfm_licitaciones/bronze.py` fija todas las columnas, incluso para un lote
vacío. No se infiere un Struct desde payloads heterogéneos:

| Campo | Tipo Polars/Parquet | Semántica |
| --- | --- | --- |
| `source` | string | Fuente autoritativa del sidecar (`ted`, `placsp`, `boe`) |
| `source_file` | string | Ruta relativa al directorio Raw del run |
| `source_member` | string/null | Nombre del miembro Atom cuando procede de un ZIP |
| `source_member_index` | uint32/null | Posición del miembro en el directorio central ZIP, desde 1, incluyendo miembros no Atom |
| `record_locator` | string | Línea JSONL (`line:N`) o posición de entrada Atom (`entry:N`), desde 1 |
| `source_record_id` | string/null | Identificador publicado; en Atom conserva el URI completo |
| `raw_sha256` | string | Checksum heredado del sidecar verificado del fichero Raw; en ZIP identifica el contenedor |
| `raw_retrieved_at` | timestamp[us, UTC] | Instante heredado del mismo sidecar, sin consultar el reloj de transformación |
| `payload_json` | string | Objeto source-specific serializado como JSON UTF-8 con claves ordenadas |

El payload se serializa una vez y se verifica su codificación UTF-8 antes de
aceptar el candidato; Parquet reutiliza esa serialización validada. Los
surrogates Unicode aislados se rechazan, incluso en campos anidados o claves.
`json.loads(payload_json)` recupera el objeto del adaptador. TED conserva sus
campos heterogéneos; Atom conserva el payload CODICE plano y `_atom_file` cuando
procede de un ZIP. Este esquema no sustituye al contrato canónico Silver.
La reproducción utiliza `source_file` + `raw_sha256` para identificar el
artefacto y su sidecar determinista, y miembro/índice/localizador para encontrar
el registro. El índice desambigua miembros con el mismo nombre dentro de un
ZIP. No se crean sidecars ni checksums independientes por miembro. Fuente y
partición/ventana del artefacto se consultan en su sidecar; no se copian ventanas
en cada fila. En la futura migración, `ProcurementEvent.ingested_at` recibirá
`raw_retrieved_at`. La persistencia Silver sigue sin cambios.

`bronze/tombstones.parquet` conserva **cada** control de borrado válido de
Atom/XML plano y ZIP, incluidos controles repetidos y snapshots anteriores.
`TOMBSTONE_SCHEMA` contiene exactamente las columnas de procedencia anteriores,
sin `payload_json`: `source_record_id` es el `ref` completo, y
`record_locator=deleted-entry:N` localiza el control desde 1, contando también
controles inválidos. Miembro, índice central ZIP, checksum y timestamp tienen
la misma semántica que en registros/rechazos; en ficheros planos el miembro y
su índice son nulos. El esquema se mantiene incluso cuando no hay controles.
Esta tabla permitirá emitir un `ProcurementEvent` con
`source_event_type="tombstone"` e `ingested_at=raw_retrieved_at` en la futura
migración Bronze→Silver; aquí todavía no se construyen eventos canónicos.

`bronze/rejections.parquet` utiliza las mismas columnas de procedencia,
más `rejection_reason` y `rejection_scope`, ambas string; no contiene
`payload_json`. Los motivos incluyen `invalid_json`, `invalid_unicode_payload`, `expected_json_object`,
`unsupported_source`, `source_mismatch`, `missing_tender_id`, `missing_title`, `invalid_atom_id`,
`invalid_xml`, `expected_atom_feed`, `invalid_zip`, `unreadable_zip_member`,
`no_atom_members`, `non_finite_amount` y `missing_tombstone_ref`. El contenido original se consulta
en Raw mediante su procedencia. Sin ID se conserva la posición; para un
documento ilegible la posición y el ID son nulos.
Si una etiqueta de fuente soportada del payload contradice la fuente del
sidecar, el registro se rechaza con `source_mismatch`. `source` y las métricas
`by_source` siempre siguen el sidecar, también ante etiquetas desconocidas o
Unicode inválido. La etiqueta conflictiva se recupera del Raw con la procedencia
del rechazo. Los errores de documento y de control también heredan checksum y
timestamp; el miembro/índice es null cuando no se llegó a abrir el contenedor.
Las corrupciones CRC, DEFLATE, BZIP2 y LZMA de un miembro se registran como
`unreadable_zip_member`, sin impedir el procesamiento de los demás miembros.

`bronze/ingestion_report.json` y `manifest.ingestion` contienen los mismos
conteos agregados y `by_source`:

- `parsed`: candidatos examinados, incluyendo los que no pueden parsearse;
  una línea JSONL no vacía o un elemento Atom `entry` es un candidato;
- `accepted`: candidatos con payload soportado, identificador y título,
  escritos en `records.parquet`;
- `rejected`: candidatos descartados con motivo y scope `record`;
- `document_errors`: documentos/contenedores ilegibles o incompatibles,
  scope `document`; no se inventa cuántas entradas contenían;
- `control_errors`: controles de borrado sin `ref`, scope `control`;
- `tombstones`: controles válidos escritos en `tombstones.parquet`, sin
  deduplicarlos ni incluirlos en `parsed`, `accepted` o `rejected`;
- `passed`: ausencia de rechazos y errores de documento/control.

Se cumple `parsed = accepted + rejected`, también por fuente, y el número de
filas en rechazos es `rejected + document_errors + control_errors`. Las líneas
en blanco y los miembros ZIP que no son Atom no son candidatos.
`placsp_entries` ahora cuenta todos los candidatos de ZIP, incluidos rechazados;
`atom_files` cuenta los documentos Atom intentados en ZIP y ficheros planos.

### Compatibilidad temporal con Silver/Gold legado

Bronze y sus métricas conservan todas las recuperaciones. La lista normalizada
que recibe el pipeline legado usa solo el snapshot más reciente de cada
`(source, partition, window_start, window_end)`, según `retrieved_at` del
sidecar. Los artefactos sin partición ni ventana siguen siendo independientes.
Un empate en el máximo timestamp con checksums distintos detiene la ejecución:
no hay evidencia suficiente para elegir una versión por nombre de fichero.
Esta selección no acredita la completitud de la descarga.

El snapshot se selecciona entero, antes del plegado de avisos y tombstones.
Así, una corrección TED/BOE no pierde frente al fichero base por orden léxico;
un ZIP corregido tampoco conserva efectos de tombstones retirados de su versión
anterior. Se mantiene el plegado por `updated` dentro del conjunto seleccionado,
con orden de recuperación como desempate. La vista legada sigue aplicando solo
tombstones de ZIP; los de Atom/XML plano ya se conservan en Bronze, pero aún
no se aplican a esta vista. No se cambia el modelo canónico ni los productos Gold.

El informe añade `superseded_artifacts` y `superseded_records` a nivel de run
para contar los artefactos y registros aceptados históricos excluidos de esa
vista. `tombstone_ids` conserva el número de refs únicos de ZIP seleccionados;
`tombstoned_removed` y `updates_folded` cuentan las operaciones legadas sobre
los registros seleccionados. Los rechazos de snapshots anteriores siguen
siendo visibles y mantienen `ingestion.passed=false`.

La aceptación Bronze comprueba la estructura y los campos identificador/título,
no certifica la validez de fechas, importes ni otros campos opcionales. Los
adaptadores históricos `parse_atom_file`, `parse_placsp_atom` e
`iter_placsp_zip` mantienen su forma de retorno para entradas válidas, pero
lanzan `ValueError` ante rechazos; el pipeline usa sus variantes con resultados
de parsing y continúa con los demás registros/documentos.

**Límite de calidad:** `quality_passed` y el código de salida de la CLI siguen
representando los gates Silver/Gold existentes. Pueden ser correctos aunque
`ingestion.passed` sea falso; deben consultarse ambos estados. Integrar los
rechazos en la decisión global de calidad queda para la tarea de quality gates,
sin cambiar aquí umbrales ni sustituir los artefactos Gold históricos.

## Silver canónico: `procurement_events`

### Grano e identidad

Una fila representa un evento o estado publicado por una fuente, no el estado
final completo de un expediente. Un procedimiento puede tener varios eventos:
anuncio, corrección, adjudicación o anulación, entre otros. Esto permite
reconstruir su evolución sin sobrescribir evidencia anterior.

Cada adaptador es responsable de construir identificadores deterministas:

- `event_id` identifica de forma única el evento dentro de la plataforma. Se
  deriva de la identidad inmutable publicada por la fuente y se prefija con la
  fuente. Si una fuente reutiliza el mismo identificador para varias versiones,
  la clave incluye su marcador de versión o timestamp de actualización.
- `procedure_id` agrupa eventos del mismo expediente cuando la fuente permite
  identificarlo. También queda prefijado por la fuente y puede ser nulo.
- Reprocesar el mismo payload debe producir el mismo `event_id`.
- Los identificadores de fuentes diferentes no se unifican en Silver. El
  linkage posterior conserva la evidencia de esa decisión.

Ejemplos ilustrativos de forma, no formatos que deban analizarse por posición:
`ted:event:123-2026` y `placsp:procedure:10000101`.

### Esquema

El orden y los tipos físicos están definidos en `PROCUREMENT_EVENT_SCHEMA`.
Solo `event_id`, `source`, `source_event_type` e `ingested_at` son obligatorios;
`cpv_codes` es una lista no nula que puede estar vacía. Los demás campos son
nulos cuando la fuente no publica el dato o su semántica todavía no puede
establecerse de forma fiable.

| Campo | Tipo Polars/Parquet | Semántica |
| --- | --- | --- |
| `event_id` | string | Clave determinista del evento, con namespace de fuente |
| `procedure_id` | string/null | Clave del expediente en esa fuente |
| `source` | string | Sistema oficial que publicó el evento |
| `source_event_type` | string | Tipo extensible y explícito, por ejemplo `notice`, `notice_revision`, `award` o `tombstone` |
| `buyer_id` | string/null | Identificador oficial del comprador cuando existe, preferentemente DIR3 en España |
| `buyer_name` | string/null | Denominación publicada del comprador |
| `title` | string/null | Título del evento o expediente |
| `description` | string/null | Descripción publicada |
| `cpv_codes` | list[string] | Todos los códigos CPV publicados, en orden estable y sin duplicados |
| `estimated_value` | decimal(20,2)/null | Presupuesto o valor estimado; no es el importe adjudicado |
| `awarded_value` | decimal(20,2)/null | Importe adjudicado cuando el evento lo publica |
| `currency` | string/null | Código ISO 4217 asociado a los importes |
| `publication_date` | date/null | Fecha oficial de publicación del evento |
| `source_updated_at` | timestamp[us, UTC]/null | Momento en que la fuente declara haber actualizado el evento |
| `deadline` | timestamp[us, UTC]/null | Fin del plazo cuando la fuente aporta hora y zona interpretables |
| `status` | string/null | Estado publicado; su armonización entre fuentes es posterior |
| `nuts_code` | string/null | Código territorial oficial; nunca se rellena desde un nombre libre |
| `country` | string/null | Código ISO 3166-1 alpha-2 |
| `source_url` | string/null | URL pública del evento o expediente |
| `ingested_at` | timestamp[us, UTC] | Timestamp de recuperación heredado de la evidencia Raw |

`publication_date` no puede derivarse de `source_updated_at`: representan
hechos diferentes. Los timestamps aceptados por el modelo deben incluir zona y
se normalizan a UTC. Los importes usan decimal de precisión fija para evitar
introducir errores binarios en agregaciones monetarias.

`source_event_type` no es todavía una enumeración cerrada porque las fuentes
publican ciclos de vida diferentes. El adaptador debe documentar los valores
que emite y no usar `status` como sustituto del tipo de evento.

### Revisiones y tombstones

Una revisión obtiene su propio `event_id`, conserva el mismo `procedure_id` y
usa un `source_event_type` explícito. Un tombstone también es una fila: puede
tener vacíos los atributos descriptivos, pero conserva identidad, fuente y
timestamps. No borra físicamente los eventos anteriores. Una vista posterior
podrá calcular el estado vigente sin perder el historial.

La implementación 0.1 todavía pliega revisiones de OpenPLACSP y elimina los
identificadores tombstoned. Esa conducta se mantiene por compatibilidad hasta
que la transformación Bronze→Silver adopte este contrato.

### Frontera de compatibilidad

`TenderRecord` continúa siendo el modelo consumido por el pipeline actual.
`procurement_event_from_tender` permite migrarlo de forma gradual, pero exige
que el adaptador entregue `event_id`, `procedure_id`, tipo de evento y las tres
decisiones temporales. No copia automáticamente `published_date`, porque en el
adaptador OpenPLACSP 0.1 ese campo procede de `updated`.

La conversión conserva el único CPV actual dentro de la lista, interpreta el
importe actual como valor estimado y no convierte el texto libre `region` en
`nuts_code`. `procurement_events_frame` genera el `DataFrame` con el esquema
completo incluso para un lote vacío, listo para la frontera Parquet ya
disponible.

El contrato no contiene supuestos sectoriales: CPV, comprador, territorio,
fechas, importes y estado sirven para obras, restauración, sanidad, logística,
energía, servicios profesionales o tecnología. Las etiquetas semánticas de
negocio pertenecen a enriquecimiento, no a esta entidad canónica.

## Silver 0.1 en producción local: `TenderRecord`

Grano actual: una versión consolidada por `(source, tender_id)`. Las revisiones
de OpenPLACSP se pliegan mediante el timestamp `updated` y los identificadores
marcados como tombstone se excluyen.

| Campo | Tipo | Regla actual |
| --- | --- | --- |
| `tender_id` | string | Identificador proporcionado por la fuente |
| `source` | string | `ted`, `placsp` o `boe`; BOE está deshabilitado en la configuración normal |
| `title` | string | Título disponible en el aviso |
| `summary` | string | Descripción disponible; cadena vacía si falta |
| `buyer` | string/null | Nombre del organismo comprador |
| `published_date` | date/null | TED: `PD`; PLACSP: actualmente deriva de `updated` |
| `amount` | number/null | Importe no negativo cuando puede interpretarse |
| `currency` | string/null | Moneda publicada; TED se marca como EUR cuando la API aporta importe normalizado |
| `country` | string/null | País o ámbito publicado |
| `url` | string/null | URL pública del aviso |
| `cpv_main` | string/null | Primer código CPV disponible |
| `buyer_id` | string/null | DIR3 cuando OpenPLACSP lo publica |
| `region` | string/null | Región textual de ejecución |
| `status` | string/null | Estado publicado por OpenPLACSP |
| `raw` | object | Payload source-specific conservado para trazabilidad |

## Gold: oportunidad 0.1

Gold aplana `TenderRecord` y añade la clasificación y el resultado del enlace:

| Campo | Tipo | Regla actual |
| --- | --- | --- |
| `category` | string | Primera categoría con keywords; fallback CPV; `Other` si no hay coincidencia |
| `category_source` | string | `keywords`, `cpv` o `none` |
| `technology_score` | int | Número de keywords distintas encontradas |
| `matched_keywords` | list | Keywords normalizadas que coincidieron |
| `dup_group` | int/null | Grupo asignado por el linkage heurístico |
| `is_canonical` | bool | Marca el representante seleccionado del grupo |
| `duplicate_of` | string/null | `tender_id` del representante actual |

Los CSV `technology_summary` y `buyer_summary` se calculan sobre registros
marcados como canónicos.

## Evaluación actual

`classifier_evaluation.json` compara la señal de keywords con prefijos CPV
inequívocos configurados. CPV actúa como proxy, no como anotación humana. La
cobertura y el soporte por clase forman parte del resultado y deben citarse al
interpretar las métricas.

La implementación actual incluye en el macro-F1 clases con soporte cero. Esa
agregación se corregirá antes de utilizarla como resultado académico; el
artefacto versionado se conserva para representar fielmente el baseline.

## Limitaciones conocidas

| Área | Situación actual | Corrección prevista |
| --- | --- | --- |
| Fechas PLACSP | `updated` se reutiliza como `published_date` | Separar publicación y actualización |
| Rechazos | Bronze los contabiliza; `quality_passed` solo evalúa Silver/Gold | Integrar el estado de ingesta en la decisión global de calidad |
| Completitud | Una partición que falla puede no impedir el run | Registrar esperadas/descargadas y estado incompleto |
| Persistencia | Bronze usa Parquet; Silver y Gold usan JSONL/CSV | Migrar las demás capas por límites a Parquet |
| TED | La configuración aplica una query tecnológica | Hacer el filtro sectorial opcional y downstream |

Estas limitaciones son trabajo pendiente conocido. No invalidan las pruebas del
comportamiento actual, pero impiden presentar todavía la versión 0.1 como la
plataforma P0 terminada.
