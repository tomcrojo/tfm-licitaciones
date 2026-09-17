# Gold/Spark contract foundation

This document freezes the **boundary and interfaces** for the next stage of the
pipeline. It does not migrate Gold 0.1 and it does not claim that current-state
or the Gold marts are implemented.

> **Merge dependency:** PR #19 must not merge before PR #20 (`fix: preserve
> PLACSP tombstone source time`) is merged and this branch is rebased onto that
> result. The temporal semantics below describe the intended post-#20 upstream
> contract; keeping the dependency explicit prevents the pre-#20 undated
> tombstone limitation from surviving by accident.

## Boundary

The production-facing boundary is:

```text
canonical Silver Parquet
    -> current-state / enrichment in PySpark
    -> Gold Parquet
```

`silver/procurement_events.parquet` remains the only input contract for this
stage. This PR does not redefine Bronze or Silver business semantics; it consumes
the canonical Silver contract, including the source-dated PLACSP tombstone
semantics introduced by PR #20 after rebase. No production code under this
foundation imports the experimental Silver Spark candidate.

Parquet remains the persisted boundary between stages. Processing timestamps,
Spark part-file names and task IDs are never data identity.

## Runtime foundation

The supported local runtime is **PySpark 4.0.1**, matching the version pinned by
the retained Silver engine experiment. PySpark remains an explicit execution
extra rather than a base dependency:

```bash
uv run --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest tests.test_spark_foundation -v
```

`tfm_licitaciones.spark_foundation` provides:

- `create_spark_session(...)`: controlled local session, UTC timezone, AQE on,
  explicit shuffle partitions and zstd Parquet;
- `canonical_silver_schema()`: Spark representation generated from the guarded
  engine-neutral canonical Silver contract;
- `read_canonical_silver(...)`: validates the inferred physical Parquet schema
  before applying the explicit canonical schema, then enforces required values
  and non-null array elements;
- `schema_from_fields(...)`: mapping from engine-neutral contracts to Spark;
- `assert_contract_schema(...)`: field/type/order validation plus value-level
  non-null checks for required columns and array elements;
- `write_typed_parquet(...)`: contract validation and Parquet write, with an
  optional pre-write sort operation.

Spark may choose different physical part-file names, partition boundaries,
filesystem enumeration order or byte layouts between runs. `order_by` sorts the
DataFrame before the distributed write but does **not** promise a globally
ordered sequence across output part-files. Determinism here means stable schema
and stable data semantics; consumers must not use part-file order or names as
identity.

The canonical Silver Spark schema is not an unguarded copy. Field order and
logical contract live in `gold_contract.CANONICAL_SILVER_FIELDS`; the offline
suite pins the full contract and maps the actual Polars dtypes from
`models.PROCUREMENT_EVENT_SCHEMA` to the engine-neutral logical types. The Spark
suite checks the resulting types/nullability and includes a real production-like
Polars -> Parquet -> Spark boundary test. Parquet nullability metadata is
deliberately not treated as authoritative because Spark relaxes it on
round-trip; required values are validated against actual rows.

## Current-state intermediate contract

Dataset name: `current_state`.

**Grain:** one row per source `procedure_id`, but only after that procedure can
be resolved deterministically from canonical Silver. `procedure_id` is already
source-namespaced, so no cross-source entity merge is implied.

Version 1 field order is defined by
`gold_contract.CURRENT_STATE_FIELDS`:

| Field | Type | Null |
| --- | --- | --- |
| `procedure_id` | string | no |
| `event_id` | string | no |
| `source` | string | no |
| `source_event_type` | string | no |
| `buyer_id` | string | yes |
| `buyer_name` | string | yes |
| `title` | string | yes |
| `description` | string | yes |
| `cpv_codes` | array<string> | no |
| `estimated_value` | decimal(20,2) | yes |
| `awarded_value` | decimal(20,2) | yes |
| `currency` | string | yes |
| `publication_date` | date | yes |
| `source_updated_at` | timestamp UTC | yes |
| `deadline` | timestamp UTC | yes |
| `status` | string | yes |
| `nuts_code` | string | yes |
| `country` | string | yes |
| `source_url` | string | yes |
| `ingested_at` | timestamp UTC | no |
| `is_deleted` | boolean | no |

All canonical fields describe the selected canonical event. `event_id` and
`ingested_at` therefore preserve the provenance of that selected Silver row.
`is_deleted` is a current-state attribute and is not added back to Silver.

`CURRENT_STATE_SCHEMA_VERSION` and `GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION` are
code-level contract versions in this foundation. No current-state/Gold Parquet
writer exists yet, so this PR does not invent an on-disk manifest format merely
to persist them. The implementation PR must persist the schema version in the
Gold/current-state build manifest before these datasets become production
artifacts.

### Temporal semantics inherited from canonical Silver after PR #20

PR #20 preserves the authoritative OpenPLACSP Atom tombstone `when` value in
Bronze as `source_deleted_at` and maps it into the existing canonical Silver
`source_updated_at` field for dated tombstone events. Therefore Gold/current-
state must use the following rules:

- A PLACSP tombstone with a valid source `when` is a source-dated deletion event.
  Its `source_updated_at` is authoritative for source event ordering at the
  canonical microsecond precision.
- Separate delete instants for the same published tombstone ref remain distinct
  historical events. Repeated observations of the same `(ref, source time)` may
  collapse only under the existing canonical provenance semantics.
- A tombstone with no published `when` remains valid **undated** deletion
  evidence. Its order relative to notices/revisions must not be invented from
  `ingested_at` or retrieval provenance.
- A published but unusable `when` remains undated deletion evidence **and** is
  observable upstream through the Bronze `invalid_tombstone_when` control
  rejection. Downstream logic must not silently treat malformed published time
  as equivalent to a trustworthy timestamp.
- `ingested_at` remains retrieval provenance only. It must never decide business
  recency or whether a tombstone wins.

### Resolution semantics that are already fixed

- Silver is the historical source of truth. A current-state builder must not
  delete or rewrite Silver history.
- Canonical duplicate/collision semantics are inherited from Silver:
  `event_id` must already be unique. Current-state must fail on a malformed
  input boundary instead of inventing another deduplication policy.
- Cross-source linkage is downstream enrichment. It must not change
  `procedure_id` grouping in this intermediate.
- Rows without `procedure_id` are **not eligible for `current_state`**. They
  remain valid canonical Silver history and must be surfaced by a future
  unresolved/issues output rather than assigned a synthetic procedure key.

### Decisions intentionally left unresolved

Even after PR #20, the contract does not contain enough verified semantics to
implement a correct general resolver yet:

1. **Notice/tombstone procedure identity compatibility.** PR #20 retains the
   published tombstone ref as the source procedure identity, while PLACSP notice
   procedure identity originates from the notice Atom identity. This foundation
   does **not** assume those published strings identify the same procedure in all
   real feeds. Before `is_deleted` can be computed, a follow-up must establish
   that relationship from real source evidence/fixtures or define an explicit
   normalization rule. A resolver must surface an issue rather than silently
   join on an unverified equality assumption.
2. **Undated event ordering.** Source-dated PLACSP revisions and source-dated
   tombstones have an authoritative timestamp after PR #20. The policy for
   genuinely undated PLACSP events and multi-event TED procedures remains
   unresolved. `publication_date` is a date, not a universal revision clock.
3. **Incomplete histories.** A corpus may begin after a procedure already
   existed. The resolver must define whether a lone revision/tombstone is
   sufficient evidence for state.
4. **Unresolved output contract.** A follow-up must freeze
   `current_state_issues` (at minimum reason + `event_id`/`procedure_id`
   provenance) before the resolver is enabled. It must include reasons capable
   of distinguishing genuinely undated ordering from other resolution failures.

Because these decisions are not fixed, this PR does **not** expose a
`build_current_state` implementation that guesses them.

## Gold principal contract

Dataset name: `open_opportunities`.

**Grain:** one row per resolved source procedure that a future Gold builder can
prove is open/actionable under an explicit harmonized status/deadline policy.

Version 1 field order is defined by
`gold_contract.GOLD_OPEN_OPPORTUNITIES_FIELDS`. It carries procedure/event
identity plus the canonical business fields required by downstream consumers.
It intentionally contains no ranking score, ML label, keyword category or
legacy linkage columns: those semantics are not implemented by this PR.

The remaining Gold decision before this dataset can be built is the explicit
definition of **open/actionable** across sources (status normalization, deadline
handling and treatment of missing values). Until then, no row should be
published merely because it is the latest resolvable event.

## Planned dimensions and marts

These are planned consumers of the boundary, not outputs of this PR:

- `dim_cpv`: official CPV reference, keyed by CPV code;
- `dim_buyers`: official buyer identity/enrichment, preferring DIR3 where
  available;
- `dim_regions`: NUTS/reference geography;
- `open_opportunities`: principal opportunity mart described above;
- `minor_contract_signals`: separate historical signal mart;
- `daily_candidate_feed`: company/profile candidate ranking with stored,
  explainable score components.

The legacy `OpportunityRecord`, keyword classifier, JSONL/CSV Gold 0.1 and
`fold_latest_updates` are historical semantics only. They are not silently
ported into these contracts.

## Stable next-PR interfaces

The following interfaces are intended to be stable after the required #20
rebase:

```text
tfm_licitaciones.gold_contract
  CANONICAL_SILVER_FIELDS
  CURRENT_STATE_FIELDS
  GOLD_OPEN_OPPORTUNITIES_FIELDS
  CURRENT_STATE_SCHEMA_VERSION
  GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION

tfm_licitaciones.spark_foundation
  PYSPARK_VERSION
  create_spark_session
  canonical_silver_schema
  read_canonical_silver
  schema_from_fields
  assert_contract_schema
  write_typed_parquet
```

The intended future CLI surface is:

```text
licitaciones-pipeline build-gold
  --silver-dir <path>
  --gold-dir <path>
```

It is deliberately **not registered yet**: a command that cannot resolve
current-state safely would be a fictitious production interface. The PR that
implements the resolver should add it once the unresolved decisions above are
closed.

## Follow-ups unlocked

1. Merge PR #20, rebase #19 onto it and rerun the full offline/Spark/experiment
   suites before #19 can merge.
2. Establish PLACSP notice/tombstone procedure identity compatibility from real
   source evidence; freeze `current_state_issues` and remaining undated ordering
   policy.
3. Persist current-state/Gold schema versions in the build manifest and
   implement Spark current-state resolution against the contracts above.
4. Add CPV/DIR3/NUTS enrichments as explicit joins.
5. Implement `open_opportunities` and then the remaining Gold marts.
6. Consolidate the `pyspark==4.0.1` runtime pin into one packaging/CI source of
   truth when the executable Gold path (`build-gold`) is introduced.
7. Register `build-gold` only when the resolver and Gold semantics are
   executable rather than placeholder behavior.
