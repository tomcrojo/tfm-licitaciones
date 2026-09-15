# AGENTS.md

## Purpose

This repository is a UCM Master's TFM about a reproducible DataOps platform for public-procurement intelligence. The goal is to build a system that can ingest heterogeneous official procurement sources, preserve raw lineage, normalize them into stable canonical entities, enrich them, apply quality controls, and publish reproducible analytical products that can feed a lightweight opportunity-discovery UI.

The implementation must remain understandable and defensible by the author. Agent throughput is useful only when changes stay reviewable.

## Context loading

Always read `AGENTS.md` first. Then load only the context needed for the assigned change instead of reading the whole repository by ritual.

Useful sources of truth when relevant:

- `README.md` for the current project surface and commands;
- `docs/architecture-v2.md` once merged, otherwise `docs/architecture.md`, for architecture decisions;
- `docs/data_contract.md` for schema/data-model changes;
- the directly affected implementation and tests;
- `docs/propuesta/propuesta-enviada.md` when a change could alter the academic scope of the TFM.

Do not assume an old README or prototype document overrides newer architecture decisions. If relevant documents disagree in a way that blocks the task, surface the conflict rather than silently choosing one.

## Source-of-truth order

For project decisions, use this precedence:

1. Explicit task/prompt for the current PR.
2. `AGENTS.md` working rules.
3. Current architecture v2, once merged.
4. Data contract and run documentation.
5. Current code and tests.
6. Historical/prototype documents.

The approved TFM proposal constrains the academic scope, even when implementation details evolve.

## Development model

The repository is developed through small, conventional pull requests.

Each agent should normally deliver exactly one targeted PR representing one feature, correction, migration step, dataset integration, experiment, or documentation change.

### Default rules

- Branch from the latest `main`, unless the task explicitly says to update an already-open PR.
- Do not create stacked PRs by default.
- Do not bundle unrelated refactors.
- Do not opportunistically fix neighboring issues.
- If a necessary prerequisite is missing, either implement the smallest prerequisite inside scope or stop and describe the follow-up.
- Prefer a smaller complete PR over a broad partially finished one.
- Preserve backwards compatibility when it materially reduces migration risk.
- Avoid architecture rewrites unless the assigned task is explicitly architectural.

A healthy PR should be understandable from its diff without requiring the reviewer to reconstruct the whole repository.

## Execution contract

Within the assigned scope, work autonomously. Do not wait for approval after every local decision.

Use this verification loop until the task is actually complete:

```text
inspect relevant context
        ↓
implement
        ↓
run the narrowest useful checks
        ↓
inspect results and diff
        ↓
fix defects / rerun
        ↓
full offline verification when feasible
        ↓
final scope + correctness self-review
        ↓
open or update one PR
```

Ask the user only when one of these is true:

- a required product or academic requirement is genuinely missing;
- there is a real architectural fork that changes a public contract or the agreed architecture;
- an irreversible or external action requires approval;
- available evidence cannot support a required claim.

Do not stop at the first passing test if the final diff still contains obvious scope creep, duplicated logic, dead code, misleading claims, or an unverified path.

If implementation uncovers a separate bug or architectural question, mention it under `Follow-ups` instead of silently absorbing it.

## PR size and shape

There is no hard line-count limit, but PRs should feel like normal engineering changes.

Good examples:

- add Parquet IO helpers;
- add ingestion-state primitives;
- migrate one pipeline boundary to Polars;
- integrate one official reference dataset;
- fix one linkage correctness issue;
- add one Airflow DAG once pipeline functions already exist;
- benchmark candidate semantic models without integrating one yet.

Bad examples:

- migrate all layers, add Airflow, add a new source, redesign Gold, and rewrite docs in one PR;
- broad cleanup unrelated to the requested feature;
- replacing working code only to impose a preferred style;
- introducing Spark/dbt/cloud infrastructure without measured need.

If the requested task starts becoming a large migration, stop at a clean compatibility boundary and describe the next PR.

## Architecture direction

The target architecture is a general-purpose public-procurement data platform. It must not assume every contract is technological.

Conceptually:

```text
official sources / reference data
            |
            v
      RAW IMMUTABLE
            |
            v
      BRONZE PARQUET
            |
            v
          SILVER
 canonical procurement entities
            |
            v
        ENRICHMENT
 CPV / buyer / geo / semantics
            |
            v
           GOLD
 opportunities / market signals /
 buyer intelligence / candidate feed
```

Technology can remain the principal TFM case study, but ingestion, canonical schemas, and CPV-based processing should support works, catering, healthcare, logistics, energy, professional services, IT, and other procurement domains.

## Data-engineering principles

### Raw

- Raw payloads are immutable.
- Keep source, retrieval window, retrieval timestamp, checksum, and enough provenance to reproduce transformations.
- A rerun must not silently overwrite a different payload under the same identity.

### Bronze

- Source-specific parsing is allowed here.
- Preserve provenance.
- Count `parsed`, `accepted`, and `rejected` records.
- Rejections must be observable with a reason; do not filter malformed records before accounting for them.

### Silver

- Represents stable, typed, canonical entities.
- Prefer official identifiers and codes over joins on names.
- `publication_date` and `source_updated_at` are distinct concepts.
- Schema design is generalist rather than technology-specific.
- Polars is the default tabular transformation engine for P0.
- Parquet is the main analytical storage format.

### Gold

- Gold is defined by user/data-product use cases, not by source system.
- Outputs must remain reproducible from upstream layers.
- CSV may exist as an interoperability/export format, not as the primary analytical store.

## Incremental ingestion and idempotency

The system must support both historical backfills and daily incremental execution.

Expected properties:

- the same window can be rerun without creating duplicates;
- source windows have explicit completeness state;
- checksums can identify identical payloads;
- partial source failures are visible and do not become successful runs;
- late corrections can be handled with a small source-appropriate lookback;
- revisions/tombstones are resolved explicitly;
- Airflow eventually orchestrates existing pipeline logic rather than becoming the place where business logic lives.

## Sources

Use official sources whenever practical. Current/planned categories include:

- TED;
- OpenPLACSP tenders;
- OpenPLACSP minor contracts;
- CPV 2008 reference data;
- DIR3 buyer reference data;
- optional territorial/statistical enrichment such as NUTS/Eurostat/INE.

When adding a source, document:

- official origin and retrieval method;
- temporal coverage;
- update cadence;
- natural keys;
- revision/deletion behavior;
- raw format;
- expected partitions/windows;
- known limitations.

Tests should use local fixtures and should not require network access.

## Classification and semantic enrichment

The semantic classifier is an enrichment component, not the central thesis contribution.

Rules:

- CPV remains the official primary taxonomy.
- Semantic business labels should be generalist and multilabel.
- The current CPV/keyword classifier can remain as an interpretable baseline.
- Prefer an existing pretrained model over training a bespoke model for P0.
- Store model identity/version and classification metadata so results are auditable.
- Do not report pseudo-labels as human ground truth.
- Evaluation should report per-label support and should not depress macro-F1 with classes that have no evaluable support.

A company-profile embedding similarity ranker is optional. A trained two-tower recommender is explicitly future work unless a later task changes this decision and provides real interaction data.

## Linkage

Cross-source linkage must actually compare different sources.

Candidate blocking and scoring should remain simple enough to explain academically. Large blocks may be subdivided or marked as not evaluated, but must never disappear silently.

Keep evidence and metrics for evaluated candidates, skipped/not-evaluated candidates, score, and method where feasible.

## Quality and observability

Quality checks must reveal problems rather than manufacture a green report.

Important signals include:

- requested vs downloaded source windows;
- parsed / accepted / rejected;
- critical-field completeness;
- uniqueness of canonical keys;
- valid dates and amounts;
- currency coherence;
- reference integrity for CPV/DIR3/NUTS when applicable;
- linkage evaluated/not-evaluated metrics;
- freshness of incremental loads;
- record counts between stages;
- duration per stage.

Do not weaken thresholds merely to obtain `passed=true`.

## Technology choices

P0 direction:

- Python for adapters and XML/CODICE parsing;
- Polars for tabular transformations;
- Parquet for analytical storage;
- Airflow for orchestration after idempotent pipeline primitives exist;
- pretrained semantic models for enrichment;
- a lightweight UI only as a serving/demo layer.

Do not add technologies for résumé value. In particular:

- Spark is optional and should be justified by measured backfill scale or a deliberately scoped benchmark.
- dbt is optional and should appear only if a SQL serving/modeling layer gives it a real role.
- distributed executors, Kubernetes, cloud deployment, Iceberg/Delta, and streaming are future-work candidates unless explicitly assigned.

## Testing

Tests are part of the feature, not follow-up work.

- Prefer deterministic unit tests and small local fixtures.
- No network dependency in the normal test suite.
- For ingestion code, mock HTTP or use captured fixtures.
- Test reruns/idempotency for incremental state changes.
- Test failure cases, not only happy paths.
- For data transformations, assert schema, key invariants, representative values, and counts where stable.
- Avoid brittle tests tied to incidental row ordering unless order is part of the contract.

If a pre-existing test fails, determine whether the task caused it. Do not silently rewrite unrelated tests to make CI green.

## Reproducibility and claims

The TFM must distinguish measured evidence from estimates.

- Never invent counts, timings, model metrics, benchmark results, source coverage, or successful runs.
- Label extrapolations as estimates.
- Keep commands/config required to reproduce measured results.
- Record code/config/model versions when they affect an experiment.
- Avoid calling a workload "Big Data" purely because the project is for a Big Data master's degree; justify scalability claims with volume, heterogeneity, velocity, or measured behavior.

## Writing the TFM memoria

Implementation and academic writing should evolve together, but coding agents should not fabricate polished thesis prose from incomplete evidence.

### During every implementation PR

Add a `Memoria notes` section to the PR body containing concise evidence that can later be turned into the thesis:

- **Problem:** what limitation existed before the PR.
- **Decision:** what was implemented or changed.
- **Rationale:** why this design was chosen over obvious alternatives.
- **Evidence:** tests, measured counts/timings/coverage/metrics produced by this PR.
- **Limitations:** what the change does not solve.
- **Relevant files:** implementation/docs/results that support the claim.

Keep this factual. Do not invent narrative or results.

### When explicitly assigned a memoria-writing task

Use merged code, merged PRs, official source documentation, and measured artifacts as evidence. Write in formal Spanish suitable for a master's thesis.

Preferred structure for technical sections:

1. problem/context;
2. design decision;
3. implementation;
4. validation/evidence;
5. limitations/trade-offs.

Explain why a technology exists in the architecture, not merely that it was used. Separate methodology from measured results. Do not copy README marketing language into the thesis.

If evidence for a statement is missing, mark it as pending rather than filling the gap with a plausible number.

## PR body template

Use approximately this structure:

```markdown
## Summary
- ...

## Scope
- ...

## Out of scope
- ...

## Validation
- `command`
- result

## Data / schema impact
- ...

## Memoria notes
- Problem: ...
- Decision: ...
- Rationale: ...
- Evidence: ...
- Limitations: ...
- Relevant files: ...

## Follow-ups
- ...
```

Do not pad the PR description. The purpose is to make review and later thesis writing easier.

## Definition of done for an agent task

A task is done when:

- the assigned behavior or document change is complete;
- scope has not expanded without justification;
- relevant tests pass;
- docs/contracts are updated only where the change makes them stale;
- the PR explains validation and data/schema impact;
- `Memoria notes` capture factual evidence;
- any discovered adjacent work is listed as follow-up rather than silently bundled.

The reviewer should be able to answer: what changed, why, how it was validated, what remains, and how this contributes to the TFM.