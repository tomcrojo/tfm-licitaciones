# Silver engine comparison (experiment, non-production)

> This directory contains reproducible experimental evidence and candidate
> implementations. It is not part of the production pipeline and nothing
> under `src/` depends on it.

## What this is

A controlled offline comparison of three Bronze-Parquet → Silver-Parquet
engine candidates (frozen python-row baseline, native Polars, native Spark)
on the same retained synthetic TED/PLACSP datasets, plus an adversarial
audit of the Spark benchmark and a same-host standalone-cluster run. It
produced the measured evidence behind the production engine decision:

- **Polars wins canonical Silver in the measured single-node envelope**
  (large profile: ~13 s Polars vs ~33 s Spark `local[16]` vs ~43 s
  standalone 3×(4 cores/4 GiB), exact parity in every measured
  run/profile within the synthetic contract).
- Spark local and same-host standalone are slower **at this scale**; the
  comparison was adversarially audited (no Python UDFs, no large collect,
  one typed JSON decode per branch, AQE on, common zstd codec, recorded
  heap, inspected shuffles).
- **Spark remains relevant** for scale-out and high-cardinality workloads:
  large cross-source joins, linkage candidate generation,
  company×opportunity candidate generation/ranking, window-heavy Gold.

Nothing here is rewritten to make any engine look better or worse: reports
are dated evidence of the runs as executed.

## Layout

```text
experiments/silver_engine_comparison/
├── README.md                  # this file
├── bench_engines.py           # 3-engine runner (fresh spawn child per engine)
├── workload.py                # historical workload pin + fingerprint (e69016f generator)
├── engines/
│   ├── python_row_reference.py  # FROZEN python-row baseline (e69016f semantics, no prod imports)
│   ├── polars_candidate.py    # native Polars candidate (synthetic contract only)
│   └── spark_candidate.py     # native Spark candidate (synthetic contract only)
├── reports/                   # dated evidence, kept byte-identical
│   ├── silver-engine-comparison-2026-09-16.md
│   ├── silver-engine-audit-2026-09-16.md
│   └── silver-distributed-2026-09-16.md
├── results/
│   └── engines-small-seed7-2026-09-16.json   # small-profile metrics JSON
└── tests/
    ├── test_engine_parity.py       # parity/collision/guard tests (Spark parts skip without PySpark)
    ├── test_workload_fingerprint.py  # historical workload pin tests
    └── test_no_production_imports.py  # src/ never imports experiments/; experiment never uses prod Silver facade
```

The directory uses underscores (`silver_engine_comparison`) instead of
hyphens so the experiment stays plainly importable without `sys.path`
hacks. The reports keep their original `src/...`/`tests/...` paths
verbatim because they describe the runs as executed; the code has since
moved here with semantics unchanged:

- `src/tfm_licitaciones/bench_engines.py` → `bench_engines.py`
- `src/tfm_licitaciones/silver_polars.py` → `engines/polars_candidate.py`
- `src/tfm_licitaciones/silver_spark.py` → `engines/spark_candidate.py`
- `tests/test_silver_engines.py` → `tests/test_engine_parity.py`

## Scope (loud limitation)

The native candidates support **only** the TED/PLACSP synthetic Bronze
contract emitted by `tfm_licitaciones.bench_silver` (scalar string/number
JSON payloads, CPV as JSON arrays of strings, whole-second `updated`
instants). They fail explicitly outside that contract and are not
production replacements. Nothing under `src/` depends on this directory.

## Frozen python-row baseline (reproducibility)

The `python-row` engine is **frozen**, not current. It executes
`engines/python_row_reference.py`, a self-contained copy of the exact
python-row semantics shipped at:

    e69016f62fb985639e17153e24b3a570f2f39c20

Why: `bench_engines.py` used to import the live production facade
`tfm_licitaciones.silver.build_procurement_events` as the `python-row`
baseline. PR #17 turns that facade into native Polars, so rerunning the
experiment after PR #17 would silently benchmark production Polars under
the `python-row` label. The frozen copy prevents that: the label keeps its
historical meaning before and after PR #17.

What is frozen (verbatim, no "improvements"): the Silver python-row
transform (`_exact_amount`, `_aware_instant`, `_single_published_value`,
`_ted_country`, TED/PLACSP/BOE event mapping, tombstone mapping,
deterministic provenance selection, canonical-collision detection) plus its
transitive transformation semantics (`_first_text`, `_parse_date`,
`_normalize_amount_text`, `_parse_amount`, `_url_from_links`,
`_as_code_list`, `normalize_ted`, `normalize_boe`) and the data containers
it materializes through (`PROCUREMENT_EVENT_SCHEMA`, `TenderRecord`,
`ProcurementEvent` with UTC coercion, `procurement_events_frame`). The
frozen module imports nothing from `tfm_licitaciones` (stdlib + polars
only); static and dynamic regression tests enforce that a future
production refactor cannot silently change it.

Intentionally shared (harness, not baseline semantics): the Bronze frame
constructors (`tfm_licitaciones.bronze`) and the parity comparator
(`tfm_licitaciones.silver_parity`). If production ever changes the
canonical schema, parity will fail loudly instead of moving the historical
baseline.

## Historical workload pin (reproducibility)

Freezing the baseline is not enough: the measured workload itself comes
from the live generator `tfm_licitaciones.bench_silver` (`PROFILES`,
`dataset_profile`, `write_bronze_parts`). A future generator change that
keeps the same row counts but alters payloads would silently move what
`--profile small --seed 7` denotes. Instead of copying the ~700-line
generator, the experiment pins it cheaply:

- generator commit: `e69016f62fb985639e17153e24b3a570f2f39c20` (verified:
  `bench_silver.py`/`bronze.py` are byte-identical between that commit and
  this branch);
- deterministic fingerprint over canonicalized logical Bronze row values
  (payload + provenance, never Parquet writer bytes) for the pinned
  workloads `tiny`/`small` with `seed=7` (see `workload.py`:
  `HISTORICAL_WORKLOADS`).

`bench_engines.run_comparison` asserts the pin for those workloads before
measuring, and `tests/test_workload_fingerprint.py` asserts it in CI
(including a sensitivity test proving the digest moves on a
count-preserving content change). Any other profile/seed runs unchecked so
exploratory runs stay possible.

## Errata (reports stay byte-identical)

The three dated reports are preserved verbatim as executed; verified
against the pre-curation blobs. One verified inconsistency, documented
here instead of editing the artifact:

- `reports/silver-distributed-2026-09-16.md` §Topología shows a single
  reproducible command with `--spark-driver-memory 3g
  --spark-executor-memory 4g`. Checked against the retained evidence, that
  configuration matches the **large** cluster run (`large-07-cluster.json`:
  driver 3g / executor 4g, table `3×(4c/4g)`), but **not** the medium
  cluster runs (`medium-07-cluster.json`, `medium-07-cluster3.json`:
  driver 2g / executor 3g, matching the table's `3×(4c/3g)` label). The
  table labels are correct; only the shared command block conflates the
  two configs. To reproduce medium on the cluster use
  `--spark-driver-memory 2g --spark-executor-memory 3g`.

The historical conclusion is unchanged: Polars won canonical Silver in the
measured single-node envelope; Spark local and same-host standalone were
slower at that scale; Spark remains relevant for distributed and
high-cardinality workloads. Do not rerun benchmarks to get prettier numbers
and do not overwrite the retained reports/results: a smoke/tiny rerun only
validates the frozen wiring, it is not replacement evidence.

## Reproduce

From the repository root (offline; Spark parts need the pinned extra):

```bash
# full experiment suite (Spark behavior tests skip without PySpark)
uv run --with-editable . python -m unittest discover \
  -s experiments/silver_engine_comparison/tests -t . -v

# with the pinned Spark
uv run --with 'pyspark==4.0.1' --with-editable . python -m unittest discover \
  -s experiments/silver_engine_comparison/tests -t . -v

# rerun the comparison (small profile, seed 7)
uv run --with 'pyspark==4.0.1' --with-editable . \
  python -m experiments.silver_engine_comparison.bench_engines \
  --profile small --seed 7 \
  --work-dir /tmp/silver-engines-small --output /tmp/silver-engines-small.json
```

The default production suite (`python -m unittest discover -s tests`)
never discovers this directory. CI (`.github/workflows/tests.yml`) runs
both the production suite and this experiment suite without PySpark (Spark
behavior tests skip there; run them locally with the pinned extra).
