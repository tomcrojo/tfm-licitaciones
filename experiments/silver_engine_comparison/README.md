# Silver engine comparison (experiment, non-production)

> This directory contains reproducible experimental evidence and candidate
> implementations. It is not part of the production pipeline and nothing
> under `src/` depends on it.

## What this is

A controlled offline comparison of three Bronze-Parquet → Silver-Parquet
engine candidates (python-row baseline, native Polars, native Spark) on the
same retained synthetic TED/PLACSP datasets, plus an adversarial audit of
the Spark benchmark and a same-host standalone-cluster run. It produced the
measured evidence behind the production engine decision:

- **Polars wins canonical Silver in the measured single-node envelope**
  (large profile: ~13 s Polars vs ~33 s Spark `local[16]` vs ~43 s
  standalone 3×(4 cores/4 GiB), exact semantic parity everywhere).
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
├── engines/
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
    └── test_no_production_imports.py  # src/ never imports experiments/
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
production replacements. The production pipeline and its frozen python-row
reference live in `src/tfm_licitaciones/` and never import from here.

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
never discovers this directory.
