# Offline validation

PR A, revision `fbd891e75493afed85b0c0da5dbf5ad503a044d3`:

```bash
uv run --locked --with 'pyspark==4.0.1' --with-editable . python -m unittest discover -s tests -v
```

476 tests passed in 395.419 s, with Spark available (no skipped tests).

PR B production mappings, revision `566e7d2`:

Same command: 483 tests passed in 428.913 s, with Spark available (no skipped tests).
New cases cover earliest valid DOC_CN dates, invalid/missing/other notice dates,
fallback-route date parity, date collisions, TED publication-day ordering,
same-day/missing-date ambiguity, and Silver → current-state → Gold preservation.

Both branches additionally ran:

```bash
uv run --locked --with-editable . python -m unittest discover -s experiments/silver_engine_comparison/tests -t . -v
```

48 tests passed on each branch; 8 Spark-only experiment tests skipped in this
base-dependency command. These results do not claim the Spark candidate engine
was benchmarked on the new real corpus. The production PySpark suites above ran.

Local logs are retained under `/tmp/tfm-pr-{a,b}-tests.log` and
`/tmp/tfm-pr-{a,b}-experiment-tests.log`. Routine Spark socket ResourceWarnings
were emitted; all suites completed successfully.
