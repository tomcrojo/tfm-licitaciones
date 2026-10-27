# v2 source-mapping baseline — 2026-10-27

Two complete, isolated Raw → Bronze → Silver → legacy Gold → Spark Gold →
DuckDB → CSV runs passed. **All measured hashes and counts match.**
`--as-of 2026-06-30` and the January–June source window match v1.
Production mapping revision: `566e7d2` (full SHA in measurements.json).

## Retained evidence

- [measurements.json](measurements.json): both runs' counts, coverage, input
  checksums/provenance, manifests and complete SHA-256 values.
- [environment.json](environment.json): actual runtime versions, config/lock hashes.
- [run-pair.log](run-pair.log): both full executions and their completion records
  (terminal progress bars omitted; the original log remains beside the outputs).
- [ingest-ted.log](ingest-ted.log): successful refresh, including recovered HTTP 429s.
- [validation.md](validation.md): offline test commands and results.
- [run_pair.sh](run_pair.sh), [measure.py](measure.py): reproduction and audit commands.

Raw payloads stay outside Git at `/mnt/pop-work/tfm-v2-2026-10-27/raw`.
Outputs are under the sibling `run1` and `run2` directories. Run 1 took
17m47s; run 2 took 17m07s, including all pipeline stages. The input root contains
six original PLACSP ZIPs and the new TED JSONL, each with its verified sidecar.
The 12 PLACSP payload/sidecar files match the frozen v1 artifact inventory.
No v1 artifact, tag, workbook, video or memoria was modified.

## Measured changes from v1

- Bronze: **359,304 accepted observations**, zero rejects; **1,046 tombstones**.
- Silver: **359,180 events**, unchanged. PLACSP: 348,230 notice snapshots and
  1,045 distinct tombstone events; TED: 9,905 notices.
- TED: **0 → 9,337** events with a procedure identity; **568** remain without
  one, all planning notices (307 pin-rtl, 130 pin-buyer, 130 pin-only, 1 pin-tran).
  The new retrieval has exactly the same notice IDs and no changed shared fields.
- Those 9,337 events identify **7,542 procedures**, including 1,290 competing
  groups. The ordering rule resolves **7,469** current TED procedures. **73**
  groups / **233** events remain ambiguous on the latest publication day.
- Overall current state: **130,118 → 137,587** procedures; issues:
  **9,905 → 801** (568 missing identities + 233 ambiguous TED events).
- Gold: **14,763 opportunities**, unchanged. The 7,469 new current TED procedures
  add to `insufficient_evidence` because TED status/deadline mapping is unchanged.
- Monthly output: **0 → 43 rows**. CPV summary CSV is byte-identical to v1;
  the other four exports change because they contain publication dates or
  date-based aggregates. Existing CPV/DIR3 availability behavior is unchanged.

## Whole-corpus declared-date coverage

V1 canonically mapped no PLACSP publication dates. V2 measures every accepted
observation and every canonical notice snapshot, not a sample:

- Bronze: **299,950 / 349,399 (85.8474%)** PLACSP observations.
- Silver: **298,913 / 348,230 (85.8378%)** PLACSP notice snapshots. Tombstones
  are excluded from this publication-date denominator and reported separately.
- Gold: **14,762 / 14,763 (99.9932%)** open opportunities; one remains null.

Coverage by Silver status is included in measurements.json. Missing dates
concentrate in planning and closed states; only 2 of 83,111 PUB snapshots lack
one. No derived fallback is warranted for the opportunity product, and a later
retrieval timestamp would misrepresent historical publication. All non-null
canonical PLACSP publication dates are declared DOC_CN dates.

Declared dates on the open cohort span **2020-06-02 → 2026-06-30**. The ingestion
window contains updates to older procedures: the 43 monthly rows do not imply
complete market coverage back to 2020. Existing status/deadline limitations and
the meaning of `as-of` remain unchanged; this is not a historical-state reconstruction.

## Hash policy and reproduction

Silver Parquet bytes are compared directly. Logical Silver/Gold hashes use all
columns serialized with Polars `write_ndjson`, sorted by `event_id` / `procedure_id`.
Gold's random Spark part filenames/physical bytes are not claimed identical.
The five final CSV hashes are byte-level SHA-256, as in the graded baseline.
Runtime timestamps and output-root paths in operational manifests are not hashed.

From the repository root, with the retained verified Raw inputs:

```bash
bash docs/evidence/v2-2026-10-27/run_pair.sh /absolute/new-output-root /absolute/verified-raw
uv run --locked --with-editable . python docs/evidence/v2-2026-10-27/measure.py \
  /absolute/new-output-root /absolute/v1/raw/ted/ted-20260101-20260630.jsonl
```

The runner refuses to reuse output directories and bounds each transform to
30 minutes and each Spark stage to 10 minutes. The measurement script fails if
hashes or counts differ. A fresh API download is a new retrieval, not a promise
of reproducing today's Raw bytes. To repeat the fetch window, use:

```bash
uv run --locked --with-editable . licitaciones-pipeline ingest --source ted \
  --start 2026-01-01 --end 2026-06-30 --raw-dir /absolute/new-raw
```

[Ordering and mapping decision](../../decisions/0001-v2-source-dates-and-ted-ordering.md).
