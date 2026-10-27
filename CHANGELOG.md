# Changelog

## v2 — Unreleased

### Source mappings — 2026-10-27

- TED procedure identities: **0 → 9,337 / 9,905 events** on the same notice IDs,
  with no changed shared payload fields. The remaining 568 are planning notices.
  Unique latest publication days resolve 7,469 current TED procedures; 73 groups
  (233 events) with latest-day ties remain explicit issues.
- Current-state procedures: **130,118 → 137,587**; issues: **9,905 → 801**.
  Silver remains **359,180** events; open Gold remains **14,763** opportunities.
- PLACSP declared publication dates: **0 → 299,950 / 349,399** Bronze observations,
  **0 → 298,913 / 348,230** Silver notice snapshots, and **0 → 14,762 / 14,763**
  Gold opportunities (99.9932%). Use the earliest valid DOC_CN IssueDate;
  retain null when absent. No derived fallback is needed for open opportunities.
- Monthly export: **0 → 43 rows**. CPV summary CSV is unchanged; the other four
  exports change with publication dates/date aggregates. Source update windows
  include older procedures; this is not extended historical source coverage.
- Two complete runs at **as-of 2026-06-30** reproduce all eight hashes (Silver
  bytes/logical, Gold logical, five CSVs) and counts. Commands, environment,
  complete hashes and whole-corpus coverage:
  [v2 evidence](docs/evidence/v2-2026-10-27/README.md).

### Initial corrections

- Macro-F1 now averages only labels with evaluable CPV proxy support; unsupported
  labels remain visible. Predictions outside the CPV map count as reference-label
  misses instead of raising an error. Evaluation reports can change.
- Legacy current-state folding compares aware revision timestamps as UTC instants.
  Invalid, missing and naive timestamps provide no temporal evidence; ties use
  batch position. Selected revisions and legacy Gold outputs can change; Silver
  event history is preserved. No full-corpus output parity is claimed for this fix.

The graded release and its retained evidence remain pinned to
[`tfm-v1.0-graded`](https://github.com/tomcrojo/tfm-licitaciones/releases/tag/tfm-v1.0-graded).
