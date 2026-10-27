# Changelog

## v2 — Unreleased

- Macro-F1 now averages only labels with evaluable CPV proxy support; unsupported
  labels remain visible. Predictions outside the CPV map count as reference-label
  misses instead of raising an error. Evaluation reports can change.
- Legacy current-state folding compares aware revision timestamps as UTC instants.
  Invalid, missing and naive timestamps provide no temporal evidence; ties use
  batch position. Selected revisions and legacy Gold outputs can change; Silver
  event history is preserved. No full-corpus output parity is claimed for this fix.

The graded release and its retained evidence remain pinned to
[`tfm-v1.0-graded`](https://github.com/tomcrojo/tfm-licitaciones/releases/tag/tfm-v1.0-graded).
