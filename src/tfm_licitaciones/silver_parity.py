"""Semantic parity check for canonical Silver frames across engines.

Compares a candidate frame (for example a future Spark output) against the
reference frame built by the current python-row baseline, reusing the
production contract (:data:`PROCUREMENT_EVENT_SCHEMA
<tfm_licitaciones.models.PROCUREMENT_EVENT_SCHEMA>`) instead of duplicating
it.

The comparison is native Polars (joins and column expressions, no per-row
Python materialization of either table) and physical-layout agnostic:

- row and file order are ignored (alignment is by ``event_id``);
- Parquet partitioning and operational timestamps outside the frame
  (``run_at``, file mtimes, ``measured_at``) are never compared;
- every Silver semantic is preserved exactly: strict schema, row count,
  unique ``event_id``, event/procedure identities, revisions as distinct
  events, tombstones as rows, the deterministic provenance selection
  (``ingested_at`` is provenance inherited from ``raw_retrieved_at`` and
  **is** compared), UTC timestamps, ``Decimal(20,2)`` amounts, and CPV array
  order.

Only a bounded diagnostic sample is collected; the verdict itself always
covers the full frames. Material-collision behaviour is parity of failure: a
dataset generated with ``with_collision=True`` must raise a ``ValueError``
naming the same ``event_id`` on every engine instead of producing a frame.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .models import PROCUREMENT_EVENT_SCHEMA

_CANONICAL_ORDER = list(PROCUREMENT_EVENT_SCHEMA.names())
_MAX_SAMPLE_IDS = 20


def _null_safe_neq(column: str) -> pl.Expr:
    """Build a null-safe ``candidate != reference`` expression for one column."""

    left, right = pl.col(column), pl.col(f"{column}__ref")
    return (left != right).fill_null(True) & ~(left.is_null() & right.is_null())


def compare_silver_frames(
    candidate: pl.DataFrame,
    reference: pl.DataFrame,
    *,
    sample_limit: int = _MAX_SAMPLE_IDS,
) -> list[str]:
    """Return human-readable differences; an empty list means parity.

    The verdict always scans the full frames with native expressions; at most
    ``sample_limit`` example ids/rows are collected as diagnostics.
    """

    differences: list[str] = []
    expected_schema = pl.Schema(PROCUREMENT_EVENT_SCHEMA)
    for label, frame in (("candidate", candidate), ("reference", reference)):
        if frame.schema != expected_schema:
            differences.append(f"{label} schema {frame.schema!r} != canonical {expected_schema!r}")
    if differences:
        return differences
    if candidate.height != reference.height:
        differences.append(f"row count {candidate.height} != reference {reference.height}")
    for label, frame in (("candidate", candidate), ("reference", reference)):
        if not frame.get_column("event_id").is_unique().all():
            differences.append(f"{label} event_id is not unique")
    if any("not unique" in item for item in differences):
        return differences

    candidate_ids = candidate.select("event_id")
    reference_ids = reference.select("event_id")
    for label, missing in (
        ("reference", reference_ids.join(candidate_ids, on="event_id", how="anti")),
        ("candidate", candidate_ids.join(reference_ids, on="event_id", how="anti")),
    ):
        count = missing.height
        if count:
            sample = missing.head(sample_limit).get_column("event_id").to_list()
            differences.append(f"{count} event_id(s) missing from {label}, e.g. {sample!r}")

    samples: list[str] = []
    for column in _CANONICAL_ORDER:
        if column == "event_id" or len(samples) >= sample_limit:
            continue
        joined = (
            candidate.select("event_id", column)
            .join(reference.select("event_id", column), on="event_id", how="inner", suffix="__ref")
            .filter(_null_safe_neq(column))
        )
        count = joined.height
        if count:
            example = joined.head(sample_limit - len(samples)).to_dicts()
            shown = [{key: repr(value) for key, value in row.items()} for row in example]
            samples.append(f"{count} row(s) differ in {column!r}, e.g. {shown!r}")
    return (differences + samples)[: sample_limit + 2]


def assert_silver_parity(candidate: pl.DataFrame, reference: pl.DataFrame) -> None:
    """Assert semantic parity, raising ``AssertionError`` with a diff summary."""

    differences = compare_silver_frames(candidate, reference)
    if differences:
        summary = "\n".join(differences)
        raise AssertionError(f"Silver frames are not semantically equal ({len(differences)} diffs):\n{summary}")


def compare_silver_parquet(candidate_path: str | Path, reference_path: str | Path) -> list[str]:
    """Load two Parquet Silver outputs and compare them for semantic parity."""

    return compare_silver_frames(pl.read_parquet(candidate_path), pl.read_parquet(reference_path))
