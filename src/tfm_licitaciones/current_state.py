"""Spark current-state resolution from canonical Silver.

Boundary::

    canonical Silver Parquet
        -> current_state Parquet
        -> current_state_issues Parquet when applicable
        -> current_state_manifest.json with frozen schema versions and counts

This module implements only the deterministic resolution semantics frozen by
the merged contracts:

- PR #19: ``CURRENT_STATE_FIELDS`` grain (one row per deterministically
  resolvable ``procedure_id``) and typed Parquet foundation.
- PR #20: dated PLACSP tombstones carry authoritative ``source_updated_at``;
  undated tombstones remain valid but unordered evidence.
- PR #21: PLACSP notices and tombstones share ``procedure_id`` by exact
  published-URI equality (RFC 6721 ``ref == atom:id``). No URL
  normalization, fuzzy matching or ingestion-order heuristic.

Semantics
---------

- Group by canonical ``procedure_id``. No cross-source merge: the key is
  already source-namespaced.
- Order business history using ``source_updated_at`` only when present.
  Dated tombstones participate normally in that ordering.
- Undated competing events are never ordered with ``ingested_at``,
  retrieval provenance, filename, row order or processing order. A
  procedure with more than one event where at least one
  ``source_updated_at`` is null is unresolved.
- A lone observed event is sufficient evidence for state even when its
  ``source_updated_at`` is null: there is no competition to order. This
  also covers single-event TED procedures (TED has no authoritative
  ``source_updated_at`` in canonical Silver).
- TED procedures require a non-null canonical ``procedure_id``. Null keys
  are never invented or joined; each null-key event becomes an issue.
- PLACSP identity is validated against the exact #21 bridge before any
  temporal decision. Unresolved identity never receives a synthetic key.
- Ties on ``source_updated_at`` are broken deterministically by
  ``event_id`` (largest wins). ``event_id`` is unique in canonical Silver,
  so the total order does not depend on physical input order.
- ``is_deleted`` is true exactly when the selected event has
  ``source_event_type == 'tombstone'``.
- Every input event that cannot contribute to a deterministic state is
  surfaced in ``current_state_issues`` with a deterministic ``reason``:

  - ``missing_procedure_id``: ``procedure_id`` is null.
  - ``missing_or_invalid_procedure_id``: PLACSP non-null key without a
    usable ``placsp:procedure:<ref>`` published ref.
  - ``unsupported_placsp_event_type``: PLACSP event type outside
    ``{notice_snapshot, tombstone}``.
  - ``event_procedure_identity_mismatch``: PLACSP ``event_id`` does not
    agree with ``procedure_id`` under the exact #21 rule.
  - ``undated_competing_events``: procedure has >1 identity-valid event
    and at least one null ``source_updated_at``.

Determinism uses only business columns (``procedure_id``,
``source_updated_at``, ``event_id``). Spark ``Window.partitionBy`` /
``orderBy`` implement grouping and ranking; ``write_typed_parquet`` sorts
before the distributed write, but consumers must treat schema and values
as the contract, never part-file order or names.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from .gold_contract import (
    CURRENT_STATE_FIELDS,
    CURRENT_STATE_ISSUES_FIELDS,
    CURRENT_STATE_ISSUES_SCHEMA_VERSION,
    CURRENT_STATE_SCHEMA_VERSION,
)
from .spark_foundation import (
    assert_contract_schema,
    canonical_silver_schema,
    read_canonical_silver,
    schema_from_fields,
    write_typed_parquet,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


REASON_MISSING_PROCEDURE_ID = "missing_procedure_id"
REASON_MISSING_OR_INVALID_PROCEDURE_ID = "missing_or_invalid_procedure_id"
REASON_UNSUPPORTED_PLACSP_EVENT_TYPE = "unsupported_placsp_event_type"
REASON_EVENT_PROCEDURE_IDENTITY_MISMATCH = "event_procedure_identity_mismatch"
REASON_UNDATED_COMPETING_EVENTS = "undated_competing_events"

PLACSP_PROCEDURE_PREFIX = "placsp:procedure:"
PLACSP_NOTICE_PREFIX = "placsp:notice:"
PLACSP_TOMBSTONE_PREFIX = "placsp:tombstone:"
PLACSP_DATED_TOMBSTONE_PREFIX = "placsp:tombstone-dated:"

CURRENT_STATE_DATASET = "current_state"
CURRENT_STATE_ISSUES_DATASET = "current_state_issues"
CURRENT_STATE_MANIFEST_FILENAME = "current_state_manifest.json"


def current_state_schema() -> "StructType":
    """Return the explicit Spark schema for ``current_state``."""

    return schema_from_fields(CURRENT_STATE_FIELDS)


def current_state_issues_schema() -> "StructType":
    """Return the explicit Spark schema for ``current_state_issues``."""

    return schema_from_fields(CURRENT_STATE_ISSUES_FIELDS)


def _identity_reason_column(frame: "DataFrame") -> "DataFrame":
    """Attach the deterministic PLACSP identity reason (null when valid).

    Non-PLACSP rows always validate here; null ``procedure_id`` rows never
    reach this helper. The checks mirror
    ``placsp_identity.resolve_placsp_identity`` without rewriting keys:

    - usable published ref requires the exact ``placsp:procedure:`` prefix
      plus a non-empty suffix;
    - ``notice_snapshot`` requires ``event_id`` to start with
      ``placsp:notice:<ref>@``;
    - ``tombstone`` requires either the undated
      ``placsp:tombstone:<ref>`` identity or the dated
      ``placsp:tombstone-dated:<micros>:<ref>`` identity where ``<micros>``
      is the canonical ASCII integer encoding (optional leading ``-`` run,
      then digits), matching Silver's int64-micros construction.
    """

    from pyspark.sql import functions as F

    with_ref = frame.withColumn(
        "___published_ref",
        F.when(
            F.startswith(F.col("procedure_id"), F.lit(PLACSP_PROCEDURE_PREFIX)),
            F.substring(F.col("procedure_id"), 18, F.length(F.col("procedure_id"))),
        ).otherwise(F.lit(None).cast("string")),
    )
    valid_ref = F.col("___published_ref").isNotNull() & (F.col("___published_ref") != F.lit(""))

    notice_ok = F.startswith(
        F.col("event_id"),
        F.concat(F.lit(PLACSP_NOTICE_PREFIX), F.col("___published_ref"), F.lit("@")),
    )
    undated_ok = F.col("event_id") == F.concat(
        F.lit(PLACSP_TOMBSTONE_PREFIX), F.col("___published_ref")
    )
    dated_starts = F.startswith(F.col("event_id"), F.lit(PLACSP_DATED_TOMBSTONE_PREFIX))
    dated_ends = F.endswith(
        F.col("event_id"), F.concat(F.lit(":"), F.col("___published_ref"))
    )
    marker = F.substring(
        F.col("event_id"),
        24,
        F.length(F.col("event_id"))
        - F.lit(len(PLACSP_DATED_TOMBSTONE_PREFIX))
        - F.length(F.concat(F.lit(":"), F.col("___published_ref"))),
    )
    marker_valid = (marker != F.lit("")) & F.regexp_like(
        F.regexp_replace(marker, F.lit("^-+"), F.lit("")), F.lit("^[0-9]+$")
    )
    dated_ok = dated_starts & dated_ends & marker_valid
    tombstone_ok = undated_ok | dated_ok

    is_notice = F.col("source_event_type") == F.lit("notice_snapshot")
    is_tombstone = F.col("source_event_type") == F.lit("tombstone")
    is_supported = is_notice | is_tombstone
    matches = (
        F.when(is_notice, notice_ok)
        .when(is_tombstone, tombstone_ok)
        .otherwise(F.lit(False))
    )

    is_placsp = F.col("source") == F.lit("placsp")
    reason = (
        F.when(~is_placsp, F.lit(None).cast("string"))
        .when(~valid_ref, F.lit(REASON_MISSING_OR_INVALID_PROCEDURE_ID))
        .when(~is_supported, F.lit(REASON_UNSUPPORTED_PLACSP_EVENT_TYPE))
        .when(~matches, F.lit(REASON_EVENT_PROCEDURE_IDENTITY_MISMATCH))
        .otherwise(F.lit(None).cast("string"))
    )
    return with_ref.withColumn("___identity_reason", reason)


def build_current_state(silver: "DataFrame") -> tuple["DataFrame", "DataFrame"]:
    """Resolve canonical Silver into ``(current_state, issues)`` frames.

    The input must already satisfy the canonical Silver contract (as
    enforced by ``read_canonical_silver``); this builder re-validates it
    and additionally fails on duplicate ``event_id`` instead of inventing
    another deduplication policy. Output frames use the explicit
    ``CURRENT_STATE_FIELDS`` / ``CURRENT_STATE_ISSUES_FIELDS`` schemas
    (column order included).
    """

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    assert_contract_schema(silver, canonical_silver_schema())

    duplicates = (
        silver.groupBy("event_id").count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicates:
        offending = (
            silver.groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .orderBy("event_id")
            .limit(1)
            .collect()[0]["event_id"]
        )
        raise ValueError(f"duplicate event_id at current-state boundary: {offending!r}")

    issues_columns = ["procedure_id", "event_id", "source", "source_event_type", "reason"]

    null_issues = silver.filter(F.col("procedure_id").isNull()).select(
        F.col("procedure_id"),
        F.col("event_id"),
        F.col("source"),
        F.col("source_event_type"),
        F.lit(REASON_MISSING_PROCEDURE_ID).alias("reason"),
    )

    keyed = silver.filter(F.col("procedure_id").isNotNull())
    keyed_with_reason = _identity_reason_column(keyed)

    identity_issues = keyed_with_reason.filter(
        F.col("___identity_reason").isNotNull()
    ).select(
        F.col("procedure_id"),
        F.col("event_id"),
        F.col("source"),
        F.col("source_event_type"),
        F.col("___identity_reason").alias("reason"),
    )

    identity_ok = keyed_with_reason.filter(
        F.col("___identity_reason").isNull()
    ).drop("___published_ref", "___identity_reason")

    procedure_window = Window.partitionBy("procedure_id")
    with_counts = identity_ok.withColumn(
        "___cnt", F.count(F.lit(1)).over(procedure_window)
    ).withColumn(
        "___undated_cnt",
        F.sum(F.when(F.col("source_updated_at").isNull(), F.lit(1)).otherwise(F.lit(0))).over(
            procedure_window
        ),
    )

    undated_issues = with_counts.filter(
        (F.col("___cnt") > 1) & (F.col("___undated_cnt") > 0)
    ).select(
        F.col("procedure_id"),
        F.col("event_id"),
        F.col("source"),
        F.col("source_event_type"),
        F.lit(REASON_UNDATED_COMPETING_EVENTS).alias("reason"),
    )

    resolvable = with_counts.filter(
        (F.col("___cnt") == 1)
        | ((F.col("___cnt") > 1) & (F.col("___undated_cnt") == 0))
    )

    rank_window = Window.partitionBy("procedure_id").orderBy(
        F.col("source_updated_at").desc_nulls_last(), F.col("event_id").desc()
    )
    ranked = resolvable.withColumn("___rn", F.row_number().over(rank_window))
    winners = ranked.filter(F.col("___rn") == 1).drop("___cnt", "___undated_cnt", "___rn")

    current = winners.withColumn(
        "is_deleted", F.col("source_event_type") == F.lit("tombstone")
    ).select(
        "procedure_id",
        "event_id",
        "source",
        "source_event_type",
        "buyer_id",
        "buyer_name",
        "title",
        "description",
        "cpv_codes",
        "estimated_value",
        "awarded_value",
        "currency",
        "publication_date",
        "source_updated_at",
        "deadline",
        "status",
        "nuts_code",
        "country",
        "source_url",
        "ingested_at",
        "is_deleted",
    )

    issues = (
        null_issues.select(*issues_columns)
        .unionByName(identity_issues.select(*issues_columns))
        .unionByName(undated_issues.select(*issues_columns))
    )

    # Enforce explicit schemas (field order and logical types) before return
    # so callers and writers observe the frozen contracts even on empty frames.
    assert_contract_schema(current, current_state_schema())
    assert_contract_schema(issues, current_state_issues_schema())
    return current, issues


def build_current_state_from_silver(
    spark: "SparkSession",
    silver_path: str | Path,
    output_dir: str | Path,
    *,
    current_state_name: str = CURRENT_STATE_DATASET,
    issues_name: str = CURRENT_STATE_ISSUES_DATASET,
    manifest_name: str = CURRENT_STATE_MANIFEST_FILENAME,
) -> dict[str, str | int]:
    """Read canonical Silver Parquet and write typed current-state outputs.

    Writes ``<output_dir>/current_state``,
    ``<output_dir>/current_state_issues`` with ``write_typed_parquet`` and
    deterministic pre-write sorts, plus a minimal
    ``<output_dir>/current_state_manifest.json`` carrying the frozen schema
    versions and row counts required by PR #19. Returns output paths and
    row counts.
    """

    silver = read_canonical_silver(spark, silver_path)
    current, issues = build_current_state(silver)

    output_root = Path(output_dir)
    current_path = output_root / current_state_name
    issues_path = output_root / issues_name
    manifest_path = output_root / manifest_name

    write_typed_parquet(
        current,
        current_path,
        expected_schema=current_state_schema(),
        order_by=("procedure_id",),
    )
    write_typed_parquet(
        issues,
        issues_path,
        expected_schema=current_state_issues_schema(),
        order_by=("event_id",),
    )
    current_rows = current.count()
    issues_rows = issues.count()
    manifest_path.write_text(
        json.dumps(
            {
                "current_state_schema_version": CURRENT_STATE_SCHEMA_VERSION,
                "current_state_issues_schema_version": CURRENT_STATE_ISSUES_SCHEMA_VERSION,
                "counts": {
                    "current_state": current_rows,
                    "current_state_issues": issues_rows,
                },
            },
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "current_state": str(current_path),
        "current_state_issues": str(issues_path),
        "manifest": str(manifest_path),
        "current_state_rows": current_rows,
        "current_state_issues_rows": issues_rows,
    }
