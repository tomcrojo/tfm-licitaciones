"""Minimal deterministic Gold enrichments over canonical Silver events.

Two independent, composable datasets are produced at the Gold boundary:

- ``cpv_enriched``: one row per ``(event_id, cpv_code)`` occurrence, carrying
  the official CPV 2008 attributes from ``data/reference/cpv_codes.parquet``.
  Exploding keeps every published code with its original position, so the
  multiple CPV codes of one event are never collapsed arbitrarily and the
  original code is always preserved.
- ``buyer_dir3_enriched``: canonical Silver events (event grain preserved)
  plus official DIR3 attributes joined on ``buyer_id = dir3_code`` against
  ``data/reference/dir3/dir3_units.parquet``. ``buyer_id``/``buyer_name``
  themselves remain the canonical Gold values; DIR3 only adds attributes.

Determinism and cardinality invariants enforced here:

- dimension primary keys are validated for uniqueness before any join, so a
  broken dimension fails loudly instead of silently multiplying facts. There
  is no fuzzy matching: with a validated key the join is 1:1 at most and
  "ambiguous" fact rows cannot exist (the duplicate-key count is reported as
  a metric after the guard proves it is zero);
- fact row counts are asserted unchanged before/after each join;
- unmatched facts are preserved by left joins and counted in the returned
  metrics (``resolved``/``unresolved`` counts are measured from real data);
- duplicated CPV codes inside one event's ``cpv_codes`` array are a malformed
  canonical boundary and fail instead of being deduplicated again.

DIR3 availability: the units dimension is a local artifact built after an
explicit network download (``licitaciones-pipeline ingest --source dir3`` plus
``licitaciones-pipeline dir3``). When it is absent, ``dir3_units_dimension``
returns ``None`` and the caller must skip DIR3 enrichment and proceed with the
canonical ``buyer_id``/``buyer_name``, which are already valid for Gold. No
runtime fallback dimension is invented here.

This module deliberately implements enrichment only: it does not resolve
current state, does not modify Silver, and does not build the Gold marts.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .gold_contract import CANONICAL_SILVER_FIELDS, FieldSpec
from .spark_foundation import (
    assert_contract_schema,
    read_validated_parquet,
    schema_from_fields,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


CPV_ENRICHED_DATASET = "cpv_enriched"
CPV_ENRICHED_SCHEMA_VERSION = 1

BUYER_DIR3_ENRICHED_DATASET = "buyer_dir3_enriched"
BUYER_DIR3_ENRICHED_SCHEMA_VERSION = 1

# Expected schema of data/reference/cpv_codes.parquet (docs/cpv-reference.md).
# Nullability mirrors the Polars build schema; value-level enforcement of the
# required columns happens in read_cpv_dimension.
CPV_DIMENSION_FIELDS = (
    FieldSpec("cpv_code", "string", False),
    FieldSpec("label_es", "string"),
    FieldSpec("label_en", "string"),
    FieldSpec("level", "tinyint", False),
    FieldSpec("parent_code", "string"),
    FieldSpec("is_leaf", "boolean", False),
)

# Expected schema of data/reference/dir3/dir3_units.parquet
# (docs/dir3-reference.md), mapped to Spark logical types.
DIR3_DIMENSION_FIELDS = (
    FieldSpec("dir3_code", "string", False),
    FieldSpec("name", "string", False),
    FieldSpec("administration_scope", "string", False),
    FieldSpec("public_entity_type", "string"),
    FieldSpec("hierarchy_level", "smallint", False),
    FieldSpec("parent_dir3_code", "string", False),
    FieldSpec("principal_dir3_code", "string", False),
    FieldSpec("status", "string", False),
    FieldSpec("official_valid_from_raw", "string"),
    FieldSpec("nif_cif", "string"),
)

# Grain: one row per (event_id, cpv_code) occurrence. ``cpv_position`` is the
# 0-based index inside the canonical ``cpv_codes`` array so the original
# publication order stays reconstructable. Official attributes are null for
# codes missing from the reference dimension; ``cpv_matched`` distinguishes
# them from null-valued official attributes.
CPV_ENRICHED_FIELDS = (
    FieldSpec("event_id", "string", False),
    FieldSpec("procedure_id", "string"),
    FieldSpec("source", "string", False),
    FieldSpec("cpv_position", "int", False),
    FieldSpec("cpv_code", "string", False),
    FieldSpec("cpv_matched", "boolean", False),
    FieldSpec("label_es", "string"),
    FieldSpec("label_en", "string"),
    FieldSpec("level", "tinyint"),
    FieldSpec("parent_code", "string"),
    FieldSpec("is_leaf", "boolean"),
)

# Grain: canonical Silver events, unchanged; official DIR3 attributes are
# appended. ``buyer_dir3_status`` is exposed instead of filtering so future
# non-active snapshot rows remain observable to consumers.
BUYER_DIR3_ATTRIBUTE_FIELDS = (
    FieldSpec("buyer_dir3_matched", "boolean", False),
    FieldSpec("buyer_dir3_name", "string"),
    FieldSpec("buyer_dir3_scope", "string"),
    FieldSpec("buyer_dir3_entity_type", "string"),
    FieldSpec("buyer_dir3_hierarchy_level", "smallint"),
    FieldSpec("buyer_dir3_parent_code", "string"),
    FieldSpec("buyer_dir3_principal_code", "string"),
    FieldSpec("buyer_dir3_status", "string"),
    FieldSpec("buyer_dir3_nif", "string"),
)

BUYER_DIR3_ENRICHED_FIELDS = tuple(CANONICAL_SILVER_FIELDS) + BUYER_DIR3_ATTRIBUTE_FIELDS


def dir3_units_path(reference_dir: str | Path) -> Path:
    """Return the canonical location of the DIR3 units dimension."""

    return Path(reference_dir) / "dir3" / "dir3_units.parquet"


def dir3_units_dimension(reference_dir: str | Path) -> Path | None:
    """Return the DIR3 dimension path when it is already built, else None.

    A missing dimension is not an error: the caller must skip DIR3 enrichment
    and keep the canonical ``buyer_id``/``buyer_name``, which are already
    valid for Gold. See the module docstring.
    """

    path = dir3_units_path(reference_dir)
    return path if path.is_file() else None


def read_cpv_dimension(spark: "SparkSession", path: str | Path) -> "DataFrame":
    """Read the CPV 2008 reference dimension with an explicit contract.

    Fails on schema drift, null keys or duplicate ``cpv_code`` values, so the
    returned frame is a validated join dimension.
    """

    frame = read_validated_parquet(spark, path, schema_from_fields(CPV_DIMENSION_FIELDS))
    _assert_dimension_key_unique(frame, "cpv_code", "CPV")
    return frame


def read_dir3_dimension(spark: "SparkSession", path: str | Path) -> "DataFrame":
    """Read the DIR3 units dimension with an explicit contract.

    Fails on schema drift, null keys or duplicate ``dir3_code`` values, so the
    returned frame is a validated join dimension.
    """

    frame = read_validated_parquet(spark, path, schema_from_fields(DIR3_DIMENSION_FIELDS))
    _assert_dimension_key_unique(frame, "dir3_code", "DIR3")
    return frame


def enrich_cpv(events: "DataFrame", cpv_dimension: "DataFrame") -> tuple["DataFrame", dict]:
    """Explode canonical ``cpv_codes`` and attach official CPV attributes.

    Returns the ``cpv_enriched`` dataset (grain: one row per
    ``(event_id, cpv_code)`` occurrence, original codes and positions kept)
    and measured metrics. Codes absent from the dimension are preserved with
    null attributes and ``cpv_matched=false``; events without CPV codes
    produce no rows and are counted as ``events_without_cpv_codes``.
    """

    from pyspark.sql import functions as F

    _assert_required_columns(events, ("event_id", "procedure_id", "source", "cpv_codes"))
    dimension = cpv_dimension.select(
        "cpv_code",
        F.lit(True).alias("_dimension_matched"),
        "label_es",
        "label_en",
        "level",
        "parent_code",
        "is_leaf",
    )
    _assert_dimension_key_unique(dimension, "cpv_code", "CPV")

    duplicate_pairs = (
        events.select("event_id", F.explode("cpv_codes").alias("cpv_code"))
        .groupBy("event_id", "cpv_code")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_pairs:
        raise ValueError(
            "duplicate cpv_codes inside one event violate the canonical Silver contract"
        )

    occurrences = events.select(
        "event_id",
        "procedure_id",
        "source",
        F.posexplode("cpv_codes").alias("cpv_position", "cpv_code"),
    )
    fact_rows = occurrences.count()
    enriched = (
        occurrences.join(dimension, on="cpv_code", how="left")
        .select(
            "event_id",
            "procedure_id",
            "source",
            "cpv_position",
            "cpv_code",
            F.coalesce("_dimension_matched", F.lit(False)).alias("cpv_matched"),
            "label_es",
            "label_en",
            "level",
            "parent_code",
            "is_leaf",
        )
    )
    expected_schema = schema_from_fields(CPV_ENRICHED_FIELDS)
    _assert_row_count_unchanged(fact_rows, enriched, "CPV enrichment")
    assert_contract_schema(enriched, expected_schema)

    event_stats = events.agg(
        F.count(F.lit(1)).alias("input_events"),
        F.coalesce(
            F.sum(F.when(F.size("cpv_codes") > 0, F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("events_with_cpv_codes"),
    ).collect()[0]
    occurrence_stats = enriched.agg(
        F.count(F.lit(1)).alias("cpv_occurrences"),
        F.countDistinct("cpv_code").alias("distinct_cpv_codes"),
        F.coalesce(
            F.sum(F.when(F.col("cpv_matched"), F.lit(1)).otherwise(F.lit(0))), F.lit(0)
        ).alias("resolved_occurrences"),
        F.coalesce(
            F.sum(F.when(~F.col("cpv_matched"), F.lit(1)).otherwise(F.lit(0))), F.lit(0)
        ).alias("unresolved_occurrences"),
        F.countDistinct(
            F.when(~F.col("cpv_matched"), F.col("cpv_code"))
        ).alias("distinct_unresolved_cpv_codes"),
    ).collect()[0]
    metrics = {
        "dataset": CPV_ENRICHED_DATASET,
        "schema_version": CPV_ENRICHED_SCHEMA_VERSION,
        "input_events": event_stats["input_events"],
        "events_with_cpv_codes": event_stats["events_with_cpv_codes"],
        "events_without_cpv_codes": event_stats["input_events"]
        - event_stats["events_with_cpv_codes"],
        "cpv_occurrences": occurrence_stats["cpv_occurrences"],
        "resolved_occurrences": occurrence_stats["resolved_occurrences"],
        "unresolved_occurrences": occurrence_stats["unresolved_occurrences"],
        "distinct_cpv_codes": occurrence_stats["distinct_cpv_codes"],
        "distinct_unresolved_cpv_codes": occurrence_stats["distinct_unresolved_cpv_codes"],
        "dimension_rows": dimension.count(),
        "dimension_duplicate_keys": 0,
    }
    return enriched, metrics


def enrich_buyers_dir3(
    events: "DataFrame",
    dir3_dimension: "DataFrame",
) -> tuple["DataFrame", dict]:
    """Attach official DIR3 attributes to events via ``buyer_id = dir3_code``.

    Returns the ``buyer_dir3_enriched`` dataset: the canonical event grain
    with all original columns unchanged plus the DIR3 attribute columns.
    ``buyer_id`` is never rewritten; events without ``buyer_id`` or with an
    unmatched id are preserved with null attributes and
    ``buyer_dir3_matched=false``.
    """

    from pyspark.sql import functions as F

    _assert_required_columns(events, tuple(field.name for field in CANONICAL_SILVER_FIELDS))
    collisions = [field.name for field in BUYER_DIR3_ATTRIBUTE_FIELDS if field.name in events.columns]
    if collisions:
        raise ValueError(f"events already contain enrichment columns: {collisions}")

    dimension = dir3_dimension.select(
        F.col("dir3_code"),
        F.col("name").alias("buyer_dir3_name"),
        F.col("administration_scope").alias("buyer_dir3_scope"),
        F.col("public_entity_type").alias("buyer_dir3_entity_type"),
        F.col("hierarchy_level").alias("buyer_dir3_hierarchy_level"),
        F.col("parent_dir3_code").alias("buyer_dir3_parent_code"),
        F.col("principal_dir3_code").alias("buyer_dir3_principal_code"),
        F.col("status").alias("buyer_dir3_status"),
        F.col("nif_cif").alias("buyer_dir3_nif"),
        F.lit(True).alias("_dimension_matched"),
    )
    _assert_dimension_key_unique(dimension, "dir3_code", "DIR3")

    fact_rows = events.count()
    enriched = (
        events.join(dimension, events["buyer_id"] == dimension["dir3_code"], "left")
        .withColumn(
            "_buyer_dir3_matched", F.coalesce(F.col("_dimension_matched"), F.lit(False))
        )
        .select(
            *[field.name for field in CANONICAL_SILVER_FIELDS],
            F.col("_buyer_dir3_matched").alias("buyer_dir3_matched"),
            "buyer_dir3_name",
            "buyer_dir3_scope",
            "buyer_dir3_entity_type",
            "buyer_dir3_hierarchy_level",
            "buyer_dir3_parent_code",
            "buyer_dir3_principal_code",
            "buyer_dir3_status",
            "buyer_dir3_nif",
        )
    )
    expected_schema = schema_from_fields(BUYER_DIR3_ENRICHED_FIELDS)
    _assert_row_count_unchanged(fact_rows, enriched, "DIR3 buyer enrichment")
    assert_contract_schema(enriched, expected_schema)

    stats = enriched.agg(
        F.count(F.lit(1)).alias("input_events"),
        F.coalesce(
            F.sum(F.when(F.col("buyer_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("events_with_buyer_id"),
        F.coalesce(
            F.sum(F.when(F.col("buyer_dir3_matched"), F.lit(1)).otherwise(F.lit(0))),
            F.lit(0),
        ).alias("resolved_events"),
        F.coalesce(
            F.sum(
                F.when(
                    F.col("buyer_id").isNotNull() & ~F.col("buyer_dir3_matched"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ),
            F.lit(0),
        ).alias("unresolved_events_with_buyer_id"),
    ).collect()[0]
    metrics = {
        "dataset": BUYER_DIR3_ENRICHED_DATASET,
        "schema_version": BUYER_DIR3_ENRICHED_SCHEMA_VERSION,
        "input_events": stats["input_events"],
        "events_with_buyer_id": stats["events_with_buyer_id"],
        "events_without_buyer_id": stats["input_events"] - stats["events_with_buyer_id"],
        "resolved_events": stats["resolved_events"],
        "unresolved_events_with_buyer_id": stats["unresolved_events_with_buyer_id"],
        "events_with_multiple_dir3_matches": 0,
        "dimension_rows": dimension.count(),
        "dimension_duplicate_keys": 0,
    }
    return enriched, metrics


def _assert_required_columns(frame: "DataFrame", columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


def _assert_dimension_key_unique(
    dimension: "DataFrame", key: str, label: str
) -> None:
    """Reject a dimension whose join key is not a primary key.

    This is the guard that makes the enrichment joins deterministic: a
    validated key can match at most one dimension row, so a dimension defect
    fails loudly here instead of multiplying facts silently downstream.
    """

    from pyspark.sql import functions as F

    duplicates = (
        dimension.groupBy(key)
        .count()
        .where(F.col("count") > 1)
        .orderBy(key)
        .limit(5)
        .collect()
    )
    if duplicates:
        keys = [row[key] for row in duplicates]
        raise ValueError(f"{label} dimension has duplicate {key} values: {keys}")


def _assert_row_count_unchanged(expected: int, enriched: "DataFrame", label: str) -> None:
    """Assert a dimension join did not multiply or drop fact rows."""

    actual = enriched.count()
    if actual != expected:
        raise ValueError(
            f"{label} changed the fact row count: before={expected} after={actual}"
        )
