"""Minimal local Spark foundation for Silver -> current-state -> Gold work.

The module is intentionally limited to reproducible session creation, canonical
Silver Parquet IO and typed Parquet output.  It contains no current-state,
enrichment, linkage, ranking or Gold business logic.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .gold_contract import FieldSpec

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


PYSPARK_VERSION = "4.0.1"
DEFAULT_MASTER = "local[*]"
DEFAULT_SHUFFLE_PARTITIONS = 8
SILVER_DATASET_FILENAME = "procurement_events.parquet"


def _spark_types():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import types as T
    except ImportError as exc:  # pragma: no cover - exercised without the extra
        raise RuntimeError(
            f"PySpark is required for Gold/Spark work; run with pyspark=={PYSPARK_VERSION}"
        ) from exc
    return SparkSession, T


def create_spark_session(
    *,
    app_name: str = "tfm-licitaciones-gold",
    master: str = DEFAULT_MASTER,
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS,
) -> "SparkSession":
    """Create the controlled local SparkSession used by production-facing code."""

    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be >= 1")
    SparkSession, _ = _spark_types()
    session = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
    if session.version != PYSPARK_VERSION:
        session.stop()
        raise RuntimeError(
            f"unsupported Spark version {session.version}; expected {PYSPARK_VERSION}"
        )
    return session


def canonical_silver_schema() -> "StructType":
    """Return the Spark schema corresponding exactly to PROCUREMENT_EVENT_SCHEMA."""

    _, T = _spark_types()
    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("procedure_id", T.StringType(), True),
            T.StructField("source", T.StringType(), False),
            T.StructField("source_event_type", T.StringType(), False),
            T.StructField("buyer_id", T.StringType(), True),
            T.StructField("buyer_name", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("cpv_codes", T.ArrayType(T.StringType(), containsNull=False), False),
            T.StructField("estimated_value", T.DecimalType(20, 2), True),
            T.StructField("awarded_value", T.DecimalType(20, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("publication_date", T.DateType(), True),
            T.StructField("source_updated_at", T.TimestampType(), True),
            T.StructField("deadline", T.TimestampType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("nuts_code", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("source_url", T.StringType(), True),
            T.StructField("ingested_at", T.TimestampType(), False),
        ]
    )


def schema_from_fields(fields: Iterable[FieldSpec]) -> "StructType":
    """Map an engine-neutral contract to a Spark StructType."""

    _, T = _spark_types()
    mapping = {
        "string": T.StringType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
        "decimal(20,2)": T.DecimalType(20, 2),
        "boolean": T.BooleanType(),
        "array<string>": T.ArrayType(T.StringType(), containsNull=False),
    }
    result = []
    for field in fields:
        try:
            spark_type = mapping[field.logical_type]
        except KeyError as exc:
            raise ValueError(f"unsupported logical type: {field.logical_type}") from exc
        result.append(T.StructField(field.name, spark_type, field.nullable))
    return T.StructType(result)


def read_canonical_silver(spark: "SparkSession", path: str | Path) -> "DataFrame":
    """Read canonical Silver Parquet with the frozen explicit Spark schema."""

    return spark.read.schema(canonical_silver_schema()).parquet(str(path))


def assert_contract_schema(frame: "DataFrame", expected: "StructType") -> None:
    """Reject type/order drift and nulls in contract-required columns.

    Parquet readers may relax StructField nullability metadata, so nullability is
    enforced on values instead of comparing that optimizer hint byte-for-byte.
    """

    actual_signature = tuple((field.name, field.dataType) for field in frame.schema)
    expected_signature = tuple((field.name, field.dataType) for field in expected)
    if actual_signature != expected_signature:
        raise ValueError(
            "schema mismatch at Parquet boundary:\n"
            f"expected={expected.simpleString()}\n"
            f"actual={frame.schema.simpleString()}"
        )
    required = [field.name for field in expected if not field.nullable]
    if required:
        from pyspark.sql import functions as F

        missing_required = [
            column
            for column in required
            if frame.where(F.col(column).isNull()).limit(1).count()
        ]
        if missing_required:
            raise ValueError(f"required columns contain nulls: {missing_required}")


def write_typed_parquet(
    frame: "DataFrame",
    path: str | Path,
    *,
    expected_schema: "StructType",
    order_by: Iterable[str] = (),
) -> None:
    """Validate schema and write deterministic logical rows as Parquet.

    Spark does not guarantee stable physical filenames across executions.  This
    helper therefore guarantees the data contract and, when ``order_by`` is
    provided, deterministic logical row ordering before the write; consumers
    must not treat part-file names or byte-for-byte Parquet layout as identity.
    """

    assert_contract_schema(frame, expected_schema)
    columns = tuple(order_by)
    if columns:
        unknown = [column for column in columns if column not in frame.columns]
        if unknown:
            raise ValueError(f"unknown order_by columns: {unknown}")
        frame = frame.orderBy(*columns)
    (
        frame.write.mode("overwrite")
        .option("compression", "zstd")
        .parquet(str(path))
    )
