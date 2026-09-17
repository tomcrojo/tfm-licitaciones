"""Minimal local Spark foundation for Silver -> current-state -> Gold work.

The module is intentionally limited to reproducible session creation, canonical
Silver Parquet IO and typed Parquet output. It contains no current-state,
enrichment, linkage, ranking or Gold business logic.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .gold_contract import CANONICAL_SILVER_FIELDS, FieldSpec

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


PYSPARK_VERSION = "4.0.1"
DEFAULT_MASTER = "local[*]"
DEFAULT_SHUFFLE_PARTITIONS = 8


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


def canonical_silver_schema() -> "StructType":
    """Return the Spark representation of the guarded canonical Silver contract."""

    return schema_from_fields(CANONICAL_SILVER_FIELDS)


def _logical_signature(schema: "StructType") -> tuple[tuple[str, str], ...]:
    return tuple((field.name, field.dataType.simpleString()) for field in schema)


def _assert_logical_schema(actual: "StructType", expected: "StructType") -> None:
    if _logical_signature(actual) != _logical_signature(expected):
        raise ValueError(
            "schema mismatch at Parquet boundary:\n"
            f"expected={expected.simpleString()}\n"
            f"actual={actual.simpleString()}"
        )


def assert_contract_schema(frame: "DataFrame", expected: "StructType") -> None:
    """Reject logical type/order drift and nulls forbidden by the contract.

    Parquet readers relax field and array-element nullability metadata even
    when an explicit schema is supplied. Those flags are therefore not used as
    type identity. Logical types are compared separately and the stronger
    nullability guarantees are checked against actual values.
    """

    _assert_logical_schema(frame.schema, expected)

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType

    missing_required = [
        field.name
        for field in expected
        if not field.nullable and frame.where(F.col(field.name).isNull()).limit(1).count()
    ]
    if missing_required:
        raise ValueError(f"required columns contain nulls: {missing_required}")

    arrays_with_null_elements = [
        field.name
        for field in expected
        if isinstance(field.dataType, ArrayType)
        and not field.dataType.containsNull
        and frame.where(F.exists(F.col(field.name), lambda item: item.isNull())).limit(1).count()
    ]
    if arrays_with_null_elements:
        raise ValueError(
            f"array columns contain null elements: {arrays_with_null_elements}"
        )


def read_canonical_silver(spark: "SparkSession", path: str | Path) -> "DataFrame":
    """Read and fully validate canonical Silver at the Parquet boundary.

    Spark can synthesize missing columns as null when an explicit schema is
    supplied. The inferred Parquet schema is therefore checked first so missing,
    reordered or mistyped columns fail at the layer boundary. The projected
    frame is then value-validated so required fields and array elements cannot
    violate the canonical contract unnoticed.
    """

    expected = canonical_silver_schema()
    inferred = spark.read.parquet(str(path))
    _assert_logical_schema(inferred.schema, expected)
    frame = spark.read.schema(expected).parquet(str(path))
    assert_contract_schema(frame, expected)
    return frame


def write_typed_parquet(
    frame: "DataFrame",
    path: str | Path,
    *,
    expected_schema: "StructType",
    order_by: Iterable[str] = (),
) -> None:
    """Validate the contract and write Parquet with an optional sort operation.

    ``order_by`` sorts the DataFrame before the distributed write, but Spark
    does not promise globally ordered rows across output part-files or stable
    physical filenames/bytes. Consumers must treat the schema and values as the
    deterministic contract, never filesystem enumeration order.
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
