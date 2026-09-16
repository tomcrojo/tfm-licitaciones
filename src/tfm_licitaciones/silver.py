"""Canonical Silver ``procurement_events`` built with PySpark.

Engine boundary: identity construction, revision/event typing, tombstone
events, decimal/timestamp casting, collision detection and deterministic
deduplication are native Spark DataFrame operations reading and writing the
Parquet boundary. There are no Python UDFs and nothing but tiny collision
summaries is ever collected to the driver.

The canonical contract is append-only: revisions keep their own ``event_id``
sharing a ``procedure_id``, tombstones become explicit ``tombstone`` events,
and no row is folded or deleted. The legacy revision folding of the
compatibility Silver path is intentionally not reproduced here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

PROCUREMENT_EVENT_COLUMNS = [
    "event_id",
    "procedure_id",
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
]

SPARK_UNAVAILABLE = {
    "engine": None,
    "reason": "pyspark is not installed; canonical procurement_events require the 'spark' extra",
}


def spark_session():
    """Return the shared local Spark session used by the Silver stage."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("tfm-licitaciones-silver")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Micros + UTC annotation keeps the Parquet boundary identical to the
        # Polars-written contract types (timestamp[us, UTC]) on read-back.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _content_marker(columns: list[str]):
    """Deterministic short hash of the canonical content of one record.

    Event identity is content-addressed: the published identifier plus its
    version marker (PLACSP ``updated``) plus this marker. Reprocessing the
    same payload reproduces the same ``event_id``; a corrected payload under
    the same published id and marker becomes a new append-only revision
    instead of an ambiguous collision. Ambiguous Raw snapshots already fail
    at the Bronze boundary, before any event is built.
    """

    from pyspark.sql import functions as F

    return F.substring(F.md5(F.to_json(F.struct(*[F.col(name) for name in columns]))), 1, 12)


_CONTENT_COLUMNS = [
    "title",
    "summary",
    "buyer",
    "buyer_id",
    "published_date",
    "amount",
    "currency",
    "country",
    "url",
    "cpv_codes",
    "region",
    "status",
    "nuts_code",
]

#: The same content seen through the canonical event column names, used by
#: the post-projection invariant guard.
_EVENT_CONTENT_COLUMNS = [
    "title",
    "description",
    "buyer_id",
    "buyer_name",
    "cpv_codes",
    "estimated_value",
    "currency",
    "publication_date",
    "status",
    "nuts_code",
    "country",
    "source_url",
]


def _records_events(records):
    """Project accepted Bronze records into canonical notice events."""

    from pyspark.sql import functions as F

    marker = _content_marker(_CONTENT_COLUMNS)
    # Sources with a stable per-notice identifier (TED ND, BOE item id):
    # the notice identity is the event identity; no procedure grouping is
    # extractable from the fetched fields, so procedure_id stays null.
    plain = records.filter(F.col("source") != "placsp").select(
        F.concat(F.col("source"), F.lit(":event:"), F.col("tender_id"), F.lit(":"), marker).alias("event_id"),
        F.lit(None).cast("string").alias("procedure_id"),
        F.col("source").alias("source"),
        F.lit("notice").alias("source_event_type"),
        F.lit(None).cast("string").alias("buyer_id"),
        F.col("buyer").alias("buyer_name"),
        F.col("title").alias("title"),
        F.col("summary").alias("description"),
        F.col("cpv_codes").alias("cpv_codes"),
        F.col("amount").cast("decimal(20,2)").alias("estimated_value"),
        F.lit(None).cast("decimal(20,2)").alias("awarded_value"),
        F.col("currency").alias("currency"),
        F.col("published_date").alias("publication_date"),
        F.lit(None).cast("timestamp").alias("source_updated_at"),
        F.lit(None).cast("timestamp").alias("deadline"),
        F.col("status").alias("status"),
        F.col("nuts_code").alias("nuts_code"),
        F.col("country").alias("country"),
        F.col("url").alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        F.col("payload_json").alias("payload_json"),
        F.col("raw_sha256").alias("raw_sha256"),
        F.col("source_file").alias("source_file"),
        F.col("record_locator").alias("record_locator"),
    )
    # OpenPLACSP reuses the Atom id across revisions, so the event identity
    # includes the published update marker and the procedure groups them.
    placsp = records.filter(F.col("source") == "placsp").select(
        F.concat(
            F.lit("placsp:event:"), F.col("tender_id"), F.lit(":"), F.col("updated"), F.lit(":"), marker
        ).alias("event_id"),
        F.concat(F.lit("placsp:procedure:"), F.col("tender_id")).alias("procedure_id"),
        F.col("source").alias("source"),
        F.lit("notice").alias("source_event_type"),
        F.col("buyer_id").alias("buyer_id"),
        F.col("buyer").alias("buyer_name"),
        F.col("title").alias("title"),
        F.col("summary").alias("description"),
        F.col("cpv_codes").alias("cpv_codes"),
        F.col("amount").cast("decimal(20,2)").alias("estimated_value"),
        F.lit(None).cast("decimal(20,2)").alias("awarded_value"),
        F.col("currency").alias("currency"),
        F.col("published_date").alias("publication_date"),
        F.coalesce(
            F.to_timestamp("updated", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp("updated", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        ).alias("source_updated_at"),
        F.lit(None).cast("timestamp").alias("deadline"),
        F.col("status").alias("status"),
        F.col("nuts_code").alias("nuts_code"),
        F.col("country").alias("country"),
        F.col("url").alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        F.col("payload_json").alias("payload_json"),
        F.col("raw_sha256").alias("raw_sha256"),
        F.col("source_file").alias("source_file"),
        F.col("record_locator").alias("record_locator"),
    )
    return plain.unionByName(placsp)


def _tombstone_events(tombstones):
    """Project every deletion control into an explicit tombstone event."""

    from pyspark.sql import functions as F

    ref_tail = F.element_at(F.split(F.col("source_record_id"), "/"), -1)
    return tombstones.select(
        F.concat(
            F.lit("placsp:tombstone:"),
            F.col("source_record_id"),
            F.lit(":"),
            F.col("source_file"),
            F.lit(":"),
            F.col("record_locator"),
        ).alias("event_id"),
        F.concat(F.lit("placsp:procedure:"), ref_tail).alias("procedure_id"),
        F.col("source").alias("source"),
        F.lit("tombstone").alias("source_event_type"),
        F.lit(None).cast("string").alias("buyer_id"),
        F.lit(None).cast("string").alias("buyer_name"),
        F.lit(None).cast("string").alias("title"),
        F.lit(None).cast("string").alias("description"),
        F.array().cast("array<string>").alias("cpv_codes"),
        F.lit(None).cast("decimal(20,2)").alias("estimated_value"),
        F.lit(None).cast("decimal(20,2)").alias("awarded_value"),
        F.lit(None).cast("string").alias("currency"),
        F.lit(None).cast("date").alias("publication_date"),
        F.lit(None).cast("timestamp").alias("source_updated_at"),
        F.lit(None).cast("timestamp").alias("deadline"),
        F.lit(None).cast("string").alias("status"),
        F.lit(None).cast("string").alias("nuts_code"),
        F.lit("ES").alias("country"),
        F.lit(None).cast("string").alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        F.lit(None).cast("string").alias("payload_json"),
        F.col("raw_sha256").alias("raw_sha256"),
        F.col("source_file").alias("source_file"),
        F.col("record_locator").alias("record_locator"),
    )


def build_procurement_events(bronze_dir: str | Path, silver_dir: str | Path, spark=None) -> dict[str, Any]:
    """Write ``silver/procurement_events.parquet`` and return a run summary.

    Deterministic rebuilds are part of the contract: identical Bronze inputs
    produce identical events, ordered by ``event_id`` in a single file.
    Event identity is content-addressed (published id + version marker +
    canonical content hash), so reprocessed payloads deduplicate naturally
    and corrected payloads become new append-only revisions.
    """

    try:
        import pyspark.sql  # noqa: F401 - availability probe
        from pyspark.sql import Window
        from pyspark.sql import functions as F
    except ImportError:
        return dict(SPARK_UNAVAILABLE)

    session = spark if spark is not None else spark_session()
    records = session.read.parquet(str(Path(bronze_dir) / "records.parquet"))
    tombstones = session.read.parquet(str(Path(bronze_dir) / "tombstones.parquet"))
    events = _records_events(records).unionByName(_tombstone_events(tombstones))

    # Structural invariant: a content-addressed event_id must never hide two
    # different canonical contents. It cannot fire for the current identity
    # rules and exists to fail loudly if those rules ever change. Retrieval
    # evidence (ingested_at, provenance) may legitimately differ between
    # duplicate occurrences of the same event.
    collisions = events.groupBy("event_id").agg(
        F.count_distinct(F.struct(*[F.col(name) for name in _EVENT_CONTENT_COLUMNS])).alias("variants")
    ).filter(F.col("variants") > 1)
    conflicting = [row["event_id"] for row in collisions.collect()]
    if conflicting:
        raise ValueError(
            "event_id collisions with different canonical content: " + ", ".join(sorted(conflicting)[:10])
        )

    # Identical events re-retrieved in several snapshots keep their earliest
    # retrieval evidence; ties break deterministically on provenance.
    deterministic = Window.partitionBy("event_id").orderBy(
        "ingested_at", "raw_sha256", "source_file", "record_locator"
    )
    canonical = (
        events.withColumn("_rank", F.row_number().over(deterministic))
        .filter(F.col("_rank") == 1)
        .select(*PROCUREMENT_EVENT_COLUMNS)
    )
    output = Path(silver_dir) / "procurement_events.parquet"
    canonical.coalesce(1).sortWithinPartitions("event_id").write.mode("overwrite").parquet(str(output))

    by_key = {
        f"{row['source']}:{row['source_event_type']}": row["count"]
        for row in canonical.groupBy("source", "source_event_type").count().collect()
    }
    return {
        "engine": "pyspark",
        "output": str(output),
        "events": sum(by_key.values()),
        "by_source_event_type": dict(sorted(by_key.items())),
    }
