from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

from tfm_licitaciones.gold_contract import (
    CANONICAL_SILVER_FIELDS,
    CURRENT_STATE_FIELDS,
    GOLD_OPEN_OPPORTUNITIES_FIELDS,
)
from tfm_licitaciones.models import ProcurementEvent, procurement_events_frame
from tfm_licitaciones.spark_foundation import (
    PYSPARK_VERSION,
    assert_contract_schema,
    canonical_silver_schema,
    create_spark_session,
    read_canonical_silver,
    schema_from_fields,
    write_typed_parquet,
)


def _valid_row() -> dict[str, object]:
    return {
        "event_id": "ted:notice:1",
        "procedure_id": "ted:procedure:A",
        "source": "ted",
        "source_event_type": "notice",
        "buyer_id": None,
        "buyer_name": "Buyer",
        "title": "Example",
        "description": None,
        "cpv_codes": ["72000000"],
        "estimated_value": Decimal("100.00"),
        "awarded_value": None,
        "currency": "EUR",
        "publication_date": date(2026, 1, 1),
        "source_updated_at": None,
        "deadline": None,
        "status": None,
        "nuts_code": None,
        "country": "ES",
        "source_url": None,
        "ingested_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }


@unittest.skipIf(pyspark is None, f"requires pyspark=={PYSPARK_VERSION}")
class SparkFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark_session(
            app_name="tfm-licitaciones-test-gold-foundation",
            master="local[1]",
            shuffle_partitions=2,
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_session_is_utc_and_controlled(self) -> None:
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
        self.assertEqual(self.spark.conf.get("spark.sql.shuffle.partitions"), "2")
        self.assertEqual(self.spark.conf.get("spark.sql.parquet.compression.codec"), "zstd")
        self.assertEqual(self.spark.version, PYSPARK_VERSION)

    def test_canonical_silver_contract_maps_exactly_to_spark(self) -> None:
        schema = canonical_silver_schema()
        self.assertFalse(schema["event_id"].nullable)
        self.assertTrue(schema["procedure_id"].nullable)
        self.assertEqual(schema["estimated_value"].dataType.simpleString(), "decimal(20,2)")
        self.assertFalse(schema["cpv_codes"].nullable)
        self.assertFalse(schema["cpv_codes"].dataType.containsNull)
        self.assertFalse(schema["ingested_at"].nullable)

    def test_reads_complete_canonical_silver_with_explicit_schema(self) -> None:
        schema = canonical_silver_schema()
        frame = self.spark.createDataFrame([_valid_row()], schema=schema)
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            frame.write.mode("overwrite").parquet(str(silver))
            loaded = read_canonical_silver(self.spark, silver)
            self.assertEqual(loaded.collect()[0]["event_id"], "ted:notice:1")

    def test_reads_real_polars_canonical_silver(self) -> None:
        event = ProcurementEvent(
            event_id="ted:notice:polars",
            procedure_id="ted:procedure:POLARS",
            source="ted",
            source_event_type="notice",
            buyer_name="Buyer",
            title="Polars production boundary",
            cpv_codes=("72000000",),
            estimated_value=Decimal("123.45"),
            currency="EUR",
            publication_date=date(2026, 1, 1),
            country="ES",
            ingested_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            procurement_events_frame([event]).write_parquet(silver)
            loaded = read_canonical_silver(self.spark, silver)
            row = loaded.collect()[0]
            self.assertEqual(row["event_id"], event.event_id)
            self.assertEqual(row["estimated_value"], Decimal("123.45"))

    def test_read_rejects_physically_missing_optional_column(self) -> None:
        schema = canonical_silver_schema()
        frame = self.spark.createDataFrame([_valid_row()], schema=schema).drop("status")
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            frame.write.mode("overwrite").parquet(str(silver))
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                read_canonical_silver(self.spark, silver)

    def test_read_rejects_null_in_required_column_after_metadata_relaxes(self) -> None:
        schema = canonical_silver_schema()
        row = _valid_row()
        row["event_id"] = None
        frame = self.spark.createDataFrame([row], schema=schema.toNullable())
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            frame.write.mode("overwrite").parquet(str(silver))
            with self.assertRaisesRegex(ValueError, "required columns contain nulls"):
                read_canonical_silver(self.spark, silver)

    def test_read_rejects_null_array_element_after_metadata_relaxes(self) -> None:
        schema = canonical_silver_schema()
        row = _valid_row()
        row["cpv_codes"] = ["72000000", None]
        frame = self.spark.createDataFrame([row], schema=schema.toNullable())
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            frame.write.mode("overwrite").parquet(str(silver))
            with self.assertRaisesRegex(ValueError, "array columns contain null elements"):
                read_canonical_silver(self.spark, silver)

    def test_typed_writer_rejects_schema_drift(self) -> None:
        schema = canonical_silver_schema()
        frame = self.spark.createDataFrame([("x",)], ["event_id"])
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                write_typed_parquet(
                    frame,
                    Path(tmp) / "out",
                    expected_schema=schema,
                    order_by=("event_id",),
                )

    def test_engine_neutral_current_state_schema_maps_to_spark(self) -> None:
        schema = schema_from_fields(CURRENT_STATE_FIELDS)
        self.assertEqual(
            tuple((field.name, field.dataType.simpleString(), field.nullable) for field in schema),
            tuple((field.name, field.logical_type, field.nullable) for field in CURRENT_STATE_FIELDS),
        )
        self.assertFalse(schema["cpv_codes"].dataType.containsNull)

    def test_engine_neutral_gold_schema_maps_to_spark(self) -> None:
        schema = schema_from_fields(GOLD_OPEN_OPPORTUNITIES_FIELDS)
        self.assertEqual(
            tuple((field.name, field.dataType.simpleString(), field.nullable) for field in schema),
            tuple(
                (field.name, field.logical_type, field.nullable)
                for field in GOLD_OPEN_OPPORTUNITIES_FIELDS
            ),
        )
        self.assertFalse(schema["cpv_codes"].dataType.containsNull)


if __name__ == "__main__":
    unittest.main()
