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

from tfm_licitaciones.gold_contract import CURRENT_STATE_FIELDS
from tfm_licitaciones.spark_foundation import (
    PYSPARK_VERSION,
    assert_contract_schema,
    canonical_silver_schema,
    create_spark_session,
    read_canonical_silver,
    schema_from_fields,
    write_typed_parquet,
)


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

    def test_reads_canonical_silver_with_explicit_schema(self) -> None:
        schema = canonical_silver_schema()
        row = {
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
        frame = self.spark.createDataFrame([row], schema=schema)
        with tempfile.TemporaryDirectory() as tmp:
            silver = Path(tmp) / "procurement_events.parquet"
            frame.write.mode("overwrite").parquet(str(silver))
            loaded = read_canonical_silver(self.spark, silver)
            assert_contract_schema(loaded, schema)
            self.assertEqual(loaded.collect()[0]["event_id"], "ted:notice:1")

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
        self.assertEqual(schema[0].name, "procedure_id")
        self.assertFalse(schema[0].nullable)
        self.assertEqual(schema[-1].name, "is_deleted")


if __name__ == "__main__":
    unittest.main()
