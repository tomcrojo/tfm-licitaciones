"""Canonical Silver procurement_events built with PySpark over Bronze Parquet.

These tests require the optional ``spark`` extra and are skipped when
PySpark is not installed, so the default offline suite stays lightweight.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from tfm_licitaciones.bronze import BRONZE_SCHEMA, TOMBSTONE_SCHEMA
from tfm_licitaciones.io import read_parquet, write_parquet
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA

try:
    from pyspark.sql import SparkSession

    from tfm_licitaciones.silver import build_procurement_events, spark_session

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

RETRIEVED_AT = datetime(2026, 2, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
LATER = datetime(2026, 3, 1, 9, 0, 0, tzinfo=timezone.utc)


def _record(
    tender_id: str,
    title: str,
    *,
    source: str = "placsp",
    updated: str = "2026-01-05T10:00:00.000+01:00",
    amount: float | None = 120000.0,
    retrieved_at: datetime = RETRIEVED_AT,
    payload: str = '{"title":"x"}',
    locator: str = "entry:1",
    source_file: str = "placsp/month.zip",
) -> dict:
    return {
        "source": source,
        "source_file": source_file,
        "source_member": "feed.atom",
        "source_member_index": 1,
        "record_locator": locator,
        "source_record_id": tender_id,
        "raw_sha256": f"{abs(hash((source, tender_id, title, retrieved_at))):064x}",
        "raw_retrieved_at": retrieved_at,
        "payload_json": payload,
        "tender_id": tender_id,
        "title": title,
        "summary": "resumen",
        "buyer": "Comprador",
        "published_date": None,
        "amount": amount,
        "currency": "EUR",
        "country": "ES",
        "url": "https://example.invalid",
        "cpv_codes": ["48000000"],
        "buyer_id": "EA0000001",
        "region": None,
        "status": "PR",
        "nuts_code": "ES300",
        "updated": updated,
        "raw_amount_present": amount is not None,
    }


def _tombstone(ref: str, *, source_file: str = "placsp/month.zip", locator: str = "deleted-entry:1") -> dict:
    return {
        "source": "placsp",
        "source_file": source_file,
        "source_member": "feed.atom",
        "source_member_index": 1,
        "record_locator": locator,
        "source_record_id": ref,
        "raw_sha256": f"{abs(hash((ref, source_file, locator))):064x}",
        "raw_retrieved_at": RETRIEVED_AT,
    }


@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark is not installed (optional 'spark' extra)")
class ProcurementEventsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bronze = self.root / "bronze"
        self.silver = self.root / "silver"

    def _build(self, records: list[dict], tombstones: list[dict] | None = None) -> pl.DataFrame:
        write_parquet(self.bronze / "records.parquet", pl.DataFrame(records, schema=BRONZE_SCHEMA))
        write_parquet(
            self.bronze / "tombstones.parquet",
            pl.DataFrame(tombstones or [], schema=TOMBSTONE_SCHEMA),
        )
        summary = build_procurement_events(self.bronze, self.silver, spark=self.spark)
        self.assertEqual(summary["engine"], "pyspark")
        return read_parquet(self.silver / "procurement_events.parquet")

    def test_schema_matches_the_canonical_contract(self) -> None:
        events = self._build([_record("10000101", "Servicio")])
        self.assertEqual(events.schema, PROCUREMENT_EVENT_SCHEMA)

    def test_revisions_are_append_only_events_of_one_procedure(self) -> None:
        events = self._build(
            [
                _record("10000101", "Contrato", updated="2026-01-05T10:00:00.000+01:00", locator="entry:1"),
                _record("10000101", "Contrato corregido", updated="2026-01-20T10:00:00.000+01:00", locator="entry:2"),
            ]
        )
        self.assertEqual(events.height, 2)
        self.assertEqual(events["procedure_id"].n_unique(), 1)
        self.assertEqual(set(events["title"]), {"Contrato", "Contrato corregido"})
        self.assertTrue((events["source_event_type"] == "notice").all())

    def test_identical_retrievals_deduplicate_keeping_earliest_evidence(self) -> None:
        events = self._build(
            [
                _record("10000101", "Contrato", retrieved_at=LATER, source_file="placsp/second.zip"),
                _record("10000101", "Contrato", retrieved_at=RETRIEVED_AT, source_file="placsp/first.zip"),
            ]
        )
        self.assertEqual(events.height, 1)
        self.assertEqual(events["ingested_at"].to_list(), [RETRIEVED_AT])

    def test_tombstones_become_explicit_events_with_procedure_identity(self) -> None:
        events = self._build(
            [_record("10000101", "Contrato")],
            [_tombstone("https://example.invalid/10000101")],
        )
        tombstone = events.filter(pl.col("source_event_type") == "tombstone")
        self.assertEqual(tombstone.height, 1)
        self.assertEqual(tombstone["procedure_id"].to_list(), ["placsp:procedure:10000101"])
        self.assertEqual(tombstone["ingested_at"].to_list(), [RETRIEVED_AT])
        self.assertEqual(tombstone["cpv_codes"].to_list(), [[]])

    def test_decimal_and_timestamp_semantics_survive_the_boundary(self) -> None:
        events = self._build([_record("10000101", "Contrato")])
        row = events.row(0, named=True)
        self.assertEqual(str(row["estimated_value"]), "120000.00")
        self.assertEqual(row["source_updated_at"], datetime(2026, 1, 5, 9, 0, 0, tzinfo=timezone.utc))

    def test_rebuild_is_deterministic(self) -> None:
        records = [
            _record("10000101", "Contrato"),
            _record("A-1", "Aviso", source="ted", updated="", amount=None, source_file="ted/enero.jsonl"),
        ]
        first = self._build(records)
        second = self._build(records)
        self.assertTrue(first.equals(second))
        self.assertEqual(first["event_id"].to_list(), sorted(first["event_id"].to_list()))

    def test_ted_notices_carry_null_procedure_without_version_marker(self) -> None:
        events = self._build([_record("A-1", "Aviso", source="ted", updated="", amount=None)])
        row = events.row(0, named=True)
        self.assertTrue(row["event_id"].startswith("ted:event:A-1:"))
        self.assertIsNone(row["procedure_id"])
        self.assertIsNone(row["estimated_value"])


if __name__ == "__main__":
    unittest.main()
