"""Tests for the Polars-native Parquet IO helpers."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from tfm_licitaciones.io import read_parquet, write_parquet


class ParquetIOTests(unittest.TestCase):
    """Verify typed DataFrame round trips through the analytical storage format."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_round_trip_preserves_schema_and_values(self) -> None:
        frame = pl.DataFrame(
            {
                "tender_id": ["10000101", "10000102"],
                "title": ["Servicio de migración a plataforma cloud", None],
                "amount": [1234.5, None],
                "position": [7, None],
                "awarded": [True, False],
                "buyer": [None, "ORGANISMO"],
                "publication_date": [date(2026, 1, 2), None],
                "source_updated_at": [datetime(2026, 1, 3, 4, 5, 6, 789), datetime(2026, 2, 1)],
                "fetched_at": [datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), None],
            }
        )
        path = self.tmp / "silver" / "tenders.parquet"
        count = write_parquet(path, frame)
        self.assertEqual(count, 2)
        self.assertTrue(path.parent.is_dir())
        result = read_parquet(path)
        self.assertEqual(result.schema, frame.schema)
        self.assertTrue(result.equals(frame))
        self.assertEqual(result["tender_id"].to_list(), ["10000101", "10000102"])
        self.assertIsNone(result["title"][1])
        self.assertEqual(result["publication_date"][0], date(2026, 1, 2))
        self.assertEqual(result["source_updated_at"][0], datetime(2026, 1, 3, 4, 5, 6, 789))
        self.assertEqual(result["fetched_at"][0], datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        self.assertIsNone(result["fetched_at"][1])

    def test_empty_frame_with_explicit_schema_round_trips(self) -> None:
        schema = pl.Schema(
            {
                "tender_id": pl.String,
                "amount": pl.Float64,
                "awarded": pl.Boolean,
                "publication_date": pl.Date,
                "source_updated_at": pl.Datetime("us"),
                "fetched_at": pl.Datetime("us", "UTC"),
            }
        )
        path = self.tmp / "empty.parquet"
        self.assertEqual(write_parquet(path, pl.DataFrame(schema=schema)), 0)
        result = read_parquet(path)
        self.assertEqual(result.height, 0)
        self.assertEqual(result.columns, list(schema.keys()))
        self.assertEqual(result.schema, schema)

    def test_write_is_overwrite_and_returns_row_count(self) -> None:
        path = self.tmp / "rewrite.parquet"
        self.assertEqual(write_parquet(path, pl.DataFrame({"tender_id": ["1"]})), 1)
        self.assertEqual(write_parquet(path, pl.DataFrame({"tender_id": ["2", "3"]})), 2)
        result = read_parquet(path)
        self.assertEqual(result.height, 2)
        self.assertEqual(result["tender_id"].to_list(), ["2", "3"])


if __name__ == "__main__":
    unittest.main()
