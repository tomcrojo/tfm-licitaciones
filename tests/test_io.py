"""Tests for the Parquet IO helpers."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from tfm_licitaciones.io import read_parquet, write_parquet


class ParquetIOTests(unittest.TestCase):
    """Verify typed round trips through the analytical storage format."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_round_trip_preserves_canonical_field_types(self) -> None:
        rows = [
            {
                "tender_id": "10000101",
                "title": "Servicio de migración a plataforma cloud",
                "amount": 1234.5,
                "position": 7,
                "awarded": True,
                "buyer": None,
                "publication_date": date(2026, 1, 2),
                "source_updated_at": datetime(2026, 1, 3, 4, 5, 6, 789),
            },
            {
                "tender_id": "10000102",
                "title": None,
                "amount": None,
                "position": None,
                "awarded": False,
                "buyer": "ORGANISMO",
                "publication_date": None,
                "source_updated_at": datetime(2026, 2, 1, 0, 0, 0),
            },
        ]
        path = self.tmp / "silver" / "tenders.parquet"
        count = write_parquet(path, rows)
        self.assertEqual(count, 2)
        self.assertTrue(path.parent.is_dir())
        self.assertEqual(read_parquet(path), rows)

    def test_all_null_column_round_trips_as_none(self) -> None:
        rows = [{"tender_id": "1", "cpv_main": None}, {"tender_id": "2", "cpv_main": None}]
        path = self.tmp / "nulls.parquet"
        write_parquet(path, rows)
        self.assertEqual(read_parquet(path), rows)

    def test_write_is_overwrite_and_returns_row_count(self) -> None:
        path = self.tmp / "rewrite.parquet"
        self.assertEqual(write_parquet(path, [{"tender_id": "1"}]), 1)
        self.assertEqual(write_parquet(path, [{"tender_id": "2"}, {"tender_id": "3"}]), 2)
        self.assertEqual([row["tender_id"] for row in read_parquet(path)], ["2", "3"])

    def test_round_trip_preserves_timezone_aware_timestamp(self) -> None:
        rows = [{"fetched_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)}]
        path = self.tmp / "tz.parquet"
        write_parquet(path, rows)
        self.assertEqual(read_parquet(path), rows)


if __name__ == "__main__":
    unittest.main()
