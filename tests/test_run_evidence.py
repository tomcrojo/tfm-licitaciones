"""Evidence verification must reject altered files and misleading row counts."""
import json
import tempfile
import unittest
from pathlib import Path

import polars as pl

from scripts.verify_run_evidence import sha256_file, verify_snapshot


class RunEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.payload = self.root / "example.csv"
        self.payload.write_text('title,value\n"two\nlines",1\n', encoding="utf-8")
        self.inventory = self.root / "inventory.json"
        self.entry = {
            "path": self.payload.name,
            "bytes": self.payload.stat().st_size,
            "sha256": sha256_file(self.payload),
            "rows": 1,
        }

    def verify(self):
        self.inventory.write_text(json.dumps({"artifacts": [self.entry]}))
        return verify_snapshot(self.root, self.inventory)

    def test_valid_csv_counts_records_not_physical_lines(self):
        self.assertEqual(self.verify()["verified_artifacts"], 1)

    def test_same_size_mutation_fails_checksum(self):
        self.payload.write_bytes(self.payload.read_bytes().replace(b",1", b",2"))
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.verify()

    def test_incorrect_row_count_fails(self):
        self.entry["rows"] = 2
        with self.assertRaisesRegex(ValueError, "row count mismatch"):
            self.verify()

    def test_missing_payload_fails(self):
        self.payload.unlink()
        with self.assertRaises(FileNotFoundError):
            self.verify()

    def test_parent_path_is_rejected(self):
        self.entry["path"] = "../outside.csv"
        with self.assertRaisesRegex(ValueError, "invalid inventory path"):
            self.verify()

    def test_parquet_rows_are_verified(self):
        self.payload = self.root / "example.parquet"
        pl.DataFrame({"value": [1, 2]}).write_parquet(self.payload)
        self.entry = {
            "path": self.payload.name,
            "bytes": self.payload.stat().st_size,
            "sha256": sha256_file(self.payload),
            "rows": 2,
        }
        self.assertTrue(self.verify()["passed"])
        self.entry["rows"] = 3
        with self.assertRaisesRegex(ValueError, "row count mismatch"):
            self.verify()
