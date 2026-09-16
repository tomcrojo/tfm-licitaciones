"""Workload-pin tests: the measured Bronze dataset must stay historical.

The ``python-row`` baseline is frozen, but the workload itself comes from
the live production generator (``tfm_licitaciones.bench_silver``). These
tests pin the exact Bronze bytes-behind-the-metrics for tiny/small seed 7
(fingerprint over canonicalized logical row values, never Parquet writer
bytes) and prove the fingerprint is sensitive to generator changes.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from experiments.silver_engine_comparison.workload import (
    HISTORICAL_WORKLOADS,
    WORKLOAD_GENERATOR_COMMIT,
    assert_historical_workload,
    fingerprint_dataset_dir,
)
from tfm_licitaciones.bench_silver import dataset_profile, write_bronze_parts


class WorkloadPinTests(unittest.TestCase):
    """Pinned historical workloads must reproduce bit-for-bit logically."""

    def test_generator_pin_is_documented(self) -> None:
        self.assertEqual(WORKLOAD_GENERATOR_COMMIT, "e69016f62fb985639e17153e24b3a570f2f39c20")
        self.assertIn(("tiny", 7, False), HISTORICAL_WORKLOADS)
        self.assertIn(("small", 7, False), HISTORICAL_WORKLOADS)

    def test_tiny_seed7_workload_matches_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "ds", seed=7, with_collision=False, **dataset_profile("tiny"))
            actual = assert_historical_workload(Path(tmp) / "ds", profile="tiny", seed=7)
        expected = HISTORICAL_WORKLOADS[("tiny", 7, False)]
        self.assertEqual(actual["sha256"], expected["sha256"])
        self.assertEqual(actual["bronze_records"], 303)
        self.assertEqual(actual["bronze_tombstones"], 7)

    def test_small_seed7_workload_matches_pin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "ds", seed=7, with_collision=False, **dataset_profile("small"))
            actual = assert_historical_workload(Path(tmp) / "ds", profile="small", seed=7)
        expected = HISTORICAL_WORKLOADS[("small", 7, False)]
        self.assertEqual(actual["sha256"], expected["sha256"])
        # Cross-checked against the retained small-profile evidence:
        # results/engines-small-seed7-2026-09-16.json (24 862 + 501 = 25 363).
        self.assertEqual(actual["bronze_records"], 24862)
        self.assertEqual(actual["bronze_tombstones"], 501)

    def test_fingerprint_is_sensitive_to_generator_changes(self) -> None:
        import polars as pl

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "ds"
            write_bronze_parts(root, seed=7, with_collision=False, **dataset_profile("tiny"))
            before = fingerprint_dataset_dir(root)
            # Simulate a count-preserving generator change: rewrite one part
            # file with a single altered payload byte.
            part = sorted((root / "records").glob("part-*.parquet"))[0]
            frame = pl.read_parquet(part)
            # Force a real content change (append a marker key to one payload).
            rows = frame.to_dicts()
            rows[0] = {**rows[0], "payload_json": rows[0]["payload_json"][:-1] + ',"_drift":1}'}
            pl.DataFrame(rows, schema=frame.schema).write_parquet(part)
            after = fingerprint_dataset_dir(root)
            self.assertNotEqual(before["sha256"], after["sha256"])
            with self.assertRaises(ValueError) as context:
                assert_historical_workload(root, profile="tiny", seed=7)
            self.assertIn("Historical workload drift", str(context.exception))

    def test_unpinned_workloads_run_unchecked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "ds", seed=11, with_collision=False, **dataset_profile("tiny"))
            actual = assert_historical_workload(Path(tmp) / "ds", profile="tiny", seed=11)
        self.assertGreater(actual["bronze_records"], 0)


if __name__ == "__main__":
    unittest.main()
