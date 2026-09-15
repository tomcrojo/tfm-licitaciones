"""Tests for the generic ingestion-state primitives."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from tfm_licitaciones.ingestion_state import (
    IngestionStatus,
    PartitionChange,
    WindowState,
    accept_payload_change,
    finalize_window,
    load_window,
    mark_failed,
    mark_running,
    new_window,
    record_partition,
    record_partition_failure,
    save_window,
    summary,
    window_path,
)

FIXED_NOW = datetime(2026, 9, 15, 10, 30, tzinfo=timezone.utc)
PARTITIONS = ["202601", "202602", "202603"]


def _new_state() -> WindowState:
    return new_window("placsp", "2026-01-01", "2026-03-31", PARTITIONS)


def _download_all(state: object) -> None:
    for index, partition in enumerate(PARTITIONS):
        record_partition(state, partition, f"sha256:{index:064d}", bytes=100 + index, now=FIXED_NOW)


class NewWindowTests(unittest.TestCase):
    """Validate construction and deterministic paths."""

    def test_new_window_is_pending_with_expected_partitions(self) -> None:
        state = _new_state()
        self.assertEqual(state.status, IngestionStatus.PENDING.value)
        self.assertEqual(state.expected_partitions, PARTITIONS)
        self.assertEqual(state.downloaded_partitions, {})
        self.assertIsNone(state.started_at)
        self.assertIsNone(state.finished_at)

    def test_window_path_is_deterministic_and_per_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = window_path(root, "placsp", "2026-01-01", "2026-03-31")
            second = window_path(root, "placsp", "2026-01-01", "2026-03-31")
            other = window_path(root, "placsp", "2026-04-01", "2026-04-30")
            ted = window_path(root, "ted", "2026-01-01", "2026-03-31")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertNotEqual(first, ted)
        self.assertEqual(first.name, "2026-01-01__2026-03-31.json")

    def test_invalid_arguments_raise(self) -> None:
        with self.assertRaises(ValueError):
            new_window("Placsp", "2026-01-01", "2026-03-31", PARTITIONS)
        with self.assertRaises(ValueError):
            new_window("../escape", "2026-01-01", "2026-03-31", PARTITIONS)
        with self.assertRaises(ValueError):
            new_window("placsp", "2026-04-01", "2026-03-31", PARTITIONS)
        with self.assertRaises(ValueError):
            new_window("placsp", "2026-01-01", "2026-03-31", ["202601", "202601"])


class PersistenceTests(unittest.TestCase):
    """Validate reproducible save/load round trips."""

    def test_save_and_load_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = _new_state()
            mark_running(state, now=FIXED_NOW)
            record_partition(state, "202601", "sha256:" + "0" * 64, bytes=100, now=FIXED_NOW)
            record_partition(state, "202602", "sha256:" + "1" * 64, bytes=101, now=FIXED_NOW)
            record_partition_failure(state, "202603", "HTTP 503")
            self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.FAILED.value)
            path = save_window(Path(tmp), state)
            loaded = load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31")
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.source, state.source)
            self.assertEqual(loaded.status, state.status)
            self.assertEqual(loaded.expected_partitions, state.expected_partitions)
            self.assertEqual(loaded.downloaded_partitions["202601"].checksum, "sha256:" + "0" * 64)
            self.assertEqual(loaded.downloaded_partitions["202601"].bytes, 100)
            self.assertEqual(loaded.downloaded_partitions["202601"].downloaded_at, FIXED_NOW.isoformat())
            self.assertEqual(loaded.failures, {"202603": "HTTP 503"})
            self.assertEqual(loaded.started_at, FIXED_NOW.isoformat())
            self.assertEqual(loaded.finished_at, FIXED_NOW.isoformat())
            self.assertTrue(path.is_file())

    def test_save_writes_stable_sorted_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = _new_state()
            _download_all(state)
            save_window(Path(tmp), state)
            text = (Path(tmp) / "placsp" / "2026-01-01__2026-03-31.json").read_text(encoding="utf-8")
            payload = json.loads(text)
            self.assertEqual(list(payload.keys()), sorted(payload.keys()))
            self.assertEqual(list(payload["downloaded_partitions"].keys()), PARTITIONS)
            save_window(Path(tmp), state)
            self.assertEqual((Path(tmp) / "placsp" / "2026-01-01__2026-03-31.json").read_text(encoding="utf-8"), text)

    def test_load_returns_none_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31"))

    def test_load_rejects_corrupt_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = window_path(Path(tmp), "placsp", "2026-01-01", "2026-03-31")
            path.parent.mkdir(parents=True)
            path.write_text("{ not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31")

    def test_load_rejects_missing_keys_unknown_status_and_future_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = window_path(Path(tmp), "placsp", "2026-01-01", "2026-03-31")
            path.parent.mkdir(parents=True)
            base = {
                "version": 1,
                "source": "placsp",
                "window_start": "2026-01-01",
                "window_end": "2026-03-31",
                "expected_partitions": [],
                "downloaded_partitions": {},
            }
            path.write_text(json.dumps({"source": "placsp"}), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31")
            path.write_text(json.dumps({**base, "status": "paused"}), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31")
            path.write_text(json.dumps({**base, "version": 2, "status": "pending"}), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_window(Path(tmp), "placsp", "2026-01-01", "2026-03-31")

    def test_interrupted_save_keeps_previous_file_intact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = _new_state()
            mark_running(state, now=FIXED_NOW)
            _download_all(state)
            finalize_window(state, now=FIXED_NOW)
            save_window(root, state)
            before = window_path(root, "placsp", "2026-01-01", "2026-03-31").read_text(encoding="utf-8")

            mark_running(state, now=FIXED_NOW)
            with mock.patch("tfm_licitaciones.ingestion_state.os.replace", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    save_window(root, state)
            after = window_path(root, "placsp", "2026-01-01", "2026-03-31").read_text(encoding="utf-8")
            self.assertEqual(after, before)
            leftovers = [p for p in window_path(root, "placsp", "2026-01-01", "2026-03-31").parent.iterdir()
                         if p.name.endswith(".tmp")]
            self.assertEqual(leftovers, [])


class IdempotencyTests(unittest.TestCase):
    """Validate rerun behaviour for identical and changed payloads."""

    def test_same_partition_and_checksum_is_idempotent(self) -> None:
        state = _new_state()
        _download_all(state)
        first = state.downloaded_partitions["202601"]
        result = record_partition(state, "202601", first.checksum, bytes=100, now=FIXED_NOW)
        self.assertEqual(result, PartitionChange.UNCHANGED)
        self.assertIs(state.downloaded_partitions["202601"], first)
        self.assertEqual(state.downloaded_partitions["202601"].downloaded_at, first.downloaded_at)

    def test_rerun_of_complete_window_does_not_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = _new_state()
            mark_running(state, now=FIXED_NOW)
            _download_all(state)
            self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.COMPLETE.value)
            save_window(root, state)

            rerun = load_window(root, "placsp", "2026-01-01", "2026-03-31")
            assert rerun is not None
            before = dict(rerun.downloaded_partitions)
            mark_running(rerun, now=FIXED_NOW)
            for partition in PARTITIONS:
                self.assertEqual(
                    record_partition(rerun, partition, before[partition].checksum, now=FIXED_NOW),
                    PartitionChange.UNCHANGED,
                )
            self.assertEqual(finalize_window(rerun, now=FIXED_NOW), IngestionStatus.COMPLETE.value)
            self.assertEqual(rerun.downloaded_partitions, before)
            self.assertEqual(len(rerun.downloaded_partitions), len(PARTITIONS))
            self.assertEqual(summary(rerun)["downloaded_partitions"], len(PARTITIONS))

    def test_changed_checksum_is_detected_and_not_overwritten(self) -> None:
        state = _new_state()
        _download_all(state)
        original = state.downloaded_partitions["202601"].checksum
        result = record_partition(state, "202601", "sha256:changed", now=FIXED_NOW)
        self.assertEqual(result, PartitionChange.CHANGED)
        self.assertEqual(state.downloaded_partitions["202601"].checksum, original)
        mismatches = state.unresolved_mismatches
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0].partition, "202601")
        self.assertEqual(mismatches[0].previous_checksum, original)
        self.assertEqual(mismatches[0].new_checksum, "sha256:changed")

    def test_window_with_unresolved_change_cannot_finalize_complete(self) -> None:
        state = _new_state()
        _download_all(state)
        record_partition(state, "202601", "sha256:changed", now=FIXED_NOW)
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.FAILED.value)

    def test_accept_payload_change_supersedes_auditable(self) -> None:
        state = _new_state()
        _download_all(state)
        original = state.downloaded_partitions["202601"].checksum
        record_partition(state, "202601", "sha256:changed", now=FIXED_NOW)
        accept_payload_change(state, "202601", "sha256:changed", now=FIXED_NOW)
        self.assertEqual(state.downloaded_partitions["202601"].checksum, "sha256:changed")
        self.assertEqual(state.downloaded_partitions["202601"].superseded_checksums, [original])
        self.assertEqual(state.unresolved_mismatches, [])
        self.assertEqual(state.checksum_mismatches[0].resolved_at, FIXED_NOW.isoformat())
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.COMPLETE.value)

    def test_accept_without_matching_mismatch_raises(self) -> None:
        state = _new_state()
        _download_all(state)
        with self.assertRaises(ValueError):
            accept_payload_change(state, "202601", "sha256:not-recorded")

    def test_unexpected_partition_raises(self) -> None:
        state = _new_state()
        with self.assertRaises(ValueError):
            record_partition(state, "209912", "sha256:x")
        with self.assertRaises(ValueError):
            record_partition_failure(state, "209912", "boom")


class StatusTests(unittest.TestCase):
    """Distinguish complete, incomplete, failed and their transitions."""

    def test_finalize_complete_only_with_full_coverage(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        _download_all(state)
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.COMPLETE.value)
        self.assertIsNotNone(state.finished_at)

    def test_finalize_incomplete_when_partition_missing_without_failure(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        record_partition(state, "202601", "sha256:a", now=FIXED_NOW)
        record_partition(state, "202602", "sha256:b", now=FIXED_NOW)
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.INCOMPLETE.value)
        self.assertEqual(state.missing_partitions, ["202603"])

    def test_finalize_failed_when_partition_download_failed(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        _download_all(state)
        record_partition_failure(state, "202602", "HTTP 503")
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.FAILED.value)
        self.assertEqual(state.failures, {"202602": "HTTP 503"})

    def test_later_success_clears_partition_failure(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        for index, partition in enumerate(PARTITIONS):
            record_partition_failure(state, partition, "HTTP 503")
            record_partition(state, partition, f"sha256:{index:064d}", now=FIXED_NOW)
        self.assertEqual(state.failures, {})
        self.assertEqual(finalize_window(state, now=FIXED_NOW), IngestionStatus.COMPLETE.value)

    def test_mark_failed_records_error_and_timestamp(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        mark_failed(state, "TLS handshake failed", now=FIXED_NOW)
        self.assertEqual(state.status, IngestionStatus.FAILED.value)
        self.assertEqual(state.errors, ["TLS handshake failed"])
        self.assertEqual(state.finished_at, FIXED_NOW.isoformat())

    def test_invalid_transitions_raise(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        _download_all(state)
        finalize_window(state, now=FIXED_NOW)
        with self.assertRaises(ValueError):
            finalize_window(state, now=FIXED_NOW)
        with self.assertRaises(ValueError):
            mark_failed(state, "late error")
        mark_running(state, now=FIXED_NOW)
        mark_failed(state, "rerun error", now=FIXED_NOW)
        with self.assertRaises(ValueError):
            mark_failed(state, "second error")

    def test_summary_reports_counts_and_gaps(self) -> None:
        state = _new_state()
        mark_running(state, now=FIXED_NOW)
        record_partition(state, "202601", "sha256:a", now=FIXED_NOW)
        record_partition_failure(state, "202602", "HTTP 503")
        record_partition(state, "202601", "sha256:changed", now=FIXED_NOW)
        report = summary(state)
        self.assertEqual(report["status"], IngestionStatus.RUNNING.value)
        self.assertEqual(report["expected_partitions"], 3)
        self.assertEqual(report["downloaded_partitions"], 1)
        self.assertEqual(report["missing_partitions"], 2)
        self.assertEqual(report["failed_partitions"], ["202602"])
        self.assertEqual(report["unresolved_mismatches"], ["202601"])
        self.assertEqual(report["errors"], 0)


if __name__ == "__main__":
    unittest.main()
