"""Tests for the offline Bronze-Parquet-to-Silver-Parquet benchmark harness."""

from __future__ import annotations

import itertools
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import polars as pl

from tfm_licitaciones.bench_silver import (
    PROFILES,
    STAGE_DESCRIPTION,
    dataset_profile,
    generate_bronze_in_memory,
    iter_tombstone_refs,
    run_benchmark,
    write_bronze_parts,
)
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA
from tfm_licitaciones.silver import build_procurement_events
from tfm_licitaciones.silver_parity import (
    assert_silver_parity,
    compare_silver_frames,
    compare_silver_parquet,
)


def build(dataset: dict) -> pl.DataFrame:
    return build_procurement_events(bronze_frame(dataset["records"]), tombstone_frame(dataset["tombstones"]))


def build_parts(records_glob: str, tombstones_glob: str) -> pl.DataFrame:
    return build_procurement_events(pl.read_parquet(records_glob), pl.read_parquet(tombstones_glob))


class GeneratorTests(unittest.TestCase):
    """Check the synthetic dataset is deterministic and covers the contract."""

    def test_same_seed_produces_identical_datasets(self) -> None:
        first = generate_bronze_in_memory(seed=7)
        second = generate_bronze_in_memory(seed=7)
        self.assertEqual(first, second)

    def test_profiles_keep_only_tiny_for_ci(self) -> None:
        self.assertEqual(set(PROFILES), {"tiny", "small", "medium", "large", "backfill"})
        tiny = dataset_profile("tiny")
        self.assertLessEqual(tiny["n_ted"] + tiny["n_placsp"], 500)
        for name in ("small", "medium", "large", "backfill"):
            self.assertGreater(dataset_profile(name)["n_ted"], tiny["n_ted"])
        with self.assertRaises(ValueError):
            dataset_profile("unknown")

    def test_covers_ted_placsp_revisions_repeats_and_tombstones(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        by_source = events.group_by("source").agg(pl.len()).sort("source").to_dicts()
        counts = {row["source"]: row["len"] for row in by_source}
        self.assertGreater(counts.get("ted", 0), 0)
        self.assertGreater(counts.get("placsp", 0), 0)
        types = set(events["source_event_type"].to_list())
        self.assertIn("tombstone", types)
        self.assertIn("notice_snapshot", types)
        # One revised procedure keeps three distinct snapshot events under one
        # procedure (a tombstone control for the same procedure is separate).
        revised = events.filter(
            (pl.col("procedure_id") == "placsp:procedure:https://example.invalid/sindicacion/00000000")
            & (pl.col("source_event_type") == "notice_snapshot")
        )
        self.assertEqual(revised.height, 3)
        self.assertEqual(revised["event_id"].n_unique(), 3)
        tombstoned = events.filter(
            pl.col("event_id") == "placsp:tombstone:https://example.invalid/sindicacion/00000000"
        )
        self.assertEqual(tombstoned.height, 1)
        # A repeated TED notice collapses to a single event.
        repeated = events.filter(pl.col("event_id") == "ted:notice:SYN-TED-000000")
        self.assertEqual(repeated.height, 1)

    def test_decimal_and_timestamp_edge_cases_are_exact(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        by_id = {row["event_id"]: row for row in events.to_dicts()}
        max_ted = by_id["ted:notice:SYN-TED-EDGE-MAX"]
        self.assertEqual(max_ted["estimated_value"], Decimal("999999999999999999.99"))
        max_placsp = by_id["placsp:notice:https://example.invalid/sindicacion/edge-max-amount@2026-01-08T12:15:00+00:00"]
        self.assertEqual(max_placsp["estimated_value"], Decimal("999999999999999999.99"))
        undated = by_id["placsp:notice:https://example.invalid/sindicacion/edge-undated@undated"]
        self.assertIsNone(undated["source_updated_at"])
        zero = by_id["ted:notice:SYN-TED-000003"]
        self.assertEqual(zero["estimated_value"], Decimal("0"))
        self.assertEqual(zero["currency"], "EUR")
        malformed = by_id["ted:notice:SYN-TED-000002"]
        self.assertIsNone(malformed["estimated_value"])
        self.assertIsNone(malformed["currency"])

    def test_cpv_codes_keep_source_order_without_duplicates(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        by_id = {row["event_id"]: row for row in events.to_dicts()}
        self.assertEqual(by_id["ted:notice:SYN-TED-000001"]["cpv_codes"], ["48662000", "30200000"])
        placsp = by_id["placsp:notice:https://example.invalid/sindicacion/00000001@2026-01-07T11:15:00+00:00"]
        self.assertEqual(placsp["cpv_codes"], ["79100000"])

    def test_deliberate_collision_fails_naming_the_event(self) -> None:
        dataset = generate_bronze_in_memory(seed=7, with_collision=True)
        with self.assertRaises(ValueError) as context:
            build(dataset)
        self.assertIn("ted:notice:SYN-TED-000000", str(context.exception))


class PartsTests(unittest.TestCase):
    """Check the bounded part writer and its deterministic layout."""

    def test_tombstone_refs_stream_without_materialization(self) -> None:
        # Pulling a prefix of a huge tombstone space must return instantly:
        # a list-building implementation could never finish this call.
        refs = iter_tombstone_refs(n_placsp=10**12)
        prefix = list(itertools.islice(refs, 3))
        self.assertEqual(
            prefix,
            [
                "https://example.invalid/sindicacion/00000000",
                "https://example.invalid/sindicacion/00000020",
                "https://example.invalid/sindicacion/00000040",
            ],
        )

    def test_tombstone_refs_end_with_a_repeat_and_stay_empty_when_small(self) -> None:
        refs = list(iter_tombstone_refs(n_placsp=120))
        self.assertEqual(len(refs), 7)
        self.assertEqual(refs[-1], refs[0])
        self.assertEqual(list(iter_tombstone_refs(n_placsp=10)), [])
        self.assertEqual(list(iter_tombstone_refs(n_placsp=0)), [])

    def test_part_writer_rejects_non_empty_dataset_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "dataset"
            write_bronze_parts(dataset, seed=7, **dataset_profile("tiny"))
            with self.assertRaises(FileExistsError):
                write_bronze_parts(dataset, seed=7, **dataset_profile("tiny"))
            # Existing artifacts are untouched: nothing was overwritten.
            self.assertEqual(len(list((dataset / "records").glob("part-*.parquet"))), 7)
            self.assertTrue((dataset / "dataset.json").exists())
            # An empty directory is accepted.
            fresh = Path(tmp) / "fresh"
            fresh.mkdir()
            manifest = write_bronze_parts(fresh, seed=7, **dataset_profile("tiny"))
            self.assertEqual(manifest["inputs"]["bronze_records"], 303)
            # Unrelated user files do not block and are left alone.
            mixed = Path(tmp) / "mixed"
            mixed.mkdir()
            (mixed / "notes.txt").write_text("user data\n", encoding="utf-8")
            write_bronze_parts(mixed, seed=7, **dataset_profile("tiny"))
            self.assertEqual((mixed / "notes.txt").read_text(encoding="utf-8"), "user data\n")

    def test_parts_cover_the_full_dataset_with_bounded_buffers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = write_bronze_parts(Path(tmp) / "dataset", seed=7, **dataset_profile("tiny"))
            self.assertEqual(manifest["inputs"]["bronze_records"], 303)
            self.assertEqual(manifest["inputs"]["bronze_tombstones"], 7)
            self.assertEqual(manifest["inputs"]["record_parts"], 7)
            self.assertEqual(manifest["inputs"]["tombstone_parts"], 1)
            parts = sorted((Path(tmp) / "dataset" / "records").glob("part-*.parquet"))
            self.assertEqual(len(parts), 7)
            for part in parts:
                self.assertLessEqual(pl.read_parquet(part).height, 50)
            self.assertTrue((Path(tmp) / "dataset" / "dataset.json").exists())

    def test_part_files_build_the_same_semantics_and_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp) / "first"
            second = Path(tmp) / "second"
            manifest = write_bronze_parts(first, seed=7, **dataset_profile("tiny"))
            other = write_bronze_parts(second, seed=7, **dataset_profile("tiny"))
            self.assertEqual(manifest, other)
            events = build_parts(str(first / "records" / "part-*.parquet"), str(first / "tombstones" / "part-*.parquet"))
            rerun = build_parts(
                str(second / "records" / "part-*.parquet"), str(second / "tombstones" / "part-*.parquet")
            )
            self.assertEqual(events.height, 274)
            assert_silver_parity(events, rerun)


class BenchmarkRunTests(unittest.TestCase):
    """Check the isolated parquet-to-parquet measurement."""

    def test_metrics_cover_required_fields_and_stay_json_serializable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp) / "work"
            metrics = run_benchmark("tiny", seed=7, work_dir=work_dir)
            self.assertEqual(metrics["implementation"], "python-row")
            self.assertIn("Polars/Parquet", metrics["implementation_detail"])
            self.assertEqual(metrics["stage"], STAGE_DESCRIPTION)
            self.assertIn("bronze-parquet-read", metrics["stage"])
            self.assertIn("silver-parquet-write", metrics["stage"])
            self.assertEqual(metrics["inputs"]["bronze_records"], 303)
            self.assertEqual(metrics["inputs"]["bronze_tombstones"], 7)
            self.assertEqual(metrics["inputs"]["record_parts"], 7)
            self.assertEqual(metrics["inputs"]["layout"]["records"], "records/part-*.parquet")
            self.assertEqual(metrics["outputs"]["silver_events"], 274)
            self.assertTrue(Path(metrics["outputs"]["silver_path"]).exists())
            self.assertGreaterEqual(metrics["timing_s"]["transform_s"], 0)
            self.assertGreaterEqual(metrics["timing_s"]["generation_s"], 0)
            self.assertGreaterEqual(metrics["timing_s"]["wall_s"], metrics["timing_s"]["transform_s"])
            self.assertAlmostEqual(
                metrics["throughput_events_per_s"],
                metrics["outputs"]["silver_events"] / metrics["timing_s"]["transform_s"],
            )
            for key in ("polars_version", "python_version", "profile", "seed", "peak_rss_bytes", "peak_rss_scope", "cpu_count"):
                self.assertIn(key, metrics)
            # The JSON documents the RSS scope truthfully: child lifetime
            # high-water mark, not a transform-only peak.
            self.assertIn("lifetime high-water mark", metrics["peak_rss_scope"])
            self.assertIn("startup before the transform timer", metrics["peak_rss_scope"])
            self.assertTrue((work_dir / "dataset.json").exists())
            json.dumps(metrics)

    def test_output_file_is_written_only_on_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "metrics.json"
            metrics = run_benchmark("tiny", seed=7, output=path)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), metrics)

    def test_isolated_collision_fails_naming_the_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError) as context:
                run_benchmark("tiny", seed=7, with_collision=True, work_dir=Path(tmp) / "work")
            self.assertIn("ted:notice:SYN-TED-000000", str(context.exception))


class ParityTests(unittest.TestCase):
    """Check semantic parity ignores layout but preserves every semantic."""

    def test_identical_and_reordered_frames_have_parity(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        assert_silver_parity(events, events.clone())
        assert_silver_parity(events.sort("event_id", descending=True), events)
        self.assertEqual(compare_silver_frames(events, events), [])

    def test_parquet_round_trip_keeps_parity(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / "a.parquet", Path(tmp) / "b.parquet"
            events.write_parquet(first)
            events.sort("event_id", descending=True).write_parquet(second)
            self.assertEqual(compare_silver_parquet(first, second), [])

    def test_content_differences_fail(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        mutated = events.with_columns(
            pl.when(pl.col("event_id") == "ted:notice:SYN-TED-000001")
            .then(pl.lit("Título cambiado"))
            .otherwise(pl.col("title"))
            .alias("title")
        )
        with self.assertRaises(AssertionError) as context:
            assert_silver_parity(mutated, events)
        self.assertIn("ted:notice:SYN-TED-000001", str(context.exception))
        missing = events.filter(pl.col("event_id") != "ted:notice:SYN-TED-000001")
        self.assertTrue(compare_silver_frames(missing, events))

    def test_schema_violations_fail(self) -> None:
        events = build(generate_bronze_in_memory(seed=7))
        self.assertTrue(compare_silver_frames(events.drop("cpv_codes"), events))
        wrong_type = events.with_columns(pl.col("title").cast(pl.Int64, strict=False))
        self.assertTrue(compare_silver_frames(wrong_type, events))

    def test_reference_schema_is_the_production_contract(self) -> None:
        self.assertEqual(list(PROCUREMENT_EVENT_SCHEMA.names()), build(generate_bronze_in_memory(seed=7)).columns)


if __name__ == "__main__":
    unittest.main()
