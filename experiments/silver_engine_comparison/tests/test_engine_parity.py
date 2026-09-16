"""Offline tests for the Silver engine experiment (polars-native, spark-native).

This suite lives under ``experiments/`` and is NOT part of the production
test suite (the default ``python -m unittest discover -s tests`` never sees
it). Run it explicitly from the repository root::

    uv run --with-editable . python -m unittest discover \\
        -s experiments/silver_engine_comparison/tests -t . -v

Normal tests run without PySpark. Spark behavior tests are skipped unless
PySpark (pinned ``pyspark==4.0.1``) plus a local JVM are available::

    uv run --with 'pyspark==4.0.1' --with-editable . python -m unittest discover \\
        -s experiments/silver_engine_comparison/tests -t . -v
"""

from __future__ import annotations

import ast
import importlib.util
import tempfile
import unittest
from pathlib import Path

import polars as pl

from experiments.silver_engine_comparison.bench_engines import (
    PARQUET_CODEC as BENCH_PARQUET_CODEC,
    output_disk_usage,
    output_parquet_usage,
    read_spark_silver,
    run_comparison,
)
from experiments.silver_engine_comparison.engines.python_row_reference import (
    FROZEN_BASELINE_COMMIT,
    build_procurement_events as build_frozen_python_row,
)
from tfm_licitaciones.bench_silver import dataset_profile, generate_bronze_in_memory, write_bronze_parts
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA
from tfm_licitaciones.silver_parity import assert_silver_parity
from experiments.silver_engine_comparison.engines.polars_candidate import (
    CONTRACT_SCOPE as POLARS_SCOPE,
)
from experiments.silver_engine_comparison.engines.polars_candidate import (
    build_from_parquet,
    build_procurement_events_polars,
)
from experiments.silver_engine_comparison.engines.spark_candidate import CONTRACT_SCOPE as SPARK_SCOPE
from experiments.silver_engine_comparison.engines.spark_candidate import INSTALL_HINT, SPARK_VERSION_PIN
from experiments.silver_engine_comparison.engines.spark_candidate import PARQUET_CODEC as SPARK_PARQUET_CODEC

SRC = Path(__file__).parent.parent.parent.parent / "src" / "tfm_licitaciones"
EXPERIMENT_SRC = Path(__file__).parent.parent

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
COLLISION_EVENT = "ted:notice:SYN-TED-000000"

# Driver row-fetching calls. They may appear ONLY in the documented
# failure-path helpers that name offending ids (bounded by limit/head);
# anywhere else they would mean driver-side canonicalization.
SPARK_FETCH_ATTRS = {
    "collect", "take", "head", "first", "show", "toPandas", "toLocalIterator",
    "toJSON", "to_pandas",
}
SPARK_FETCH_HELPERS = {"_failure_ids", "_check_experiment_scope"}
# Driver-side materialization and Python-execution patterns that would make
# the Spark candidate non-native. ``F.coalesce`` (row-wise SQL null
# coalescing) is explicitly NOT in this set: only DataFrame ``.coalesce()``
# (repartition, checked separately) is forbidden alongside ``coalesce(1)``.
SPARK_FORBIDDEN_ATTRS = {
    "udf", "pandas_udf", "applyInPandas", "mapInPandas", "mapInArrow",
    "applyInArrow", "to_numpy",
    "toArrow", "to_dicts", "iter_rows", "ProcurementEvent",
    "loads", "pandas", "numpy",
}
SPARK_FORBIDDEN_NAMES = {"udf", "pandas_udf", "collect", "pandas", "numpy"}

# Per-row Python patterns that would make the Polars candidate non-native.
POLARS_FORBIDDEN_ATTRS = {
    "iter_rows", "to_dicts", "to_dict", "map_elements", "apply",
    "to_pandas", "iter_slices", "collect",
}
POLARS_FORBIDDEN_NAMES = {"ProcurementEvent", "loads", "json"}
# ``to_list`` may appear ONLY in the failure-path helpers (bounded fetches
# to name offending ids); the success path fetches zero rows.
POLARS_FETCH_ATTRS = {"to_list"}
POLARS_FETCH_HELPERS = {"_failure_ids", "_check_experiment_scope"}


def _identifiers(path: Path) -> tuple[set[str], set[str], ast.Module]:
    """Return ``(attributes, names, tree)`` used in a Python source file."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    attrs: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            attrs.add(node.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    return attrs, names, tree


def _fetch_calls_outside(tree: ast.Module, fetch: set[str], helpers: set[str]) -> list[str]:
    """Describe driver-fetch calls outside the documented failure helpers."""

    offenders: list[str] = []

    def _visit(node: ast.AST, function: str | None) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _visit(child, child.name)
            elif isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                if child.func.attr in fetch and function not in helpers:
                    offenders.append(f"{function or '<module>'}: .{child.func.attr}()")
                _visit(child, function)
            else:
                _visit(child, function)

    _visit(tree, None)
    return offenders


def _dataframe_coalesce_calls(tree: ast.Module) -> list[str]:
    """Describe ``.coalesce(...)`` calls that are NOT ``F.coalesce(...)``."""

    offenders: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "coalesce"
            and not (isinstance(node.func.value, ast.Name) and node.func.value.id == "F")
        ):
            offenders.append(ast.dump(node.func))
    return offenders


def build_reference(seed: int = 7, with_collision: bool = False) -> pl.DataFrame:
    """Build the historical oracle with the frozen experiment-local baseline.

    This MUST NOT use the mutable production Silver facade
    (``tfm_licitaciones.silver``): after PR #17 that facade is native
    Polars, while the ``python-row`` label keeps meaning the e69016f
    semantics frozen in ``engines/python_row_reference.py``.
    """

    dataset = generate_bronze_in_memory(seed=seed, with_collision=with_collision)
    return build_frozen_python_row(
        bronze_frame(dataset["records"]), tombstone_frame(dataset["tombstones"])
    )


class ScopeLabelsTests(unittest.TestCase):
    """The experiment limitation must be stated loudly in code."""

    def test_contract_scope_labels_are_loud(self) -> None:
        for scope in (POLARS_SCOPE, SPARK_SCOPE):
            self.assertIn("experiment-only", scope)
            self.assertIn("TED/PLACSP", scope)
            self.assertIn("synthetic", scope)

    def test_spark_pin_and_hint_are_documented(self) -> None:
        self.assertEqual(SPARK_VERSION_PIN, "4.0.1")
        self.assertIn("pyspark==4.0.1", INSTALL_HINT)


class StaticGuardTests(unittest.TestCase):
    """Statically forbid non-native patterns in both candidate engines."""

    def test_polars_engine_has_no_per_row_python(self) -> None:
        attrs, names, tree = _identifiers(EXPERIMENT_SRC / "engines" / "polars_candidate.py")
        self.assertFalse(attrs & POLARS_FORBIDDEN_ATTRS, attrs & POLARS_FORBIDDEN_ATTRS)
        self.assertFalse(names & POLARS_FORBIDDEN_NAMES, names & POLARS_FORBIDDEN_NAMES)
        self.assertEqual(_fetch_calls_outside(tree, POLARS_FETCH_ATTRS, POLARS_FETCH_HELPERS), [])

    def test_polars_engine_decodes_each_payload_once(self) -> None:
        # Locks the single-decode design: fields must come from one
        # explicitly typed struct decode per branch, never from repeated
        # per-field scans that reparse the payload.
        source = (EXPERIMENT_SRC / "engines" / "polars_candidate.py").read_text(encoding="utf-8")
        self.assertNotIn("json_path_match", source)
        self.assertIn("json_decode", source)

    def test_spark_engine_has_no_udf_collect_or_driver_canonicalization(self) -> None:
        attrs, names, tree = _identifiers(EXPERIMENT_SRC / "engines" / "spark_candidate.py")
        self.assertFalse(attrs & SPARK_FORBIDDEN_ATTRS, attrs & SPARK_FORBIDDEN_ATTRS)
        self.assertFalse(names & SPARK_FORBIDDEN_NAMES, names & SPARK_FORBIDDEN_NAMES)
        self.assertEqual(_dataframe_coalesce_calls(tree), [])
        # Row fetches exist only in the failure-path helpers that name
        # offending ids; the success path performs zero driver fetches
        # (proven dynamically by test_success_path_never_collects_to_the_driver).
        self.assertEqual(
            _fetch_calls_outside(tree, SPARK_FETCH_ATTRS, SPARK_FETCH_HELPERS), []
        )


class FrozenBaselineIsolationTests(unittest.TestCase):
    """The historical python-row oracle must not track production refactors.

    ``python-row`` is frozen from e69016f in
    ``engines/python_row_reference.py``. After PR #17 the production facade
    ``tfm_licitaciones.silver.build_procurement_events`` becomes native
    Polars: any experiment import of that facade as the benchmark oracle
    would silently relabel Polars as ``python-row``. These guards fail
    loudly instead.
    """

    FROZEN_COMMIT = "e69016f62fb985639e17153e24b3a570f2f39c20"
    # Production Silver facade/reference modules that must never serve as
    # the historical oracle. ``silver_parity`` is intentionally NOT here:
    # it only compares frames, it does not define the baseline transform.
    FORBIDDEN_FACADE_MODULES = frozenset(
        {
            "tfm_licitaciones.silver",
            "tfm_licitaciones.silver_reference",
            "tfm_licitaciones.silver_native",
        }
    )

    @staticmethod
    def _imported_modules(path: Path) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    modules.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    modules.add(node.module)
        return modules

    @classmethod
    def _is_forbidden(cls, module: str) -> bool:
        return any(
            module == forbidden or module.startswith(forbidden + ".")
            for forbidden in cls.FORBIDDEN_FACADE_MODULES
        )

    def test_frozen_commit_is_documented(self) -> None:
        self.assertEqual(FROZEN_BASELINE_COMMIT, self.FROZEN_COMMIT)
        from experiments.silver_engine_comparison.engines import python_row_reference

        self.assertEqual(python_row_reference.FROZEN_BASELINE_COMMIT, self.FROZEN_COMMIT)
        self.assertEqual(python_row_reference.IMPLEMENTATION, "python-row")

    def test_frozen_module_has_no_production_imports(self) -> None:
        modules = self._imported_modules(EXPERIMENT_SRC / "engines" / "python_row_reference.py")
        offenders = sorted(
            module for module in modules if module == "tfm_licitaciones" or module.startswith("tfm_licitaciones.")
        )
        self.assertEqual(offenders, [])

    def test_no_experiment_module_uses_production_silver_facade(self) -> None:
        offenders: list[str] = []
        for path in sorted(EXPERIMENT_SRC.rglob("*.py")):
            for module in sorted(self._imported_modules(path)):
                if self._is_forbidden(module):
                    offenders.append(f"{path.relative_to(EXPERIMENT_SRC.parent.parent)}: {module}")
        self.assertEqual(offenders, [])


class FrozenBaselineIndependenceTests(unittest.TestCase):
    """Dynamically prove the frozen baseline ignores production mutations."""

    def test_frozen_baseline_survives_broken_production_facade(self) -> None:
        from unittest import mock

        dataset = generate_bronze_in_memory(seed=7)
        records = bronze_frame(dataset["records"])
        tombstones = tombstone_frame(dataset["tombstones"])
        expected = build_frozen_python_row(records, tombstones)
        self.assertGreater(expected.height, 0)
        with mock.patch(
            "tfm_licitaciones.silver.build_procurement_events",
            side_effect=AssertionError("production facade must not be used"),
        ):
            actual = build_frozen_python_row(records, tombstones)
        assert_silver_parity(actual, expected)

    def test_frozen_baseline_ignores_mutated_production_normalize(self) -> None:
        from unittest import mock

        dataset = generate_bronze_in_memory(seed=7)
        records = bronze_frame(dataset["records"])
        tombstones = tombstone_frame(dataset["tombstones"])
        expected = build_frozen_python_row(records, tombstones)
        with mock.patch(
            "tfm_licitaciones.normalize._first_text",
            side_effect=lambda *args, **kwargs: "MUTATED",
        ), mock.patch(
            "tfm_licitaciones.normalize.normalize_ted",
            side_effect=AssertionError("production normalize must not be used"),
        ), mock.patch(
            "tfm_licitaciones.normalize.normalize_boe",
            side_effect=AssertionError("production normalize must not be used"),
        ):
            actual = build_frozen_python_row(records, tombstones)
        assert_silver_parity(actual, expected)


class PolarsEngineTests(unittest.TestCase):
    """Native Polars parity and collision behavior (no Spark needed)."""

    def test_parity_on_tiny_in_memory_dataset(self) -> None:
        dataset = generate_bronze_in_memory(seed=7)
        candidate = build_procurement_events_polars(
            bronze_frame(dataset["records"]), tombstone_frame(dataset["tombstones"])
        )
        assert_silver_parity(candidate, build_reference())

    def test_parity_on_retained_part_files(self) -> None:
        # The reference must come from the SAME part files: part writers
        # assign per-part retrieved_at provenance, unlike the in-memory
        # helper where every row shares one timestamp. The oracle is the
        # frozen experiment-local python-row baseline, never production.
        from experiments.silver_engine_comparison.engines.python_row_reference import (
            build_procurement_events as baseline,
        )

        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "dataset", seed=7, **dataset_profile("tiny"))
            records_glob = str(Path(tmp) / "dataset" / "records" / "part-*.parquet")
            tombstones_glob = str(Path(tmp) / "dataset" / "tombstones" / "part-*.parquet")
            candidate = build_from_parquet(records_glob, tombstones_glob)
            reference = baseline(pl.read_parquet(records_glob), pl.read_parquet(tombstones_glob))
        assert_silver_parity(candidate, reference)

    def test_empty_bronze_builds_typed_empty_silver(self) -> None:
        frame = build_procurement_events_polars(bronze_frame([]), tombstone_frame([]))
        self.assertEqual(frame.schema, PROCUREMENT_EVENT_SCHEMA)
        self.assertEqual(frame.height, 0)

    def test_collision_fails_naming_the_same_event(self) -> None:
        dataset = generate_bronze_in_memory(seed=7, with_collision=True)
        with self.assertRaises(ValueError) as context:
            build_procurement_events_polars(
                bronze_frame(dataset["records"]), tombstone_frame(dataset["tombstones"])
            )
        self.assertIn(COLLISION_EVENT, str(context.exception))


@unittest.skipUnless(HAS_PYSPARK, "PySpark not installed (experiment-only dependency)")
class SparkEngineTests(unittest.TestCase):
    """Native Spark parity, collision and no-collect behavior."""

    @staticmethod
    def _session():
        from experiments.silver_engine_comparison.engines.spark_candidate import build_session

        try:
            return build_session(master="local[2]")
        except Exception as exc:  # noqa: BLE001 - no JVM means skip, not fail
            raise unittest.SkipTest(f"no local JVM for Spark: {exc}") from exc

    def test_parity_on_tiny_part_files(self) -> None:
        from experiments.silver_engine_comparison.engines.python_row_reference import (
            build_procurement_events as baseline,
        )
        from experiments.silver_engine_comparison.engines.spark_candidate import (
            build_spark_events,
            unpersist_frames,
            write_silver_spark,
        )

        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "dataset", seed=7, **dataset_profile("tiny"))
            records_glob = str(Path(tmp) / "dataset" / "records" / "part-*.parquet")
            tombstones_glob = str(Path(tmp) / "dataset" / "tombstones" / "part-*.parquet")
            session = self._session()
            try:
                frame, cached = build_spark_events(session, records_glob, tombstones_glob)
                out = Path(tmp) / "silver-spark"
                write_silver_spark(frame, str(out))
                unpersist_frames(cached)
            finally:
                session.stop()
            candidate = read_spark_silver(out)
            reference = baseline(pl.read_parquet(records_glob), pl.read_parquet(tombstones_glob))
        assert_silver_parity(candidate, reference)

    def test_array_distinct_preserves_first_occurrence_order_on_pinned_build(self) -> None:
        # The engine relies on array_distinct first-occurrence order ONLY on
        # the pinned build; this test pins it (full-frame parity on every
        # comparison run guards it independently).
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        session = self._session()
        try:
            schema = T.StructType([T.StructField("codes", T.ArrayType(T.StringType()))])
            rows = [
                (["72415000", "48662000", "72415000"],),
                (["b", "a", "b", "c", "a"],),
                (["2", "10", "2", "1"],),
                (["x"],),
                ([],),
            ]
            frame = session.createDataFrame(rows, schema)
            got = [list(row[0]) for row in frame.select(F.array_distinct("codes").alias("d")).collect()]
        finally:
            session.stop()
        self.assertEqual(
            got,
            [["72415000", "48662000"], ["b", "a", "c"], ["2", "10", "1"], ["x"], []],
        )

    def test_collision_fails_naming_the_same_event(self) -> None:
        from experiments.silver_engine_comparison.engines.spark_candidate import build_spark_events

        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(
                Path(tmp) / "dataset", seed=7, with_collision=True, **dataset_profile("tiny")
            )
            session = self._session()
            try:
                with self.assertRaises(ValueError) as context:
                    build_spark_events(
                        session,
                        str(Path(tmp) / "dataset" / "records" / "part-*.parquet"),
                        str(Path(tmp) / "dataset" / "tombstones" / "part-*.parquet"),
                    )
            finally:
                session.stop()
        self.assertIn(COLLISION_EVENT, str(context.exception))

    def test_success_path_never_collects_to_the_driver(self) -> None:
        from pyspark.sql.classic.dataframe import DataFrame

        from experiments.silver_engine_comparison.engines.spark_candidate import (
            build_spark_events,
            unpersist_frames,
            write_silver_spark,
        )

        def _forbidden(name: str):
            def _raise(*args: object, **kwargs: object) -> object:
                raise AssertionError(f"forbidden driver call: DataFrame.{name}")
            return _raise

        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "dataset", seed=7, **dataset_profile("tiny"))
            session = self._session()
            originals = {}
            for method in ("collect", "toPandas", "toLocalIterator"):
                originals[method] = getattr(DataFrame, method)
                setattr(DataFrame, method, _forbidden(method))
            try:
                frame, cached = build_spark_events(
                    session,
                    str(Path(tmp) / "dataset" / "records" / "part-*.parquet"),
                    str(Path(tmp) / "dataset" / "tombstones" / "part-*.parquet"),
                )
                out = Path(tmp) / "silver-spark"
                write_silver_spark(frame, str(out))
                unpersist_frames(cached)
            finally:
                for method, original in originals.items():
                    setattr(DataFrame, method, original)
                session.stop()
            # Row counts come from the written Parquet, never from a
            # post-write recomputation of the lazy plan.
            self.assertEqual(read_spark_silver(out).height, 274)

    def test_spark_write_uses_pinned_zstd_codec(self) -> None:
        from experiments.silver_engine_comparison.engines.spark_candidate import (
            build_spark_events,
            unpersist_frames,
            write_silver_spark,
        )

        with tempfile.TemporaryDirectory() as tmp:
            write_bronze_parts(Path(tmp) / "dataset", seed=7, **dataset_profile("tiny"))
            session = self._session()
            try:
                frame, cached = build_spark_events(
                    session,
                    str(Path(tmp) / "dataset" / "records" / "part-*.parquet"),
                    str(Path(tmp) / "dataset" / "tombstones" / "part-*.parquet"),
                )
                out = Path(tmp) / "silver-spark"
                write_silver_spark(frame, str(out))
                unpersist_frames(cached)
            finally:
                session.stop()
            # Spark embeds the codec in the file name: snappy here would
            # mean the write-boundary pin regressed.
            self.assertTrue(list(out.glob("*.zstd.parquet")), list(out.iterdir()))
            self.assertFalse(list(out.glob("*.snappy.parquet")))

    def test_tiny_comparison_records_codec_and_heap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            metrics = run_comparison(
                "tiny", seed=7, work_dir=Path(tmp) / "work",
                engines=("python-row", "spark-native"),
            )
        report = metrics["engines"]["spark-native"]
        self.assertTrue(
            metrics["parity"]["per_engine"]["spark-native"]["parity_ok"]
        )
        self.assertEqual(report["parquet_codec"], "zstd")
        self.assertIsNone(report["spark_driver_memory"])
        self.assertGreater(report["jvm_max_heap_bytes"], 0)
        self.assertEqual(report["outputs"]["silver_events"], 274)


class RunnerHelperTests(unittest.TestCase):
    """Offline checks for the comparison runner that need no engine run."""

    def test_write_boundary_codec_is_pinned_identically(self) -> None:
        # Spark defaults to snappy while Polars defaults to zstd: without
        # the pin, Spark outputs were ~5x larger on identical content.
        self.assertEqual(SPARK_PARQUET_CODEC, "zstd")
        self.assertEqual(BENCH_PARQUET_CODEC, SPARK_PARQUET_CODEC)

    def test_cli_exposes_reproducible_spark_driver_memory(self) -> None:
        from experiments.silver_engine_comparison.bench_engines import build_parser

        args = build_parser().parse_args(["--profile", "tiny"])
        self.assertIsNone(args.spark_driver_memory)
        self.assertIsNone(args.spark_executor_memory)
        self.assertIsNone(args.spark_executor_cores)
        self.assertIsNone(args.spark_executor_instances)
        self.assertIsNone(args.spark_eventlog_dir)
        args = build_parser().parse_args(
            ["--profile", "tiny", "--spark-driver-memory", "8g"]
        )
        self.assertEqual(args.spark_driver_memory, "8g")

    def test_cluster_topology_flags_are_accepted(self) -> None:
        from experiments.silver_engine_comparison.bench_engines import build_parser

        args = build_parser().parse_args(
            [
                "--profile", "tiny",
                "--spark-master", "spark://127.0.0.1:7077",
                "--spark-executor-memory", "4g",
                "--spark-executor-cores", "4",
                "--spark-executor-instances", "3",
                "--spark-eventlog-dir", "/tmp/spark-events",
            ]
        )
        self.assertEqual(args.spark_master, "spark://127.0.0.1:7077")
        self.assertEqual(args.spark_executor_memory, "4g")
        self.assertEqual(args.spark_executor_cores, "4")
        self.assertEqual(args.spark_executor_instances, "3")

    def test_summarize_eventlog_counts_stages_and_shuffle(self) -> None:
        import json as _json

        from experiments.silver_engine_comparison.bench_engines import summarize_eventlog

        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "app-1"
            log.write_text(
                "\n".join(
                    [
                        _json.dumps({"Event": "SparkListenerJobStart", "Job ID": 0}),
                        _json.dumps(
                            {
                                "Event": "SparkListenerExecutorAdded",
                                "Executor ID": "1",
                                "Executor Info": {
                                    "Host": "127.0.0.1",
                                    "Total Cores": 4,
                                },
                            }
                        ),
                        _json.dumps(
                            {
                                "Event": "SparkListenerStageCompleted",
                                "Stage Info": {
                                    "Stage ID": 1,
                                    "Stage Name": "scan",
                                    "Number of Tasks": 7,
                                    "Accumulables": [
                                        {
                                            "Name": "internal.metrics.shuffle.read.remoteBytesRead",
                                            "Value": 100,
                                        },
                                        {
                                            "Name": "internal.metrics.shuffle.write.bytesWritten",
                                            "Value": 50,
                                        },
                                        {
                                            "Name": "internal.metrics.diskBytesSpilled",
                                            "Value": 0,
                                        },
                                        {
                                            "Name": "internal.metrics.memoryBytesSpilled",
                                            "Value": 0,
                                        },
                                        {
                                            "Name": "internal.metrics.jvmGCTime",
                                            "Value": 12,
                                        },
                                    ],
                                },
                            }
                        ),
                        "not json",
                    ]
                ),
                encoding="utf-8",
            )
            summary = summarize_eventlog(log)
        self.assertEqual(summary["jobs"], 1)
        self.assertEqual(
            summary["executors"],
            [{"executor_id": "1", "host": "127.0.0.1", "total_cores": 4}],
        )
        self.assertEqual(len(summary["stages"]), 1)
        self.assertEqual(
            summary["totals"],
            {
                "num_tasks": 7,
                "shuffle_read_bytes": 100,
                "shuffle_write_bytes": 50,
                "disk_spilled_bytes": 0,
                "memory_spilled_bytes": 0,
                "jvm_gc_ms": 12,
            },
        )
        self.assertEqual(summarize_eventlog(Path(tmp) / "absent")["totals"]["num_tasks"], 0)

    def test_output_disk_usage_reports_parquet_and_artifact_totals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "silver.parquet"
            path.write_bytes(b"x" * 100)
            self.assertEqual(output_disk_usage(path), (100, 1))
            self.assertEqual(output_parquet_usage(path), (100, 1))
            directory = Path(tmp) / "silver-spark"
            (directory / "a").mkdir(parents=True)
            (directory / "a" / "part-0.parquet").write_bytes(b"y" * 40)
            (directory / "a" / "part-1.parquet").write_bytes(b"z" * 10)
            (directory / "_SUCCESS").write_bytes(b"")
            self.assertEqual(output_parquet_usage(directory), (50, 2))
            self.assertEqual(output_disk_usage(directory), (50, 3))

    def test_read_spark_silver_restores_utc_without_shifting_values(self) -> None:
        reference = build_reference()
        naive = reference.with_columns(
            pl.col("source_updated_at").dt.replace_time_zone(None).dt.cast_time_unit("ns"),
            pl.col("deadline").dt.replace_time_zone(None).dt.cast_time_unit("ns"),
            pl.col("ingested_at").dt.replace_time_zone(None).dt.cast_time_unit("ns"),
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "silver-spark"
            out.mkdir()
            naive.write_parquet(out / "part-00000.parquet")
            restored = read_spark_silver(out)
        assert_silver_parity(restored, reference)

    def test_tiny_comparison_without_spark(self) -> None:        # Focused runner validation without the Spark engine: two isolated
        # children, same retained dataset, parity against python-row.
        with tempfile.TemporaryDirectory() as tmp:
            metrics = run_comparison(
                "tiny", seed=7, work_dir=Path(tmp) / "work",
                engines=("python-row", "polars-native"),
            )
        self.assertEqual(metrics["profile"], "tiny")
        self.assertGreater(metrics["timing_s"]["generation_s"], 0)
        self.assertEqual(metrics["dataset"]["bronze_records"], 303)
        for engine in ("python-row", "polars-native"):
            parity = metrics["parity"]["per_engine"][engine]
            self.assertTrue(parity["parity_ok"], parity["differences"])
            report = metrics["engines"][engine]
            self.assertEqual(report["outputs"]["silver_events"], 274)
            self.assertEqual(report["parquet_codec"], "zstd")
            self.assertGreaterEqual(report["timing_s"]["engine_init_s"], 0)
            self.assertGreater(report["timing_s"]["transform_s"], 0)
            self.assertGreaterEqual(report["timing_s"]["wall_s"], report["timing_s"]["transform_s"])
            self.assertGreater(report["peak_tree_rss_bytes"], 0)
            self.assertGreater(report["outputs"]["parquet_bytes"], 0)
            self.assertEqual(
                report["outputs"]["parquet_bytes"], report["outputs"]["artifact_bytes"]
            )
            self.assertIn(report["rss_method"], ("psutil", "procfs"))
        self.assertIn("cpu_count", metrics["hardware"])
        self.assertGreater(metrics["validation_s"]["parity_and_counts_s"], 0)


if __name__ == "__main__":
    unittest.main()
