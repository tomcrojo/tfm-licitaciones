"""Parity and native-execution tests for the production Polars Silver engine.

The frozen python-row semantics live in ``tfm_licitaciones.silver_reference``
and the production engine in ``tfm_licitaciones.silver_native``: this suite
asserts that both produce exactly the same frames, the same explicit
failures (including message text) on an adversarial shape matrix, and that
the native engine never uses per-row Python on its success path. On the
subset of the contract covered by the experimental engine candidates, the
production engine must also match ``silver_polars`` and, when PySpark is
available, ``silver_spark``.
"""

from __future__ import annotations

import ast
import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from test_silver import (
    BOE_PAYLOAD,
    PLACSP_PAYLOAD,
    TED_PAYLOAD,
    record_row,
    tombstone_row,
)
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA
from tfm_licitaciones.silver import build_procurement_events
from tfm_licitaciones.silver_parity import assert_silver_parity
from tfm_licitaciones.silver_reference import build_procurement_events_reference

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
SRC = Path(__file__).parent.parent / "src" / "tfm_licitaciones"
LATER = datetime(2026, 3, 5, 13, 0, tzinfo=timezone.utc)


def _ted(payload, **kwargs):
    kwargs.setdefault("source", "ted")
    kwargs.setdefault("source_file", "ted/a.jsonl")
    return record_row(payload, **kwargs)


def _placsp(payload, **kwargs):
    kwargs.setdefault("source", "placsp")
    kwargs.setdefault("source_file", "placsp/f.zip")
    return record_row(payload, **kwargs)


def _boe(payload, **kwargs):
    kwargs.setdefault("source", "boe")
    kwargs.setdefault("source_file", "boe/a.jsonl")
    return record_row(payload, **kwargs)


def _frames(records, tombstones=()):
    records_frame = bronze_frame(list(records))
    tombstones_frame = tombstone_frame(list(tombstones))
    return (
        build_procurement_events_reference(records_frame, tombstones_frame),
        build_procurement_events(records_frame, tombstones_frame),
    )


def _parity_cases():
    return [
        ("empty", [], ()),
        ("base three sources", [_ted(TED_PAYLOAD), _placsp(PLACSP_PAYLOAD), _boe(BOE_PAYLOAD)], ()),
        ("reversed input order", [_boe(BOE_PAYLOAD), _placsp(PLACSP_PAYLOAD), _ted(TED_PAYLOAD)], ()),
        (
            "repeated tombstones across retrievals",
            (),
            (
                tombstone_row("https://example.invalid/1", source_file="placsp/repeat.zip", retrieved_at=LATER),
                tombstone_row("https://example.invalid/1"),
            ),
        ),
        # --- TED shape matrix ---
        ("ted scalar cpv string", [_ted({**TED_PAYLOAD, "PC": "72415000"})], ()),
        ("ted scalar cpv number", [_ted({**TED_PAYLOAD, "PC": 72415000})], ()),
        ("ted cpv alias fallback", [_ted({**TED_PAYLOAD, "PC": [], "cpv": ["48000000"]})], ()),
        ("ted cpv duplicates keep order", [_ted({**TED_PAYLOAD, "PC": ["3", "1", "3", "2", "1"]})], ()),
        ("ted plain-string title", [_ted({**TED_PAYLOAD, "TI": "Servicio cloud"})], ()),
        ("ted dict title without spa", [_ted({**TED_PAYLOAD, "TI": {"deu": "X", "eng": "Y"}})], ()),
        ("ted empty spa does not fall through", [_ted({**TED_PAYLOAD, "TI": {"spa": ""}, "title": "fallback"})], ()),
        ("ted buyer-name plain", [_ted({**TED_PAYLOAD, "buyer-name": "Comprador"})], ()),
        ("ted list title first non-empty", [_ted({**TED_PAYLOAD, "TI": ["", "X"]})], ()),
        (
            "ted links preference",
            [_ted({**TED_PAYLOAD, "url": "", "links": {"html": {"DEU": "http://d", "SPA": "http://s"}}})],
            (),
        ),
        (
            "ted links first-value fallback",
            [_ted({**TED_PAYLOAD, "url": "", "links": {"html": {"FRA": "http://f", "ITA": "http://i"}}})],
            (),
        ),
        ("ted notice_type", [_ted({**TED_PAYLOAD, "notice-type": "Contract notice"})], ()),
        ("ted nd falls through empty", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": "N9"})], ()),
        ("ted hyphenated publication-date alias", [_ted({"publication-date": "2026-02-02", **{k: v for k, v in TED_PAYLOAD.items() if k != "PD"}})], ()),
        ("ted invalid publication date null", [_ted({**TED_PAYLOAD, "PD": "31/12/2025"})], ()),
        ("ted alpha-3 and alpha-2 country", [
            _ted({**TED_PAYLOAD, "ND": "A1", "CY": "ESP"}),
            _ted({**TED_PAYLOAD, "ND": "A2", "CY": "FR"}),
            _ted({**TED_PAYLOAD, "ND": "A3", "CY": "Spain"}),
        ], ()),
        # --- TED amounts ---
        ("ted amount separators", [
            _ted({**TED_PAYLOAD, "ND": f"N{i}", "estimated-value-lot": text})
            for i, text in enumerate(("98000,00", "1.234,50", "1,234.50", "12.5.5", "45000", "1 000,25"))
        ], ()),
        ("ted amount numeric json number", [_ted({**TED_PAYLOAD, "estimated-value-lot": 125000.5, "currency": "EUR"})], ()),
        ("ted amount zero numeric beats later fields", [
            _ted({**{k: v for k, v in TED_PAYLOAD.items() if k != "currency"}, "estimated-value-lot": 0, "framework-maximum-value-lot": 100, "currency": "EUR"})
        ], ()),
        ("ted amount invalid stays null with currency", [_ted({**TED_PAYLOAD, "estimated-value-lot": "abc", "currency": "EUR"})], ()),
        ("ted amount negative stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": "-5"})], ()),
        ("ted amount trailing zero scale passes", [_ted({**TED_PAYLOAD, "estimated-value-lot": "1.230"})], ()),
        ("ted amount exponent text", [_ted({**TED_PAYLOAD, "estimated-value-lot": "1.5e3"})], ()),
        ("ted amount explicit null skips field", [
            _ted({**TED_PAYLOAD, "estimated-value-lot": None, "framework-maximum-value-lot": "50.00"})
        ], ()),
        # --- PLACSP ---
        ("placsp subsecond updated", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:00.123456+01:00"})], ()),
        ("placsp naive updated undated", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:00"})], ()),
        ("placsp zulu updated", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T09:00:00Z"})], ()),
        ("placsp primary present-null keeps null", [
            _placsp({"amount_estimated_overall": None, "amount_estimated_overall_raw": "-5.0", **PLACSP_PAYLOAD})
        ], ()),
        ("placsp legacy float without raw", [_placsp({k: v for k, v in PLACSP_PAYLOAD.items() if not k.endswith("_raw")})], ()),
        ("placsp negative amount passes gate", [
            _placsp({"amount_estimated_overall": -5.0, "amount_estimated_overall_raw": "-5.0", **PLACSP_PAYLOAD})
        ], ()),
        ("placsp tax-only amounts", [
            _placsp({k: v for k, v in PLACSP_PAYLOAD.items() if not k.startswith("amount_estimated_overall")})
        ], ()),
        ("placsp scalar cpv", [_placsp({**PLACSP_PAYLOAD, "cpv": "72415000"})], ()),
        ("placsp revisions share procedure", [
            _placsp(PLACSP_PAYLOAD),
            _placsp({**PLACSP_PAYLOAD, "updated": "2026-01-09T12:00:00.000+01:00", "title": "Rectificación"}),
        ], ()),
        # --- BOE ---
        ("boe aliases", [
            _boe({"identificador": "B9", "titulo": "Título", "department": "Org", **{k: v for k, v in BOE_PAYLOAD.items() if k not in ("item_id", "title", "department")}})
        ], ()),
        ("boe invalid date null", [_boe({**BOE_PAYLOAD, "publication_date": "no-una-fecha"})], ()),
        # --- dedup / provenance selection ---
        (
            "repeated observations select minimum provenance",
            (
                _ted(TED_PAYLOAD, source_file="ted/repeat.jsonl", retrieved_at=LATER),
                _ted(TED_PAYLOAD, source_file="ted/first.jsonl"),
            ),
            (),
        ),
        (
            "member index null orders after concrete",
            (
                _placsp(PLACSP_PAYLOAD, source_member=None, source_member_index=None),
                _placsp(PLACSP_PAYLOAD, source_member="feed.atom", source_member_index=3),
            ),
            (),
        ),
        (
            "canonically equivalent repeat deduplicates",
            (_ted(TED_PAYLOAD), _ted({**TED_PAYLOAD, "AC": [{"amount": 1}], "extra": "campo"})),
            (),
        ),
        (
            "tombstone alongside notices",
            (_placsp(PLACSP_PAYLOAD),),
            (tombstone_row("https://example.invalid/999"),),
        ),
    ]


class ReferenceParityTests(unittest.TestCase):
    """The native engine must equal the frozen reference on every shape."""

    def test_parity_on_adversarial_matrix(self) -> None:
        for name, records, tombstones in _parity_cases():
            with self.subTest(case=name):
                reference, native = _frames(records, tombstones)
                self.assertEqual(native.schema, PROCUREMENT_EVENT_SCHEMA)
                assert_silver_parity(native, reference)
                assert_silver_parity(reference, native)

    def test_native_determinism_and_row_order_independence(self) -> None:
        records = [
            record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip"),
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/b.jsonl"),
        ]
        first = build_procurement_events(bronze_frame(records), tombstone_frame([]))
        second = build_procurement_events(bronze_frame(list(reversed(records))), tombstone_frame([]))
        third = build_procurement_events(bronze_frame(records), tombstone_frame([]))
        self.assertTrue(first.equals(second))
        self.assertTrue(first.equals(third))
        identifiers = first["event_id"].to_list()
        self.assertEqual(identifiers, sorted(identifiers))


class FailureParityTests(unittest.TestCase):
    """Explicit failures must match the reference, including message text."""

    def _assert_same_failure(self, records, tombstones=()) -> None:
        with self.assertRaises(ValueError) as reference_error:
            build_procurement_events_reference(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        with self.assertRaises(ValueError) as native_error:
            build_procurement_events(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        self.assertEqual(str(reference_error.exception), str(native_error.exception))

    def test_collision_messages_are_identical(self) -> None:
        corrected = {**TED_PAYLOAD, "TI": {"spa": "Título corregido"}}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(corrected, source="ted", source_file="ted/b.jsonl"),
            )
        )

    def test_cpv_order_collision_message_is_identical(self) -> None:
        reordered = {**TED_PAYLOAD, "PC": ["48662000", "72415000"]}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(reordered, source="ted", source_file="ted/b.jsonl"),
            )
        )

    def test_placsp_same_instant_amount_collision_is_identical(self) -> None:
        corrected = {**PLACSP_PAYLOAD, "amount_estimated_overall_raw": "240001.0", "amount_estimated_overall": 240001.0}
        self._assert_same_failure(
            (
                record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/a.zip"),
                record_row(corrected, source="placsp", source_file="placsp/b.zip"),
            )
        )

    def test_undated_material_difference_is_identical(self) -> None:
        first = {**PLACSP_PAYLOAD, "updated": ""}
        second = {**first, "title": "Otro título"}
        self._assert_same_failure(
            (
                record_row(first, source="placsp", source_file="placsp/a.zip"),
                record_row(second, source="placsp", source_file="placsp/b.zip"),
            )
        )

    def test_boe_material_difference_is_identical(self) -> None:
        corrected = {**BOE_PAYLOAD, "title": "Título corregido"}
        self._assert_same_failure(
            (
                record_row(BOE_PAYLOAD, source="boe", source_file="boe/a.jsonl"),
                record_row(corrected, source="boe", source_file="boe/b.jsonl", retrieved_at=LATER),
            )
        )

    def test_amount_error_messages_are_identical(self) -> None:
        for amount in ("100.005", "1234567890123456789", "123456789012345.6789", "1e99999"):
            with self.subTest(amount=amount):
                self._assert_same_failure(
                    (record_row({**TED_PAYLOAD, "estimated-value-lot": amount}, source="ted", source_file="ted/a.jsonl"),)
                )

    def test_missing_identity_failures_match_reference(self) -> None:
        for payload, message in (
            ({**TED_PAYLOAD, "ND": ""}, "TED bronze record without a published notice number"),
            ({**PLACSP_PAYLOAD, "atom_id": ""}, "PLACSP bronze record without an atom id"),
            ({**BOE_PAYLOAD, "item_id": ""}, "BOE bronze record without an item id"),
        ):
            with self.subTest(message=message):
                source = payload["_source"]
                rows = [record_row(payload, source=source, source_file=f"{source}/a.jsonl")]
                with self.assertRaises(ValueError) as reference_error:
                    build_procurement_events_reference(bronze_frame(rows), tombstone_frame([]))
                self.assertIn(message, str(reference_error.exception))
                with self.assertRaisesRegex(ValueError, message):
                    build_procurement_events(bronze_frame(rows), tombstone_frame([]))


class NativeExecutionGuardTests(unittest.TestCase):
    """The success path must stay free of per-row Python."""

    FORBIDDEN_ATTRS = {"iter_rows", "to_dict", "map_elements", "apply", "to_pandas", "iter_slices"}
    FORBIDDEN_NAMES = {"ProcurementEvent", "loads", "json"}
    FETCH_ATTRS = {"to_list", "to_dicts", "row"}
    FETCH_HELPERS = {
        "_raise_missing_identity",
        "_raise_unsupported_sources",
        "_raise_unsupported_tombstones",
        "_raise_amounts",
        "_raise_collisions",
    }

    def test_silver_native_has_no_per_row_python(self) -> None:
        tree = ast.parse((SRC / "silver_native.py").read_text(encoding="utf-8"))
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                in_failure_helper = node.name in self.FETCH_HELPERS
                for child in ast.walk(node):
                    if isinstance(child, ast.Attribute):
                        if child.attr in self.FORBIDDEN_ATTRS:
                            violations.append(f"{node.name}: .{child.attr}")
                        if child.attr in self.FETCH_ATTRS and not in_failure_helper:
                            violations.append(f"{node.name}: .{child.attr} outside failure helpers")
                    if isinstance(child, ast.Name) and child.id in self.FORBIDDEN_NAMES:
                        violations.append(f"{node.name}: {child.id}")
        self.assertEqual(violations, [])

    def test_silver_facade_wires_the_native_engine(self) -> None:
        from tfm_licitaciones import silver, silver_native

        self.assertEqual(silver.IMPLEMENTATION, "polars-native")
        self.assertEqual(silver.IMPLEMENTATION_DETAIL, silver_native.IMPLEMENTATION_DETAIL)
        records = bronze_frame([record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")])
        self.assertTrue(
            silver.build_procurement_events(records, tombstone_frame([])).equals(
                silver_native.build_procurement_events_native(records, tombstone_frame([]))
            )
        )


class ExperimentParityTests(unittest.TestCase):
    """Production Polars must also match the experiment engine candidates."""

    def _synthetic_frames(self):
        from tfm_licitaciones.bench_silver import generate_bronze_in_memory

        dataset = generate_bronze_in_memory(seed=7, n_ted=60, n_placsp=60)
        return bronze_frame(dataset["records"]), tombstone_frame(dataset["tombstones"])

    def test_parity_with_experiment_polars_on_synthetic_contract(self) -> None:
        from tfm_licitaciones.silver_polars import build_procurement_events_polars

        records, tombstones = self._synthetic_frames()
        reference = build_procurement_events_reference(records, tombstones)
        native = build_procurement_events(records, tombstones)
        experiment = build_procurement_events_polars(records, tombstones)
        assert_silver_parity(native, reference)
        assert_silver_parity(native, experiment)
        assert_silver_parity(experiment, reference)

    @unittest.skipUnless(HAS_PYSPARK, "PySpark (pyspark==4.0.1) not installed")
    def test_parity_with_experiment_spark_on_synthetic_contract(self) -> None:
        from tfm_licitaciones.bench_engines import read_spark_silver
        from tfm_licitaciones.bench_silver import write_bronze_parts
        from tfm_licitaciones.silver_spark import build_spark_events, build_session

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = write_bronze_parts(root, seed=7, n_ted=60, n_placsp=60, rows_per_part=50)
            records_glob = str(root / manifest["layout"]["records"])
            tombstones_glob = str(root / manifest["layout"]["tombstones"])
            session = build_session()
            try:
                frame, _ = build_spark_events(session, records_glob, tombstones_glob)
                output = root / "silver"
                frame.write.mode("overwrite").parquet(str(output))
                spark_output = read_spark_silver(output)
                records = pl.read_parquet(records_glob)
                tombstones = pl.read_parquet(tombstones_glob)
                native = build_procurement_events(records, tombstones)
                reference = build_procurement_events_reference(records, tombstones)
                assert_silver_parity(native, reference)
                assert_silver_parity(native, spark_output)
            finally:
                session.stop()


if __name__ == "__main__":
    unittest.main()
