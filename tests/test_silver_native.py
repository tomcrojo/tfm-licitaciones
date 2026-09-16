"""Parity and native-execution tests for the production Polars Silver engine.

The frozen python-row semantics live in ``tfm_licitaciones.silver_reference``
and the production engine in ``tfm_licitaciones.silver_native``: this suite
asserts that both produce exactly the same frames, the same explicit
failures (including message text) on an adversarial shape matrix, and that
the native engine never uses per-row Python on its success path. Cross-engine
parity with the experimental candidates lives in the experiment area
(``experiments/silver_engine_comparison/tests/``), not here.

Coverage: identities (including the ``"0"`` vs ``0`` truthiness matrix as
primary and fallback values), procedure/buyer IDs, CPV scalar/list/order/
dedup, localized-text recursion, JSON escape semantics, amount presence vs
truthiness, exact Decimal boundaries, timestamps, provenance-minimum
selection, repeated observations, collisions, unsupported/null sources,
tombstones, multi-error failure ordering and determinism.
"""

from __future__ import annotations

import ast
import json
import random
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
        (
            "ted links empty first value stays null",
            [_ted({**TED_PAYLOAD, "url": "", "links": {"html": {"ELL": "", "ZUL": "http://g"}}})],
            (),
        ),
        (
            "ted links first value wins in document order",
            [_ted({**TED_PAYLOAD, "url": "", "links": {"html": {"ELL": "http://first", "ZUL": "http://g"}}})],
            (),
        ),
        ("ted country unicode uppercase", [_ted({**TED_PAYLOAD, "ND": "U1", "CY": "ÉS"})], ()),
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
        # --- Identity truthiness matrix (primary values) ---
        ("ted ND string zero beats fallback", [_ted({**TED_PAYLOAD, "ND": "0", "notice_id": "N9"})], ()),
        ("ted ND numeric zero falls through", [_ted({**TED_PAYLOAD, "ND": 0, "notice_id": "N9"})], ()),
        ("ted ND string false beats fallback", [_ted({**TED_PAYLOAD, "ND": "false", "notice_id": "N9"})], ()),
        ("ted ND boolean false falls through", [_ted({**TED_PAYLOAD, "ND": False, "notice_id": "N9"})], ()),
        ("ted ND boolean true beats fallback", [_ted({**TED_PAYLOAD, "ND": True, "notice_id": "N9"})], ()),
        ("ted ND string null beats fallback", [_ted({**TED_PAYLOAD, "ND": "null", "notice_id": "N9"})], ()),
        ("ted ND json null falls through", [_ted({**TED_PAYLOAD, "ND": None, "notice_id": "N9"})], ()),
        ("ted ND empty list falls through", [_ted({**TED_PAYLOAD, "ND": [], "notice_id": "N9"})], ()),
        ("ted ND string brackets beats fallback", [_ted({**TED_PAYLOAD, "ND": "[]", "notice_id": "N9"})], ()),
        ("ted ND empty dict falls through", [_ted({**TED_PAYLOAD, "ND": {}, "notice_id": "N9"})], ()),
        ("ted ND string braces beats fallback", [_ted({**TED_PAYLOAD, "ND": "{}", "notice_id": "N9"})], ()),
        ("ted ND empty string falls through", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": "N9"})], ()),
        ("ted ND missing with notice_id fallback", [_ted({**{k: v for k, v in TED_PAYLOAD.items() if k != "ND"}, "notice_id": "N9"})], ()),
        ("ted ND non-empty list resolves text", [_ted({**TED_PAYLOAD, "ND": ["A"]})], ()),
        ("ted ND localized dict resolves text", [_ted({**TED_PAYLOAD, "ND": {"spa": "Z"}})], ()),
        # --- Identity truthiness matrix (fallback values) ---
        ("ted notice_id string zero", [_ted({**{k: v for k, v in TED_PAYLOAD.items() if k != "ND"}, "notice_id": "0"})], ()),
        ("ted notice_id numeric zero resolves text", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": 0})], ()),
        ("ted notice_id string false", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": "false"})], ()),
        ("ted notice_id boolean false resolves text", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": False})], ()),
        ("ted notice_id string brackets", [_ted({**TED_PAYLOAD, "ND": "", "notice_id": "[]"})], ()),
        ("ted amount malformed gate nulls", [
            _ted({**TED_PAYLOAD, "ND": "M1", "estimated-value-lot": "1e"}),
            _ted({**TED_PAYLOAD, "ND": "M2", "estimated-value-lot": "e10"}),
            _ted({**TED_PAYLOAD, "ND": "M3", "estimated-value-lot": "--5"}),
        ], ()),
        # --- BOE / PLACSP identities ---
        ("boe item string zero", [_boe({**BOE_PAYLOAD, "item_id": "0"})], ()),
        ("boe identificador fallback", [_boe({"identificador": "B9", "titulo": "T", "department": "D", **{k: v for k, v in BOE_PAYLOAD.items() if k not in ("item_id", "title", "department")}})], ()),
        ("placsp atom string zero", [_placsp({**PLACSP_PAYLOAD, "atom_id": "0"})], ()),
        ("placsp atom numeric zero resolves text", [_placsp({**PLACSP_PAYLOAD, "atom_id": 0})], ()),
        # --- Procedure / buyer IDs ---
        ("ted procedure single numeric", [_ted({**TED_PAYLOAD, "ND": "P9", "procedure-identifier": 123})], ()),
        ("ted buyer single numeric dir", [_ted({**TED_PAYLOAD, "buyer-identifier": 7})], ()),
        ("ted buyer BI fallback", [_ted({**TED_PAYLOAD, "buyer-identifier": [], "BI": "E00000001"})], ()),
        ("ted buyer both present ambiguous", [_ted({**TED_PAYLOAD, "buyer-identifier": "A", "BI": "B"})], ()),
        # --- CPV matrix ---
        ("ted cpv boolean scalar", [_ted({**TED_PAYLOAD, "PC": True})], ()),
        ("ted cpv string brackets scalar", [_ted({**TED_PAYLOAD, "PC": "[]"})], ()),
        ("ted cpv null scalar falls back", [_ted({**TED_PAYLOAD, "PC": None, "cpv": ["48000000"]})], ()),
        ("ted cpv list with empties and numbers", [_ted({**TED_PAYLOAD, "PC": ["", 72415000, "72415000", " 48662000 "]})], ()),
        ("ted cpv missing both", [_ted({k: v for k, v in TED_PAYLOAD.items() if k not in ("PC", "cpv")})], ()),
        ("placsp cpv numeric scalar", [_placsp({**PLACSP_PAYLOAD, "cpv": 72415000})], ()),
        # --- Localized text recursion ---
        ("ted TI eng list", [_ted({**TED_PAYLOAD, "TI": {"eng": ["Some title"]}})], ()),
        ("ted TI spa empty blocks fallback", [_ted({**TED_PAYLOAD, "TI": {"spa": [], "eng": "Fallback"}})], ()),
        ("ted TI spa empty string blocks fallback", [_ted({**TED_PAYLOAD, "TI": {"spa": "", "eng": "Fallback"}})], ()),
        ("ted TI missing spa falls through", [_ted({**TED_PAYLOAD, "TI": {"eng": "Fallback"}})], ()),
        ("ted TI numeric scalar", [_ted({**TED_PAYLOAD, "TI": 123})], ()),
        ("ted TI boolean scalar", [_ted({**TED_PAYLOAD, "TI": True})], ()),
        ("ted TI null falls to title", [_ted({**TED_PAYLOAD, "TI": None, "title": "Plain"})], ()),
        ("ted TI empty list falls to title", [_ted({**TED_PAYLOAD, "TI": [], "title": "Plain"})], ()),
        ("ted TI list skips empties", [_ted({**TED_PAYLOAD, "TI": ["", "Second"]})], ()),
        ("ted buyer dict with list and scalar", [_ted({**TED_PAYLOAD, "buyer-name": {"spa": ["A"], "eng": "B"}})], ()),
        # --- JSON escapes ---
        ("ted title escaped quote", [_ted({**TED_PAYLOAD, "TI": 'a"b'})], ()),
        ("ted title backslash", [_ted({**TED_PAYLOAD, "TI": 'a\\b'})], ()),
        ("ted title newline", [_ted({**TED_PAYLOAD, "TI": "a\nb"})], ()),
        ("ted title tab", [_ted({**TED_PAYLOAD, "TI": "a\tb"})], ()),
        ("ted title unicode escape", [_ted({**TED_PAYLOAD, "TI": "caf\u00e9 \u4e2d"})], ()),
        ("ted title non-ascii", [_ted({**TED_PAYLOAD, "TI": "h\u00e9llo \u2713"})], ()),
        ("ted title quoted string", [_ted({**TED_PAYLOAD, "TI": '"hello"'})], ()),
        ("ted localized escaped list", [_ted({**TED_PAYLOAD, "TI": {"eng": ['x"y', "a\nb"]}})], ()),
        ("ted localized escaped fallback", [_ted({**TED_PAYLOAD, "TI": {"deu": "caf\u00e9", "eng": "plain"}})], ()),
        # --- Amount presence vs truthiness ---
        ("ted amount boolean false stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": False})], ()),
        ("ted amount boolean true stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": True})], ()),
        ("ted amount empty list stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": []})], ()),
        ("ted amount zero float string", [_ted({**TED_PAYLOAD, "estimated-value-lot": "0.00"})], ()),
        ("ted amount plus sign", [_ted({**TED_PAYLOAD, "estimated-value-lot": "+5"})], ()),
        ("ted amount leading dot", [_ted({**TED_PAYLOAD, "estimated-value-lot": ".5"})], ()),
        ("ted amount trailing dot", [_ted({**TED_PAYLOAD, "estimated-value-lot": "5."})], ()),
        ("placsp zero amount with raw", [_placsp({**PLACSP_PAYLOAD, "amount_estimated_overall": 0, "amount_estimated_overall_raw": "0.00"})], ()),
        ("placsp overall json null falls to tax", [_placsp({**PLACSP_PAYLOAD, "amount_estimated_overall": None, "amount_estimated_overall_raw": None})], ()),
        ("placsp legacy spaced string value", [
            _placsp({
                **{k: v for k, v in PLACSP_PAYLOAD.items() if not k.startswith("amount_estimated_overall") and not k.endswith("_raw")},
                "amount_tax_exclusive": " 120 ",
            })
        ], ()),
        # --- Timestamps ---
        ("placsp space separator instant", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08 10:00:00+01:00"})], ()),
        ("placsp lowercase z undated", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T09:00:00z"})], ()),
        ("placsp missing updated undated", [_placsp({k: v for k, v in PLACSP_PAYLOAD.items() if k != "updated"})], ()),
        ("placsp updated list resolves", [_placsp({**PLACSP_PAYLOAD, "updated": ["2026-01-08T10:00:00.000+01:00"]})], ()),
        ("ted PD with time and zone", [_ted({**TED_PAYLOAD, "PD": "2026-01-05T00:00:00Z"})], ()),
        # --- Provenance selection ---
        ("sha decides identical provenance tie", (
            _ted(TED_PAYLOAD, source_file="ted/same.jsonl", raw_sha256="b" * 64),
            _ted(TED_PAYLOAD, source_file="ted/same.jsonl", raw_sha256="a" * 64),
        ), ()),
        ("equivalent decimals dedup despite raw text", (
            _ted(TED_PAYLOAD, source_file="ted/a.jsonl"),
            _ted({**TED_PAYLOAD, "estimated-value-lot": "125000.500"}, source_file="ted/b.jsonl", retrieved_at=LATER),
        ), ()),
        # --- Collisions on more fields live in FailureParityTests ---
        # --- Independent-audit regressions (Astra) ---
        ("audit microsecond .123 event id", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:00.123+01:00"})], ()),
        ("audit microsecond .001 event id", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:00.001+01:00"})], ()),
        ("audit localized list first non-empty", [_ted({**TED_PAYLOAD, "TI": {"eng": ["", "Useful title"]}})], ()),
        ("audit nested ND after top level", [_ted({**TED_PAYLOAD, "ND": 0, "notice_id": "N9", "extra": {"ND": "x"}})], ()),
        ("audit nested ND before top level", [_ted({**TED_PAYLOAD, "extra": {"ND": "x"}, "ND": 0, "notice_id": "N9"})], ()),
        ("audit amount NaN string stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": "NaN"})], ()),
        ("audit amount nan lowercase stays null", [_ted({**TED_PAYLOAD, "estimated-value-lot": "nan"})], ()),
        ("audit amount underscores exact", [_ted({**TED_PAYLOAD, "estimated-value-lot": "1_000.00"})], ()),
        ("audit amount underscores scalar", [_ted({**TED_PAYLOAD, "estimated-value-lot": "1_000"})], ()),
        ("audit amount invalid underscores null", [_ted({**TED_PAYLOAD, "estimated-value-lot": "1__000"})], ()),
        ("audit float exponent event id", [_ted({**TED_PAYLOAD, "ND": 1e-7})], ()),
        ("audit float exponent title", [_ted({**TED_PAYLOAD, "TI": 1e-7})], ()),
        ("audit localized numeric spa is not a string", [_ted({**TED_PAYLOAD, "TI": {"spa": 0, "eng": "X"}})], ()),
        ("audit localized boolean spa is not a string", [_ted({**TED_PAYLOAD, "TI": {"spa": False, "eng": "X"}})], ()),
        ("audit hourly offset domain", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00+01:00"})], ()),
        ("audit leap second rejected", [_placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:60+01:00"})], ()),
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
        for amount in (
            "100.005",
            "1234567890123456789",
            "123456789012345.6789",
            "1e99999",
            # Adversarial exponent/shape boundary (blocker 3). Malformed
            # gate failures ("1e", "e10", "--5") stay null instead and are
            # covered by the success matrix.
            "1e-3",
            "1.0000000000000001e-2",
            "1e18",
            "1e25",
            "1e26",
            "999999999999999999999999999999",
            "1000000000000000000.00",
        ):
            with self.subTest(amount=amount):
                self._assert_same_failure(
                    (record_row({**TED_PAYLOAD, "estimated-value-lot": amount}, source="ted", source_file="ted/a.jsonl"),)
                )

    def test_placsp_amount_error_messages_are_identical(self) -> None:
        # NB: the legacy float value must stay JSON-finite (Bronze rejects
        # non-finite); the raw text alone decides the Decimal failure.
        for raw, value in (
            ("100.005", 100.005),
            ("1e-3", 0.001),
            ("1e18", 1e18),
            ("1e99999", 1e18),
        ):
            with self.subTest(raw=raw):
                payload = {**PLACSP_PAYLOAD, "amount_estimated_overall_raw": raw, "amount_estimated_overall": value}
                self._assert_same_failure(
                    (record_row(payload, source="placsp", source_file="placsp/a.zip"),)
                )

    def test_currency_collision_message_is_identical(self) -> None:
        other_currency = {**TED_PAYLOAD, "currency": "USD"}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(other_currency, source="ted", source_file="ted/b.jsonl"),
            )
        )

    def test_publication_date_collision_message_is_identical(self) -> None:
        other_date = {**TED_PAYLOAD, "PD": "2026-02-01Z"}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(other_date, source="ted", source_file="ted/b.jsonl"),
            )
        )

    def test_buyer_name_collision_message_is_identical(self) -> None:
        other_buyer = {**TED_PAYLOAD, "buyer-name": {"spa": "Otro comprador"}}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(other_buyer, source="ted", source_file="ted/b.jsonl"),
            )
        )

    def test_procedure_id_collision_message_is_identical(self) -> None:
        first = {**TED_PAYLOAD, "ND": "SAME", "procedure-identifier": "PROC-1"}
        second = {**TED_PAYLOAD, "ND": "SAME", "procedure-identifier": "PROC-2"}
        self._assert_same_failure(
            (
                record_row(first, source="ted", source_file="ted/a.jsonl"),
                record_row(second, source="ted", source_file="ted/b.jsonl", retrieved_at=LATER),
            )
        )

    def test_title_null_vs_value_collision_is_identical(self) -> None:
        untitled = {k: v for k, v in TED_PAYLOAD.items() if k not in ("TI", "title")}
        self._assert_same_failure(
            (
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(untitled, source="ted", source_file="ted/b.jsonl", retrieved_at=LATER),
            )
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

    def test_falsy_identity_shapes_fail_identically(self) -> None:
        # Falsy-but-present identity shapes that never resolve to text must
        # raise the identical missing-identity failure (blocker 1 matrix).
        cases = [
            ("ted", {**TED_PAYLOAD, "ND": "", "notice_id": []}),
            ("ted", {**TED_PAYLOAD, "ND": "", "notice_id": {}}),
            ("ted", {k: v for k, v in TED_PAYLOAD.items() if k not in ("ND", "notice_id")}),
            ("boe", {**BOE_PAYLOAD, "item_id": 0}),
            ("boe", {**BOE_PAYLOAD, "item_id": "", "identificador": []}),
        ]
        for source, payload in cases:
            with self.subTest(payload=str(payload.get("ND", payload.get("item_id", "?")))):
                self._assert_same_failure(
                    (record_row(payload, source=source, source_file=f"{source}/a.jsonl"),)
                )

    def test_audit_amount_failures_match_reference(self) -> None:
        # Positive infinity passes the TED float gate and then fails the
        # Decimal decision; the quantize rounding carry over 26 all-nine
        # integer digits must win over the scale failure.
        for amount in (
            "Infinity",
            "inf",
            "99999999999999999999999999.999",
            "99999999999999999999999999.995",
        ):
            with self.subTest(amount=amount):
                self._assert_same_failure(
                    (record_row({**TED_PAYLOAD, "estimated-value-lot": amount}, source="ted", source_file="ted/a.jsonl"),)
                )

    def test_audit_placsp_special_text_failures_match_reference(self) -> None:
        # Quiet NaN quantizes to NaN and fails the ``quantized != value``
        # scale check; infinity raises InvalidOperation instead. Neither
        # may abort the batch with the wrong classification.
        for raw in ("NaN", "Infinity"):
            with self.subTest(raw=raw):
                payload = {**PLACSP_PAYLOAD, "amount_estimated_overall_raw": raw, "amount_estimated_overall": 1.0}
                self._assert_same_failure(
                    (record_row(payload, source="placsp", source_file="placsp/a.zip"),)
                )

    def test_audit_collision_first_conflict_fields_match_reference(self) -> None:
        # Three observations: the first conflict wins, not the union of
        # every difference against the first row.
        def collision_row(title: str, buyer: str, member: int) -> dict:
            return record_row(
                {**TED_PAYLOAD, "ND": "X", "TI": {"spa": title}, "buyer-name": {"spa": buyer}},
                source="ted",
                source_file="ted/a.jsonl",
                source_member=f"part-{member}",
                source_member_index=member,
            )

        cases = {
            "title first": [collision_row("A", "B", 1), collision_row("A2", "B", 2), collision_row("A", "B2", 3)],
            "buyer first": [collision_row("A", "B", 1), collision_row("A", "B2", 3), collision_row("A2", "B", 2)],
            "both in second": [collision_row("A", "B", 1), collision_row("A2", "B2", 2)],
            "both in third": [collision_row("A", "B", 1), collision_row("A", "B", 2), collision_row("A2", "B2", 3)],
        }
        for name, rows in cases.items():
            with self.subTest(case=name):
                self._assert_same_failure(rows)


class SourceValidationTests(unittest.TestCase):
    """Null/unsupported sources must fail explicitly, never drop rows."""

    def _assert_same_failure(self, records, tombstones=()) -> None:
        with self.assertRaises(ValueError) as reference_error:
            build_procurement_events_reference(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        with self.assertRaises(ValueError) as native_error:
            build_procurement_events(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        self.assertEqual(str(reference_error.exception), str(native_error.exception))

    def _record_with_source(self, source) -> dict:
        row = record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")
        row["source"] = source
        return row

    def _tombstone_with_source(self, ref: str, source) -> dict:
        row = tombstone_row(ref)
        row["source"] = source
        return row

    def test_record_null_source_matches_reference(self) -> None:
        self._assert_same_failure([self._record_with_source(None)])

    def test_record_unsupported_source_matches_reference(self) -> None:
        for source in ("unknown", "", "TED", "placsp "):
            with self.subTest(source=source):
                self._assert_same_failure([self._record_with_source(source)])

    def test_record_unsupported_only_never_returns_empty_frame(self) -> None:
        # No silent row loss: a single unsupported row must raise, not vanish.
        with self.assertRaises(ValueError):
            build_procurement_events(
                bronze_frame([self._record_with_source("unknown")]), tombstone_frame([])
            )

    def test_tombstone_null_source_matches_reference(self) -> None:
        self._assert_same_failure([], [self._tombstone_with_source("https://example.invalid/1", None)])

    def test_tombstone_unsupported_source_matches_reference(self) -> None:
        for source in ("ted", "boe", "unknown", ""):
            with self.subTest(source=source):
                self._assert_same_failure([], [self._tombstone_with_source("https://example.invalid/1", source)])

    def test_tombstone_source_is_never_rewritten(self) -> None:
        # A null-source tombstone must raise the unsupported-source error,
        # not a missing-ref error and not a silent PLACSP row.
        with self.assertRaisesRegex(ValueError, "Unsupported tombstone source"):
            build_procurement_events(
                bronze_frame([]),
                tombstone_frame([self._tombstone_with_source("", None)]),
            )


class MultiErrorOrderingTests(unittest.TestCase):
    """Batches with several independent errors raise the reference's first."""

    def _assert_same_failure(self, records, tombstones=()) -> None:
        with self.assertRaises(ValueError) as reference_error:
            build_procurement_events_reference(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        with self.assertRaises(ValueError) as native_error:
            build_procurement_events(bronze_frame(list(records)), tombstone_frame(list(tombstones)))
        self.assertEqual(str(reference_error.exception), str(native_error.exception))

    def _ted_amount_row(self, notice: str, amount: str, locator: str) -> dict:
        return record_row(
            {**TED_PAYLOAD, "ND": notice, "estimated-value-lot": amount},
            source="ted",
            source_file="ted/a.jsonl",
            record_locator=locator,
        )

    def _ted_identity_row(self, locator: str) -> dict:
        return record_row({**TED_PAYLOAD, "ND": ""}, source="ted", source_file="ted/a.jsonl", record_locator=locator)

    def test_early_scale_error_beats_later_unrepresentable(self) -> None:
        self._assert_same_failure(
            [self._ted_amount_row("A", "100.005", "line:1"), self._ted_amount_row("B", "1e99999", "line:2")]
        )

    def test_reversed_scale_and_unrepresentable(self) -> None:
        self._assert_same_failure(
            [self._ted_amount_row("B", "1e99999", "line:1"), self._ted_amount_row("A", "100.005", "line:2")]
        )

    def test_early_missing_identity_beats_later_amount(self) -> None:
        self._assert_same_failure([self._ted_identity_row("line:1"), self._ted_amount_row("C", "100.005", "line:2")])

    def test_early_amount_beats_later_missing_identity(self) -> None:
        self._assert_same_failure([self._ted_amount_row("C", "100.005", "line:1"), self._ted_identity_row("line:2")])

    def test_valid_records_plus_invalid_tombstone(self) -> None:
        self._assert_same_failure(
            [record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")],
            [tombstone_row("")],
        )

    def test_bad_record_beats_bad_tombstone(self) -> None:
        self._assert_same_failure([self._ted_identity_row("line:1")], [tombstone_row("")])

    def test_row_level_error_beats_would_be_collision(self) -> None:
        self._assert_same_failure(
            [
                record_row({**TED_PAYLOAD, "ND": "X", "TI": {"spa": "A"}}, source="ted", source_file="ted/a.jsonl", record_locator="line:1"),
                record_row({**TED_PAYLOAD, "ND": "X", "TI": {"spa": "B"}}, source="ted", source_file="ted/b.jsonl", record_locator="line:2"),
                self._ted_amount_row("", "100.005", "line:3"),
            ]
        )

    def test_collision_rows_first_still_lose_to_later_row_error(self) -> None:
        self._assert_same_failure(
            [
                record_row({**TED_PAYLOAD, "ND": "X", "TI": {"spa": "A"}}, source="ted", source_file="ted/a.jsonl", record_locator="line:1"),
                record_row({**TED_PAYLOAD, "ND": "X", "TI": {"spa": "B"}}, source="ted", source_file="ted/b.jsonl", record_locator="line:2"),
                self._ted_identity_row("line:3"),
            ]
        )

    def test_two_invalid_sources_first_position_wins(self) -> None:
        first = record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl", record_locator="line:1")
        first["source"] = "bad-one"
        second = record_row(TED_PAYLOAD, source="ted", source_file="ted/b.jsonl", record_locator="line:2")
        second["source"] = "bad-two"
        self._assert_same_failure([first, second])

    def test_two_invalid_sources_reversed(self) -> None:
        first = record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl", record_locator="line:1")
        first["source"] = "bad-two"
        second = record_row(TED_PAYLOAD, source="ted", source_file="ted/b.jsonl", record_locator="line:2")
        second["source"] = "bad-one"
        self._assert_same_failure([first, second])

    def test_unsupported_source_beats_later_amount(self) -> None:
        bad = record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl", record_locator="line:1")
        bad["source"] = "unknown"
        self._assert_same_failure([bad, self._ted_amount_row("C", "100.005", "line:2")])

    def test_early_amount_beats_later_unsupported_source(self) -> None:
        bad = record_row(TED_PAYLOAD, source="ted", source_file="ted/b.jsonl", record_locator="line:2")
        bad["source"] = "unknown"
        self._assert_same_failure([self._ted_amount_row("C", "100.005", "line:1"), bad])

    def test_tombstone_bad_source_beats_later_missing_ref(self) -> None:
        bad_source = tombstone_row("https://example.invalid/1", record_locator="deleted-entry:1")
        bad_source["source"] = "ted"
        self._assert_same_failure([], [bad_source, tombstone_row("", record_locator="deleted-entry:2")])

    def test_tombstone_missing_ref_beats_later_bad_source(self) -> None:
        bad_source = tombstone_row("https://example.invalid/1", record_locator="deleted-entry:2")
        bad_source["source"] = "ted"
        self._assert_same_failure([], [tombstone_row("", record_locator="deleted-entry:1"), bad_source])


class EscapeParityTests(unittest.TestCase):
    """Escaped/unicode strings must decode exactly like the reference."""

    def test_exact_decoded_values(self) -> None:
        cases = {
            "quote": ('a"b', 'a"b'),
            "backslash": ('a\\b', 'a\\b'),
            "newline": ("a\nb", "a\nb"),
            "tab": ("a\tb", "a\tb"),
            "unicode-escape": ("caf\u00e9 \u4e2d", "caf\u00e9 \u4e2d"),
            "non-ascii": ("h\u00e9llo \u2713", "h\u00e9llo \u2713"),
            "surrounding-quotes": ('"hello"', '"hello"'),
            "crlf": ("a\r\nb", "a\r\nb"),
        }
        for name, (payload_title, expected) in cases.items():
            with self.subTest(name=name):
                frame = build_procurement_events(
                    bronze_frame([record_row({**TED_PAYLOAD, "TI": payload_title}, source="ted", source_file="ted/a.jsonl")]),
                    tombstone_frame([]),
                )
                self.assertEqual(frame["title"][0], expected)

    def test_escaped_values_match_reference_in_all_positions(self) -> None:
        payloads = [
            ({**TED_PAYLOAD, "ND": "E1", "TI": 'x"y', "description-glo": "a\nb", "buyer-name": {"spa": ["c\\d"]}}, "ted"),
            ({**TED_PAYLOAD, "ND": "E2", "TI": {"eng": ['q"q', "caf\u00e9"]}}, "ted"),
            ({**PLACSP_PAYLOAD, "title": 't"t', "summary": "s\\s"}, "placsp"),
            ({**BOE_PAYLOAD, "item_id": "E4", "title": "b\u00e9b", "department": 'd"d'}, "boe"),
        ]
        records = [
            record_row(payload, source=source, source_file=f"{source}/a.jsonl")
            for payload, source in payloads
        ]
        reference = build_procurement_events_reference(bronze_frame(records), tombstone_frame([]))
        native = build_procurement_events(bronze_frame(records), tombstone_frame([]))
        assert_silver_parity(native, reference)


class AdapterContractTests(unittest.TestCase):
    """Guard the flat-domain handling that the native engine assumes (blocker 6).

    JSON type probes are path-aware structural queries and no longer rely on
    fixture shapes (see ``_is_json_string``); these tests keep the *flat
    value extraction* domain honest on the checked-in fixtures, the Atom
    parser output and the benchmark generator sample: flat localized dicts
    without nested members, flat scalar arrays and adapter-shaped
    timestamps/dates. They do not, on their own, prove the engine correct
    on unseen payloads: the differential and adversarial matrices in this
    file are the semantic authority.
    """

    # Top-level keys probed with ``is_string`` per source branch.
    TED_PROBED = {
        "ND", "notice_id", "TI", "title", "description-glo", "description",
        "buyer-name", "buyer", "PC", "cpv", "estimated-value-lot",
        "framework-maximum-value-lot", "amount", "currency",
        "procedure-identifier", "BT-04-notice", "buyer-identifier", "BI",
        "notice-type", "CY", "country", "PD", "publication_date",
        "publication-date", "url",
    }
    PLACSP_PROBED = {
        "atom_id", "updated", "buyer_dir3", "buyer", "title", "summary",
        "cpv", "status_code", "nuts_code", "url",
        "amount_estimated_overall", "amount_tax_exclusive",
        "amount_estimated_overall_raw", "amount_tax_exclusive_raw",
        "amount_estimated_overall_currency", "amount_tax_exclusive_currency",
    }
    BOE_PROBED = {
        "item_id", "identificador", "title", "titulo", "buyer",
        "department", "summary", "publication_date", "url",
    }
    LOCALIZED_KEYS = {"TI", "description-glo", "buyer-name"}

    def _fixture_payloads(self) -> list[dict]:
        payloads = []
        fixtures = Path(__file__).parent / "fixtures" / "raw"
        for name in ("ted-sample.jsonl", "boe-sample.jsonl"):
            with open(fixtures / name, encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        payloads.append(json.loads(line))
        from tfm_licitaciones.atom import parse_atom_batch

        batch = parse_atom_batch((Path(__file__).parent / "fixtures" / "placsp" / "mini-placsp.atom").read_bytes())
        payloads.extend(entry["payload"] for entry in batch.entries)
        from tfm_licitaciones.bench_silver import generate_bronze_in_memory

        sample = generate_bronze_in_memory(seed=7, n_ted=30, n_placsp=30)
        payloads.extend(json.loads(row["payload_json"]) for row in sample["records"])
        return payloads

    def _nested_keys(self, value, depth: int = 0) -> set[str]:
        found: set[str] = set()
        if isinstance(value, dict):
            for key, item in value.items():
                if depth > 0:
                    found.add(key)
                found |= self._nested_keys(item, depth + 1)
        elif isinstance(value, list):
            for item in value:
                found |= self._nested_keys(item, depth + 1)
        return found

    def test_probed_keys_never_appear_nested(self) -> None:
        probed = self.TED_PROBED | self.PLACSP_PROBED | self.BOE_PROBED
        for payload in self._fixture_payloads():
            nested = self._nested_keys(payload)
            # Characterization of the retained fixture/generator shapes: the
            # type probes are path-aware and handle shadowing exactly (see
            # FailureParityTests and the audit regressions), so this only
            # documents that the checked-in corpus keeps identity/field keys
            # top-level. ``title``/``buyer``/``summary``/``url`` double as
            # nested language-container values inside links.html-style
            # structures.
            overlap = (nested & probed) - {"title", "buyer", "summary", "url"}
            self.assertEqual(overlap, set(), f"nested probed keys in {payload.get('_source')} payload")

    def test_localized_values_are_flat(self) -> None:
        for payload in self._fixture_payloads():
            for key in self.LOCALIZED_KEYS & set(payload):
                value = payload[key]
                self.assertIsInstance(value, dict, f"{key} must stay a localized dict")
                for lang, item in value.items():
                    self.assertNotIsInstance(item, dict, f"{key}.{lang} must not nest dicts")
                    if isinstance(item, list):
                        for element in item:
                            self.assertNotIsInstance(
                                element, (dict, list), f"{key}.{lang} must stay a flat list"
                            )

    def test_code_and_name_arrays_are_flat_scalars(self) -> None:
        for payload in self._fixture_payloads():
            for key in ("PC", "cpv"):
                value = payload.get(key)
                if isinstance(value, list):
                    for element in value:
                        self.assertNotIsInstance(element, (dict, list), f"{key} must stay flat")

    def test_amount_values_are_flat_scalars(self) -> None:
        for payload in self._fixture_payloads():
            for key in (
                "estimated-value-lot", "framework-maximum-value-lot", "amount",
                "amount_estimated_overall", "amount_tax_exclusive",
            ):
                value = payload.get(key)
                if value is not None:
                    self.assertNotIsInstance(value, (dict, list), f"{key} must stay a flat scalar")

    def test_amount_texts_have_no_underscores(self) -> None:
        # Characterization of the retained synthetic corpus only: PEP-515
        # underscore amounts are handled exactly by the engine (parity and
        # failure cases above), so this no longer guards engine semantics.
        amount_keys = {
            "estimated-value-lot", "framework-maximum-value-lot", "amount",
            "amount_estimated_overall_raw", "amount_tax_exclusive_raw",
        }
        for payload in self._fixture_payloads():
            for key in amount_keys & set(payload):
                value = payload[key]
                if isinstance(value, str):
                    self.assertNotIn("_", value, f"{key} must not contain underscores")

    def test_fixture_updated_shapes_agree_between_engines(self) -> None:
        from tfm_licitaciones.silver_native import build_procurement_events_native

        seen: set[str] = set()
        for payload in self._fixture_payloads():
            if "updated" in payload and isinstance(payload["updated"], str):
                seen.add(payload["updated"])
        seen |= {"", "not-a-timestamp", "2026-01-08T10:00:00", "2026-01-08T09:00:00z",
                 "2026-01-08 10:00:00+01:00", "2026-01-08T10:00:00.123456+01:00",
                 "2026-01-08T10:00:00.123456789+01:00"}
        for updated in sorted(seen):
            with self.subTest(updated=updated):
                payload = {**PLACSP_PAYLOAD, "updated": updated}
                records = bronze_frame([record_row(payload, source="placsp", source_file="placsp/a.zip")])
                reference = build_procurement_events_reference(records, tombstone_frame([]))
                native = build_procurement_events_native(records, tombstone_frame([]))
                assert_silver_parity(native, reference)

    def test_fixture_date_shapes_agree_between_engines(self) -> None:
        from tfm_licitaciones.silver_native import build_procurement_events_native

        seen: set[str] = set()
        for payload in self._fixture_payloads():
            for key in ("PD", "publication_date", "publication-date"):
                if key in payload and isinstance(payload[key], str):
                    seen.add(payload[key])
        seen |= {"", "31/12/2025", "no-una-fecha", "2026-02-02", "2026-01-05T00:00:00Z"}
        for text in sorted(seen):
            with self.subTest(text=text):
                payload = {**TED_PAYLOAD, "PD": text}
                payload.pop("publication_date", None)
                payload.pop("publication-date", None)
                records = bronze_frame([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                reference = build_procurement_events_reference(records, tombstone_frame([]))
                native = build_procurement_events_native(records, tombstone_frame([]))
                assert_silver_parity(native, reference)


class TemporalDomainParityTests(unittest.TestCase):
    """The native timestamp port must match ``datetime.fromisoformat``.

    The reference accepts every timezone-aware ISO-8601 shape CPython
    parses (and rejects the rest) and normalizes it to UTC; the native
    engine ports that parser as Polars expressions. This differential test
    feeds both the same corpus: explicit audit shapes plus a generated
    cross product of date forms, separators, times and offsets.
    """

    EXPLICIT = [
        # Audit shapes and CPython acceptance boundaries.
        "2026-01-08T10:00:00Z", "2026-01-08T10:00Z", "2026-01-08T10:00+01:00",
        "2026-01-08T10:00+01", "2026-01-08T10:00:00+01", "2026-01-08T10:00:00+0100",
        "2026-01-08T10:00:00+01:00:00", "2026-01-08T10:00:00+010000",
        "2026-01-08T10:00:60+01:00", "2026-01-08T10:60:00+01:00",
        "2026-01-08T24:00:00+01:00", "2026-13-08T10:00:00+01:00",
        "2026-02-30T10:00:00+01:00", "2024-02-29T10:00:00+01:00",
        "2026-01-08T10:00:00,5+01:00", "2026-01-08T10:00:00.123456789+01:00",
        "2026-01-08T10:00:00.123456789123+01:00", "2026-01-08 10:00:00+01:00",
        "2026-01-08x10:00:00+01:00", "20260108T100000+0100",
        "2026-W02-1T10:00:00Z", "2026-W02T10:00:00Z", "2026W021T100000Z",
        "2026-01-08T10:00:00+01:30", "2026-01-08T10:00:00-05:00",
        "2026-01-08T10:00:00+24:00", "2026-01-08T10:00:00.5+01:00",
        "2026-01-08T10:00:00.123+0100", "2026-01-08T10:00:00z",
        "2026-01-08T10", "2026-01-08", "20260108", "2026-W02-1",
        "2026-01-08T10.30Z", "2026-01-08T100000Z", "2026-01-08T10:00:00.Z",
        "2026-01-08T10:00:00+01:00:59", "2026-01-08T10:00:00+00:60",
        "2026-01-08T10:00:00+000060", "2026-01-08T10:00:00+23:59:59",
        "2026-01-08T10:00:00+99:00", "2026-01-08T10:00:00+01:99",
        "2026-01-08T10:00:00+01:00:99", "0000-01-01T00:00:00Z",
        "0000-W01-1T00:00:00Z", "2026-01-08T10:00:00+14:00",
        "2026-01-08T10:00:00-14:00", "2026-01-08T10:00:00+15:00",
        "2026-W53-7T10:00:00Z", "2025-W53-1T10:00:00Z", "2026-W00-1T00:00:00Z",
        "2026-W01-8T00:00:00Z", "2026-01-08T10:00:00.000001+01:00",
        "2026-01-08T10:00:00.0000001+01:00", "2026-01-08T10:00:00+01:00:00.5",
        # Embedded ``Z`` and reference state-machine quirks: the reference
        # expands every ``Z`` to ``+00:00`` before parsing, so these must
        # resolve through the same textual step.
        "2026-01-08Z+01", "2026-01-08Z00:00+01", "2026-W02-1Z7+01:00.5",
        "2026-01-08100000+01", "20260108100000+01", "2026-W02100000Z",
        "2026-01-08T10:00:00,5+01:00", "2026-01-08T100000Z",
        "", "not-a-timestamp", "2026-1-08T10:00:00Z", "26-01-08T10:00:00Z",
    ]

    @staticmethod
    def _generated() -> list[str]:
        cases: list[str] = []
        for date in ("2026-01-08", "2026-W02-1", "2026-W02", "20260108", "2026W021", "2026-W53-7", "2025-W53-1"):
            for separator in ("T", " ", "x", ""):
                for time in ("10", "10:00", "10:00:00", "100000", "10:00:00.5", "10,30", "10:00:00.Z"):
                    for offset in ("Z", "+01:00", "+01", "+0100", "+01:00:00", "-05:00", "+14:00", "+24:00"):
                        cases.append(f"{date}{separator}{time}{offset}")
        return cases

    @staticmethod
    def _wild() -> list[str]:
        # Deterministic pseudo-random wild corpus and mutations of valid
        # timestamps: pins the reference state-machine quirks (arbitrary
        # separators, glued fractions, second colons, embedded ``Z``).
        rng = random.Random(20260108)
        alphabet = "0123456789:.,+-ZT xW"
        cases = {
            "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 24)))
            for _ in range(1000)
        }
        bases = (
            "2026-01-08T10:00:00.123456+01:00",
            "2026-W02-1T10:00Z",
            "20260108T100000+0100",
        )
        for base in bases:
            for _ in range(200):
                position = rng.randrange(len(base))
                replacement = rng.choice(alphabet)
                operation = rng.choice(("insert", "delete", "replace"))
                if operation == "insert":
                    mutated = base[:position] + replacement + base[position:]
                elif operation == "delete":
                    mutated = base[:position] + base[position + 1 :]
                else:
                    mutated = base[:position] + replacement + base[position + 1 :]
                cases.add(mutated)
        return sorted(cases)

    @staticmethod
    def _expected(text: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)

    def test_native_parser_matches_fromisoformat_over_generated_corpus(self) -> None:
        from tfm_licitaciones.silver_native import _attach_aware_instant

        cases = sorted(set(self.EXPLICIT + self._generated() + self._wild()))
        frame = pl.DataFrame({"text": cases}).lazy()
        actual = (
            _attach_aware_instant(frame, "text")
            .select("__updated")
            .collect()["__updated"]
            .to_list()
        )
        for text, got in zip(cases, actual):
            with self.subTest(text=text):
                self.assertEqual(got, self._expected(text))

    # Representative end-to-end sample: the parser differential above
    # already covers the full corpus cheaply; building full frames is slow,
    # so only one shape per temporal class goes through the engines.
    FRAME_CASES = (
        "2026-01-08T10:00:00Z",
        "2026-01-08T10:00+01:00",
        "2026-01-08T10:00:60+01:00",
        "2026-01-08T10:00:00.123+01:00",
        "20260108T100000+0100",
        "2026-W02-1T10:00:00Z",
        "2026-01-08T10",
        "2026-01-08",
        "2026-01-08Z+01",
        "2026-01-08100000+01",
        "2026-01-08T100000Z",
        "2026-01-08T10.30Z",
        "",
        "not-a-timestamp",
        "2026-01-08T10:00:00+24:00",
    )

    def test_frame_timestamps_match_reference_on_corpus(self) -> None:
        # End-to-end: event_id marker and source_updated_at must match the
        # frozen reference for every accepted/rejected timestamp shape.
        for text in self.FRAME_CASES:
            with self.subTest(text=text):
                payload = {**PLACSP_PAYLOAD, "updated": text}
                records = bronze_frame([record_row(payload, source="placsp", source_file="placsp/a.zip")])
                reference = build_procurement_events_reference(records, tombstone_frame([]))
                native = build_procurement_events(records, tombstone_frame([]))
                assert_silver_parity(native, reference)


class FrozenReferenceGuardTests(unittest.TestCase):
    """The parity oracle must not follow mutable production semantics."""

    def test_reference_module_has_no_production_imports(self) -> None:
        tree = ast.parse((SRC / "silver_reference.py").read_text(encoding="utf-8"))
        violations: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                violations.extend(
                    alias.name for alias in node.names if alias.name.split(".")[0] == "tfm_licitaciones"
                )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level or module.split(".")[0] == "tfm_licitaciones":
                    violations.append(("." * node.level) + module)
        self.assertEqual(violations, [])

    def test_reference_ignores_mutated_normalize_semantics(self) -> None:
        from unittest import mock

        records = bronze_frame([record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")])
        before = build_procurement_events_reference(records, tombstone_frame([]))
        with mock.patch("tfm_licitaciones.normalize._first_text", return_value="HACKED"):
            after = build_procurement_events_reference(records, tombstone_frame([]))
        self.assertTrue(before.equals(after))
        self.assertEqual(after["title"][0], "Servicio de migración cloud")


class DeterminismTests(unittest.TestCase):
    """Repeated and shuffled builds must produce the exact same frame."""

    def test_five_shuffled_builds_are_identical(self) -> None:
        records = [
            record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip"),
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/b.jsonl"),
            record_row({**TED_PAYLOAD, "ND": "N2"}, source="ted", source_file="ted/c.jsonl"),
        ]
        tombstones = [tombstone_row("https://example.invalid/999")]
        first = build_procurement_events(bronze_frame(records), tombstone_frame(tombstones))
        rng = random.Random(42)
        for _ in range(5):
            shuffled = list(records)
            rng.shuffle(shuffled)
            rerun = build_procurement_events(bronze_frame(shuffled), tombstone_frame(tombstones))
            self.assertTrue(first.equals(rerun))
        self.assertEqual(first["event_id"].to_list(), sorted(first["event_id"].to_list()))

    def test_benchmark_sample_is_deterministic(self) -> None:
        from tfm_licitaciones.bench_silver import generate_bronze_in_memory

        sample = generate_bronze_in_memory(seed=7, n_ted=30, n_placsp=30)
        first = build_procurement_events(
            bronze_frame(sample["records"]), tombstone_frame(sample["tombstones"])
        )
        second = build_procurement_events(
            bronze_frame(list(reversed(sample["records"]))), tombstone_frame(sample["tombstones"])
        )
        self.assertTrue(first.equals(second))
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

    # Per-row Python mechanisms: forbidden everywhere, including helpers.
    FORBIDDEN_ATTRS = {
        "iter_rows",
        "iter_slices",
        "map_elements",
        "map_batches",
        "apply",
        "to_pandas",
        "to_dict",
    }
    # Bounded driver fetches: confined to explicit failure helpers.
    FETCH_ATTRS = {"to_dicts", "to_list", "to_series", "get_column", "row", "rows"}
    FORBIDDEN_NAMES = {"ProcurementEvent", "loads", "dumps", "json"}
    # Bounded diagnostic fetches (at most _MAX_FAILURE_IDS rows to name the
    # offending row in an explicit ValueError) may only appear in these
    # explicit failure helpers. Everything else must stay lazy/native.
    FETCH_HELPERS = {
        "_fetch_first_by_order",
        "_fetch_first_unsupported_source",
        "_raise_failures_in_reference_order",
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
                        elif child.attr in self.FETCH_ATTRS and not in_failure_helper:
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


if __name__ == "__main__":
    unittest.main()
