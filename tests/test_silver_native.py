"""Parity, routing and native-execution tests for the hybrid Silver engine.

Production Silver is hybrid: ``tfm_licitaciones.silver`` asks
``tfm_licitaciones.silver_guard`` whether the complete batch belongs to the
narrow native domain and otherwise hands the original frames to the frozen
python-row reference. This suite asserts that both routes produce exactly
the same frames and the same explicit failures (including message text) on
an adversarial shape matrix, exercises the routing boundary itself (native
for eligible batches, whole-batch fallback with the original frames, no
try/except hiding), and keeps the native kernel free of per-row Python.
Cross-engine parity with the experimental candidates lives in the
experiment area (``experiments/silver_engine_comparison/tests/``), not here.

Coverage: identities (including the ``"0"`` vs ``0`` truthiness matrix and
numeric identities routed to the reference), procedure/buyer IDs, CPV
scalar/list/order/dedup, localized-text recursion, JSON escape semantics,
amount presence vs truthiness, exact Decimal boundaries, timestamps,
publication dates, provenance-minimum selection, repeated observations,
collisions, unsupported/null sources, tombstones, multi-error failure
ordering, determinism, the Raw->Bronze->public-API audit regressions
(second and third independent audits, including the whole-document JSON
domain boundaries) and the frozen reference's independence.
"""

from __future__ import annotations

import ast
import json
import random
import re
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import polars as pl

from raw_fixtures import evidence_for_fixture
from test_silver import (
    BOE_PAYLOAD,
    PLACSP_PAYLOAD,
    TED_PAYLOAD,
    record_row,
    tombstone_row,
)
from tfm_licitaciones.bronze import bronze_frame, load_raw_records, tombstone_frame
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA
from tfm_licitaciones.silver import build_procurement_events
from tfm_licitaciones.silver_guard import (
    REASON_AMOUNT,
    REASON_COUNTRY,
    REASON_DOCUMENT_DEPTH,
    REASON_NUMBER_DOMAIN,
    REASON_STRING_DOMAIN,
    REASON_TOMBSTONE_REF,
    assess_native_eligibility,
)
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
        # --- Numeric, temporal and localized-text regressions ---
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

    # Independent restatement of the guard's strict temporal domain.
    _STRICT = re.compile(
        r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-](\d{2}):(\d{2}))$"
    )

    @classmethod
    def _eligible(cls, text: str) -> bool:
        stripped = text.strip()
        if stripped == "":
            return True
        match = cls._STRICT.fullmatch(stripped)
        if match is not None:
            year, month, day, hour, minute, second = (int(part) for part in match.groups()[:6])
            zone, offset_hour, offset_minute = match.groups()[7], match.groups()[8], match.groups()[9]
            return (
                2 <= year <= 9998
                and 1 <= month <= 12
                and 1 <= day <= 31
                and hour <= 23
                and minute <= 59
                and second <= 59
                and (zone == "Z" or (int(offset_hour) <= 23 and int(offset_minute) <= 59))
            )
        # Outside the strict pattern the native kernel yields null, which is
        # exact when the reference is also undated.
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return True
        return parsed.tzinfo is None or parsed.utcoffset() is None

    def test_native_parser_matches_fromisoformat_on_eligible_subset(self) -> None:
        # Inside the admitted domain the kernel itself must be exact; outside
        # it the public API routes the batch to the reference (next tests).
        from tfm_licitaciones.silver_native import _aware_instant

        cases = sorted(
            {
                case
                for case in self.EXPLICIT + self._generated() + self._wild()
                if self._eligible(case)
            }
        )
        frame = pl.DataFrame({"text": cases})
        actual = frame.select(_aware_instant(pl.col("text")).alias("instant"))["instant"].to_list()
        for text, got in zip(cases, actual):
            with self.subTest(text=text):
                self.assertEqual(got, self._expected(text))

    def test_guard_temporal_domain_matches_strict_predicate(self) -> None:
        for text in sorted(set(self.EXPLICIT + [
            "2026-01-08T10:00:00+00:00:00.5",
            "2026-01-08T10:00:00-00:00:00.5",
        ])):
            with self.subTest(text=text):
                payload = {**PLACSP_PAYLOAD, "updated": text}
                records = bronze_frame([record_row(payload, source="placsp", source_file="placsp/a.zip")])
                eligibility = assess_native_eligibility(records, tombstone_frame([]))
                self.assertEqual(eligibility.eligible, self._eligible(text), eligibility.reason)

    # Representative end-to-end sample: the kernel differential above covers
    # the full corpus cheaply; building full frames is slow, so only one
    # shape per temporal class goes through the public API.
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
        "2026-01-08T10:00:00+00:00:00.5",
        "2026-01-08T10:00:00-00:00:00.5",
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
                public = build_procurement_events(records, tombstone_frame([]))
                assert_silver_parity(public, reference)


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


class GuardBoundaryTests(unittest.TestCase):
    """The hybrid routing boundary: guard first, whole-batch fallback."""

    @staticmethod
    def _eligible_records():
        return bronze_frame(
            [
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/a.zip"),
                record_row(BOE_PAYLOAD, source="boe", source_file="boe/a.jsonl"),
            ]
        )

    def test_eligible_batch_runs_native_and_never_reference(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        records = self._eligible_records()
        tombstones = tombstone_frame([tombstone_row("https://example.invalid/1")])
        with mock.patch.object(
            silver, "build_procurement_events_native", wraps=silver.build_procurement_events_native
        ) as native, mock.patch.object(
            silver, "build_procurement_events_reference", wraps=silver.build_procurement_events_reference
        ) as reference:
            result = silver.build_procurement_events(records, tombstones)
        self.assertEqual(native.call_count, 1)
        reference.assert_not_called()
        self.assertIs(native.call_args.args[0], records)
        self.assertIs(native.call_args.args[1], tombstones)
        self.assertEqual(result.height, 4)

    def test_single_ineligible_row_sends_whole_batch_to_reference(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        records = bronze_frame(
            [
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row({**TED_PAYLOAD, "ND": 1e-5}, source="ted", source_file="ted/b.jsonl"),
                record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/a.zip"),
            ]
        )
        tombstones = tombstone_frame([tombstone_row("https://example.invalid/1")])
        with mock.patch.object(
            silver, "build_procurement_events_native", side_effect=AssertionError("native must not run")
        ) as native, mock.patch.object(
            silver, "build_procurement_events_reference", wraps=silver.build_procurement_events_reference
        ) as reference:
            result = silver.build_procurement_events(records, tombstones)
        native.assert_not_called()
        # The reference receives the exact original frames, unfiltered.
        self.assertIs(reference.call_args.args[0], records)
        self.assertIs(reference.call_args.args[1], tombstones)
        expected = build_procurement_events_reference(records, tombstones)
        self.assertTrue(result.equals(expected))

    def test_native_exception_propagates_and_never_falls_back(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        records = self._eligible_records()
        with mock.patch.object(
            silver, "build_procurement_events_native", side_effect=RuntimeError("boom")
        ), mock.patch.object(silver, "build_procurement_events_reference") as reference:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                silver.build_procurement_events(records, tombstone_frame([]))
        reference.assert_not_called()

    def test_route_logging_reports_route_reason_and_counts(self) -> None:
        records = self._eligible_records()
        with self.assertLogs("tfm_licitaciones.silver", level="INFO") as captured:
            build_procurement_events(records, tombstone_frame([]))
        eligible_record = captured.records[-1]
        self.assertEqual(eligible_record.route, "native")
        self.assertIsNone(eligible_record.fallback_reason)
        self.assertEqual(eligible_record.records, records.height)
        self.assertEqual(eligible_record.tombstones, 0)

        ineligible = bronze_frame(
            [record_row({**TED_PAYLOAD, "ND": 1e-5}, source="ted", source_file="ted/a.jsonl")]
        )
        with self.assertLogs("tfm_licitaciones.silver", level="INFO") as captured:
            build_procurement_events(ineligible, tombstone_frame([]))
        fallback_record = captured.records[-1]
        self.assertEqual(fallback_record.route, "reference-fallback")
        self.assertEqual(fallback_record.fallback_reason, "identity_out_of_domain")
        self.assertEqual(fallback_record.records, 1)

    @staticmethod
    def _ted_case(payload) -> dict:
        return record_row(payload, source="ted", source_file="ted/a.jsonl")

    @staticmethod
    def _placsp_case(payload) -> dict:
        return record_row(payload, source="placsp", source_file="placsp/a.zip")

    @staticmethod
    def _boe_case(payload) -> dict:
        return record_row(payload, source="boe", source_file="boe/a.jsonl")

    def _adversarial_cases(self):
        ted = self._ted_case
        placsp = self._placsp_case
        boe = self._boe_case
        cases = [
            # Identities: strings, falsy fallbacks, numbers, bools, containers.
            ("ted identity digits", [ted({**TED_PAYLOAD, "ND": "000123"})], ()),
            ("ted identity numeric zero fallback", [ted({**TED_PAYLOAD, "ND": 0, "notice_id": "N9"})], ()),
            ("ted identity exponent number", [ted({**TED_PAYLOAD, "ND": 1e-5})], ()),
            ("ted identity big int", [ted({**TED_PAYLOAD, "ND": 10**20})], ()),
            ("ted identity bool", [ted({**TED_PAYLOAD, "ND": True})], ()),
            ("ted identity empty list fallback", [ted({**TED_PAYLOAD, "ND": [], "notice_id": "N9"})], ()),
            ("ted identity empty string fallback", [ted({**TED_PAYLOAD, "ND": "", "notice_id": "N9"})], ()),
            ("ted identity missing", [ted({k: v for k, v in TED_PAYLOAD.items() if k not in ("ND", "notice_id")})], ()),
            # Localized text recursion and JSON types.
            ("ted text plain", [ted({**TED_PAYLOAD, "TI": "Plain"})], ()),
            ("ted text spa", [ted({**TED_PAYLOAD, "TI": {"spa": "Título"}})], ()),
            ("ted text eng list empties", [ted({**TED_PAYLOAD, "TI": {"eng": ["", "B"]}})], ()),
            ("ted text eng numbers", [ted({**TED_PAYLOAD, "TI": {"eng": [1, 2]}})], ()),
            ("ted text eng null element", [ted({**TED_PAYLOAD, "TI": {"eng": [None, "B"]}})], ()),
            ("ted text nested dict", [ted({**TED_PAYLOAD, "TI": {"spa": {"x": "y"}}})], ()),
            ("ted text list of dicts", [ted({**TED_PAYLOAD, "TI": [{"spa": "B"}]})], ()),
            ("ted text empty list fallback", [ted({**TED_PAYLOAD, "TI": [], "title": "Fallback"})], ()),
            ("ted text dict null member", [ted({**TED_PAYLOAD, "TI": {"eng": None, "spa": "B"}})], ()),
            # Codes.
            ("ted cpv string scalar", [ted({**TED_PAYLOAD, "PC": "72415000"})], ()),
            ("ted cpv number scalar", [ted({**TED_PAYLOAD, "PC": 72415000})], ()),
            ("ted cpv number list", [ted({**TED_PAYLOAD, "PC": [72415000]})], ()),
            ("ted cpv empty list fallback", [ted({**TED_PAYLOAD, "PC": [], "cpv": ["48000000"]})], ()),
            # Amounts.
            ("ted amount plain text", [ted({**TED_PAYLOAD, "estimated-value-lot": "125000.50"})], ()),
            ("ted amount number", [ted({**TED_PAYLOAD, "estimated-value-lot": 125000.5})], ()),
            ("ted amount malformed text", [ted({**TED_PAYLOAD, "estimated-value-lot": "n/a"})], ()),
            ("ted amount NaN", [ted({**TED_PAYLOAD, "estimated-value-lot": "NaN"})], ()),
            ("ted amount underscores", [ted({**TED_PAYLOAD, "estimated-value-lot": "1_000.00"})], ()),
            ("ted amount exponent", [ted({**TED_PAYLOAD, "estimated-value-lot": "1e3"})], ()),
            ("ted amount scale", [ted({**TED_PAYLOAD, "estimated-value-lot": "100.005"})], ()),
            ("ted amount precision", [ted({**TED_PAYLOAD, "estimated-value-lot": "1234567890123456789"})], ()),
            ("ted amount limit", [ted({**TED_PAYLOAD, "estimated-value-lot": "999999999999999999.99"})], ()),
            ("ted amount negative", [ted({**TED_PAYLOAD, "estimated-value-lot": "-5"})], ()),
            ("ted amount zero", [ted({**TED_PAYLOAD, "estimated-value-lot": "0"})], ()),
            ("ted amount bool", [ted({**TED_PAYLOAD, "estimated-value-lot": True})], ()),
            ("ted amount dict", [ted({**TED_PAYLOAD, "estimated-value-lot": {"x": 1}})], ()),
            ("ted amount exponent number", [ted({**TED_PAYLOAD, "estimated-value-lot": 1e-05})], ()),
            # Dates.
            ("ted date strict", [ted({**TED_PAYLOAD, "PD": "2026-01-05"})], ()),
            ("ted date suffix", [ted({**TED_PAYLOAD, "PD": "2026-01-05Z"})], ()),
            ("ted date basic", [ted({**TED_PAYLOAD, "PD": "20260108"})], ()),
            ("ted date loose", [ted({**TED_PAYLOAD, "PD": "2026-1-8"})], ()),
            ("ted date invalid day", [ted({**TED_PAYLOAD, "PD": "2026-02-30"})], ()),
            ("ted date number", [ted({**TED_PAYLOAD, "PD": 20260108})], ()),
            # PLACSP.
            ("placsp base", [placsp(PLACSP_PAYLOAD)], ()),
            ("placsp offset seconds", [placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:00+00:00:00.5"})], ()),
            ("placsp hour offset", [placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00+01:00"})], ()),
            ("placsp leap second", [placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T10:00:60+01:00"})], ()),
            ("placsp updated number", [placsp({**PLACSP_PAYLOAD, "updated": 20260108})], ()),
            ("placsp updated list", [placsp({**PLACSP_PAYLOAD, "updated": ["2026-01-08T10:00:00+01:00"]})], ()),
            ("placsp updated naive", [placsp({**PLACSP_PAYLOAD, "updated": "2026-01-11T10:15:00"})], ()),
            ("placsp updated invalid", [placsp({**PLACSP_PAYLOAD, "updated": "not-a-timestamp"})], ()),
            ("placsp updated lowercase z", [placsp({**PLACSP_PAYLOAD, "updated": "2026-01-08T09:00:00z"})], ()),
            (
                "placsp updated year-edge offset",
                [placsp({**PLACSP_PAYLOAD, "updated": "0001-01-01T00:00:00+23:59"})],
                (),
            ),
            ("placsp atom numeric", [placsp({**PLACSP_PAYLOAD, "atom_id": 0})], ()),
            (
                "placsp legacy numeric",
                [placsp({k: v for k, v in PLACSP_PAYLOAD.items() if not k.startswith("amount_estimated_overall")})],
                (),
            ),
            ("placsp raw malformed", [placsp({**PLACSP_PAYLOAD, "amount_estimated_overall_raw": "n/a"})], ()),
            ("placsp raw empty", [placsp({**PLACSP_PAYLOAD, "amount_estimated_overall_raw": ""})], ()),
            (
                # Regression: the kernel resolves the legacy fallback of the
                # unchosen amount field eagerly, so a shape it cannot decode
                # must exclude the batch even when raw text is present.
                "placsp unchosen tax nested array",
                [placsp({**PLACSP_PAYLOAD, "amount_tax_exclusive": [[1]]})],
                (),
            ),
            (
                "placsp amount nested array with raw",
                [
                    placsp(
                        {
                            **PLACSP_PAYLOAD,
                            "amount_estimated_overall": [[1]],
                            "amount_estimated_overall_raw": "240000.0",
                        }
                    )
                ],
                (),
            ),
            (
                "placsp value null raw malformed",
                [placsp({**PLACSP_PAYLOAD, "amount_estimated_overall": None, "amount_estimated_overall_raw": "n/a"})],
                (),
            ),
            (
                "placsp negative amount",
                [placsp({**PLACSP_PAYLOAD, "amount_estimated_overall": -5.0, "amount_estimated_overall_raw": "-5.0"})],
                (),
            ),
            ("placsp cpv scalar", [placsp({**PLACSP_PAYLOAD, "cpv": "72415000"})], ()),
            ("placsp cpv number list", [placsp({**PLACSP_PAYLOAD, "cpv": [72415000]})], ()),
            ("placsp title dict", [placsp({**PLACSP_PAYLOAD, "title": {"spa": "x"}})], ()),
            # BOE.
            ("boe base", [boe(BOE_PAYLOAD)], ()),
            ("boe basic date", [boe({**BOE_PAYLOAD, "publication_date": "20260108"})], ()),
            ("boe item number", [boe({**BOE_PAYLOAD, "item_id": 7})], ()),
            ("boe title dict", [boe({**BOE_PAYLOAD, "title": {"spa": "x"}})], ()),
            # Mixed batches and tombstones.
            (
                "mixed eligible plus big-int identity",
                [ted(TED_PAYLOAD), ted({**TED_PAYLOAD, "ND": 10**20, "TI": {"spa": "Big"}}), placsp(PLACSP_PAYLOAD)],
                (),
            ),
            ("tombstone valid only", [], (tombstone_row("https://example.invalid/1"),)),
            (
                "tombstone bad source",
                [],
                (dict(tombstone_row("https://example.invalid/1"), source="ted"),),
            ),
            ("tombstone missing ref", [], (tombstone_row(""),)),
            (
                "eligible record plus invalid tombstone",
                [ted(TED_PAYLOAD)],
                (dict(tombstone_row("https://example.invalid/1"), source=None),),
            ),
        ]
        return cases

    def test_error_batches_route_to_reference_and_keep_first_error(self) -> None:
        # Row-level failures are outside the native domain on purpose: the
        # reference decides the first historical error, and reversing the
        # error order still reproduces it exactly.
        from unittest import mock

        from tfm_licitaciones import silver

        amount_error = record_row(
            {**TED_PAYLOAD, "ND": "A", "estimated-value-lot": "100.005"},
            source="ted",
            source_file="ted/a.jsonl",
            record_locator="line:1",
        )
        identity_error = record_row(
            {**TED_PAYLOAD, "ND": ""},
            source="ted",
            source_file="ted/b.jsonl",
            record_locator="line:2",
        )
        for label, rows in (
            ("amount first", [amount_error, identity_error]),
            ("identity first", [identity_error, amount_error]),
        ):
            with self.subTest(case=label):
                records = bronze_frame(rows)
                self.assertFalse(assess_native_eligibility(records, tombstone_frame([])).eligible)
                with mock.patch.object(
                    silver,
                    "build_procurement_events_native",
                    side_effect=AssertionError("native must not run"),
                ):
                    with self.assertRaises(ValueError) as public_error:
                        silver.build_procurement_events(records, tombstone_frame([]))
                with self.assertRaises(ValueError) as reference_error:
                    build_procurement_events_reference(records, tombstone_frame([]))
                self.assertEqual(str(public_error.exception), str(reference_error.exception))

    def test_guard_eligible_implies_native_parity_and_fallback_is_exact(self) -> None:
        # Adversarial guard invariant: admitted batches are executed by the
        # native kernel and must equal the reference; rejected batches must
        # match the reference through the public API.
        from tfm_licitaciones import silver_native

        for name, records, tombstones in self._adversarial_cases():
            with self.subTest(case=name):
                records_frame = bronze_frame(list(records))
                tombstones_frame = tombstone_frame(list(tombstones))
                eligibility = assess_native_eligibility(records_frame, tombstones_frame)
                try:
                    expected = ("OK", build_procurement_events_reference(records_frame, tombstones_frame))
                except Exception as exc:  # noqa: BLE001
                    expected = ("RAISE", f"{type(exc).__name__}: {exc}")
                if eligibility.eligible:
                    try:
                        native = (
                            "OK",
                            silver_native.build_procurement_events_native(records_frame, tombstones_frame),
                        )
                    except Exception as exc:  # noqa: BLE001
                        native = ("RAISE", f"{type(exc).__name__}: {exc}")
                    self.assertEqual(native[0], expected[0], f"native/ref status: {native!r} vs {expected!r}")
                    if expected[0] == "OK":
                        assert_silver_parity(native[1], expected[1])
                    else:
                        self.assertEqual(native[1], expected[1])
                try:
                    public = ("OK", build_procurement_events(records_frame, tombstones_frame))
                except Exception as exc:  # noqa: BLE001
                    public = ("RAISE", f"{type(exc).__name__}: {exc}")
                self.assertEqual(public[0], expected[0], f"public/ref status: {public!r} vs {expected!r}")
                if expected[0] == "OK":
                    assert_silver_parity(public[1], expected[1])
                else:
                    self.assertEqual(public[1], expected[1])


class RawBronzeHarness:
    """Shared Raw JSONL -> Bronze loading and public/reference comparison."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self._counter = 0

    def _frames(self, source: str, payloads: list[dict]):
        # A fresh raw root per call keeps repeated calls inside one test
        # method isolated: accumulating files would merge unrelated events
        # under the same identities and produce spurious collisions.
        self._counter += 1
        raw = Path(self.temporary.name) / f"raw-{self._counter}"
        directory = raw / source
        directory.mkdir(parents=True)
        path = directory / "sample.jsonl"
        path.write_text("\n".join(json.dumps(payload) for payload in payloads) + "\n", encoding="utf-8")
        evidence_for_fixture(raw, path, source)
        loaded = load_raw_records(raw)
        self.assertEqual(loaded["ingestion"]["rejected"], 0, loaded["rejections"])
        return bronze_frame(loaded["bronze"]), tombstone_frame(loaded["tombstones"])

    def _assert_public_matches_reference(self, records, tombstones) -> pl.DataFrame:
        try:
            reference = ("OK", build_procurement_events_reference(records, tombstones))
        except Exception as exc:  # noqa: BLE001
            reference = ("RAISE", f"{type(exc).__name__}: {exc}")
        try:
            public = ("OK", build_procurement_events(records, tombstones))
        except Exception as exc:  # noqa: BLE001
            public = ("RAISE", f"{type(exc).__name__}: {exc}")
        self.assertEqual(public[0], reference[0])
        if reference[0] == "OK":
            assert_silver_parity(public[1], reference[1])
            return public[1]
        self.assertEqual(public[1], reference[1])
        return pl.DataFrame(schema=PROCUREMENT_EVENT_SCHEMA)


class RawBronzeRegressionTests(RawBronzeHarness, unittest.TestCase):
    """Second-audit cases through Raw -> Bronze -> public API."""

    def test_r2_1_localized_text_domain(self) -> None:
        payloads = [
            {**TED_PAYLOAD, "ND": "R21-A", "TI": {"eng": [1, 2]}},
            {**TED_PAYLOAD, "ND": "R21-B", "TI": {"eng": [None, "Useful title"]}},
            {**TED_PAYLOAD, "ND": "R21-C", "TI": [{"spa": "Useful title"}]},
        ]
        records, tombstones = self._frames("ted", payloads)
        self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
        public = self._assert_public_matches_reference(records, tombstones)
        by_id = {row["event_id"]: row["title"] for row in public.iter_rows(named=True)}
        self.assertEqual(by_id["ted:notice:R21-A"], "1")
        self.assertEqual(by_id["ted:notice:R21-B"], "Useful title")
        self.assertEqual(by_id["ted:notice:R21-C"], "Useful title")

    def test_r2_3_numeric_identities_keep_every_event(self) -> None:
        large_one = 100000000000000000001
        large_two = 100000000000000000002
        payloads = [
            {**TED_PAYLOAD, "ND": 1e-5, "TI": {"spa": "Uno"}},
            {**TED_PAYLOAD, "ND": large_one, "TI": {"spa": "Dos"}},
            {**TED_PAYLOAD, "ND": large_two, "TI": {"spa": "Tres"}},
        ]
        records, tombstones = self._frames("ted", payloads)
        self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
        public = self._assert_public_matches_reference(records, tombstones)
        self.assertEqual(public.height, 3)
        identifiers = set(public["event_id"].to_list())
        self.assertIn("ted:notice:1e-05", identifiers)
        self.assertIn(f"ted:notice:{large_one}", identifiers)
        self.assertIn(f"ted:notice:{large_two}", identifiers)

    def test_r2_2_offset_seconds_stay_in_the_reference_domain(self) -> None:
        payloads = [
            {**PLACSP_PAYLOAD, "atom_id": "https://example.invalid/a", "updated": "2026-01-08T10:00:00+00:00:00.5"},
            {**PLACSP_PAYLOAD, "atom_id": "https://example.invalid/b", "updated": "2026-01-08T10:00:00-00:00:00.5"},
        ]
        records, tombstones = self._frames("placsp", payloads)
        self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
        public = self._assert_public_matches_reference(records, tombstones)
        expected = {
            "placsp:notice:https://example.invalid/a@2026-01-08T10:00:00+00:00",
            "placsp:notice:https://example.invalid/b@2026-01-08T10:00:00+00:00",
        }
        self.assertEqual(set(public["event_id"].to_list()), expected)

    def test_r2_4_publication_date_domain(self) -> None:
        payloads = [
            {**TED_PAYLOAD, "ND": "R24-A", "PD": "20260108"},
            {**TED_PAYLOAD, "ND": "R24-B", "PD": "2026-1-8"},
        ]
        records, tombstones = self._frames("ted", payloads)
        self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
        public = self._assert_public_matches_reference(records, tombstones)
        by_id = {row["event_id"]: row["publication_date"] for row in public.iter_rows(named=True)}
        self.assertEqual(by_id["ted:notice:R24-A"], date(2026, 1, 8))
        self.assertIsNone(by_id["ted:notice:R24-B"])


class R3AuditRegressionTests(RawBronzeHarness, unittest.TestCase):
    """Third-audit (R3) false-positive regressions through Raw -> Bronze.

    The audited head accepted every one of these synthetic payloads as
    eligible and then diverged (silent field loss, collapsed identities,
    different errors or different values). Test success there was not
    parity, so each case pins Bronze acceptance with zero rejections, the
    routing/guard reason and exact public/reference parity of frames or
    error messages.
    """

    @staticmethod
    def _nested(levels: int):
        value = "leaf"
        for _ in range(levels):
            value = [value]
        return value

    def test_r3_1_unused_large_integer_matches_reference(self) -> None:
        records, tombstones = self._frames("ted", [{**TED_PAYLOAD, "unused": 10**400}])
        eligibility = assess_native_eligibility(records, tombstones)
        self.assertFalse(eligibility.eligible)
        self.assertEqual(eligibility.reason, REASON_NUMBER_DOMAIN)
        public = self._assert_public_matches_reference(records, tombstones)
        self.assertEqual(public.height, 1)
        row = public.row(0, named=True)
        self.assertEqual(row["event_id"], "ted:notice:N1")
        self.assertEqual(row["title"], "Servicio de migración cloud")
        self.assertEqual(row["buyer_name"], "Centro de Sistemas")
        self.assertEqual(row["cpv_codes"], ["72415000", "48662000"])
        self.assertEqual(row["estimated_value"], Decimal("125000.50"))
        self.assertEqual(row["source_url"], "https://example.invalid/ted/N1")

    def test_r3_1_unused_deep_nesting_matches_reference(self) -> None:
        records, tombstones = self._frames("ted", [{**TED_PAYLOAD, "unused": self._nested(140)}])
        eligibility = assess_native_eligibility(records, tombstones)
        self.assertFalse(eligibility.eligible)
        self.assertEqual(eligibility.reason, REASON_DOCUMENT_DEPTH)
        public = self._assert_public_matches_reference(records, tombstones)
        self.assertEqual(public.height, 1)
        row = public.row(0, named=True)
        self.assertEqual(row["event_id"], "ted:notice:N1")
        self.assertEqual(row["title"], "Servicio de migración cloud")
        self.assertEqual(row["estimated_value"], Decimal("125000.50"))

    def test_r3_1_identities_do_not_collapse(self) -> None:
        payloads = [
            {**TED_PAYLOAD, "ND": identity, "unused": 10**400} for identity in ("N1", "N2")
        ]
        records, tombstones = self._frames("ted", payloads)
        self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
        public = self._assert_public_matches_reference(records, tombstones)
        self.assertEqual(public.height, 2)
        self.assertEqual(public["event_id"].to_list(), ["ted:notice:N1", "ted:notice:N2"])
        for row in public.iter_rows(named=True):
            self.assertEqual(row["title"], "Servicio de migración cloud")
            self.assertEqual(row["estimated_value"], Decimal("125000.50"))

    def test_r3_2_structured_amount_raw_matches_reference(self) -> None:
        structured = [{"x": "y"}]
        without_overall = {
            key: value for key, value in PLACSP_PAYLOAD.items() if key != "amount_estimated_overall"
        }
        without_tax = {
            key: value for key, value in PLACSP_PAYLOAD.items() if key != "amount_tax_exclusive"
        }
        variants = {
            "null value structured raw": {
                **PLACSP_PAYLOAD,
                "amount_estimated_overall": None,
                "amount_estimated_overall_raw": structured,
            },
            "absent key structured raw": {
                **without_overall,
                "amount_estimated_overall_raw": structured,
            },
            "null unchosen tax raw": {
                **PLACSP_PAYLOAD,
                "amount_tax_exclusive": None,
                "amount_tax_exclusive_raw": structured,
            },
            "absent unchosen tax key": {
                **without_tax,
                "amount_tax_exclusive_raw": structured,
            },
        }
        for name, payload in variants.items():
            with self.subTest(case=name):
                records, tombstones = self._frames("placsp", [payload])
                eligibility = assess_native_eligibility(records, tombstones)
                self.assertFalse(eligibility.eligible)
                self.assertEqual(eligibility.reason, REASON_AMOUNT)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertEqual(public.height, 1)
                self.assertEqual(
                    public["title"][0], "Servicio de migración a plataforma cloud"
                )

    def test_r3_3_infinity_spellings_match_reference(self) -> None:
        # The reference normalizes these to +inf after the gate and raises;
        # the kernel would keep null, so they must fall back.
        for text in (" inf ", "i n f", "inf..", "Infinity", "infinity", "+inf", "i.n.f"):
            with self.subTest(amount=text):
                records, tombstones = self._frames(
                    "ted", [{**TED_PAYLOAD, "estimated-value-lot": text}]
                )
                eligibility = assess_native_eligibility(records, tombstones)
                self.assertFalse(eligibility.eligible)
                self.assertEqual(eligibility.reason, REASON_AMOUNT)
                self._assert_public_matches_reference(records, tombstones)

    def test_r3_3_proven_digit_free_amounts_stay_native_and_null(self) -> None:
        for text in ("", "   ", "\t", "n/a", "N/A", "abc", "nan", "NaN", "-inf", "-Infinity"):
            with self.subTest(amount=text):
                records, tombstones = self._frames(
                    "ted", [{**TED_PAYLOAD, "estimated-value-lot": text}]
                )
                self.assertTrue(assess_native_eligibility(records, tombstones).eligible)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertIsNone(public["estimated_value"][0])
                self.assertIsNone(public["currency"][0])

    def test_r3_4_control_whitespace_identity_and_text(self) -> None:
        cases = {
            "identity": ({**TED_PAYLOAD, "ND": "\x1cN1\x1c"}, "ted:notice:N1", "Servicio de migración cloud"),
            "text": ({**TED_PAYLOAD, "TI": "\x1cTítulo\x1c"}, "ted:notice:N1", "Título"),
        }
        for name, (payload, event_id, title) in cases.items():
            with self.subTest(case=name):
                records, tombstones = self._frames("ted", [payload])
                eligibility = assess_native_eligibility(records, tombstones)
                self.assertFalse(eligibility.eligible)
                self.assertEqual(eligibility.reason, REASON_STRING_DOMAIN)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertEqual(public["event_id"].to_list(), [event_id])
                self.assertEqual(public["title"][0], title)

    def test_r3_4_control_identity_deduplicates_in_both_orders(self) -> None:
        payloads = [{**TED_PAYLOAD, "ND": "N1"}, {**TED_PAYLOAD, "ND": "\x1cN1\x1c"}]
        for label, order in (("control second", payloads), ("control first", list(reversed(payloads)))):
            with self.subTest(case=label):
                records, tombstones = self._frames("ted", order)
                self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertEqual(public.height, 1)
                self.assertEqual(public["event_id"].to_list(), ["ted:notice:N1"])

    def test_r3_4_control_whitespace_tombstone_matches_reference(self) -> None:
        # Raw -> Bronze cannot carry U+001C through the Atom XML tombstone
        # path, so the typed Bronze row is the boundary the guard sees.
        records = bronze_frame(
            [record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")]
        )
        tombstones = tombstone_frame([tombstone_row("\x1cREF\x1c")])
        eligibility = assess_native_eligibility(records, tombstones)
        self.assertFalse(eligibility.eligible)
        self.assertEqual(eligibility.reason, REASON_TOMBSTONE_REF)
        public = self._assert_public_matches_reference(records, tombstones)
        self.assertIn("placsp:tombstone:REF", public["event_id"].to_list())

    def test_r3_5_link_selection_before_stripping_matches_reference(self) -> None:
        variants = {
            "whitespace ENG blocks SPA": (
                {"url": None, "links": {"html": {"ENG": " ", "SPA": "https://example.invalid"}}},
                None,
            ),
            "empty ENG falls through": (
                {"url": None, "links": {"html": {"ENG": "", "SPA": "https://example.invalid"}}},
                "https://example.invalid",
            ),
            "whitespace url falls to link": (
                {"url": " ", "links": {"html": {"ENG": "https://eng.invalid"}}},
                "https://eng.invalid",
            ),
            # Bronze serializes payloads with sorted keys, so the first entry
            # in document order is the alphabetically first html key in both
            # engines; the reference iterates its parsed dict and the kernel
            # scans the serialized text.
            "first entry without preferred language": (
                {"url": None, "links": {"html": {"ITA": "https://ita.invalid", "ZUL": "https://zul.invalid"}}},
                "https://ita.invalid",
            ),
            "empty first entry stays null": (
                {"url": None, "links": {"html": {"AAA": "", "ZUL": "https://zul.invalid"}}},
                None,
            ),
            "empty html stays null": ({"url": None, "links": {"html": {}}}, None),
        }
        for name, (override, expected) in variants.items():
            with self.subTest(case=name):
                records, tombstones = self._frames("ted", [{**TED_PAYLOAD, **override}])
                eligibility = assess_native_eligibility(records, tombstones)
                self.assertTrue(eligibility.eligible, eligibility.reason)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertEqual(public["source_url"][0], expected)

    def test_r3_6_country_domain_matches_reference(self) -> None:
        cases = {
            "uncased letter pair": ("A中", False, "A中"),
            "unicode uppercase pair": ("ÉS", False, "ÉS"),
            "ascii uppercase pair": ("AB", True, "AB"),
            "alpha3 mapping": ("ESP", True, "ES"),
            "lowercase pair null in both": ("es", True, None),
            "alphanumeric pair null in both": ("A1", True, None),
            "long value null in both": ("Spain", True, None),
        }
        for name, (country, eligible, expected) in cases.items():
            with self.subTest(case=name):
                records, tombstones = self._frames("ted", [{**TED_PAYLOAD, "CY": country}])
                verdict = assess_native_eligibility(records, tombstones)
                self.assertEqual(verdict.eligible, eligible, verdict.reason)
                if not eligible:
                    self.assertEqual(verdict.reason, REASON_COUNTRY)
                public = self._assert_public_matches_reference(records, tombstones)
                self.assertEqual(public["country"][0], expected)


class GuardDomainBoundaryTests(unittest.TestCase):
    """Boundaries of the whole-document and field restrictions closed for R3."""

    @staticmethod
    def _ted_records(**override):
        return bronze_frame(
            [record_row({**TED_PAYLOAD, **override}, source="ted", source_file="ted/a.jsonl")]
        )

    def _assert_route(self, records, eligible, reason=None) -> None:
        verdict = assess_native_eligibility(records, tombstone_frame([]))
        self.assertEqual(verdict.eligible, eligible, verdict.reason)
        if not eligible:
            self.assertEqual(verdict.reason, reason)

    def _assert_public_parity(self, records) -> pl.DataFrame:
        tombstones = tombstone_frame([])
        try:
            reference = ("OK", build_procurement_events_reference(records, tombstones))
        except Exception as exc:  # noqa: BLE001
            reference = ("RAISE", f"{type(exc).__name__}: {exc}")
        try:
            public = ("OK", build_procurement_events(records, tombstones))
        except Exception as exc:  # noqa: BLE001
            public = ("RAISE", f"{type(exc).__name__}: {exc}")
        self.assertEqual(public[0], reference[0])
        if reference[0] == "OK":
            assert_silver_parity(public[1], reference[1])
            return public[1]
        self.assertEqual(public[1], reference[1])
        return pl.DataFrame(schema=PROCUREMENT_EVENT_SCHEMA)

    @staticmethod
    def _nested(levels: int):
        value = "leaf"
        for _ in range(levels):
            value = [value]
        return value

    def test_container_depth_boundary(self) -> None:
        for levels, eligible in ((1, True), (63, True), (64, False), (140, False)):
            with self.subTest(levels=levels):
                records = self._ted_records(unused=self._nested(levels))
                self._assert_route(records, eligible, REASON_DOCUMENT_DEPTH)
                self._assert_public_parity(records)

    def test_integer_domain_boundary(self) -> None:
        for value, eligible in (
            (0, True),
            (2**63 - 1, True),
            (-(2**63), True),
            (2**63, False),
            (-(2**63) - 1, False),
            (10**100, False),
        ):
            with self.subTest(value=str(value)[:24]):
                records = self._ted_records(unused=value)
                self._assert_route(records, eligible, REASON_NUMBER_DOMAIN)
                self._assert_public_parity(records)

    def test_non_finite_float_boundary(self) -> None:
        # JSON cannot carry these values; CPython accepts the non-standard
        # tokens emitted by ``json.dumps`` and the guard checks the parsed
        # float before the backend can silently fail.
        for value, eligible in (
            (1.5, True),
            (1e308, True),
            (float("inf"), False),
            (float("-inf"), False),
            (float("nan"), False),
        ):
            with self.subTest(value=repr(value)):
                records = self._ted_records(unused=value)
                self._assert_route(records, eligible, REASON_NUMBER_DOMAIN)
                self._assert_public_parity(records)

    def test_divergent_separator_strings_are_ineligible(self) -> None:
        for name, override in {
            "unused value": {"unused": "\x1c"},
            "unused embedded": {"unused": "a\x1cb"},
            "unused key": {"unused\x1c": "x"},
            "nested list": {"unused": {"a": ["\x1d"]}},
            "identity": {"ND": "\x1cN1"},
            "text": {"TI": "X\x1f"},
        }.items():
            with self.subTest(case=name):
                records = self._ted_records(**override)
                self._assert_route(records, False, REASON_STRING_DOMAIN)
                self._assert_public_parity(records)

    def test_digit_free_amount_boundary(self) -> None:
        for text, eligible in (
            ("", True),
            ("   ", True),
            ("\t", True),
            ("n/a", True),
            ("N/A", True),
            ("n.a.", True),
            ("abc", True),
            ("nan", True),
            ("NaN", True),
            ("-inf", True),
            ("-Infinity", True),
            ("inf", False),
            ("+inf", False),
            ("INF", False),
            ("i n f", False),
            ("inf..", False),
            ("infinity", False),
            ("i.n.f", False),
            ("inf.", False),
            (".inf", False),
            ("inf,inity", False),
        ):
            with self.subTest(amount=text):
                records = self._ted_records(**{"estimated-value-lot": text})
                self._assert_route(records, eligible, REASON_AMOUNT)
                self._assert_public_parity(records)

    def test_equivalent_whitespace_controls_stay_eligible(self) -> None:
        for text in ("\x1b", "\u200b", "\ufeff"):
            with self.subTest(text=repr(text)):
                records = self._ted_records(unused=text)
                self._assert_route(records, True)
                self._assert_public_parity(records)
        records = self._ted_records(TI="\xa0Título\u3000")
        self._assert_route(records, True)
        public = self._assert_public_parity(records)
        self.assertEqual(public["title"][0], "Título")

    def test_placsp_raw_shape_boundary(self) -> None:
        without_overall = {
            key: value for key, value in PLACSP_PAYLOAD.items() if key != "amount_estimated_overall"
        }
        cases = {
            "null value plain raw": (
                {**PLACSP_PAYLOAD, "amount_estimated_overall": None,
                 "amount_estimated_overall_raw": "240000.0"},
                True,
            ),
            "absent key plain raw": (
                {**without_overall, "amount_estimated_overall_raw": "240000.0"}, True
            ),
            "null value null raw": (
                {**PLACSP_PAYLOAD, "amount_estimated_overall": None,
                 "amount_estimated_overall_raw": None},
                True,
            ),
            "null value array raw": (
                {**PLACSP_PAYLOAD, "amount_estimated_overall": None,
                 "amount_estimated_overall_raw": [1]},
                False,
            ),
            "absent key array raw": (
                {**without_overall, "amount_estimated_overall_raw": [1]}, False
            ),
            "null value object raw": (
                {**PLACSP_PAYLOAD, "amount_estimated_overall": None,
                 "amount_estimated_overall_raw": {"x": 1}},
                False,
            ),
        }
        for name, (payload, eligible) in cases.items():
            with self.subTest(case=name):
                records = bronze_frame(
                    [record_row(payload, source="placsp", source_file="placsp/a.zip")]
                )
                self._assert_route(records, eligible, REASON_AMOUNT)
                self._assert_public_parity(records)


class R3BatchRoutingTests(unittest.TestCase):
    """R3 shapes at the routing boundary: whole-batch fallback and controls."""

    def test_single_ineligible_row_routes_whole_batch_with_original_frames(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        good = record_row(TED_PAYLOAD, source="ted", source_file="ted/good.jsonl", record_locator="line:1")
        bad = record_row(
            {**TED_PAYLOAD, "ND": "N9", "unused": 10**400},
            source="ted",
            source_file="ted/bad.jsonl",
            record_locator="line:2",
        )
        tombstone = tombstone_row("https://example.invalid/1")
        for label, rows in (("bad second", [good, bad]), ("bad first", [bad, good])):
            with self.subTest(case=label):
                records = bronze_frame(list(rows))
                tombstones = tombstone_frame([tombstone])
                self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
                with mock.patch.object(
                    silver,
                    "build_procurement_events_native",
                    side_effect=AssertionError("native must not run"),
                ) as native, mock.patch.object(
                    silver,
                    "build_procurement_events_reference",
                    wraps=silver.build_procurement_events_reference,
                ) as reference:
                    public = silver.build_procurement_events(records, tombstones)
                native.assert_not_called()
                self.assertIs(reference.call_args.args[0], records)
                self.assertIs(reference.call_args.args[1], tombstones)
                expected = build_procurement_events_reference(records, tombstones)
                self.assertTrue(public.equals(expected))
                self.assertEqual(public.height, 3)

    def test_collision_behind_ineligible_row_raises_reference_error(self) -> None:
        plain = {**TED_PAYLOAD, "ND": "X", "TI": {"spa": "B"}}
        ineligible = {**TED_PAYLOAD, "ND": "X", "TI": {"spa": "A"}, "unused": 10**400}
        for label, payload_order in (
            ("plain first", (plain, ineligible)),
            ("ineligible first", (ineligible, plain)),
        ):
            rows = [
                record_row(payload, source="ted", source_file=source_file)
                for payload, source_file in zip(payload_order, ("ted/a.jsonl", "ted/b.jsonl"))
            ]
            with self.subTest(case=label):
                records = bronze_frame(list(rows))
                tombstones = tombstone_frame([])
                self.assertFalse(assess_native_eligibility(records, tombstones).eligible)
                with self.assertRaises(ValueError) as reference_error:
                    build_procurement_events_reference(records, tombstones)
                with self.assertRaises(ValueError) as public_error:
                    build_procurement_events(records, tombstones)
                self.assertEqual(str(public_error.exception), str(reference_error.exception))
                self.assertIn("ted:notice:X", str(public_error.exception))

    def test_in_domain_r3_controls_execute_native(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        without_overall = {
            key: value for key, value in PLACSP_PAYLOAD.items() if key != "amount_estimated_overall"
        }
        cases = {
            "placsp null value plain raw": (
                "placsp",
                {**PLACSP_PAYLOAD, "amount_estimated_overall": None,
                 "amount_estimated_overall_raw": "240000.0"},
            ),
            "placsp absent key plain raw": (
                "placsp",
                {**without_overall, "amount_estimated_overall_raw": "240000.0"},
            ),
            "ted whitespace amount": (
                "ted",
                {**TED_PAYLOAD, "estimated-value-lot": "  ", "unused": 2**63 - 1},
            ),
            "ted whitespace preferred link": (
                "ted",
                {**TED_PAYLOAD, "url": None,
                 "links": {"html": {"ENG": " ", "SPA": "https://example.invalid"}}},
            ),
        }
        for name, (source, payload) in cases.items():
            with self.subTest(case=name):
                records = bronze_frame(
                    [record_row(payload, source=source, source_file=f"{source}/a.jsonl")]
                )
                tombstones = tombstone_frame([])
                self.assertTrue(assess_native_eligibility(records, tombstones).eligible)
                with mock.patch.object(
                    silver,
                    "build_procurement_events_native",
                    wraps=silver.build_procurement_events_native,
                ) as native:
                    public = silver.build_procurement_events(records, tombstones)
                self.assertEqual(native.call_count, 1)
                assert_silver_parity(
                    public, build_procurement_events_reference(records, tombstones)
                )

    def test_representative_r3_control_batch_runs_native(self) -> None:
        from unittest import mock

        from tfm_licitaciones import silver

        records = bronze_frame(
            [
                record_row(
                    {
                        **TED_PAYLOAD,
                        "ND": "C1",
                        "CY": "AB",
                        "estimated-value-lot": "  ",
                        "url": "",
                        "links": {"html": {"ENG": "", "SPA": "https://spa.invalid"}},
                        "unused": R3AuditRegressionTests._nested(63),
                    },
                    source="ted",
                    source_file="ted/a.jsonl",
                ),
                record_row(
                    {
                        **PLACSP_PAYLOAD,
                        "amount_estimated_overall": None,
                        "amount_estimated_overall_raw": "240000.0",
                    },
                    source="placsp",
                    source_file="placsp/a.zip",
                ),
                record_row(BOE_PAYLOAD, source="boe", source_file="boe/a.jsonl"),
            ]
        )
        tombstones = tombstone_frame([tombstone_row("https://example.invalid/1")])
        self.assertTrue(assess_native_eligibility(records, tombstones).eligible)
        with mock.patch.object(
            silver,
            "build_procurement_events_native",
            wraps=silver.build_procurement_events_native,
        ) as native:
            public = silver.build_procurement_events(records, tombstones)
        self.assertEqual(native.call_count, 1)
        reference = build_procurement_events_reference(records, tombstones)
        assert_silver_parity(public, reference)
        rows = {row["event_id"]: row for row in public.iter_rows(named=True)}
        self.assertEqual(rows["ted:notice:C1"]["country"], "AB")
        self.assertEqual(rows["ted:notice:C1"]["source_url"], "https://spa.invalid")


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
    """The native kernel's success path must stay free of per-row Python."""

    # Per-row Python mechanisms: forbidden everywhere in the kernel.
    FORBIDDEN_ATTRS = {
        "iter_rows",
        "iter_slices",
        "map_elements",
        "map_batches",
        "apply",
        "to_pandas",
        "to_dict",
    }
    # Bounded driver fetches: confined to the explicit collision diagnostic.
    FETCH_ATTRS = {"to_dicts", "to_list", "to_series", "get_column", "row", "rows"}
    FORBIDDEN_NAMES = {"ProcurementEvent", "loads", "dumps", "json"}
    # The collision diagnostic is the only place rows reach the driver.
    FETCH_HELPERS = {"_raise_collisions"}

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

    def test_guard_uses_no_polars_udfs_and_stays_independent(self) -> None:
        # The selector is authorized to use CPython (json.loads, iterating the
        # batch columns), but it must not smuggle per-row Python into Polars
        # nor depend on the mutable engines it routes between.
        tree = ast.parse((SRC / "silver_guard.py").read_text(encoding="utf-8"))
        violations: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in {"map_elements", "map_batches", "apply"}:
                violations.append(f".{node.attr}")
            if isinstance(node, ast.ImportFrom) and (node.module or "") in {"silver_native", "silver_reference"}:
                violations.append(f"import {node.module}")
            if isinstance(node, ast.Import):
                violations.extend(
                    alias.name for alias in node.names if alias.name.split(".")[-1] in {"silver_native", "silver_reference"}
                )
        self.assertEqual(violations, [])

    def test_silver_facade_labels_the_hybrid_route(self) -> None:
        from tfm_licitaciones import silver, silver_native

        self.assertEqual(silver.IMPLEMENTATION, "hybrid-native-reference")
        self.assertIn("hybrid", silver.IMPLEMENTATION_DETAIL)
        self.assertNotEqual(silver.IMPLEMENTATION, silver_native.IMPLEMENTATION)
        records = bronze_frame([record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl")])
        self.assertTrue(
            silver.build_procurement_events(records, tombstone_frame([])).equals(
                silver_native.build_procurement_events_native(records, tombstone_frame([]))
            )
        )


if __name__ == "__main__":
    unittest.main()
