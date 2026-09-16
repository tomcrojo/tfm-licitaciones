"""Tests for canonical Silver procurement events built from typed Bronze."""

from __future__ import annotations

import json
import re
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from raw_fixtures import RETRIEVED_AT, evidence_for_fixture
from tfm_licitaciones.atom import parse_atom_batch
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.io import read_parquet, write_parquet
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA
from tfm_licitaciones.pipeline import run_pipeline
from tfm_licitaciones.silver import build_procurement_events

UTC = timezone.utc
LATER = RETRIEVED_AT + timedelta(days=3, hours=4)
ATOM_ID = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
TED_PAYLOAD: dict[str, Any] = {
    "_source": "ted",
    "ND": "N1",
    "PD": "2026-01-05Z",
    "TI": {"spa": "Servicio de migración cloud"},
    "description-glo": {"spa": "Migración a nube híbrida"},
    "buyer-name": {"spa": ["Centro de Sistemas"]},
    "CY": "ESP",
    "PC": ["72415000", "48662000", "72415000"],
    "estimated-value-lot": "125000.50",
    "currency": "EUR",
    "url": "https://example.invalid/ted/N1",
}
PLACSP_PAYLOAD: dict[str, Any] = {
    "_source": "placsp",
    "tender_no": "10000101",
    "atom_id": ATOM_ID,
    "title": "Servicio de migración a plataforma cloud",
    "summary": "Id licitación: S1/2026; Estado: EV",
    "updated": "2026-01-08T10:00:00.000+01:00",
    "url": "https://example.invalid/placsp/10000101",
    "status_code": "EV",
    "buyer": "Centro de Sistemas y Tecnologías de la Información",
    "buyer_dir3": "E00000001",
    "nuts_code": "ES300",
    "region": "Madrid",
    "cpv": ["72415000"],
    "amount_tax_exclusive": 120000.0,
    "amount_tax_exclusive_currency": "EUR",
    "amount_tax_exclusive_raw": "120000.0",
    "amount_estimated_overall": 240000.0,
    "amount_estimated_overall_currency": "EUR",
    "amount_estimated_overall_raw": "240000.0",
}
BOE_PAYLOAD: dict[str, Any] = {
    "_source": "boe",
    "item_id": "B1",
    "title": "Anuncio de contratación de servicios",
    "summary": "Contexto normativo",
    "department": "Organismo de Demo",
    "publication_date": "2026-01-05",
    "url": "https://example.invalid/boe/B1",
}


def record_row(
    payload: dict[str, Any],
    *,
    source: str,
    source_file: str,
    retrieved_at: datetime = RETRIEVED_AT,
    raw_sha256: str = "a" * 64,
    source_member: str | None = None,
    source_member_index: int | None = None,
    record_locator: str = "line:1",
) -> dict[str, Any]:
    return {
        "source": source,
        "source_file": source_file,
        "source_member": source_member,
        "source_member_index": source_member_index,
        "record_locator": record_locator,
        "source_record_id": None,
        "raw_sha256": raw_sha256,
        "raw_retrieved_at": retrieved_at,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def tombstone_row(
    ref: str,
    *,
    source_file: str = "placsp/placsp-202601.zip",
    retrieved_at: datetime = RETRIEVED_AT,
    raw_sha256: str = "b" * 64,
    source_member: str | None = None,
    source_member_index: int | None = None,
    record_locator: str = "deleted-entry:1",
) -> dict[str, Any]:
    return {
        "source": "placsp",
        "source_file": source_file,
        "source_member": source_member,
        "source_member_index": source_member_index,
        "record_locator": record_locator,
        "source_record_id": ref,
        "raw_sha256": raw_sha256,
        "raw_retrieved_at": retrieved_at,
    }


def events(records: list[dict[str, Any]], tombstones: list[dict[str, Any]] | None = None):
    return build_procurement_events(bronze_frame(records), tombstone_frame(tombstones or []))


def placsp_notice_id(atom_id: str, marker: str) -> str:
    return f"placsp:notice:{atom_id}@{marker}"


class CanonicalMappingTests(unittest.TestCase):
    """Adapter identity, temporal and field-mapping semantics."""

    def test_empty_bronze_builds_typed_empty_canonical_silver(self) -> None:
        frame = events([])
        self.assertEqual(frame.schema, PROCUREMENT_EVENT_SCHEMA)
        self.assertEqual(frame.height, 0)

    def test_typed_empty_canonical_silver_round_trips_through_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "procurement_events.parquet"
            frame = build_procurement_events(bronze_frame([]), tombstone_frame([]))
            write_parquet(path, frame)
            reloaded = read_parquet(path)
            self.assertEqual(reloaded.height, 0)
            self.assertEqual(reloaded.schema, PROCUREMENT_EVENT_SCHEMA)

    def test_ted_record_maps_deterministic_event(self) -> None:
        frame = events([record_row(TED_PAYLOAD, source="ted", source_file="ted/sample.jsonl")])
        row = frame.row(0, named=True)
        self.assertEqual(row["event_id"], "ted:notice:N1")
        self.assertIsNone(row["procedure_id"])
        self.assertEqual(row["source"], "ted")
        self.assertEqual(row["source_event_type"], "notice")
        self.assertEqual(row["buyer_name"], "Centro de Sistemas")
        self.assertEqual(row["title"], "Servicio de migración cloud")
        self.assertEqual(row["publication_date"], date(2026, 1, 5))
        self.assertEqual(row["estimated_value"], Decimal("125000.50"))
        self.assertEqual(row["currency"], "EUR")
        self.assertEqual(row["country"], "ES")
        self.assertEqual(row["source_url"], "https://example.invalid/ted/N1")
        self.assertIsNone(row["buyer_id"])
        self.assertIsNone(row["awarded_value"])

    def test_ted_hyphenated_publication_date_alias_maps_to_publication_date(self) -> None:
        payload = {key: value for key, value in TED_PAYLOAD.items() if key != "PD"}
        payload["publication-date"] = "2026-01-05"
        frame = events([record_row(payload, source="ted", source_file="ted/sample.jsonl")])
        row = frame.row(0, named=True)
        self.assertEqual(row["publication_date"], date(2026, 1, 5))

    def test_ted_notice_type_becomes_explicit_event_type(self) -> None:
        payload = {**TED_PAYLOAD, "notice-type": "Contract notice"}
        frame = events([record_row(payload, source="ted", source_file="ted/sample.jsonl")])
        self.assertEqual(frame["source_event_type"][0], "notice:Contract notice")

    def test_placsp_record_maps_deterministic_event(self) -> None:
        frame = events([record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip",
                                   source_member="feed.atom", source_member_index=2,
                                   record_locator="entry:1")])
        row = frame.row(0, named=True)
        self.assertEqual(row["event_id"], placsp_notice_id(ATOM_ID, "2026-01-08T09:00:00+00:00"))
        self.assertEqual(row["procedure_id"], f"placsp:procedure:{ATOM_ID}")
        self.assertEqual(row["source"], "placsp")
        self.assertEqual(row["source_event_type"], "notice_snapshot")
        self.assertEqual(row["buyer_id"], "E00000001")
        self.assertEqual(row["status"], "EV")
        self.assertEqual(row["nuts_code"], "ES300")
        self.assertEqual(row["country"], "ES")
        # amount_estimated_overall is preferred over amount_tax_exclusive.
        self.assertEqual(row["estimated_value"], Decimal("240000.00"))
        self.assertEqual(row["currency"], "EUR")
        self.assertEqual(row["cpv_codes"], ["72415000"])

    def test_boe_record_remains_supported_compatibility_mapping(self) -> None:
        frame = events([record_row(BOE_PAYLOAD, source="boe", source_file="boe/sample.jsonl")])
        row = frame.row(0, named=True)
        self.assertEqual(row["event_id"], "boe:notice:B1")
        self.assertIsNone(row["procedure_id"])
        self.assertEqual(row["source_event_type"], "notice")
        self.assertEqual(row["buyer_name"], "Organismo de Demo")
        self.assertEqual(row["publication_date"], date(2026, 1, 5))
        self.assertEqual(row["country"], "ES")
        self.assertEqual(row["cpv_codes"], [])
        self.assertIsNone(row["estimated_value"])
        self.assertIsNone(row["deadline"])

    def test_ingested_at_comes_exactly_from_raw_retrieved_at(self) -> None:
        payload = {**PLACSP_PAYLOAD, "atom_id": "https://example.invalid/x"}
        rows = [
            record_row(payload, source="placsp", source_file="placsp/late.zip", retrieved_at=LATER),
            record_row(payload, source="placsp", source_file="placsp/early.zip", retrieved_at=RETRIEVED_AT),
        ]
        frame = events(rows)
        self.assertEqual(frame.height, 1)
        self.assertEqual(frame["ingested_at"][0], RETRIEVED_AT)
        self.assertEqual(frame["ingested_at"][0].microsecond, 123456)

    def test_placsp_updated_is_source_updated_at_not_publication_date(self) -> None:
        frame = events([record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip")])
        row = frame.row(0, named=True)
        self.assertEqual(row["source_updated_at"], datetime(2026, 1, 8, 9, 0, tzinfo=UTC))
        self.assertIsNone(row["publication_date"])

    def test_multiple_cpvs_preserved_in_stable_deduplicated_order(self) -> None:
        frame = events([record_row(TED_PAYLOAD, source="ted", source_file="ted/sample.jsonl")])
        self.assertEqual(frame["cpv_codes"][0].to_list(), ["72415000", "48662000"])

    def test_cpv_order_difference_is_a_material_collision(self) -> None:
        reordered = {**TED_PAYLOAD, "PC": ["48662000", "72415000"]}
        with self.assertRaisesRegex(ValueError, "ted:notice:N1"):
            events([
                record_row(TED_PAYLOAD, source="ted", source_file="ted/a.jsonl"),
                record_row(reordered, source="ted", source_file="ted/b.jsonl"),
            ])

    def test_nullable_fields_stay_null_instead_of_guessed(self) -> None:
        payload = {
            **TED_PAYLOAD,
            "deadline-receipt-tender-date-lot": "2026-02-01Z",
            "place-of-performance-city-proc": {"spa": "Madrid"},
            "buyer-identifier": ["DIR-A", "DIR-B"],
        }
        frame = events([record_row(payload, source="ted", source_file="ted/sample.jsonl")])
        row = frame.row(0, named=True)
        self.assertIsNone(row["deadline"])
        self.assertIsNone(row["status"])
        self.assertIsNone(row["nuts_code"])
        self.assertIsNone(row["source_updated_at"])
        # Ambiguous buyer identifier stays null instead of picking one.
        self.assertIsNone(row["buyer_id"])

    def test_placsp_naive_or_invalid_updated_uses_undated_marker(self) -> None:
        for updated in ("2026-01-08T10:00:00", "not-a-timestamp", ""):
            with self.subTest(updated=updated):
                payload = {**PLACSP_PAYLOAD, "updated": updated}
                frame = events([record_row(payload, source="placsp", source_file="placsp/f.zip")])
                row = frame.row(0, named=True)
                self.assertEqual(row["event_id"], placsp_notice_id(ATOM_ID, "undated"))
                self.assertIsNone(row["source_updated_at"])
                self.assertIsNone(row["publication_date"])

    def test_two_materially_different_undated_entries_fail_explicitly(self) -> None:
        first = {**PLACSP_PAYLOAD, "updated": ""}
        second = {**first, "title": "Otro título del mismo expediente"}
        with self.assertRaisesRegex(ValueError, "undated"):
            events([
                record_row(first, source="placsp", source_file="placsp/a.zip"),
                record_row(second, source="placsp", source_file="placsp/b.zip"),
            ])


class DeterminismTests(unittest.TestCase):
    """Repeated builds, deduplication and deterministic ordering."""

    def test_repeated_rebuild_produces_exact_frame_equality(self) -> None:
        rows = [
            record_row(TED_PAYLOAD, source="ted", source_file="ted/sample.jsonl"),
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip"),
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/sample.jsonl"),
        ]
        first = events(rows)
        second = events(list(reversed(rows)))
        self.assertTrue(first.equals(second))
        self.assertEqual(first.schema, PROCUREMENT_EVENT_SCHEMA)

    def test_repeated_source_event_deduplicates_to_one_identity(self) -> None:
        rows = [
            record_row(TED_PAYLOAD, source="ted", source_file="ted/repeat.jsonl", retrieved_at=LATER,
                       raw_sha256="c" * 64),
            record_row(TED_PAYLOAD, source="ted", source_file="ted/first.jsonl", retrieved_at=RETRIEVED_AT),
        ]
        frame = events(rows)
        self.assertEqual(frame["event_id"].to_list(), ["ted:notice:N1"])
        # The minimum provenance tuple selects the first retrieval.
        self.assertEqual(frame["ingested_at"][0], RETRIEVED_AT)

    def test_boe_material_difference_same_item_id_fails_explicitly(self) -> None:
        corrected = {**BOE_PAYLOAD, "title": "Título corregido del mismo anuncio"}
        with self.assertRaisesRegex(ValueError, "boe:notice:B1"):
            events([
                record_row(BOE_PAYLOAD, source="boe", source_file="boe/first.jsonl"),
                record_row(corrected, source="boe", source_file="boe/corrected.jsonl", retrieved_at=LATER),
            ])

    def test_boe_identical_repeats_deduplicate_selecting_minimum_retrieved_at(self) -> None:
        rows = [
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/repeat.jsonl", retrieved_at=LATER,
                       raw_sha256="c" * 64),
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/first.jsonl", retrieved_at=RETRIEVED_AT),
        ]
        frame = events(rows)
        self.assertEqual(frame["event_id"].to_list(), ["boe:notice:B1"])
        self.assertEqual(frame.height, 1)
        self.assertEqual(frame["ingested_at"][0], RETRIEVED_AT)

    def test_member_index_nullability_keeps_dedup_deterministic(self) -> None:
        rows = [
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/plain.atom",
                       source_member=None, source_member_index=None),
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/plain.atom",
                       source_member="feed.atom", source_member_index=3),
        ]
        frame = events(rows)
        # Same retrieval time and file: the concrete member index is ordered
        # before the null one, and both observations collapse to one event.
        self.assertEqual(frame.height, 1)

    def test_frame_is_sorted_ascending_by_unique_event_id(self) -> None:
        rows = [
            record_row(BOE_PAYLOAD, source="boe", source_file="boe/sample.jsonl"),
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip"),
            record_row(TED_PAYLOAD, source="ted", source_file="ted/sample.jsonl"),
        ]
        frame = events(rows, [tombstone_row("https://example.invalid/999")])
        identifiers = frame["event_id"].to_list()
        self.assertEqual(identifiers, sorted(identifiers))
        self.assertEqual(len(identifiers), len(set(identifiers)))

    def test_materially_different_same_event_id_mappings_fail(self) -> None:
        corrected = {**TED_PAYLOAD, "TI": {"spa": "Título corregido"}}
        with self.assertRaisesRegex(ValueError, "ted:notice:N1.*title"):
            events([
                record_row(TED_PAYLOAD, source="ted", source_file="ted/first.jsonl"),
                record_row(corrected, source="ted", source_file="ted/corrected.jsonl", retrieved_at=LATER),
            ])

    def test_placsp_same_instant_amount_change_fails(self) -> None:
        corrected = {**PLACSP_PAYLOAD, "amount_estimated_overall_raw": "240001.0",
                     "amount_estimated_overall": 240001.0}
        pattern = re.escape(placsp_notice_id(ATOM_ID, "2026-01-08T09:00:00+00:00"))
        with self.assertRaisesRegex(ValueError, pattern):
            events([
                record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/a.zip"),
                record_row(corrected, source="placsp", source_file="placsp/b.zip"),
            ])


class HistoryTests(unittest.TestCase):
    """Revisions, tombstones and corrected snapshots preserve history."""

    def test_placsp_revision_gets_distinct_event_id_and_shared_procedure(self) -> None:
        revision = {**PLACSP_PAYLOAD, "updated": "2026-01-09T12:00:00.000+01:00", "title": "Rectificación"}
        frame = events([
            record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/base.zip"),
            record_row(revision, source="placsp", source_file="placsp/revised.zip", retrieved_at=LATER),
        ])
        self.assertEqual(frame.height, 2)
        self.assertEqual(frame["event_id"].to_list(), [
            placsp_notice_id(ATOM_ID, "2026-01-08T09:00:00+00:00"),
            placsp_notice_id(ATOM_ID, "2026-01-09T11:00:00+00:00"),
        ])
        self.assertEqual(frame["procedure_id"].unique().to_list(), [f"placsp:procedure:{ATOM_ID}"])
        self.assertEqual(frame["source_event_type"].to_list(), ["notice_snapshot", "notice_snapshot"])

    def test_ted_procedure_identifier_shares_procedure_or_stays_null(self) -> None:
        with_id = {**TED_PAYLOAD, "ND": "P1", "procedure-identifier": "2026/0001"}
        frame = events([record_row(with_id, source="ted", source_file="ted/a.jsonl")])
        self.assertEqual(frame["procedure_id"][0], "ted:procedure:2026/0001")
        ambiguous = {**TED_PAYLOAD, "ND": "P2", "procedure-identifier": "2026/0002",
                     "BT-04-notice": "2026/0003"}
        frame = events([record_row(ambiguous, source="ted", source_file="ted/b.jsonl")])
        self.assertIsNone(frame["procedure_id"][0])
        frame = events([record_row(TED_PAYLOAD, source="ted", source_file="ted/c.jsonl")])
        self.assertIsNone(frame["procedure_id"][0])

    def test_ted_procedure_identifier_is_shared_across_notices(self) -> None:
        first = {**TED_PAYLOAD, "ND": "A", "procedure-identifier": "PROC-1"}
        second = {**TED_PAYLOAD, "ND": "B", "procedure-identifier": "PROC-1"}
        frame = events([
            record_row(first, source="ted", source_file="ted/a.jsonl"),
            record_row(second, source="ted", source_file="ted/b.jsonl"),
        ])
        self.assertEqual(frame["procedure_id"].to_list(), ["ted:procedure:PROC-1"] * 2)

    def test_tombstone_is_a_canonical_event_and_does_not_delete(self) -> None:
        ref = "https://example.invalid/999"
        frame = events(
            [record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip")],
            [tombstone_row(ref)],
        )
        rows = {row["event_id"]: row for row in frame.iter_rows(named=True)}
        self.assertEqual(set(rows), {placsp_notice_id(ATOM_ID, "2026-01-08T09:00:00+00:00"),
                                     f"placsp:tombstone:{ref}"})
        tombstone = rows[f"placsp:tombstone:{ref}"]
        self.assertEqual(tombstone["source_event_type"], "tombstone")
        self.assertEqual(tombstone["procedure_id"], f"placsp:procedure:{ref}")
        self.assertEqual(tombstone["source"], "placsp")
        self.assertEqual(tombstone["ingested_at"], RETRIEVED_AT)
        self.assertIsNone(tombstone["source_updated_at"])
        self.assertIsNone(tombstone["title"])
        self.assertEqual(tombstone["cpv_codes"], [])

    def test_repeated_tombstones_collapse_across_retrievals(self) -> None:
        ref = "https://example.invalid/999"
        frame = events([], [
            tombstone_row(ref, source_file="placsp/repeat.zip", retrieved_at=LATER),
            tombstone_row(ref, source_file="placsp/first.zip", retrieved_at=RETRIEVED_AT,
                          source_member="feed.atom", source_member_index=1, record_locator="deleted-entry:2"),
            tombstone_row(ref, source_file="placsp/first.zip", retrieved_at=RETRIEVED_AT),
        ])
        self.assertEqual(frame["event_id"].to_list(), [f"placsp:tombstone:{ref}"])
        self.assertEqual(frame["ingested_at"][0], RETRIEVED_AT)

    def test_tombstone_omitted_by_later_snapshot_keeps_history(self) -> None:
        ref = "https://example.invalid/999"
        # Bronze retains the deletion control from the first artifact even
        # though the later corrected snapshot no longer contains it.
        frame = events(
            [record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/corrected.zip", retrieved_at=LATER)],
            [tombstone_row(ref, source_file="placsp/original.zip", retrieved_at=RETRIEVED_AT)],
        )
        self.assertIn(f"placsp:tombstone:{ref}", frame["event_id"].to_list())

    def test_corrected_raw_snapshot_new_version_marker_makes_new_event(self) -> None:
        correction = {**TED_PAYLOAD, "ND": "N1-C", "TI": {"spa": "Título corregido"}}
        frame = events([
            record_row(TED_PAYLOAD, source="ted", source_file="ted/first.jsonl"),
            record_row(correction, source="ted", source_file="ted/corrected.jsonl", retrieved_at=LATER),
        ])
        self.assertEqual(frame["event_id"].to_list(), ["ted:notice:N1", "ted:notice:N1-C"])
        self.assertEqual(frame["ingested_at"].to_list(), [RETRIEVED_AT, LATER])

    def test_canonically_equivalent_repeat_in_corrected_snapshot_deduplicates(self) -> None:
        # A correction that only touches non-canonical payload fields is a
        # repeated observation of the same source event.
        corrected = {**TED_PAYLOAD, "AC": [{"amount": 1}], "extra": "nuevo campo libre"}
        frame = events([
            record_row(TED_PAYLOAD, source="ted", source_file="ted/first.jsonl"),
            record_row(corrected, source="ted", source_file="ted/corrected.jsonl", retrieved_at=LATER),
        ])
        self.assertEqual(frame["event_id"].to_list(), ["ted:notice:N1"])
        self.assertEqual(frame["ingested_at"].to_list(), [RETRIEVED_AT])


class AmountTests(unittest.TestCase):
    """Exact decimal semantics from published text."""

    def test_ted_amounts_parse_exact_decimals_from_source_text(self) -> None:
        cases = {
            "125000.50": Decimal("125000.50"),
            "98000,00": Decimal("98000.00"),
            "1.234,50": Decimal("1234.50"),
            "45000": Decimal("45000.00"),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                payload = {**TED_PAYLOAD, "estimated-value-lot": text}
                frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                self.assertEqual(frame["estimated_value"][0], expected)

    def test_ted_numeric_zero_beats_later_amount_fields(self) -> None:
        payload = {key: value for key, value in TED_PAYLOAD.items() if key != "currency"}
        payload.update({"estimated-value-lot": 0, "framework-maximum-value-lot": 100, "amount": 200})
        frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
        self.assertEqual(frame["estimated_value"][0], Decimal("0.00"))
        self.assertEqual(str(frame["estimated_value"][0]), "0.00")
        self.assertEqual(frame["currency"][0], "EUR")

    def test_ted_zero_amount_without_explicit_currency_maps_eur(self) -> None:
        for zero in (0, "0"):
            with self.subTest(zero=zero):
                payload = {key: value for key, value in TED_PAYLOAD.items() if key != "currency"}
                payload["estimated-value-lot"] = zero
                frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                self.assertEqual(frame["estimated_value"][0], Decimal("0.00"))
                self.assertEqual(frame["currency"][0], "EUR")

    def test_ted_invalid_primary_amount_does_not_fall_through(self) -> None:
        for invalid in ("abc", "-5", ""):
            with self.subTest(invalid=invalid):
                payload = {key: value for key, value in TED_PAYLOAD.items() if key != "currency"}
                payload.update({"estimated-value-lot": invalid, "framework-maximum-value-lot": 100})
                frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                self.assertIsNone(frame["estimated_value"][0])
                self.assertIsNone(frame["currency"][0])

    def test_ted_invalid_primary_amount_with_explicit_currency_stays_fully_null(self) -> None:
        for invalid in ("abc", "-5", ""):
            with self.subTest(invalid=invalid):
                payload = dict(TED_PAYLOAD)
                payload.update({
                    "estimated-value-lot": invalid,
                    "framework-maximum-value-lot": 100,
                    "currency": "EUR",
                })
                frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                self.assertIsNone(frame["estimated_value"][0])
                self.assertIsNone(frame["currency"][0])

    def test_placsp_invalid_primary_amount_with_explicit_currency_stays_fully_null(self) -> None:
        payload = dict(PLACSP_PAYLOAD)
        payload.update({
            "amount_estimated_overall": None,
            "amount_estimated_overall_raw": "-5.0",
            "amount_estimated_overall_currency": "EUR",
        })
        frame = events([record_row(payload, source="placsp", source_file="placsp/f.zip")])
        self.assertIsNone(frame["estimated_value"][0])
        self.assertIsNone(frame["currency"][0])

    def test_placsp_amounts_prefer_retained_raw_text(self) -> None:
        frame = events([record_row(PLACSP_PAYLOAD, source="placsp", source_file="placsp/f.zip")])
        self.assertEqual(frame["estimated_value"][0], Decimal("240000.00"))
        self.assertEqual(str(frame["estimated_value"][0]), "240000.00")

    def test_legacy_placsp_payload_without_raw_text_uses_exact_float_str(self) -> None:
        legacy = {key: value for key, value in PLACSP_PAYLOAD.items() if not key.endswith("_raw")}
        legacy["amount_estimated_overall"] = 98652.35
        frame = events([record_row(legacy, source="placsp", source_file="placsp/old.zip")])
        self.assertEqual(frame["estimated_value"][0], Decimal("98652.35"))

    def test_excess_scale_fails_explicitly(self) -> None:
        payload = {**TED_PAYLOAD, "estimated-value-lot": "100.005"}
        with self.assertRaisesRegex(ValueError, "more than two fractional digits"):
            events([record_row(payload, source="ted", source_file="ted/a.jsonl")])

    def test_excess_precision_fails_explicitly(self) -> None:
        payload = {**TED_PAYLOAD, "estimated-value-lot": "1234567890123456789"}
        with self.assertRaisesRegex(ValueError, "precision"):
            events([record_row(payload, source="ted", source_file="ted/a.jsonl")])

    def test_malformed_or_negative_optional_amounts_remain_null(self) -> None:
        for text in ("abc", "-5", ""):
            with self.subTest(text=text):
                payload = {**TED_PAYLOAD, "estimated-value-lot": text, "currency": ""}
                frame = events([record_row(payload, source="ted", source_file="ted/a.jsonl")])
                self.assertIsNone(frame["estimated_value"][0])
                self.assertIsNone(frame["currency"][0])

    def test_placsp_currency_comes_from_the_chosen_amount_field(self) -> None:
        payload = dict(PLACSP_PAYLOAD)
        del payload["amount_estimated_overall"], payload["amount_estimated_overall_currency"]
        del payload["amount_estimated_overall_raw"]
        payload["amount_tax_exclusive_currency"] = None
        frame = events([record_row(payload, source="placsp", source_file="placsp/f.zip")])
        self.assertEqual(frame["estimated_value"][0], Decimal("120000.00"))
        self.assertIsNone(frame["currency"][0])

    def test_atom_parser_retains_published_amount_text(self) -> None:
        fixture = Path(__file__).parent / "fixtures/placsp/mini-placsp.atom"
        batch = parse_atom_batch(fixture.read_bytes())
        payload = next(entry["payload"] for entry in batch.entries if entry["payload"]["tender_no"] == "10000101")
        self.assertEqual(payload["amount_estimated_overall_raw"], "240000.0")
        self.assertEqual(payload["amount_tax_exclusive_raw"], "120000.0")
        self.assertEqual(payload["amount_estimated_overall"], 240000.0)

    def test_canonical_amount_from_real_atom_fixture_is_exact(self) -> None:
        fixture = Path(__file__).parent / "fixtures/placsp/mini-placsp.atom"
        batch = parse_atom_batch(fixture.read_bytes())
        rows = [
            record_row(
                {key: value for key, value in entry["payload"].items() if key != "_atom_file"},
                source="placsp", source_file="placsp/mini.zip", source_member="mini.atom",
                source_member_index=1, record_locator=entry["record_locator"],
            )
            for entry in batch.entries
        ]
        frame = events(rows, [tombstone_row("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001")])
        self.assertEqual(frame.height, 3)
        first = frame.row(0, named=True)
        self.assertEqual(first["estimated_value"], Decimal("240000.00"))


class PipelineIntegrationTests(unittest.TestCase):
    """End-to-end guarantees: persistence, manifest and Gold compatibility."""

    def test_pipeline_writes_canonical_silver_and_removes_stale_legacy_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "ted"
            raw.mkdir(parents=True)
            (raw / "sample.jsonl").write_text(json.dumps(TED_PAYLOAD) + "\n", encoding="utf-8")
            evidence_for_fixture(root / "raw", raw / "sample.jsonl", "ted")
            output = root / "data"
            silver = output / "silver"
            silver.mkdir(parents=True)
            (silver / "tenders.jsonl").write_text("{}\n", encoding="utf-8")
            run_pipeline(raw_dir=root / "raw", output_root=output)
            self.assertFalse((silver / "tenders.jsonl").exists())
            self.assertTrue((silver / "procurement_events.parquet").exists())
            frame = read_parquet(silver / "procurement_events.parquet")
            self.assertEqual(frame.schema, PROCUREMENT_EVENT_SCHEMA)
            self.assertEqual(frame["event_id"].to_list(), ["ted:notice:N1"])
            # Reruns do not recreate the legacy artifact.
            run_pipeline(raw_dir=root / "raw", output_root=output)
            self.assertFalse((silver / "tenders.jsonl").exists())
            self.assertTrue(frame.equals(read_parquet(silver / "procurement_events.parquet")))

    def test_failed_canonical_build_still_removes_both_stale_silver_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "ted"
            raw.mkdir(parents=True)
            # First establish a pre-existing canonical Parquet via a
            # successful run; a later material collision must not leave it
            # looking current.
            (raw / "sample.jsonl").write_text(json.dumps(TED_PAYLOAD) + "\n", encoding="utf-8")
            evidence_for_fixture(root / "raw", raw / "sample.jsonl", "ted")
            output = root / "data"
            run_pipeline(raw_dir=root / "raw", output_root=output)
            silver = output / "silver"
            self.assertTrue((silver / "procurement_events.parquet").exists())
            # Two materially different payloads under the same notice number
            # force the explicit same-event-ID canonical collision.
            corrected = {**TED_PAYLOAD, "TI": {"spa": "Título corregido"}}
            (raw / "sample.jsonl").write_text(
                json.dumps(TED_PAYLOAD) + "\n" + json.dumps(corrected) + "\n", encoding="utf-8"
            )
            evidence_for_fixture(root / "raw", raw / "sample.jsonl", "ted")
            (silver / "tenders.jsonl").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ted:notice:N1"):
                run_pipeline(raw_dir=root / "raw", output_root=output)
            self.assertFalse((silver / "tenders.jsonl").exists())
            self.assertFalse((silver / "procurement_events.parquet").exists())

    def test_manifest_counts_and_source_counts_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "ted"
            raw.mkdir(parents=True)
            (raw / "sample.jsonl").write_text(json.dumps(TED_PAYLOAD) + "\n", encoding="utf-8")
            evidence_for_fixture(root / "raw", raw / "sample.jsonl", "ted")
            result = run_pipeline(raw_dir=root / "raw", output_root=root / "data")
            manifest = result["manifest"]
            self.assertEqual(set(manifest["counts"]), {
                "bronze_records", "silver_procurement_events", "legacy_current_state_records",
                "gold_opportunities", "gold_canonical_opportunities",
            })
            self.assertNotIn("source_counts", manifest)
            self.assertEqual(manifest["silver_source_counts"], {"ted": 1})
            self.assertEqual(manifest["legacy_source_counts"], {"ted": 1})
            self.assertEqual(manifest["counts"]["bronze_records"], 1)
            self.assertEqual(manifest["counts"]["silver_procurement_events"], 1)

    def test_legacy_gold_collapses_revisions_while_canonical_silver_retains_them(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "placsp"
            raw.mkdir(parents=True)
            entry = f'''<feed xmlns="http://www.w3.org/2005/Atom"
              xmlns:at="http://purl.org/atompub/tombstones/1.0">
              <entry><id>{ATOM_ID}</id><title>Servicio cloud</title>
              <updated>2026-01-08T10:00:00.000+01:00</updated></entry>
              <entry><id>{ATOM_ID}</id><title>Servicio cloud rectificado</title>
              <updated>2026-01-10T10:00:00.000+01:00</updated></entry>
              <at:deleted-entry ref="https://example.invalid/999"/>
            </feed>'''.encode()
            path = raw / "placsp-202601.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("feed.atom", entry)
            evidence_for_fixture(root / "raw", path, "placsp")
            result = run_pipeline(raw_dir=root / "raw", output_root=root / "data")
            counts = result["manifest"]["counts"]
            # Two notice revisions and one tombstone remain canonical history.
            self.assertEqual(counts["silver_procurement_events"], 3)
            # The legacy current state folds the revision and drops nothing else.
            self.assertEqual(counts["legacy_current_state_records"], 1)
            self.assertEqual(counts["gold_opportunities"], 1)
            self.assertEqual(result["manifest"]["silver_source_counts"], {"placsp": 3})
            self.assertEqual(result["manifest"]["legacy_source_counts"], {"placsp": 1})
            gold = root / "data" / "gold"
            self.assertTrue((gold / "opportunities.jsonl").exists())
            self.assertTrue((gold / "opportunities.csv").exists())
            frame = read_parquet(root / "data" / "silver" / "procurement_events.parquet")
            self.assertEqual(frame["event_id"].to_list(), [
                placsp_notice_id(ATOM_ID, "2026-01-08T09:00:00+00:00"),
                placsp_notice_id(ATOM_ID, "2026-01-10T09:00:00+00:00"),
                "placsp:tombstone:https://example.invalid/999",
            ])


if __name__ == "__main__":
    unittest.main()
