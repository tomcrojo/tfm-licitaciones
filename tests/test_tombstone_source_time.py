"""Regression coverage for source-dated PLACSP tombstones."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from raw_fixtures import evidence_for_fixture
from tfm_licitaciones.atom import parse_atom_batch
from tfm_licitaciones.bronze import bronze_frame, load_raw_records, tombstone_frame
from tfm_licitaciones.silver import build_procurement_events

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001"
RETRIEVED = datetime(2026, 1, 11, 8, 0, tzinfo=UTC)


def _row(
    source_deleted_at: datetime | None,
    *,
    ref: str = REF,
    retrieved_at: datetime = RETRIEVED,
    source_file: str = "placsp/202601.zip",
    locator: str = "deleted-entry:1",
) -> dict:
    return {
        "source": "placsp",
        "source_file": source_file,
        "source_member": "feed.atom",
        "source_member_index": 1,
        "record_locator": locator,
        "source_record_id": ref,
        "raw_sha256": "a" * 64,
        "raw_retrieved_at": retrieved_at,
        "source_deleted_at": source_deleted_at,
    }


def _record_row(payload: dict, *, source: str = "ted") -> dict:
    return {
        "source": source,
        "source_file": f"{source}/sample.jsonl",
        "source_member": None,
        "source_member_index": None,
        "record_locator": "line:1",
        "source_record_id": None,
        "raw_sha256": "b" * 64,
        "raw_retrieved_at": RETRIEVED,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def _event_id(when: datetime, *, ref: str = REF) -> str:
    micros = (when.astimezone(UTC) - EPOCH) // timedelta(microseconds=1)
    return f"placsp:tombstone-dated:{micros}:{ref}"


class TombstoneSourceTimeTests(unittest.TestCase):
    def test_atom_when_is_retained_and_normalized_to_utc(self) -> None:
        xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="{REF}" when="2026-01-10T18:01:39.351+02:00"/>
        </feed>'''
        batch = parse_atom_batch(xml)
        self.assertEqual(len(batch.tombstone_rows), 1)
        self.assertEqual(batch.rejections, [])
        self.assertEqual(
            batch.tombstone_rows[0]["source_deleted_at"],
            datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC),
        )

    def test_lowercase_z_is_accepted_as_utc_source_time(self) -> None:
        xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="{REF}" when="2026-01-10T18:01:39.351z"/>
        </feed>'''
        batch = parse_atom_batch(xml)
        self.assertEqual(batch.rejections, [])
        self.assertEqual(
            batch.tombstone_rows[0]["source_deleted_at"],
            datetime(2026, 1, 10, 18, 1, 39, 351000, tzinfo=UTC),
        )

    def test_missing_when_is_legitimate_undated_evidence(self) -> None:
        xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="{REF}"/>
        </feed>'''
        batch = parse_atom_batch(xml)
        self.assertIsNone(batch.tombstone_rows[0]["source_deleted_at"])
        self.assertEqual(batch.rejections, [])

    def test_published_but_unusable_when_is_retained_and_observable(self) -> None:
        for value in ("", "not-a-time", "2026-01-10T18:01:39"):
            with self.subTest(value=value):
                xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
                    xmlns:at="http://purl.org/atompub/tombstones/1.0">
                  <at:deleted-entry ref="{REF}" when="{value}"/>
                </feed>'''
                batch = parse_atom_batch(xml)
                self.assertEqual(len(batch.tombstone_rows), 1)
                self.assertIsNone(batch.tombstone_rows[0]["source_deleted_at"])
                self.assertEqual(len(batch.rejections), 1)
                rejection = batch.rejections[0]
                self.assertEqual(rejection["source_record_id"], REF)
                self.assertEqual(rejection["rejection_reason"], "invalid_tombstone_when")
                self.assertEqual(rejection["rejection_scope"], "control")

    def test_invalid_when_reaches_ingestion_control_error_metrics(self) -> None:
        xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="{REF}" when="not-a-time"/>
        </feed>'''
        with tempfile.TemporaryDirectory() as temporary:
            raw = Path(temporary) / "raw"
            raw.mkdir()
            path = raw / "control.atom"
            path.write_text(xml, encoding="utf-8")
            evidence_for_fixture(raw, path, "placsp")
            loaded = load_raw_records(raw)
        self.assertEqual(loaded["ingestion"]["control_errors"], 1)
        self.assertFalse(loaded["ingestion"]["passed"])
        self.assertEqual(loaded["ingestion"]["tombstones"], 1)
        self.assertEqual(loaded["rejections"][0]["rejection_reason"], "invalid_tombstone_when")
        self.assertEqual(loaded["rejections"][0]["source_record_id"], REF)

    def test_two_delete_cycles_keep_distinct_events_and_shared_procedure(self) -> None:
        first = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        second = datetime(2026, 1, 12, 9, 30, tzinfo=UTC)
        events = build_procurement_events(
            bronze_frame([]),
            tombstone_frame([_row(first), _row(second, locator="deleted-entry:2")]),
        )
        self.assertEqual(events.height, 2)
        rows = {row["event_id"]: row for row in events.iter_rows(named=True)}
        self.assertEqual(set(rows), {_event_id(first), _event_id(second)})
        for when in (first, second):
            row = rows[_event_id(when)]
            self.assertEqual(row["procedure_id"], f"placsp:procedure:{REF}")
            self.assertEqual(row["source_updated_at"], when)
            self.assertEqual(row["source_event_type"], "tombstone")

    def test_old_marker_text_in_ref_cannot_collide_with_dated_identity(self) -> None:
        when = datetime(1970, 1, 1, 0, 0, 0, 123, tzinfo=UTC)
        micros = (when - EPOCH) // timedelta(microseconds=1)
        base_ref = "https://example.invalid/X"
        crafted_ref = f"{base_ref}@deleted-us:{micros}"
        events = build_procurement_events(
            bronze_frame([]),
            tombstone_frame([
                _row(when, ref=base_ref),
                _row(None, ref=crafted_ref, locator="deleted-entry:2"),
            ]),
        )
        self.assertEqual(events.height, 2)
        self.assertEqual(
            set(events["event_id"].to_list()),
            {_event_id(when, ref=base_ref), f"placsp:tombstone:{crafted_ref}"},
        )

    def test_same_source_delete_observed_twice_still_deduplicates_by_provenance(self) -> None:
        when = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        early = RETRIEVED
        late = RETRIEVED + timedelta(days=2)
        events = build_procurement_events(
            bronze_frame([]),
            tombstone_frame([
                _row(when, retrieved_at=late, source_file="placsp/late.zip"),
                _row(when, retrieved_at=early, source_file="placsp/early.zip"),
            ]),
        )
        self.assertEqual(events.height, 1)
        self.assertEqual(events["event_id"][0], _event_id(when))
        self.assertEqual(events["source_updated_at"][0], when)
        self.assertEqual(events["ingested_at"][0], early)

    def test_dated_tombstone_executes_native_route_when_original_row_is_eligible(self) -> None:
        when = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        with self.assertLogs("tfm_licitaciones.silver", level="INFO") as captured:
            events = build_procurement_events(bronze_frame([]), tombstone_frame([_row(when)]))
        route_records = [record for record in captured.records if record.getMessage() == "silver_batch_route"]
        self.assertEqual(len(route_records), 1)
        self.assertEqual(route_records[0].route, "native")
        self.assertEqual(events["event_id"][0], _event_id(when))

    def test_source_time_survives_reference_fallback_route(self) -> None:
        when = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        # Non-ASCII TED country text is intentionally outside the native
        # eligibility domain but remains valid for the frozen reference,
        # forcing this batch through the reference-fallback route.
        ted = {
            "_source": "ted",
            "ND": "N-FALLBACK",
            "TI": "Fallback control",
            "CY": "España",
        }
        with self.assertLogs("tfm_licitaciones.silver", level="INFO") as captured:
            events = build_procurement_events(
                bronze_frame([_record_row(ted)]),
                tombstone_frame([_row(when)]),
            )
        route_records = [record for record in captured.records if record.getMessage() == "silver_batch_route"]
        self.assertEqual(len(route_records), 1)
        self.assertEqual(route_records[0].route, "reference-fallback")
        rows = {row["event_id"]: row for row in events.iter_rows(named=True)}
        tombstone = rows[_event_id(when)]
        self.assertEqual(tombstone["procedure_id"], f"placsp:procedure:{REF}")
        self.assertEqual(tombstone["source_updated_at"], when)

    def test_invalid_original_ref_is_not_hidden_by_source_time_decoration(self) -> None:
        when = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        with self.assertRaisesRegex(ValueError, "PLACSP tombstone without a full ref"):
            build_procurement_events(
                bronze_frame([]),
                tombstone_frame([_row(when, ref="   ")]),
            )

    def test_legacy_undated_tombstone_keeps_historical_identity(self) -> None:
        events = build_procurement_events(bronze_frame([]), tombstone_frame([_row(None)]))
        self.assertEqual(events.height, 1)
        self.assertEqual(events["event_id"][0], f"placsp:tombstone:{REF}")
        self.assertEqual(events["procedure_id"][0], f"placsp:procedure:{REF}")
        self.assertIsNone(events["source_updated_at"][0])


if __name__ == "__main__":
    unittest.main()
