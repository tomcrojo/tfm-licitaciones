"""Regression coverage for source-dated PLACSP tombstones."""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone

from tfm_licitaciones.atom import parse_atom_batch
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.silver import build_procurement_events

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001"
RETRIEVED = datetime(2026, 1, 11, 8, 0, tzinfo=UTC)


def _row(
    source_deleted_at: datetime | None,
    *,
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
        "source_record_id": REF,
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


def _event_id(when: datetime) -> str:
    micros = (when.astimezone(UTC) - EPOCH) // timedelta(microseconds=1)
    return f"placsp:tombstone:{REF}@deleted-us:{micros}"


class TombstoneSourceTimeTests(unittest.TestCase):
    def test_atom_when_is_retained_and_normalized_to_utc(self) -> None:
        xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="{REF}" when="2026-01-10T18:01:39.351+02:00"/>
        </feed>'''
        batch = parse_atom_batch(xml)
        self.assertEqual(len(batch.tombstone_rows), 1)
        self.assertEqual(
            batch.tombstone_rows[0]["source_deleted_at"],
            datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC),
        )

    def test_missing_or_unusable_when_stays_explicitly_undated(self) -> None:
        for attribute in ("", ' when="not-a-time"', ' when="2026-01-10T18:01:39"'):
            with self.subTest(attribute=attribute):
                xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
                    xmlns:at="http://purl.org/atompub/tombstones/1.0">
                  <at:deleted-entry ref="{REF}"{attribute}/>
                </feed>'''
                batch = parse_atom_batch(xml)
                self.assertIsNone(batch.tombstone_rows[0]["source_deleted_at"])

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
        events = build_procurement_events(
            bronze_frame([_record_row(ted)]),
            tombstone_frame([_row(when)]),
        )
        rows = {row["event_id"]: row for row in events.iter_rows(named=True)}
        tombstone = rows[_event_id(when)]
        self.assertEqual(tombstone["procedure_id"], f"placsp:procedure:{REF}")
        self.assertEqual(tombstone["source_updated_at"], when)

    def test_legacy_undated_tombstone_keeps_historical_identity(self) -> None:
        events = build_procurement_events(bronze_frame([]), tombstone_frame([_row(None)]))
        self.assertEqual(events.height, 1)
        self.assertEqual(events["event_id"][0], f"placsp:tombstone:{REF}")
        self.assertEqual(events["procedure_id"][0], f"placsp:procedure:{REF}")
        self.assertIsNone(events["source_updated_at"][0])


if __name__ == "__main__":
    unittest.main()
