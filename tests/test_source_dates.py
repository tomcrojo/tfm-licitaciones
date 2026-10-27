"""Declared CODICE publication dates through both Silver routes."""
from __future__ import annotations

import json
import unittest
from datetime import date, datetime, timezone
from unittest import mock

from tfm_licitaciones import silver
from tfm_licitaciones.atom import parse_atom_batch
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.silver import build_procurement_events


def _payload(dates: list[str], notice_type: str = "DOC_CN") -> dict:
    references = "".join(
        f"<cpe:AdditionalPublicationDocumentReference><cbc:IssueDate>{day}</cbc:IssueDate>"
        "</cpe:AdditionalPublicationDocumentReference>" for day in dates
    )
    xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2"
      xmlns:cpe="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2"
      xmlns:cpex="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2">
      <entry><id>https://example.invalid/123</id><title>Notice</title>
      <updated>2026-03-01T10:00:00Z</updated><cpe:ValidNoticeInfo>
      <cpex:NoticeTypeCode>{notice_type}</cpex:NoticeTypeCode>
      <cpe:AdditionalPublicationStatus>{references}</cpe:AdditionalPublicationStatus>
      </cpe:ValidNoticeInfo></entry></feed>'''
    return parse_atom_batch(xml).entries[0]["payload"]


def _row(payload: dict, locator: str = "entry:1") -> dict:
    return dict(source="placsp", source_file="placsp/a.atom", source_member=None,
                source_member_index=None, record_locator=locator, source_record_id=None,
                raw_sha256="a" * 64, raw_retrieved_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
                payload_json=json.dumps(payload, sort_keys=True))


class SourceDateTests(unittest.TestCase):
    def test_earliest_declared_date_ignores_invalid_dates_and_xml_order(self):
        for days in (["2026-02-05", "invalid", "2026-01-10"],
                     ["2026-01-10", "2026-02-05", "2026-02-30"]):
            payload = _payload(days)
            self.assertEqual(payload["publication_date"], "2026-01-10")
            events = build_procurement_events(bronze_frame([_row(payload)]), tombstone_frame([]))
            self.assertEqual(events["publication_date"][0], date(2026, 1, 10))
            self.assertEqual(events["source_updated_at"][0].month, 3)

    def test_other_notice_types_and_missing_or_invalid_dates_stay_null(self):
        for payload in (_payload(["2026-01-10"], "DOC_CAN"), _payload([]), _payload(["bad"])):
            self.assertIsNone(payload["publication_date"])
            events = build_procurement_events(bronze_frame([_row(payload)]), tombstone_frame([]))
            self.assertIsNone(events["publication_date"][0])

    def test_fallback_route_preserves_declared_date(self):
        payload = _payload(["2026-01-10"])
        payload["title"] = {"spa": "Título estructurado"}
        with mock.patch.object(silver, "build_procurement_events_reference",
                               wraps=silver.build_procurement_events_reference) as reference:
            events = build_procurement_events(bronze_frame([_row(payload)]), tombstone_frame([]))
        reference.assert_called_once()
        self.assertEqual(events["publication_date"][0], date(2026, 1, 10))

    def test_date_conflicts_cannot_hide_behind_duplicate_event_collapse(self):
        first = _payload(["2026-01-10"])
        for second_date in ("2026-01-11", None):
            second = {**first, "publication_date": second_date}
            with self.assertRaisesRegex(ValueError, "differing fields: publication_date"):
                build_procurement_events(
                    bronze_frame([_row(first), _row(second, "entry:2")]), tombstone_frame([]))
