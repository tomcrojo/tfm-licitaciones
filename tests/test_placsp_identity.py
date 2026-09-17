from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.placsp_identity import (
    PLACSP_IDENTITY_RULE,
    PLACSP_PROCEDURE_PREFIX,
    resolve_placsp_identity,
)
from tfm_licitaciones.silver import build_procurement_events


REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
RETRIEVED_AT = datetime(2026, 1, 11, 9, 0, tzinfo=timezone.utc)


def _record_row() -> dict[str, object]:
    payload = {
        "_source": "placsp",
        "atom_id": REF,
        "title": "Servicio de ejemplo",
        "updated": "2026-01-10T10:00:00+00:00",
        "cpv": ["72415000"],
    }
    return {
        "source": "placsp",
        "source_file": "placsp/example.atom",
        "source_member": None,
        "source_member_index": None,
        "record_locator": "entry:1",
        "source_record_id": REF,
        "raw_sha256": "a" * 64,
        "raw_retrieved_at": RETRIEVED_AT,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def _tombstone_row(*, dated: bool) -> dict[str, object]:
    return {
        "source": "placsp",
        "source_file": "placsp/example.atom",
        "source_member": None,
        "source_member_index": None,
        "record_locator": "deleted-entry:1",
        "source_record_id": REF,
        "raw_sha256": "a" * 64,
        "raw_retrieved_at": RETRIEVED_AT,
        "source_deleted_at": (
            datetime(2026, 1, 10, 18, 1, 39, 351000, tzinfo=timezone.utc)
            if dated
            else None
        ),
    }


class PlacspIdentityRuleTests(unittest.TestCase):
    def test_contract_is_exact_published_uri_equality(self) -> None:
        self.assertEqual(PLACSP_IDENTITY_RULE, "exact-rfc6721-ref-equals-atom-id")
        procedure_id = f"{PLACSP_PROCEDURE_PREFIX}{REF}"

        notice = resolve_placsp_identity(
            event_id=f"placsp:notice:{REF}@2026-01-10T10:00:00+00:00",
            procedure_id=procedure_id,
            source_event_type="notice_snapshot",
        )
        tombstone = resolve_placsp_identity(
            event_id=f"placsp:tombstone:{REF}",
            procedure_id=procedure_id,
            source_event_type="tombstone",
        )

        self.assertEqual(notice.status, "resolved")
        self.assertEqual(tombstone.status, "resolved")
        self.assertEqual(notice.published_ref, REF)
        self.assertEqual(tombstone.published_ref, REF)
        self.assertEqual(notice.procedure_id, tombstone.procedure_id)

    def test_dated_tombstone_identity_uses_same_published_ref(self) -> None:
        procedure_id = f"{PLACSP_PROCEDURE_PREFIX}{REF}"
        result = resolve_placsp_identity(
            event_id=f"placsp:tombstone-dated:1768068099351000:{REF}",
            procedure_id=procedure_id,
            source_event_type="tombstone",
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.published_ref, REF)

    def test_mismatch_is_observable_not_normalized(self) -> None:
        result = resolve_placsp_identity(
            event_id=f"placsp:tombstone:{REF}/different",
            procedure_id=f"{PLACSP_PROCEDURE_PREFIX}{REF}",
            source_event_type="tombstone",
        )
        self.assertEqual(result.status, "unresolved")
        self.assertEqual(result.reason, "event_procedure_identity_mismatch")

    def test_missing_or_future_identity_stays_unresolved(self) -> None:
        missing = resolve_placsp_identity(
            event_id="placsp:notice:x@undated",
            procedure_id=None,
            source_event_type="notice_snapshot",
        )
        future = resolve_placsp_identity(
            event_id=f"placsp:award:{REF}",
            procedure_id=f"{PLACSP_PROCEDURE_PREFIX}{REF}",
            source_event_type="award",
        )
        self.assertEqual(missing.status, "unresolved")
        self.assertEqual(missing.reason, "missing_or_invalid_procedure_id")
        self.assertEqual(future.status, "unresolved")
        self.assertEqual(future.reason, "unsupported_placsp_event_type")

    def test_canonical_silver_notice_and_dated_tombstone_share_procedure_identity(self) -> None:
        events = build_procurement_events(
            bronze_frame([_record_row()]),
            tombstone_frame([_tombstone_row(dated=True)]),
        )
        notice = events.filter(events["source_event_type"] == "notice_snapshot").row(0, named=True)
        tombstone = events.filter(events["source_event_type"] == "tombstone").row(0, named=True)

        expected = f"{PLACSP_PROCEDURE_PREFIX}{REF}"
        self.assertEqual(notice["procedure_id"], expected)
        self.assertEqual(tombstone["procedure_id"], expected)
        self.assertIsNotNone(tombstone["source_updated_at"])

        for row in (notice, tombstone):
            resolution = resolve_placsp_identity(
                event_id=row["event_id"],
                procedure_id=row["procedure_id"],
                source_event_type=row["source_event_type"],
            )
            self.assertEqual(resolution.status, "resolved")
            self.assertEqual(resolution.published_ref, REF)

    def test_undated_tombstone_keeps_identity_even_when_temporal_order_is_unknown(self) -> None:
        events = build_procurement_events(
            bronze_frame([_record_row()]),
            tombstone_frame([_tombstone_row(dated=False)]),
        )
        tombstone = events.filter(events["source_event_type"] == "tombstone").row(0, named=True)
        resolution = resolve_placsp_identity(
            event_id=tombstone["event_id"],
            procedure_id=tombstone["procedure_id"],
            source_event_type=tombstone["source_event_type"],
        )
        self.assertEqual(resolution.status, "resolved")
        self.assertIsNone(tombstone["source_updated_at"])


if __name__ == "__main__":
    unittest.main()
