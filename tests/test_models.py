"""Tests for the canonical Silver procurement-event contract."""

from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from tfm_licitaciones.io import read_parquet, write_parquet
from tfm_licitaciones.models import (
    PROCUREMENT_EVENT_SCHEMA,
    ProcurementEvent,
    TenderRecord,
    procurement_event_from_tender,
    procurement_events_frame,
)


class ProcurementEventTests(unittest.TestCase):
    """Verify the model boundary without migrating the existing pipeline."""

    def test_parquet_round_trip_preserves_canonical_schema_and_values(self) -> None:
        event = ProcurementEvent(
            event_id="placsp:event:123:2026-01-08T09:00:00Z",
            procedure_id="placsp:procedure:123",
            source="placsp",
            source_event_type="notice_revision",
            buyer_id="E00000001",
            buyer_name="Ayuntamiento de Ejemplo",
            title="Servicio de mantenimiento",
            description="Mantenimiento preventivo y correctivo",
            cpv_codes=("50000000", "50300000"),
            estimated_value=Decimal("120000.00"),
            awarded_value=Decimal("118500.25"),
            currency="EUR",
            publication_date=date(2026, 1, 7),
            source_updated_at=datetime(2026, 1, 8, 10, tzinfo=timezone(timedelta(hours=1))),
            deadline=datetime(2026, 2, 8, 23, 59, tzinfo=timezone.utc),
            status="open",
            nuts_code="ES300",
            country="ES",
            source_url="https://example.invalid/procedure/123",
            ingested_at=datetime(2026, 1, 8, 10, 5, tzinfo=timezone.utc),
        )

        frame = procurement_events_frame([event])
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "procurement_events.parquet"
            self.assertEqual(write_parquet(path, frame), 1)
            result = read_parquet(path)

        self.assertEqual(result.schema, PROCUREMENT_EVENT_SCHEMA)
        self.assertTrue(result.equals(frame))
        self.assertEqual(result["cpv_codes"][0].to_list(), ["50000000", "50300000"])
        self.assertEqual(result["estimated_value"][0], Decimal("120000.00"))
        self.assertEqual(result["source_updated_at"][0], datetime(2026, 1, 8, 9, tzinfo=timezone.utc))

    def test_empty_batch_keeps_the_full_schema(self) -> None:
        frame = procurement_events_frame([])

        self.assertEqual(frame.height, 0)
        self.assertEqual(frame.schema, PROCUREMENT_EVENT_SCHEMA)

    def test_legacy_conversion_requires_temporal_decisions(self) -> None:
        tender = TenderRecord(
            tender_id="123",
            source="placsp",
            title="Servicio de mantenimiento",
            summary="Descripción",
            buyer="Ayuntamiento de Ejemplo",
            published_date=date(2026, 1, 8),
            amount=120000.0,
            currency="EUR",
            cpv_main="50000000",
            buyer_id="E00000001",
            region="Madrid",
            status="EV",
        )
        source_updated_at = datetime(2026, 1, 8, 9, tzinfo=timezone.utc)

        event = procurement_event_from_tender(
            tender,
            event_id="placsp:event:123:2026-01-08T09:00:00Z",
            procedure_id="placsp:procedure:123",
            source_event_type="notice_snapshot",
            publication_date=None,
            source_updated_at=source_updated_at,
            deadline=None,
            ingested_at=datetime(2026, 1, 8, 9, 5, tzinfo=timezone.utc),
        )

        self.assertIsNone(event.publication_date)
        self.assertEqual(event.source_updated_at, source_updated_at)
        self.assertEqual(event.estimated_value, Decimal("120000.0"))
        self.assertEqual(event.cpv_codes, ("50000000",))
        self.assertIsNone(event.nuts_code)

    def test_naive_timestamps_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "ingested_at must include a timezone"):
            ProcurementEvent(
                event_id="ted:event:1",
                procedure_id=None,
                source="ted",
                source_event_type="notice",
                ingested_at=datetime(2026, 1, 1),
            )

    def test_duplicate_cpv_codes_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "cpv_codes must not contain duplicates"):
            ProcurementEvent(
                event_id="ted:event:1",
                procedure_id="ted:procedure:1",
                source="ted",
                source_event_type="notice",
                cpv_codes=("72000000", "72000000"),
                ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
