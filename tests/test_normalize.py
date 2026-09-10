"""Tests for source adapters and the common silver contract."""

from __future__ import annotations

import unittest

from tfm_licitaciones.atom import iter_placsp_zip
from tfm_licitaciones.normalize import _parse_amount, normalize_boe, normalize_placsp, normalize_record, normalize_ted
from pathlib import Path


class NormalizeTests(unittest.TestCase):
    """Verify localized source structures are normalized consistently."""

    def test_ted_localized_fields_and_date(self) -> None:
        record = normalize_ted(
            {
                "_source": "ted",
                "ND": "1",
                "PD": "2024-01-02Z",
                "TI": {"spa": ["Servicio cloud"]},
                "buyer-name": {"spa": ["ORGANISMO"]},
                "framework-maximum-value-lot": "1.234,50",
            }
        )
        self.assertEqual(record.title, "Servicio cloud")
        self.assertEqual(record.buyer, "ORGANISMO")
        self.assertEqual(record.published_date.isoformat(), "2024-01-02")
        self.assertEqual(record.amount, 1234.5)

    def test_ted_prefers_estimated_value_and_links_url(self) -> None:
        record = normalize_ted(
            {
                "_source": "ted",
                "ND": "2",
                "PD": "2026-01-05+01:00",
                "TI": {"spa": "Plataforma de datos"},
                "estimated-value-lot": "98000.00",
                "framework-maximum-value-lot": "150000.00",
                "PC": ["72000000", "48000000"],
                "links": {"html": {"ENG": "https://ted.europa.eu/en/notice/-/detail/2-2026"}},
            }
        )
        self.assertEqual(record.amount, 98000.0)
        self.assertEqual(record.url, "https://ted.europa.eu/en/notice/-/detail/2-2026")
        self.assertEqual(record.raw["PC"], ["72000000", "48000000"])
        self.assertEqual(record.cpv_main, "72000000")
        self.assertEqual(record.currency, "EUR")

    def test_ted_amount_without_currency_is_eur(self) -> None:
        record = normalize_ted({"_source": "ted", "ND": "3", "TI": {"spa": "X"}, "estimated-value-lot": "100"})
        self.assertEqual(record.currency, "EUR")
        self.assertIsNone(normalize_ted({"_source": "ted", "ND": "4", "TI": {"spa": "X"}}).currency)

    def test_placsp_codice_payload_maps_all_fields(self) -> None:
        payloads, tombstones, _ = iter_placsp_zip(Path(__file__).parent / "fixtures" / "raw" / "placsp" / "placsp-202601.zip")
        record = normalize_record(next(item for item in payloads if item["tender_no"] == "10000101"))
        self.assertEqual(record.source, "placsp")
        self.assertEqual(record.tender_id, "10000101")
        self.assertEqual(record.title, "Servicio de migración a plataforma cloud y soporte de aplicaciones")
        self.assertEqual(record.buyer, "Centro de Sistemas y Tecnologías de la Información")
        self.assertEqual(record.buyer_id, "E00000001")
        self.assertEqual(record.cpv_main, "72415000")
        self.assertEqual(record.amount, 120000.0)
        self.assertEqual(record.currency, "EUR")
        self.assertEqual(record.country, "ES")
        self.assertEqual(record.region, "Madrid")
        self.assertEqual(record.status, "EV")
        self.assertEqual(record.published_date.isoformat(), "2026-01-08")

    def test_placsp_tombstoned_record_is_identifiable(self) -> None:
        payloads, tombstones, _ = iter_placsp_zip(Path(__file__).parent / "fixtures" / "raw" / "placsp" / "placsp-202601.zip")
        withdrawn = {ref.rsplit("/", 1)[-1] for ref in tombstones}
        self.assertIn("99900001", withdrawn)
        self.assertFalse(withdrawn & {item["tender_no"] for item in payloads})

    def test_boe_record_uses_item_identifier(self) -> None:
        record = normalize_boe({"_source": "boe", "item_id": "B1", "title": "Anuncio"})
        self.assertEqual(record.tender_id, "B1")
        self.assertEqual(record.source, "boe")

    def test_amount_parser_handles_decimal_conventions(self) -> None:
        self.assertEqual(_parse_amount("125000.50"), 125000.5)
        self.assertEqual(_parse_amount("125.000,50"), 125000.5)
        self.assertEqual(_parse_amount("125000,50"), 125000.5)


if __name__ == "__main__":
    unittest.main()
