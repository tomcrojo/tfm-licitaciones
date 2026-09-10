"""Tests for the OpenPLACSP Atom/CODICE adapters."""

from __future__ import annotations

import unittest
from pathlib import Path

from tfm_licitaciones.atom import iter_placsp_zip, parse_atom_file

FIXTURES = Path(__file__).parent / "fixtures"


class AtomTests(unittest.TestCase):
    """Ensure CODICE entries map to the same silver contract as JSON sources."""

    def test_parse_atom_entry(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom" xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2" xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2" xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2" xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2">
          <entry>
            <id>https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/12345</id>
            <title>Servicio de datos</title>
            <summary>Analítica pública</summary>
            <updated>2024-02-01T09:00:00Z</updated>
            <link href="https://example.invalid/placsp/1" />
          </entry>
        </feed>"""
        path = FIXTURES / "placsp" / "single-entry.atom"
        path.write_text(xml, encoding="utf-8")
        try:
            records = parse_atom_file(path)
        finally:
            path.unlink()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].source, "placsp")
        self.assertEqual(records[0].tender_id, "12345")
        self.assertEqual(records[0].published_date.isoformat(), "2024-02-01")

    def test_placsp_zip_payloads_and_tombstones(self) -> None:
        payloads, tombstones, atoms = iter_placsp_zip(FIXTURES / "raw" / "placsp" / "placsp-202601.zip")
        self.assertEqual(atoms, 1)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(
            tombstones,
            {"https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001"},
        )
        cloud = next(item for item in payloads if item["tender_no"] == "10000101")
        self.assertEqual(cloud["buyer_dir3"], "E00000001")
        self.assertEqual(cloud["cpv"], ["72415000"])
        self.assertEqual(cloud["amount_tax_exclusive"], 120000.0)
        self.assertEqual(cloud["amount_tax_exclusive_currency"], "EUR")
        self.assertEqual(cloud["status_code"], "EV")
        self.assertEqual(cloud["nuts_code"], "ES300")
        self.assertEqual(cloud["region"], "Madrid")
        self.assertEqual(cloud["contract_folder_id"], "S1/2026")


if __name__ == "__main__":
    unittest.main()
