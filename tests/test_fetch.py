"""Tests for live-source query and BOE flattening helpers."""

from __future__ import annotations

import unittest
from datetime import date

from tfm_licitaciones.fetch import build_ted_query, flatten_boe_sumario, split_window


class FetchTests(unittest.TestCase):
    """Exercise deterministic helpers without making network calls."""

    def test_ted_query_contains_country_terms_and_dates(self) -> None:
        query = build_ted_query("ESP", ["cloud", "big data"], date(2024, 1, 1), date(2024, 1, 31))
        self.assertIn("CY=ESP", query)
        self.assertIn('FT~"cloud"', query)
        self.assertIn("PD>=20240101", query)
        self.assertIn("PD<=20240131", query)

    def test_window_is_contiguous_and_bounded(self) -> None:
        self.assertEqual(
            split_window(date(2024, 1, 1), date(2024, 1, 5), 3),
            [(date(2024, 1, 1), date(2024, 1, 3)), (date(2024, 1, 4), date(2024, 1, 5))],
        )

    def test_boe_flatten_accepts_nested_payload(self) -> None:
        payload = {
            "data": {
                "sumario": {
                    "diario": [{
                        "seccion": [{
                            "nombre": "3. ANUNCIOS",
                            "departamento": {
                                "nombre": "Ministerio de Hacienda",
                                "epigrafe": [{"item": [{"identificador": "B1", "titulo": "Anuncio"}]}],
                            },
                        }]
                    }]
                }
            }
        }
        rows = flatten_boe_sumario(payload, date(2024, 1, 1))
        self.assertEqual(rows[0]["item_id"], "B1")
        self.assertEqual(rows[0]["publication_date"], "2024-01-01")
        self.assertEqual(rows[0]["section"], "3. ANUNCIOS")
        self.assertEqual(rows[0]["department"], "Ministerio de Hacienda")
