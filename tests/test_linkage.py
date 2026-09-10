"""Tests for cross-source record linkage."""

from __future__ import annotations

import unittest
from datetime import date

from tfm_licitaciones.linkage import link_duplicates
from tfm_licitaciones.models import TenderRecord


class LinkageTests(unittest.TestCase):
    """Check blocking, similarity and canonical selection end to end."""

    def _record(self, identifier: str, title: str, buyer: str, day: int, source: str = "ted", cpv: str | None = "48000000") -> TenderRecord:
        return TenderRecord(identifier, source, title, buyer=buyer, published_date=date(2024, 3, day), cpv_main=cpv)

    def test_cross_source_pair_links_and_keeps_earliest_canonical(self) -> None:
        ted = self._record("A-1", "Migración a plataforma cloud y soporte de aplicaciones", "Centro de Sistemas", 8, "ted")
        placsp = self._record("10000101", "Servicio de migración a plataforma cloud y soporte de aplicaciones", "Centro de Sistemas", 9, "placsp")
        result = link_duplicates([ted, placsp], threshold=0.6)
        stats = result.stats
        self.assertEqual(stats["linked_pairs"], 1)
        self.assertEqual(stats["linked_groups"], 1)
        self.assertEqual(stats["duplicated_records"], 1)
        self.assertTrue(result.assignments[("ted", "A-1")]["is_canonical"])
        self.assertFalse(result.assignments[("placsp", "10000101")]["is_canonical"])
        self.assertEqual(result.assignments[("placsp", "10000101")]["duplicate_of"], "A-1")
        self.assertEqual(result.assignments[("ted", "A-1")]["dup_group"], 1)

    def test_different_procedures_do_not_link(self) -> None:
        first = self._record("A-1", "Mantenimiento de instalaciones deportivas municipales", "Ayuntamiento de Cuenca", 8, cpv="45212200")
        second = self._record("B-2", "Suministro de material de oficina y papelería", "Ayuntamiento de Cuenca", 9, cpv="30190000")
        result = link_duplicates([first, second], threshold=0.6)
        self.assertEqual(result.stats["linked_pairs"], 0)
        self.assertEqual(result.assignments, {})

    def test_outside_window_pairs_are_not_candidates(self) -> None:
        first = self._record("A-1", "Migración a plataforma cloud", "Centro de Sistemas", 1)
        second = self._record("B-2", "Migración a plataforma cloud", "Centro de Sistemas", 28)
        result = link_duplicates([first, second], threshold=0.6, window_days=7)
        self.assertEqual(result.stats["candidate_pairs"], 0)
        self.assertEqual(result.stats["linked_pairs"], 0)


if __name__ == "__main__":
    unittest.main()
