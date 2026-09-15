"""Tests for cross-source record linkage."""

from __future__ import annotations

import unittest
from datetime import date

from tfm_licitaciones.linkage import link_duplicates
from tfm_licitaciones.models import TenderRecord


class LinkageTests(unittest.TestCase):
    """Check blocking, similarity and canonical selection end to end."""

    def _record(
        self,
        identifier: str,
        title: str,
        buyer: str = "Centro de Sistemas",
        day: int = 8,
        source: str = "ted",
        cpv: str | None = "48000000",
        month: int = 3,
        published: bool = True,
    ) -> TenderRecord:
        published_date = date(2024, month, day) if published else None
        return TenderRecord(identifier, source, title, buyer=buyer, published_date=published_date, cpv_main=cpv)

    def test_cross_source_pair_links_and_keeps_earliest_canonical(self) -> None:
        ted = self._record("A-1", "Migración a plataforma cloud y soporte de aplicaciones", day=8, source="ted")
        placsp = self._record(
            "10000101", "Servicio de migración a plataforma cloud y soporte de aplicaciones", day=9, source="placsp"
        )
        result = link_duplicates([ted, placsp], threshold=0.6)
        stats = result.stats
        self.assertEqual(stats["candidate_pairs"], 1)
        self.assertEqual(stats["evaluated_pairs"], 1)
        self.assertEqual(stats["linked_pairs"], 1)
        self.assertEqual(stats["linked_groups"], 1)
        self.assertEqual(stats["duplicated_records"], 1)
        self.assertTrue(result.assignments[("ted", "A-1")]["is_canonical"])
        self.assertFalse(result.assignments[("placsp", "10000101")]["is_canonical"])
        self.assertEqual(result.assignments[("placsp", "10000101")]["duplicate_of"], "A-1")
        self.assertEqual(result.assignments[("ted", "A-1")]["dup_group"], 1)

    def test_same_source_similar_records_do_not_link(self) -> None:
        title = "Migración a plataforma cloud y soporte de aplicaciones"
        first = self._record("A-1", title, day=8, source="ted")
        second = self._record("A-2", title, day=9, source="ted")
        result = link_duplicates([first, second], threshold=0.6)
        self.assertEqual(result.stats["candidate_pairs"], 0)
        self.assertEqual(result.stats["evaluated_pairs"], 0)
        self.assertEqual(result.stats["linked_pairs"], 0)
        self.assertEqual(result.assignments, {})

    def test_mixed_block_counts_only_cross_source_candidates(self) -> None:
        title = "Suministro e instalacion de mobiliario de oficina"
        ted_first = self._record("T-1", f"Renovacion de {title}", day=8, source="ted")
        ted_second = self._record("T-2", title, day=9, source="ted")
        placsp = self._record("P-1", title, day=9, source="placsp")
        result = link_duplicates([ted_first, ted_second, placsp], threshold=0.6)
        # Same-source pair (T-1, T-2) is never a candidate; only 2 cross pairs.
        self.assertEqual(result.stats["candidate_pairs"], 2)
        self.assertEqual(result.stats["evaluated_pairs"], 2)
        self.assertEqual(result.stats["linked_pairs"], 2)
        self.assertEqual(result.stats["linked_groups"], 1)
        # Transitive grouping may still hold same-source records together.
        self.assertEqual({("ted", "T-1"), ("ted", "T-2"), ("placsp", "P-1")}, set(result.assignments))
        self.assertTrue(all(assignment["dup_group"] == 1 for assignment in result.assignments.values()))
        self.assertEqual(result.stats["duplicated_records"], 2)

    def test_large_block_above_old_threshold_is_fully_evaluated(self) -> None:
        title = "Servicio de limpieza y mantenimiento integral de edificios"
        records = [
            self._record(f"T-{position:02d}", title, day=8, source="ted") for position in range(1, 24)
        ] + [self._record(f"P-{position:02d}", title, day=8, source="placsp") for position in range(1, 24)]
        result = link_duplicates(records, threshold=0.75)
        self.assertEqual(result.stats["candidate_pairs"], 23 * 23)
        self.assertGreater(result.stats["candidate_pairs"], 500)
        # Nothing silently skipped: every candidate was evaluated and linked.
        self.assertEqual(result.stats["evaluated_pairs"], result.stats["candidate_pairs"])
        self.assertEqual(result.stats["linked_pairs"], result.stats["candidate_pairs"])
        self.assertEqual(result.stats["linked_groups"], 1)
        self.assertEqual(result.stats["duplicated_records"], 45)
        canonical = [key for key, assignment in result.assignments.items() if assignment["is_canonical"]]
        self.assertEqual(canonical, [("placsp", "P-01")])

    def test_date_exactly_at_window_boundary_is_candidate(self) -> None:
        first = self._record("A-1", "Migración a plataforma cloud", day=1, source="ted")
        second = self._record("B-2", "Migración a plataforma cloud", day=8, source="placsp")
        result = link_duplicates([first, second], threshold=0.6, window_days=7)
        self.assertEqual(result.stats["candidate_pairs"], 1)
        self.assertEqual(result.stats["linked_pairs"], 1)

    def test_date_just_beyond_window_is_not_candidate(self) -> None:
        first = self._record("A-1", "Migración a plataforma cloud", day=1, source="ted")
        second = self._record("B-2", "Migración a plataforma cloud", day=9, source="placsp")
        result = link_duplicates([first, second], threshold=0.6, window_days=7)
        self.assertEqual(result.stats["candidate_pairs"], 0)
        self.assertEqual(result.stats["linked_pairs"], 0)

    def test_outside_window_pairs_are_not_candidates(self) -> None:
        first = self._record("A-1", "Migración a plataforma cloud", day=1, source="ted")
        second = self._record("B-2", "Migración a plataforma cloud", day=28, source="placsp")
        result = link_duplicates([first, second], threshold=0.6, window_days=7)
        self.assertEqual(result.stats["candidate_pairs"], 0)
        self.assertEqual(result.stats["linked_pairs"], 0)

    def test_transitive_group_across_three_sources(self) -> None:
        ted = self._record("T-1", "alfa beta gamma", day=8, source="ted")
        placsp = self._record("P-1", "alfa beta gamma delta epsilon", day=9, source="placsp")
        boe = self._record("B-1", "delta epsilon", day=10, source="boe")
        result = link_duplicates([ted, placsp, boe], threshold=0.5)
        self.assertEqual(result.stats["candidate_pairs"], 3)
        self.assertEqual(result.stats["evaluated_pairs"], 3)
        # ted-placsp and placsp-boe link; ted-boe shares no tokens.
        self.assertEqual(result.stats["linked_pairs"], 2)
        self.assertEqual(result.stats["linked_groups"], 1)
        self.assertEqual(result.stats["duplicated_records"], 2)
        self.assertTrue(result.assignments[("ted", "T-1")]["is_canonical"])
        self.assertEqual(result.assignments[("boe", "B-1")]["duplicate_of"], "T-1")

    def test_group_ids_and_assignments_ignore_input_order(self) -> None:
        group_one = [
            self._record("T-1", "obra alfa beta", day=8, source="ted", buyer="Ayuntamiento Uno"),
            self._record("P-1", "obra alfa beta", day=9, source="placsp", buyer="Ayuntamiento Uno"),
        ]
        group_two = [
            self._record("T-2", "obra gamma delta", day=8, source="ted", buyer="Ayuntamiento Dos"),
            self._record("P-2", "obra gamma delta", day=9, source="placsp", buyer="Ayuntamiento Dos"),
        ]
        forward = link_duplicates(group_one + group_two)
        backward = link_duplicates(group_two + group_one)
        self.assertEqual(forward.assignments, backward.assignments)
        self.assertEqual(forward.stats, backward.stats)
        self.assertEqual(forward.assignments[("ted", "T-1")]["dup_group"], 1)
        self.assertEqual(forward.assignments[("ted", "T-2")]["dup_group"], 2)

    def test_canonical_tiebreak_prefers_longer_title_on_same_date(self) -> None:
        ted = self._record("A-1", "Obra puente", day=9, source="ted")
        placsp = self._record("B-2", "Obra de acondicionamiento del puente", day=9, source="placsp")
        result = link_duplicates([ted, placsp], threshold=0.4)
        self.assertEqual(result.stats["linked_pairs"], 1)
        self.assertTrue(result.assignments[("placsp", "B-2")]["is_canonical"])
        self.assertEqual(result.assignments[("ted", "A-1")]["duplicate_of"], "B-2")

    def test_empty_and_untokenizable_titles_are_counted_but_never_scored(self) -> None:
        cases = [
            ("", "Migración a plataforma cloud"),
            ("de la el y", "Migración a plataforma cloud"),
            ("   ", "Migración a plataforma cloud"),
            (None, "Migración a plataforma cloud"),
        ]
        for left_title, right_title in cases:
            with self.subTest(title=left_title):
                first = self._record("A-1", left_title, day=8, source="ted")
                second = self._record("B-2", right_title, day=9, source="placsp")
                result = link_duplicates([first, second], threshold=0.6)
                self.assertEqual(result.stats["candidate_pairs"], 1)
                self.assertEqual(result.stats["evaluated_pairs"], 0)
                self.assertEqual(result.stats["linked_pairs"], 0)
                self.assertEqual(result.assignments, {})

    def test_missing_publication_date_is_excluded_and_reported(self) -> None:
        dated = self._record("A-1", "Migración a plataforma cloud", day=8, source="ted")
        undated = self._record("B-2", "Migración a plataforma cloud", day=8, source="placsp", published=False)
        result = link_duplicates([dated, undated], threshold=0.6)
        self.assertEqual(result.stats["candidate_pairs"], 0)
        self.assertEqual(result.stats["records_without_date"], 1)
        self.assertEqual(result.assignments, {})

    def test_same_tender_id_across_sources_keeps_distinct_assignments(self) -> None:
        title = "Migración a plataforma cloud"
        ted = self._record("X-1", title, day=8, source="ted")
        placsp_same_id = self._record("X-1", title, day=9, source="placsp")
        placsp_other_id = self._record("Y-9", title, day=9, source="placsp")
        result = link_duplicates([ted, placsp_same_id, placsp_other_id], threshold=0.6)
        self.assertEqual(set(result.assignments), {("ted", "X-1"), ("placsp", "X-1"), ("placsp", "Y-9")})
        canonical = [key for key, assignment in result.assignments.items() if assignment["is_canonical"]]
        self.assertEqual(canonical, [("ted", "X-1")])
        # duplicate_of provably points at the canonical (see Y-9 -> X-1); for the
        # colliding ("placsp", "X-1") row the bare tender_id cannot disambiguate,
        # so is_canonical is the authoritative flag (schema limitation, see PR).
        self.assertEqual(result.assignments[("placsp", "Y-9")]["duplicate_of"], "X-1")

    def test_empty_input_produces_empty_result(self) -> None:
        result = link_duplicates([])
        self.assertEqual(result.assignments, {})
        self.assertEqual(
            result.stats,
            {
                "candidate_pairs": 0,
                "evaluated_pairs": 0,
                "linked_pairs": 0,
                "linked_groups": 0,
                "duplicated_records": 0,
                "records_without_date": 0,
                "threshold": 0.75,
                "window_days": 7,
            },
        )

    def test_blocking_excludes_different_buyer_and_cpv_division(self) -> None:
        title = "Suministro de material de oficina y papeleria"
        different_division = link_duplicates(
            [
                self._record("A-1", title, day=8, cpv="45212200"),
                self._record("B-2", title, day=9, source="placsp", cpv="30190000"),
            ],
            threshold=0.6,
        )
        self.assertEqual(different_division.stats["candidate_pairs"], 0)
        different_buyer = link_duplicates(
            [
                self._record("A-1", title, day=8, buyer="Ayuntamiento Uno"),
                self._record("B-2", title, day=9, source="placsp", buyer="Ayuntamiento Dos"),
            ],
            threshold=0.6,
        )
        self.assertEqual(different_buyer.stats["candidate_pairs"], 0)
        # Same division (48), different full CPV code: still one candidate.
        same_division = link_duplicates(
            [
                self._record("A-1", title, day=8, cpv="48000000"),
                self._record("B-2", title, day=9, source="placsp", cpv="48900000"),
            ],
            threshold=0.6,
        )
        self.assertEqual(same_division.stats["candidate_pairs"], 1)

    def test_same_block_dissimilar_titles_do_not_link(self) -> None:
        first = self._record(
            "A-1", "Mantenimiento de instalaciones deportivas municipales", day=8,
        )
        second = self._record(
            "B-2", "Suministro de material de oficina y papeleria", day=9, source="placsp",
        )
        result = link_duplicates([first, second], threshold=0.6)
        # Same block, cross-source, in window: rejected by score, not by blocking.
        self.assertEqual(result.stats["candidate_pairs"], 1)
        self.assertEqual(result.stats["evaluated_pairs"], 1)
        self.assertEqual(result.stats["linked_pairs"], 0)
        self.assertEqual(result.assignments, {})

    def test_similarity_threshold_is_inclusive(self) -> None:
        first = self._record("A-1", "Migración a plataforma cloud", day=8, source="ted")
        second = self._record("B-2", "Migración a plataforma cloud", day=9, source="placsp")
        result = link_duplicates([first, second], threshold=1.0)
        self.assertEqual(result.stats["evaluated_pairs"], 1)
        self.assertEqual(result.stats["linked_pairs"], 1)

    def test_canonical_final_tiebreak_uses_tender_id_before_source(self) -> None:
        ted = self._record("A-2", "Obra del puente nuevo", day=9, source="ted")
        placsp = self._record("A-1", "Obra del puente viejo", day=9, source="placsp")
        result = link_duplicates([ted, placsp], threshold=0.4)
        self.assertEqual(result.stats["linked_pairs"], 1)
        # Same date and equal title length: the smaller tender id wins.
        self.assertTrue(result.assignments[("placsp", "A-1")]["is_canonical"])
        self.assertEqual(result.assignments[("ted", "A-2")]["duplicate_of"], "A-1")


if __name__ == "__main__":
    unittest.main()
