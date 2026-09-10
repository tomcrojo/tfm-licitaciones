"""Tests for explainable scoring, CPV hybrid rules and quality gates."""

from __future__ import annotations

import unittest
from datetime import date

from tfm_licitaciones.classify import cpv_category, score_tender
from tfm_licitaciones.models import TenderRecord
from tfm_licitaciones.quality import validate_records

THRESHOLDS = {
    "max_title_null_share": 0.05,
    "max_publication_date_null_share": 0.05,
    "max_invalid_amount_share": 0.10,
    "min_technology_match_share": 0.20,
    "max_duplicate_share": 0.35,
}
CPV_MAP = {
    "Cloud": ["72415"],
    "Data & Analytics": ["723"],
    "Software & Development": ["48", "7221"],
    "Infrastructure & Hardware": ["324"],
}


class ClassifyQualityTests(unittest.TestCase):
    """Check transparent categories and batch-level quality evidence."""

    def test_scoring_is_accent_insensitive_and_preserves_all_matches(self) -> None:
        tender = TenderRecord("1", "ted", "Inteligencia Artificial", "Nube y business intelligence")
        result = score_tender(
            tender,
            [
                {"name": "Cloud", "keywords": ["nube"]},
                {"name": "AI", "keywords": ["inteligencia artificial"]},
                {"name": "Data", "keywords": ["business intelligence"]},
            ],
        )
        self.assertEqual(result.category, "Cloud")
        self.assertEqual(result.technology_score, 3)
        self.assertIn("inteligencia artificial", result.matched_keywords)
        self.assertEqual(result.category_source, "keywords")

    def test_cpv_fallback_assigns_category_when_no_keywords(self) -> None:
        tender = TenderRecord("1", "placsp", "Servicios de tratamiento de datos", cpv_main="72310000")
        result = score_tender(
            tender,
            [{"name": "Data & Analytics", "keywords": ["big data"]}],
            CPV_MAP,
        )
        self.assertEqual(result.category, "Data & Analytics")
        self.assertEqual(result.category_source, "cpv")
        self.assertEqual(result.technology_score, 0)

    def test_keywords_win_over_cpv_signal(self) -> None:
        tender = TenderRecord("1", "ted", "Software de gestion cloud", cpv_main="72310000")
        result = score_tender(tender, [{"name": "Cloud", "keywords": ["cloud"]}], CPV_MAP)
        self.assertEqual(result.category, "Cloud")
        self.assertEqual(result.category_source, "keywords")

    def test_cpv_category_prefers_longest_prefix(self) -> None:
        mapping = {"Software & Development": ["72", "7221"], "Cloud": ["72415"]}
        self.assertEqual(cpv_category("72212000", mapping), "Software & Development")
        self.assertEqual(cpv_category("72415000", mapping), "Cloud")
        self.assertIsNone(cpv_category("45212200", mapping))
        self.assertIsNone(cpv_category(None, mapping))

    def test_quality_passes_for_complete_unique_batch(self) -> None:
        tender = TenderRecord("1", "ted", "Servicio cloud", published_date=date(2024, 1, 1))
        opportunity = score_tender(tender, [{"name": "Cloud", "keywords": ["cloud"]}])
        report = validate_records([tender], [opportunity], THRESHOLDS)
        self.assertTrue(report["passed"])
        self.assertEqual(report["issue_count"], 0)
        self.assertEqual(report["metrics"]["duplicate_share"], 0.0)

    def test_quality_detects_duplicate_source_identifier(self) -> None:
        first = TenderRecord("1", "ted", "Cloud", published_date=date(2024, 1, 1))
        second = TenderRecord("1", "ted", "Cloud", published_date=date(2024, 1, 2))
        opportunities = [score_tender(item, [{"name": "Cloud", "keywords": ["cloud"]}]) for item in [first, second]]
        report = validate_records([first, second], opportunities, THRESHOLDS)
        self.assertFalse(report["passed"])
        self.assertIn("duplicate_keys", [check["name"] for check in report["checks"]])

    def test_quality_gates_linkage_duplicate_share(self) -> None:
        base = {
            "candidate_pairs": 2,
            "linked_pairs": 1,
            "linked_groups": 1,
            "duplicated_records": 1,
            "threshold": 0.75,
            "window_days": 7,
        }
        records = [
            TenderRecord(str(i), "ted", "Servicio cloud", published_date=date(2024, 1, i))
            for i in range(1, 5)
        ]
        opportunities = [score_tender(item, [{"name": "Cloud", "keywords": ["cloud"]}]) for item in records]
        passed = validate_records(records, opportunities, THRESHOLDS, linkage_stats=base)
        failed = validate_records(records, opportunities, {**THRESHOLDS, "max_duplicate_share": 0.10}, linkage_stats=base)
        self.assertTrue(passed["passed"])
        self.assertFalse(failed["passed"])
        self.assertEqual(passed["metrics"]["duplicate_share"], 0.25)
        self.assertEqual(failed["metrics"]["duplicate_share"], 0.25)

    def test_quality_distinguishes_missing_and_malformed_amount(self) -> None:
        tender = TenderRecord(
            "1",
            "ted",
            "Servicio cloud",
            published_date=date(2024, 1, 1),
            raw={"amount": "not-a-number"},
        )
        opportunity = score_tender(tender, [{"name": "Cloud", "keywords": ["cloud"]}])
        report = validate_records(
            [tender],
            [opportunity],
            {**THRESHOLDS, "max_invalid_amount_share": 0.0},
        )
        self.assertFalse(report["passed"])
        self.assertEqual(report["metrics"]["invalid_amount_share"], 1.0)


if __name__ == "__main__":
    unittest.main()
