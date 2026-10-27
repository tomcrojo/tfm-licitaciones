"""Tests for the CPV-based evaluation of the keyword classifier."""

from __future__ import annotations

import unittest
from datetime import date

from tfm_licitaciones.evaluation import evaluate_against_cpv
from tfm_licitaciones.models import OpportunityRecord, TenderRecord

CPV_MAP = {
    "Cloud": ["72415"],
    "Data & Analytics": ["723"],
    "Software & Development": ["48"],
}


def _opportunity(identifier: str, cpv: str | None, category: str, source_signal: str = "keywords") -> OpportunityRecord:
    return OpportunityRecord(
        tender=TenderRecord(identifier, "ted", "título", cpv_main=cpv, published_date=date(2024, 1, 1)),
        category=category,
        technology_score=1 if source_signal == "keywords" else 0,
        matched_keywords=("kw",) if source_signal == "keywords" else (),
        category_source=source_signal,
    )


class EvaluationTests(unittest.TestCase):
    """Verify multiclass precision/recall against the CPV proxy."""

    def test_metrics_count_hits_misses_and_support(self) -> None:
        opportunities = [
            # hit: keywords say Cloud, CPV agrees
            _opportunity("1", "72415000", "Cloud"),
            # miss: keywords say Software, CPV says Data
            _opportunity("2", "72310000", "Software & Development"),
            # keyword miss (Other) for a CPV Cloud notice
            _opportunity("3", "72415000", "Other"),
            # unmapped CPV: excluded from the evaluation
            _opportunity("4", "45212200", "Other"),
            # CPV fallback prediction: excluded to avoid circularity
            _opportunity("5", "48000000", "Software & Development", source_signal="cpv"),
        ]
        report = evaluate_against_cpv(opportunities, CPV_MAP)
        self.assertEqual(report["evaluated_records"], 3)
        self.assertEqual(report["total_records"], 5)
        cloud = report["per_category"]["Cloud"]
        self.assertEqual(cloud["support"], 2)
        self.assertEqual(cloud["precision"], 1.0)
        self.assertEqual(cloud["recall"], 0.5)
        data = report["per_category"]["Data & Analytics"]
        self.assertEqual(data["support"], 1)
        self.assertEqual(data["recall"], 0.0)
        self.assertEqual(data["precision"], 0.0)
        self.assertEqual(report["macro_f1"], 0.3333)

    def test_macro_f1_excludes_labels_without_cpv_support(self) -> None:
        report = evaluate_against_cpv(
            [_opportunity("1", "72415000", "Cloud")],
            {**CPV_MAP, "AI & Machine Learning": [], "Cybersecurity": []},
        )
        self.assertEqual(report["macro_f1"], 1.0)
        self.assertEqual(report["per_category"]["Cybersecurity"]["support"], 0)
        self.assertEqual(report["per_category"]["Software & Development"]["support"], 0)

    def test_unsupported_prediction_still_counts_as_supported_label_miss(self) -> None:
        report = evaluate_against_cpv(
            [
                _opportunity("1", "72415000", "Cloud"),
                _opportunity("2", "72415000", "Cybersecurity"),
            ],
            {**CPV_MAP, "Cybersecurity": []},
        )
        self.assertEqual(report["macro_f1"], 0.6667)
        self.assertEqual(report["per_category"]["Cloud"]["recall"], 0.5)
        self.assertEqual(report["per_category"]["Cybersecurity"]["precision"], 0.0)

    def test_keyword_category_absent_from_cpv_map_counts_as_miss(self) -> None:
        report = evaluate_against_cpv(
            [_opportunity("1", "72310000", "Cybersecurity")], CPV_MAP,
        )
        self.assertEqual(report["evaluated_records"], 1)
        self.assertEqual(report["per_category"]["Data & Analytics"]["support"], 1)
        self.assertEqual(report["per_category"]["Data & Analytics"]["recall"], 0.0)
        self.assertEqual(report["macro_f1"], 0.0)

    def test_corpus_without_evaluable_proxy_labels_reports_zero_macro_f1(self) -> None:
        report = evaluate_against_cpv(
            [
                _opportunity("1", "72415000", "Cloud", source_signal="cpv"),
                _opportunity("2", "45212200", "Cloud"),
            ],
            CPV_MAP,
        )
        self.assertEqual(report["evaluated_records"], 0)
        self.assertEqual(report["macro_f1"], 0.0)
        self.assertEqual(report["coverage"], 0.0)

    def test_empty_corpus_reports_zeroed_report(self) -> None:
        report = evaluate_against_cpv([], CPV_MAP)
        self.assertEqual(report["evaluated_records"], 0)
        self.assertEqual(report["coverage"], 0.0)
        self.assertEqual(report["macro_f1"], 0.0)


if __name__ == "__main__":
    unittest.main()
