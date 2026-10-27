"""Hierarchical evaluation of the keyword classifier against CPV evidence."""

from __future__ import annotations

from typing import Any

from .classify import cpv_category
from .models import OpportunityRecord


def evaluate_against_cpv(
    opportunities: list[OpportunityRecord],
    cpv_map: dict[str, list[str]],
) -> dict[str, Any]:
    """Measure keyword categories against CPV-derived labels.

    The CPV 2008 vocabulary predates cloud, AI and cybersecurity procurement,
    so only unambiguous prefixes act as proxy labels. Precision, recall and
    F1 compare the keyword verdict with the CPV verdict on the subset of
    notices whose CPV maps to one of the keyword categories; macro averages
    weight equally the categories with CPV support in that subset. Categories
    without support remain visible in the per-category report.
    """

    labels = sorted(cpv_map)
    tp = {label: 0 for label in labels}
    fp = {label: 0 for label in labels}
    fn = {label: 0 for label in labels}
    support = {label: 0 for label in labels}
    evaluated = 0
    for item in opportunities:
        if item.category_source == "cpv":
            # Decided by the same CPV mapping used as ground truth: excluded
            # to keep the keyword evaluation free of circularity.
            continue
        gold = cpv_category(item.tender.cpv_main, cpv_map)
        if gold is None:
            continue
        evaluated += 1
        support[gold] += 1
        predicted = item.category
        if predicted == gold:
            tp[gold] += 1
            continue
        fn[gold] += 1
        if predicted in fp:
            fp[predicted] += 1
    per_category = {}
    f1_scores: list[float] = []
    for label in labels:
        precision = tp[label] / (tp[label] + fp[label]) if tp[label] + fp[label] else 0.0
        recall = tp[label] / (tp[label] + fn[label]) if tp[label] + fn[label] else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if support[label] > 0:
            f1_scores.append(f1)
        per_category[label] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "support": support[label],
        }
    return {
        "method": "keyword classifier vs unambiguous CPV prefixes",
        "evaluated_records": evaluated,
        "total_records": len(opportunities),
        "coverage": round(evaluated / len(opportunities), 4) if opportunities else 0.0,
        "macro_f1": round(sum(f1_scores) / len(f1_scores), 4) if f1_scores else 0.0,
        "per_category": per_category,
        "notes": [
            "CPV 2008 lacks explicit AI and cybersecurity codes; those categories are keyword-only.",
            "Predictions use only the keyword signal (category_source=keywords); CPV fallback is excluded to avoid circularity.",
            "CPV labels are a proxy, not human ground truth; macro-F1 includes only categories with evaluated CPV support.",
        ],
    }
