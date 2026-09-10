"""Gold-layer analytical marts generated with deterministic Python code."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .models import OpportunityRecord


def opportunities_rows(opportunities: list[OpportunityRecord]) -> list[dict[str, Any]]:
    """Create flat rows suitable for BI tools and CSV export."""

    rows = [item.to_dict() for item in opportunities]
    for row in rows:
        row["matched_keywords"] = ", ".join(row["matched_keywords"])
        row["published_date"] = row["published_date"] or ""
    return sorted(rows, key=lambda row: (-row["technology_score"], row["tender_id"]))


def technology_summary(opportunities: list[OpportunityRecord]) -> list[dict[str, Any]]:
    """Aggregate opportunity volume and score by category and month."""

    counts: Counter[tuple[str, str]] = Counter()
    scores: defaultdict[tuple[str, str], int] = defaultdict(int)
    for item in opportunities:
        month = item.tender.published_date.strftime("%Y-%m") if item.tender.published_date else "unknown"
        key = (item.category, month)
        counts[key] += 1
        scores[key] += item.technology_score
    return [
        {"category": category, "publication_month": month, "n_tenders": counts[(category, month)], "technology_score": scores[(category, month)]}
        for category, month in sorted(counts)
    ]


def buyer_summary(opportunities: list[OpportunityRecord]) -> list[dict[str, Any]]:
    """Aggregate tender count and score by buyer and technology category."""

    grouped: defaultdict[tuple[str, str], list[OpportunityRecord]] = defaultdict(list)
    for item in opportunities:
        grouped[(item.tender.buyer or "(unknown)", item.category)].append(item)
    rows = [
        {"buyer": buyer, "category": category, "n_tenders": len(items), "technology_score": sum(i.technology_score for i in items)}
        for (buyer, category), items in grouped.items()
    ]
    return sorted(rows, key=lambda row: (-row["n_tenders"], row["buyer"], row["category"]))
