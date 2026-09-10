"""Batch-level data-quality rules and a machine-readable quality report."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .models import OpportunityRecord, QualityIssue, TenderRecord


def validate_records(
    records: list[TenderRecord],
    opportunities: list[OpportunityRecord],
    thresholds: dict[str, Any],
    linkage_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate completeness, uniqueness, validity and relevance gates.

    The linkage layer reports cross-source duplicates before this runs, so
    the duplicate share is gated with its own threshold instead of being
    conflated with intra-source key uniqueness.
    """

    issues: list[QualityIssue] = []
    for record in records:
        if not record.tender_id.strip():
            issues.append(QualityIssue("<missing>", "required_tender_id", "Tender id is empty"))
        if not record.source.strip():
            issues.append(QualityIssue(record.tender_id or "<missing>", "required_source", "Source is empty"))
        if not record.title.strip():
            issues.append(QualityIssue(record.tender_id or "<missing>", "required_title", "Title is empty"))
        if record.amount is not None and record.amount < 0:
            issues.append(QualityIssue(record.tender_id, "non_negative_amount", "Amount is negative"))

    counts = Counter((record.source, record.tender_id) for record in records)
    for source_id, count in counts.items():
        if count > 1:
            issues.append(
                QualityIssue(
                    source_id[1],
                    "unique_source_tender_id",
                    f"Duplicate key {source_id[0]}:{source_id[1]} appears {count} times",
                )
            )

    total = len(records)
    title_null_share = _share(total, sum(not r.title.strip() for r in records))
    date_null_share = _share(total, sum(r.published_date is None for r in records))
    # Amount is optional at notice level in both TED and BOE. A missing amount
    # is valid, but an informed value that the adapter could not parse is not.
    invalid_amount_share = _share(total, sum(_has_invalid_amount(r) for r in records))
    technology_matches = sum(item.technology_score > 0 for item in opportunities)
    technology_match_share = _share(total, technology_matches)
    duplicated = int(linkage_stats.get("duplicated_records", 0)) if linkage_stats else 0
    duplicate_share = _share(total, duplicated)

    checks = [
        _check("not_empty", total, 1, ">=", total >= 1),
        _check("title_null_share", title_null_share, thresholds["max_title_null_share"], "<=", title_null_share <= thresholds["max_title_null_share"]),
        _check("publication_date_null_share", date_null_share, thresholds["max_publication_date_null_share"], "<=", date_null_share <= thresholds["max_publication_date_null_share"]),
        _check("invalid_amount_share", invalid_amount_share, thresholds["max_invalid_amount_share"], "<=", invalid_amount_share <= thresholds["max_invalid_amount_share"]),
        _check("technology_match_share", technology_match_share, thresholds["min_technology_match_share"], ">=", technology_match_share >= thresholds["min_technology_match_share"]),
        _check("duplicate_keys", len([i for i in issues if i.rule == "unique_source_tender_id"]), 0, "=", not any(i.rule == "unique_source_tender_id" for i in issues)),
        _check(
            "duplicate_share",
            duplicate_share,
            thresholds.get("max_duplicate_share", 1.0),
            "<=",
            duplicate_share <= thresholds.get("max_duplicate_share", 1.0),
        ),
    ]
    return {
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "record_count": total,
        "issue_count": len(issues),
        "issues": [issue.to_dict() for issue in issues],
        "metrics": {
            "title_null_share": title_null_share,
            "publication_date_null_share": date_null_share,
            "invalid_amount_share": invalid_amount_share,
            "technology_match_share": technology_match_share,
            "duplicate_share": duplicate_share,
        },
        "checks": checks,
        "passed": all(check["passed"] for check in checks),
    }


def _share(total: int, count: int) -> float:
    """Return a rounded fraction, using zero for an empty batch."""

    return round(count / total, 4) if total else 0.0


def _has_invalid_amount(record: TenderRecord) -> bool:
    """Detect a malformed amount while treating absent optional values as valid."""

    raw_keys = ("estimated-value-lot", "framework-maximum-value-lot", "amount")
    raw_value = next((record.raw.get(key) for key in raw_keys if key in record.raw), None)
    return raw_value not in (None, "", []) and record.amount is None


def _check(name: str, value: Any, threshold: Any, operator: str, passed: bool) -> dict[str, Any]:
    """Build one stable quality-gate result."""

    return {"name": name, "value": value, "threshold": threshold, "operator": operator, "passed": passed}
