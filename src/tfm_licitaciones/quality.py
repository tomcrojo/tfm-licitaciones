"""Batch-level data-quality rules and a machine-readable quality report.

Engine boundary: completeness, uniqueness and validity metrics are native
Polars aggregations over the silver frame.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from .models import OpportunityRecord, QualityIssue, TenderRecord


_RAW_AMOUNT_KEYS = ("estimated-value-lot", "framework-maximum-value-lot", "amount")


def validate_frames(
    silver: pl.DataFrame,
    scored: pl.DataFrame,
    thresholds: dict[str, Any],
    linkage_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate completeness, uniqueness, validity and relevance gates.

    The linkage layer reports cross-source duplicates before this runs, so
    the duplicate share is gated with its own threshold instead of being
    conflated with intra-source key uniqueness.
    """

    issues: list[QualityIssue] = []
    empty_title = pl.col("title").is_null() | (pl.col("title").str.strip_chars() == "")
    empty_id = pl.col("tender_id").is_null() | (pl.col("tender_id").str.strip_chars() == "")
    empty_source = pl.col("source").is_null() | (pl.col("source").str.strip_chars() == "")
    identifier = pl.when(empty_id).then(pl.lit("<missing>")).otherwise(pl.col("tender_id"))

    def _issue_rows(condition: pl.Expr, rule: str, message: str) -> None:
        for row in silver.filter(condition).select(identifier.alias("tender_id")).iter_rows():
            issues.append(QualityIssue(row, rule, message))

    _issue_rows(empty_id, "required_tender_id", "Tender id is empty")
    _issue_rows(empty_source, "required_source", "Source is empty")
    _issue_rows(empty_title, "required_title", "Title is empty")
    _issue_rows(pl.col("amount") < 0, "non_negative_amount", "Amount is negative")

    duplicate_keys = silver.group_by("source", "tender_id").len().filter(pl.col("len") > 1)
    for source, tender_id, count in duplicate_keys.iter_rows():
        issues.append(
            QualityIssue(
                tender_id,
                "unique_source_tender_id",
                f"Duplicate key {source}:{tender_id} appears {count} times",
            )
        )

    total = silver.height
    title_null_share = _share(total, silver.filter(empty_title).height)
    date_null_share = _share(total, silver.filter(pl.col("published_date").is_null()).height)
    # Amount is optional at notice level in both TED and BOE. A missing amount
    # is valid, but an informed value that the adapter could not parse is not.
    invalid_amount = silver.filter(pl.col("raw_amount_present") & pl.col("amount").is_null()).height
    invalid_amount_share = _share(total, invalid_amount)
    technology_matches = scored.filter(pl.col("technology_score") > 0).height if scored.height else 0
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


def validate_records(
    records: list[TenderRecord],
    opportunities: list[OpportunityRecord],
    thresholds: dict[str, Any],
    linkage_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Row-level API over the frame implementation."""

    def _raw_amount_present(record: TenderRecord) -> bool:
        raw_value = next((record.raw.get(key) for key in _RAW_AMOUNT_KEYS if key in record.raw), None)
        return raw_value not in (None, "", [])

    silver = pl.DataFrame(
        {
            "tender_id": [record.tender_id for record in records],
            "source": [record.source for record in records],
            "title": [record.title for record in records],
            "published_date": [record.published_date for record in records],
            "amount": [record.amount for record in records],
            "raw_amount_present": [_raw_amount_present(record) for record in records],
        },
        schema={
            "tender_id": pl.String,
            "source": pl.String,
            "title": pl.String,
            "published_date": pl.Date,
            "amount": pl.Float64,
            "raw_amount_present": pl.Boolean,
        },
    )
    scored = pl.DataFrame(
        {"technology_score": [item.technology_score for item in opportunities]},
        schema={"technology_score": pl.Int64},
    )
    return validate_frames(silver, scored, thresholds, linkage_stats=linkage_stats)


def _share(total: int, count: int) -> float:
    """Return a rounded fraction, using zero for an empty batch."""

    return round(count / total, 4) if total else 0.0


def _check(name: str, value: Any, threshold: Any, operator: str, passed: bool) -> dict[str, Any]:
    """Build one stable quality-gate result."""

    return {"name": name, "value": value, "threshold": threshold, "operator": operator, "passed": passed}
