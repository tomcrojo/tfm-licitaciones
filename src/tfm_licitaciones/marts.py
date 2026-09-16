"""Gold-layer analytical marts computed with native Polars aggregations."""

from __future__ import annotations

from typing import Any

import polars as pl

from .models import OpportunityRecord

#: Columns of the scored silver frame consumed by the marts.
_MART_COLUMNS = [
    "tender_id",
    "source",
    "title",
    "summary",
    "buyer",
    "published_date",
    "amount",
    "currency",
    "country",
    "url",
    "cpv_codes",
    "buyer_id",
    "region",
    "status",
    "category",
    "category_source",
    "technology_score",
    "matched_keywords",
    "dup_group",
    "is_canonical",
    "duplicate_of",
]


def opportunities_rows(scored: pl.DataFrame) -> list[dict[str, Any]]:
    """Create flat rows suitable for BI tools and CSV export."""

    month = pl.when(pl.col("published_date").is_not_null()).then(
        pl.col("published_date").dt.strftime("%Y-%m")
    ).otherwise(pl.lit(""))
    rows = (
        scored.select(
            pl.col(_MART_COLUMNS).exclude("cpv_codes", "matched_keywords", "published_date"),
            cpv_main=pl.col("cpv_codes").list.first(),
            matched_keywords=pl.col("matched_keywords").list.join(", "),
            published_date=month,
        )
        .sort("technology_score", "tender_id", descending=[True, False])
    )
    return rows.to_dicts()


def technology_summary(canonical: pl.DataFrame) -> list[dict[str, Any]]:
    """Aggregate opportunity volume and score by category and month."""

    month = pl.when(pl.col("published_date").is_not_null()).then(
        pl.col("published_date").dt.strftime("%Y-%m")
    ).otherwise(pl.lit("unknown"))
    return (
        canonical.group_by(category=pl.col("category"), publication_month=month)
        .agg(n_tenders=pl.len(), technology_score=pl.col("technology_score").sum())
        .sort("category", "publication_month")
        .to_dicts()
    )


def buyer_summary(canonical: pl.DataFrame) -> list[dict[str, Any]]:
    """Aggregate tender count and score by buyer and technology category."""

    return (
        canonical.group_by(
            buyer=pl.col("buyer").fill_null("(unknown)"),
            category=pl.col("category"),
        )
        .agg(n_tenders=pl.len(), technology_score=pl.col("technology_score").sum())
        .sort("n_tenders", "buyer", "category", descending=[True, False, False])
        .to_dicts()
    )


def opportunities_frame(opportunities: list[OpportunityRecord]) -> pl.DataFrame:
    """Assemble scored records into the frame consumed by the marts."""

    return pl.DataFrame(
        {
            "tender_id": [item.tender.tender_id for item in opportunities],
            "source": [item.tender.source for item in opportunities],
            "title": [item.tender.title for item in opportunities],
            "summary": [item.tender.summary for item in opportunities],
            "buyer": [item.tender.buyer for item in opportunities],
            "published_date": [item.tender.published_date for item in opportunities],
            "amount": [item.tender.amount for item in opportunities],
            "currency": [item.tender.currency for item in opportunities],
            "country": [item.tender.country for item in opportunities],
            "url": [item.tender.url for item in opportunities],
            "cpv_codes": [[item.tender.cpv_main] if item.tender.cpv_main else [] for item in opportunities],
            "buyer_id": [item.tender.buyer_id for item in opportunities],
            "region": [item.tender.region for item in opportunities],
            "status": [item.tender.status for item in opportunities],
            "category": [item.category for item in opportunities],
            "category_source": [item.category_source for item in opportunities],
            "technology_score": [item.technology_score for item in opportunities],
            "matched_keywords": [list(item.matched_keywords) for item in opportunities],
            "dup_group": [item.dup_group for item in opportunities],
            "is_canonical": [item.is_canonical for item in opportunities],
            "duplicate_of": [item.duplicate_of for item in opportunities],
        },
        schema={
            "tender_id": pl.String,
            "source": pl.String,
            "title": pl.String,
            "summary": pl.String,
            "buyer": pl.String,
            "published_date": pl.Date,
            "amount": pl.Float64,
            "currency": pl.String,
            "country": pl.String,
            "url": pl.String,
            "cpv_codes": pl.List(pl.String),
            "buyer_id": pl.String,
            "region": pl.String,
            "status": pl.String,
            "category": pl.String,
            "category_source": pl.String,
            "technology_score": pl.Int64,
            "matched_keywords": pl.List(pl.String),
            "dup_group": pl.Int64,
            "is_canonical": pl.Boolean,
            "duplicate_of": pl.String,
        },
    )
