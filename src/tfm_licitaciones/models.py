"""Data contracts shared by the ingestion, quality and analytics layers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any


@dataclass(frozen=True)
class TenderRecord:
    """Represent one normalized tender at the silver-layer grain."""

    tender_id: str
    source: str
    title: str
    summary: str = ""
    buyer: str | None = None
    published_date: date | None = None
    amount: float | None = None
    currency: str | None = None
    country: str | None = None
    url: str | None = None
    cpv_main: str | None = None
    buyer_id: str | None = None
    region: str | None = None
    status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record using ISO dates for JSONL persistence."""

        payload = asdict(self)
        if self.published_date is not None:
            payload["published_date"] = self.published_date.isoformat()
        return payload


@dataclass(frozen=True)
class OpportunityRecord:
    """Represent a scored tender at the gold-layer grain."""

    tender: TenderRecord
    category: str
    technology_score: int
    matched_keywords: tuple[str, ...]
    category_source: str = "keywords"
    dup_group: int | None = None
    is_canonical: bool = True
    duplicate_of: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Flatten the scored record for analytical JSONL output."""

        tender = self.tender.to_dict()
        tender.update(
            {
                "category": self.category,
                "technology_score": self.technology_score,
                "matched_keywords": list(self.matched_keywords),
                "category_source": self.category_source,
                "dup_group": self.dup_group,
                "is_canonical": self.is_canonical,
                "duplicate_of": self.duplicate_of,
            }
        )
        return tender


@dataclass(frozen=True)
class QualityIssue:
    """Describe one failed validation for a tender or a batch."""

    tender_id: str
    rule: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Serialize the issue for the quality report."""

        return asdict(self)
