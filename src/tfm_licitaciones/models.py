"""Data contracts shared by the ingestion, quality and analytics layers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import polars as pl


PROCUREMENT_EVENT_SCHEMA = pl.Schema(
    {
        "event_id": pl.String,
        "procedure_id": pl.String,
        "source": pl.String,
        "source_event_type": pl.String,
        "buyer_id": pl.String,
        "buyer_name": pl.String,
        "title": pl.String,
        "description": pl.String,
        "cpv_codes": pl.List(pl.String),
        "estimated_value": pl.Decimal(precision=20, scale=2),
        "awarded_value": pl.Decimal(precision=20, scale=2),
        "currency": pl.String,
        "publication_date": pl.Date,
        "source_updated_at": pl.Datetime("us", "UTC"),
        "deadline": pl.Datetime("us", "UTC"),
        "status": pl.String,
        "nuts_code": pl.String,
        "country": pl.String,
        "source_url": pl.String,
        "ingested_at": pl.Datetime("us", "UTC"),
    }
)


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
class ProcurementEvent:
    """Represent one source-published event in the canonical Silver contract."""

    event_id: str
    procedure_id: str | None
    source: str
    source_event_type: str
    ingested_at: datetime
    buyer_id: str | None = None
    buyer_name: str | None = None
    title: str | None = None
    description: str | None = None
    cpv_codes: tuple[str, ...] = ()
    estimated_value: Decimal | None = None
    awarded_value: Decimal | None = None
    currency: str | None = None
    publication_date: date | None = None
    source_updated_at: datetime | None = None
    deadline: datetime | None = None
    status: str | None = None
    nuts_code: str | None = None
    country: str | None = None
    source_url: str | None = None

    def __post_init__(self) -> None:
        """Enforce the identifiers and UTC timestamp semantics of the contract."""

        for field_name in ("event_id", "source", "source_event_type"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.procedure_id is not None and not self.procedure_id.strip():
            raise ValueError("procedure_id must be null or non-empty")
        if any(not code.strip() for code in self.cpv_codes):
            raise ValueError("cpv_codes must not contain empty values")
        if len(set(self.cpv_codes)) != len(self.cpv_codes):
            raise ValueError("cpv_codes must not contain duplicates")
        for field_name in ("source_updated_at", "deadline", "ingested_at"):
            value = getattr(self, field_name)
            if value is None:
                continue
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must include a timezone")
            object.__setattr__(self, field_name, value.astimezone(timezone.utc))

    def to_row(self) -> dict[str, Any]:
        """Serialize native values in the stable Polars schema order."""

        return {
            "event_id": self.event_id,
            "procedure_id": self.procedure_id,
            "source": self.source,
            "source_event_type": self.source_event_type,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "title": self.title,
            "description": self.description,
            "cpv_codes": list(self.cpv_codes),
            "estimated_value": self.estimated_value,
            "awarded_value": self.awarded_value,
            "currency": self.currency,
            "publication_date": self.publication_date,
            "source_updated_at": self.source_updated_at,
            "deadline": self.deadline,
            "status": self.status,
            "nuts_code": self.nuts_code,
            "country": self.country,
            "source_url": self.source_url,
            "ingested_at": self.ingested_at,
        }


def procurement_events_frame(events: Iterable[ProcurementEvent]) -> pl.DataFrame:
    """Build a typed Silver frame, preserving the schema for an empty batch."""

    return pl.DataFrame((event.to_row() for event in events), schema=PROCUREMENT_EVENT_SCHEMA)


def procurement_event_from_tender(
    tender: TenderRecord,
    *,
    event_id: str,
    procedure_id: str | None,
    source_event_type: str,
    publication_date: date | None,
    source_updated_at: datetime | None,
    deadline: datetime | None,
    ingested_at: datetime,
) -> ProcurementEvent:
    """Map the legacy model without guessing identity or temporal semantics."""

    return ProcurementEvent(
        event_id=event_id,
        procedure_id=procedure_id,
        source=tender.source,
        source_event_type=source_event_type,
        buyer_id=tender.buyer_id,
        buyer_name=tender.buyer,
        title=tender.title or None,
        description=tender.summary or None,
        cpv_codes=(tender.cpv_main,) if tender.cpv_main else (),
        estimated_value=Decimal(str(tender.amount)) if tender.amount is not None else None,
        currency=tender.currency,
        publication_date=publication_date,
        source_updated_at=source_updated_at,
        deadline=deadline,
        status=tender.status,
        country=tender.country,
        source_url=tender.url,
        ingested_at=ingested_at,
    )


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
