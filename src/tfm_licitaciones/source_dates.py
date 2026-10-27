"""v2 declared publication-date extension shared by both Silver routes.

The frozen reference and native benchmark kernel retain their v1 semantics.
This extension checks all observations before joining dates to canonical events,
so a new date cannot silently bypass the event collision contract.
"""

from __future__ import annotations

import json

import polars as pl

from .normalize import _parse_date
from .silver_reference import _aware_instant, _first_text


def apply_declared_publication_dates(events: pl.DataFrame, records: pl.DataFrame) -> pl.DataFrame:
    """Carry declared PLACSP dates into Silver without inventing source time."""

    placsp = records.filter(pl.col("source") == "placsp")
    if placsp.is_empty():
        return events
    observations = {}
    for payload_json in placsp.get_column("payload_json"):
        payload = json.loads(payload_json)
        atom_id = _first_text(payload.get("atom_id"))
        updated = _aware_instant(payload.get("updated"))
        marker = updated.isoformat() if updated is not None else "undated"
        event_id = f"placsp:notice:{atom_id}@{marker}"
        declared = _parse_date(payload.get("publication_date"))
        if event_id in observations and observations[event_id] != declared:
            raise ValueError(
                f"Conflicting canonical mappings for event_id {event_id!r}; differing fields: publication_date"
            )
        observations[event_id] = declared
    dates = pl.DataFrame(
        {"event_id": list(observations), "__declared_date": list(observations.values())},
        schema={"event_id": pl.String, "__declared_date": pl.Date},
    )
    return (
        events.join(dates, on="event_id", how="left", maintain_order="left")
        .with_columns(publication_date=pl.coalesce("__declared_date", "publication_date"))
        .drop("__declared_date")
    )
