"""Transparent technology classification with keyword and CPV signals."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from .models import OpportunityRecord, TenderRecord


def normalize_text(value: str) -> str:
    """Lowercase text and remove diacritics for stable multilingual matching."""

    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char)).lower()


def _keyword_pattern(keyword: str) -> re.Pattern[str]:
    """Compile an accent-insensitive word-boundary pattern."""

    normalized = normalize_text(keyword.strip())
    return re.compile(r"(?<!\w)" + re.escape(normalized) + r"(?!\w)")


def cpv_category(cpv_main: str | None, cpv_map: dict[str, list[str]]) -> str | None:
    """Map a CPV code to a category via ordered prefixes, if unambiguous.

    Longer prefixes win so classes (``7221``) take precedence over their
    division (``72``). ``None`` means the code carries no clear signal and
    the caller must fall back to another source of evidence.
    """

    if not cpv_main:
        return None
    code = cpv_main.strip()
    best: tuple[int, str] | None = None
    for category, prefixes in cpv_map.items():
        for prefix in prefixes:
            if code.startswith(prefix) and (best is None or len(prefix) > best[0]):
                best = (len(prefix), category)
    return best[1] if best else None


def score_tender(
    tender: TenderRecord,
    categories: list[dict[str, Any]],
    cpv_map: dict[str, list[str]] | None = None,
) -> OpportunityRecord:
    """Score and classify a tender using ordered, explainable rules.

    The primary signal remains the keyword ruleset; when no keyword matches
    and the notice carries an unambiguous CPV code, the CPV mapping assigns
    the category. ``category_source`` records which signal decided so the
    assignment stays auditable end to end.
    """

    searchable = normalize_text(f"{tender.title} {tender.summary}")
    matched: list[str] = []
    category = "Other"
    for definition in categories:
        category_matches = [
            normalize_text(keyword)
            for keyword in definition.get("keywords", [])
            if _keyword_pattern(str(keyword)).search(searchable)
        ]
        if category == "Other" and category_matches:
            category = str(definition["name"])
        matched.extend(category_matches)
    unique_matches = tuple(sorted(set(matched)))
    category_source = "keywords" if unique_matches else "none"
    if not unique_matches and cpv_map is not None:
        hinted = cpv_category(tender.cpv_main, cpv_map)
        if hinted is not None:
            category = hinted
            category_source = "cpv"
    return OpportunityRecord(
        tender=tender,
        category=category,
        technology_score=len(unique_matches),
        matched_keywords=unique_matches,
        category_source=category_source,
    )
