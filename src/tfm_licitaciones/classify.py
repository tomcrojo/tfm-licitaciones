"""Transparent technology classification with keyword and CPV signals.

Engine boundary: keyword matching runs as vectorized Polars ``str.contains``
passes with patterns compiled once per batch, not once per record.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import polars as pl

from .models import OpportunityRecord, TenderRecord


def normalize_text(value: str) -> str:
    """Lowercase text and remove diacritics for stable multilingual matching."""

    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char)).lower()


def _escape_regex(value: str) -> str:
    """Escape regex metacharacters for the regex engine Polars uses."""

    return re.sub(r"([\\.^$|?*+()\[\]{}])", r"\\\1", value)


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


def _keyword_rules(categories: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    """Precompile one word-boundary pattern per keyword, once per batch."""

    rules: list[tuple[str, str, str]] = []
    for definition in categories:
        name = str(definition["name"])
        for keyword in definition.get("keywords", []):
            normalized = normalize_text(str(keyword).strip())
            if normalized:
                rules.append((name, normalized, r"\b" + _escape_regex(normalized) + r"\b"))
    return rules


def _cpv_expression(cpv_map: dict[str, list[str]] | None) -> pl.Expr | None:
    """Longest-prefix CPV mapping as a native expression chain."""

    if cpv_map is None:
        return None
    entries: list[tuple[int, int, str, str]] = []
    for order, (category, prefixes) in enumerate(cpv_map.items()):
        for prefix in prefixes:
            entries.append((-len(prefix), order, prefix, category))
    entries.sort()
    code = pl.col("cpv_main").str.strip_chars()
    chain = [
        pl.when(code.str.starts_with(prefix)).then(pl.lit(category))
        for _, _, prefix, category in entries
    ]
    return pl.coalesce(chain) if chain else None


def score_frame(
    frame: pl.DataFrame,
    categories: list[dict[str, Any]],
    cpv_map: dict[str, list[str]] | None = None,
) -> pl.DataFrame:
    """Classify and score every silver record with vectorized expressions.

    The frame requires ``title``, ``summary`` and ``cpv_codes`` columns. The
    primary signal remains the keyword ruleset; when no keyword matches and
    the notice carries an unambiguous CPV code, the CPV mapping assigns the
    category. ``category_source`` records which signal decided so the
    assignment stays auditable end to end.
    """

    rules = _keyword_rules(categories)
    titles = frame["title"].fill_null("").to_list()
    summaries = frame["summary"].fill_null("").to_list()
    search = [normalize_text(f"{title} {summary}") for title, summary in zip(titles, summaries)]
    frame = frame.with_columns(
        pl.Series("search_text", search, dtype=pl.String),
        cpv_main=pl.col("cpv_codes").list.first(),
    )
    if rules:
        frame = frame.with_columns(
            [pl.col("search_text").str.contains(pattern).alias(f"__kw{index}")
             for index, (_, _, pattern) in enumerate(rules)]
        )
        matched = pl.concat_list(
            [
                pl.when(pl.col(f"__kw{index}")).then(pl.lit(keyword)).otherwise(pl.lit(None, dtype=pl.String))
                for index, (_, keyword, _) in enumerate(rules)
            ]
        ).list.drop_nulls().list.unique().list.sort()
        any_match = pl.any_horizontal([pl.col(f"__kw{index}") for index in range(len(rules))])
        category_flags: list[pl.Expr] = []
        for definition in categories:
            indexes = [index for index, (owner, _, _) in enumerate(rules) if owner == str(definition["name"])]
            if indexes:
                category_flags.append(
                    pl.when(pl.any_horizontal([pl.col(f"__kw{index}") for index in indexes]))
                    .then(pl.lit(str(definition["name"])))
                )
    else:
        matched = pl.lit([], dtype=pl.List(pl.String))
        any_match = pl.lit(False)
        category_flags = []
    cpv_hint = _cpv_expression(cpv_map)
    category_chain = [*category_flags]
    if cpv_hint is not None:
        category_chain.append(cpv_hint)
    category_chain.append(pl.lit("Other"))
    source_chain = [pl.when(any_match).then(pl.lit("keywords"))]
    if cpv_hint is not None:
        source_chain.append(pl.when(cpv_hint.is_not_null()).then(pl.lit("cpv")))
    source_chain.append(pl.lit("none"))
    return frame.with_columns(
        matched_keywords=matched,
        technology_score=matched.list.len(),
        category=pl.coalesce(category_chain),
        category_source=pl.coalesce(source_chain),
    ).drop([name for name in frame.columns if name.startswith("__kw") or name in {"search_text", "cpv_main"}])


def score_tender(
    tender: TenderRecord,
    categories: list[dict[str, Any]],
    cpv_map: dict[str, list[str]] | None = None,
) -> OpportunityRecord:
    """Score one record through the same vectorized implementation."""

    frame = pl.DataFrame(
        {
            "title": [tender.title],
            "summary": [tender.summary or ""],
            "cpv_codes": [[tender.cpv_main] if tender.cpv_main else []],
        },
        schema={"title": pl.String, "summary": pl.String, "cpv_codes": pl.List(pl.String)},
    )
    row = score_frame(frame, categories, cpv_map).row(0, named=True)
    return OpportunityRecord(
        tender=tender,
        category=row["category"],
        technology_score=row["technology_score"],
        matched_keywords=tuple(row["matched_keywords"]),
        category_source=row["category_source"],
    )
