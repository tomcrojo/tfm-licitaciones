"""Cross-source record linkage with blocking and TF-IDF title similarity.

Engine boundary: blocking and candidate-pair generation run as a native
self-join in Polars; TF-IDF scoring operates only on the records involved in
candidate pairs and union-find over the linked edges stays in Python because
it is inherently iterative.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import polars as pl

from .classify import normalize_text
from .models import TenderRecord


@dataclass(frozen=True)
class LinkageResult:
    """Grouped linkage outcomes ready for gold enrichment."""

    assignments: dict[tuple[str, str], dict[str, Any]]
    stats: dict[str, Any]


_LINKAGE_SCHEMA = {
    "source": pl.String,
    "tender_id": pl.String,
    "title": pl.String,
    "buyer": pl.String,
    "published_date": pl.Date,
    "cpv_codes": pl.List(pl.String),
}


def _tokenize(text: str) -> list[str]:
    """Split normalized text into alphanumeric word tokens."""

    tokens: list[str] = []
    current: list[str] = []
    for char in normalize_text(text):
        if char.isalnum():
            current.append(char)
        elif current:
            token = "".join(current)
            if len(token) > 2:
                tokens.append(token)
            current = []
    tail = "".join(current)
    if len(tail) > 2:
        tokens.append(tail)
    return tokens


def _tfidf_vectors(documents: list[str]) -> list[dict[int, float]]:
    """Compute unit TF-IDF vectors with smoothed inverse document frequency."""

    tokenized = [_tokenize(document) for document in documents]
    vocabulary: dict[str, int] = {}
    for tokens in tokenized:
        for token in tokens:
            vocabulary.setdefault(token, len(vocabulary))
    document_frequency: defaultdict[int, int] = defaultdict(int)
    for tokens in tokenized:
        for token in set(tokens):
            document_frequency[vocabulary[token]] += 1
    total = len(tokenized)
    vectors: list[dict[int, float]] = []
    for tokens in tokenized:
        counts: defaultdict[int, int] = defaultdict(int)
        for token in tokens:
            counts[vocabulary[token]] += 1
        vector: dict[int, float] = {}
        for index, count in counts.items():
            weight = (1.0 + count) * math.log((1.0 + total) / (1.0 + document_frequency[index])) + 1.0
            vector[index] = weight
        norm = math.sqrt(sum(weight * weight for weight in vector.values()))
        vectors.append({index: weight / norm for index, weight in vector.items()} if norm else {})
    return vectors


def _blocking_keys(frame: pl.DataFrame) -> pl.DataFrame:
    """Add the normalized buyer and CPV-division blocking keys."""

    buyers = [normalize_text(buyer) if buyer else "" for buyer in frame["buyer"].to_list()]
    tokens = (
        pl.col("buyer_norm")
        .str.split(" ")
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 2))
        .list.head(4)
        .list.join(" ")
    )
    division = pl.col("cpv_codes").list.first().str.slice(0, 2)
    return (
        frame.with_columns(pl.Series("buyer_norm", buyers, dtype=pl.String))
        .with_columns(
            buyer_key=pl.when(tokens.str.len_chars() > 0).then(tokens).otherwise(pl.lit("sin-comprador")),
            division=pl.when(division.fill_null("").str.len_chars() > 0)
            .then(division)
            .otherwise(pl.lit("xx")),
        )
        .drop("buyer_norm")
    )


def _candidate_pairs(
    dated: pl.DataFrame, window_days: int, pair_budget: int
) -> list[tuple[int, int]]:
    """Discover cross-source pairs with a memory-linear sliding window.

    Blocking keys are assigned by the engine, but the pairs themselves are
    discovered per block with a date-sorted sliding walk: records of a single
    source inside a block never become pairs, and only in-window ranges are
    visited. Materializing the block self-join instead would allocate every
    same-source pair before filtering it away, which is exactly how
    mega-blocks OOM the runner. The pair list is the only quadratic
    structure and is capped by ``pair_budget`` with a loud error.
    """

    window = timedelta(days=window_days)
    index_of = dict(enumerate(dated["index"].to_list()))
    buyers = dated["buyer_key"].to_list()
    divisions = dated["division"].to_list()
    sources = dated["source"].to_list()
    dates = dated["published_date"].to_list()

    blocks: dict[tuple[str, str], list[int]] = defaultdict(list)
    for order, buyer in enumerate(buyers):
        blocks[(buyer, divisions[order])].append(order)

    pairs: list[tuple[int, int]] = []
    for members in blocks.values():
        members.sort(key=lambda order: dates[order])
        if len({sources[order] for order in members}) < 2:
            continue
        for cursor, order in enumerate(members):
            published = dates[order]
            for other in members[cursor + 1 :]:
                if (dates[other] - published) > window:
                    break
                if sources[other] == sources[order]:
                    continue
                pairs.append((index_of[order], index_of[other]))
                if len(pairs) > pair_budget:
                    raise ValueError(
                        f"Linkage pair budget exceeded (>{pair_budget} candidate pairs). "
                        "Refine blocking or run the Spark linkage path."
                    )
    return pairs


def link_frame(
    frame: pl.DataFrame,
    *,
    window_days: int = 7,
    threshold: float = 0.75,
    pair_budget: int = 5_000_000,
) -> LinkageResult:
    """Group records that describe the same procedure across sources.

    Blocking uses the normalized buyer plus the CPV division. Only records
    from different sources may become candidate pairs, and candidates must
    fall inside a publication window whose ends are inclusive; they are
    then scored by TF-IDF cosine similarity over title tokens. Summaries
    are structurally different across sources (TED descriptions vs PLACSP
    metadata lines), so only titles participate in the score. Every
    cross-source pair inside the window is counted as a candidate and no
    block is ever skipped for size; pairs whose side has an empty or
    untokenizable title are counted but not scored, so ``evaluated_pairs``
    may be lower than ``candidate_pairs``. Linked pairs are merged with
    union-find, so same-source records may share a group when a record
    from another source bridges them, but never through a direct
    same-source pair. Each group keeps one canonical notice chosen by
    earliest publication date, then longest title, then tender id and
    source. Group numbers are assigned after sorting groups by their
    smallest ``(source, tender_id)`` member, so they do not depend on
    input order. Records without a publication date can never be
    window-checked and are therefore never candidates; ``stats`` reports
    how many were excluded.
    """

    prepared = _blocking_keys(
        frame.select(
            pl.col("source"),
            pl.col("tender_id"),
            pl.col("title").fill_null(""),
            pl.col("buyer"),
            pl.col("published_date"),
            pl.col("cpv_codes"),
        ).with_row_index("index")
    )
    dated = prepared.drop_nulls("published_date")
    pairs = _candidate_pairs(dated, window_days, pair_budget)
    candidate_pairs = len(pairs)

    sources = prepared["source"].to_list()
    tender_ids = prepared["tender_id"].to_list()
    titles = prepared["title"].to_list()
    dates = prepared["published_date"].to_list()

    parents = list(range(len(sources)))

    def find(node: int) -> int:
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    def union(left: int, right: int) -> None:
        parents[find(left)] = find(right)

    evaluated_pairs = 0
    linked_pairs = 0
    if candidate_pairs:
        involved = sorted({index for pair in pairs for index in pair})
        vectors = _tfidf_vectors([titles[index] for index in involved])
        lookup = dict(zip(involved, vectors))
        for left, right in pairs:
            left_vector, right_vector = lookup[left], lookup[right]
            if not left_vector or not right_vector:
                continue
            evaluated_pairs += 1
            similarity = sum(
                weight * right_vector[index] for index, weight in left_vector.items() if index in right_vector
            )
            if similarity >= threshold:
                union(left, right)
                linked_pairs += 1

    groups: defaultdict[int, list[int]] = defaultdict(list)
    for index in range(len(sources)):
        groups[find(index)].append(index)
    linked = sorted(
        (members for members in groups.values() if len(members) > 1),
        key=lambda members: min((sources[index], tender_ids[index]) for index in members),
    )
    assignments: dict[tuple[str, str], dict[str, Any]] = {}
    for group_counter, members in enumerate(linked, start=1):
        canonical = min(
            members,
            key=lambda index: (
                dates[index] or date.max,
                -len(titles[index]),
                tender_ids[index],
                sources[index],
            ),
        )
        for index in members:
            assignments[(sources[index], tender_ids[index])] = {
                "dup_group": group_counter,
                "is_canonical": index == canonical,
                "duplicate_of": None if index == canonical else tender_ids[canonical],
            }
    stats = {
        "candidate_pairs": candidate_pairs,
        "evaluated_pairs": evaluated_pairs,
        "linked_pairs": linked_pairs,
        "linked_groups": len(linked),
        "duplicated_records": sum(len(members) - 1 for members in linked),
        "records_without_date": sum(published is None for published in dates),
        "threshold": threshold,
        "window_days": window_days,
    }
    return LinkageResult(assignments=assignments, stats=stats)


def link_duplicates(
    records: list[TenderRecord],
    *,
    window_days: int = 7,
    threshold: float = 0.75,
    pair_budget: int = 5_000_000,
) -> LinkageResult:
    """Row-level API over the frame implementation."""

    frame = pl.DataFrame(
        {
            "source": [record.source for record in records],
            "tender_id": [record.tender_id for record in records],
            "title": [record.title for record in records],
            "buyer": [record.buyer for record in records],
            "published_date": [record.published_date for record in records],
            "cpv_codes": [[record.cpv_main] if record.cpv_main else [] for record in records],
        },
        schema=_LINKAGE_SCHEMA,
    )
    return link_frame(frame, window_days=window_days, threshold=threshold, pair_budget=pair_budget)
