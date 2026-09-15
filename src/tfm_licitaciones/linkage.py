"""Cross-source record linkage with blocking and TF-IDF title similarity."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from .classify import normalize_text
from .models import TenderRecord


@dataclass(frozen=True)
class LinkageResult:
    """Grouped linkage outcomes ready for gold enrichment."""

    assignments: dict[tuple[str, str], dict[str, Any]]
    stats: dict[str, Any]


def _buyer_key(record: TenderRecord) -> str:
    """Build a normalized blocking key from the buyer name."""

    if record.buyer:
        tokens = [token for token in normalize_text(record.buyer).split() if len(token) > 2]
        return " ".join(tokens[:4]) or "sin-comprador"
    return "sin-comprador"


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


def link_duplicates(
    records: list[TenderRecord],
    *,
    window_days: int = 7,
    threshold: float = 0.75,
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

    blocks: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        division = (record.cpv_main or "")[:2] or "xx"
        blocks[(_buyer_key(record), division)].append(index)

    parents = list(range(len(records)))

    def find(node: int) -> int:
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    def union(left: int, right: int) -> None:
        parents[find(left)] = find(right)

    candidate_pairs = 0
    evaluated_pairs = 0
    linked_pairs = 0
    window = timedelta(days=window_days)
    for indexes in blocks.values():
        dated = sorted(
            (index for index in indexes if records[index].published_date is not None),
            key=lambda index: records[index].published_date,
        )
        if len({records[index].source for index in dated}) < 2:
            continue
        pair_targets: list[tuple[int, int]] = []
        for position, index in enumerate(dated):
            published = records[index].published_date
            for other in dated[position + 1 :]:
                if (records[other].published_date - published) > window:
                    break
                if records[other].source == records[index].source:
                    continue
                pair_targets.append((index, other))
        candidate_pairs += len(pair_targets)
        if not pair_targets:
            continue
        involved = sorted({index for pair in pair_targets for index in pair})
        vectors = _tfidf_vectors([records[index].title or "" for index in involved])
        lookup = dict(zip(involved, vectors))
        for left, right in pair_targets:
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
    for index in range(len(records)):
        groups[find(index)].append(index)
    linked = sorted(
        (members for members in groups.values() if len(members) > 1),
        key=lambda members: min((records[index].source, records[index].tender_id) for index in members),
    )
    assignments: dict[tuple[str, str], dict[str, Any]] = {}
    for group_counter, members in enumerate(linked, start=1):
        canonical = min(
            members,
            key=lambda index: (
                records[index].published_date or date.max,
                -len(records[index].title or ""),
                records[index].tender_id,
                records[index].source,
            ),
        )
        for index in members:
            record = records[index]
            assignments[(record.source, record.tender_id)] = {
                "dup_group": group_counter,
                "is_canonical": index == canonical,
                "duplicate_of": None if index == canonical else records[canonical].tender_id,
            }
    stats = {
        "candidate_pairs": candidate_pairs,
        "evaluated_pairs": evaluated_pairs,
        "linked_pairs": linked_pairs,
        "linked_groups": len(linked),
        "duplicated_records": sum(len(members) - 1 for members in linked),
        "records_without_date": sum(record.published_date is None for record in records),
        "threshold": threshold,
        "window_days": window_days,
    }
    return LinkageResult(assignments=assignments, stats=stats)
