"""Manual differential fuzzer for the hybrid canonical Silver guard.

Not collected by unittest discovery (the file does not match ``test*.py``)
and never part of CI: the permanent differential, routing and boundary
assertions live in ``tests/test_silver_native.py``. This harness only bounds
a synthetic mutation corpus of the checked-in fixtures; finishing without
divergences is not a universal equivalence proof.

Reproducible runs from the repository root (Python 3.11.16, Polars 1.44.2):

    uv run --with-editable . python tests/fuzz_silver_guard.py --iterations 3000 --seed 20260917
    uv run --with-editable . python tests/fuzz_silver_guard.py --iterations 5000 --seed 4242

Each iteration mutates 1-4 top-level fields of a TED/PLACSP/BOE fixture with
adversarial values (numeric boundaries, control separators, unicode letters,
infinity spellings, nested containers, malformed timestamps, ...), builds the
typed Bronze frames, asks the guard and compares the public API against the
frozen reference (and the native kernel directly when the batch is eligible).
Exit status 1 means a divergence was found and printed as JSON.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter

from test_silver import BOE_PAYLOAD, PLACSP_PAYLOAD, TED_PAYLOAD, record_row, tombstone_row
from tfm_licitaciones.bronze import bronze_frame, tombstone_frame
from tfm_licitaciones.silver import build_procurement_events as public
from tfm_licitaciones.silver_guard import assess_native_eligibility
from tfm_licitaciones.silver_native import build_procurement_events_native as native
from tfm_licitaciones.silver_parity import assert_silver_parity
from tfm_licitaciones.silver_reference import build_procurement_events_reference as reference

VALUES = [
    None, True, False, 0, 1, -1, 125000.5, 123456789012345.67, 1e-5, 1e18,
    2**63 - 1, 2**63, 10**100,
    "", " ", "  ", "\t", "\x1c", "\x1cN1\x1c", "N1", "ÉS", "A中", "es", "AB", "ESP",
    "1.5", "+5", "-5", "1e3", " inf ", "i n f", "inf..", "Infinity", "nan", "abc",
    "n/a", "2026-01-05", "2026-01-05Z", "31/12/2025", "2026-13-01",
    "2026-01-08T10:00:00Z", {"spa": "T"}, {"spa": ""}, {"eng": ["x"]}, {"x": "y"},
    {}, [], ["a", "b"], [None], [1], [["nested"]], "https://example.invalid/x",
    {"x:": "Y"}, {'"spa': "X", "spa": ["Y"]}, "\u1c89A", "A\ua7cb",
    {"html": {"x:": "https://example.invalid"}},
]
TED_KEYS = [
    "ND", "notice_id", "TI", "title", "PC", "cpv", "estimated-value-lot",
    "framework-maximum-value-lot", "amount", "currency", "CY", "country",
    "url", "PD", "publication_date", "notice-type", "links", "unused", '"ND', '"TI',
]
PLACSP_KEYS = [
    "atom_id", "updated", "title", "summary", "cpv", "url",
    "amount_estimated_overall", "amount_estimated_overall_raw",
    "amount_tax_exclusive", "amount_tax_exclusive_raw",
    "amount_estimated_overall_currency", "unused", '"amount_estimated_overall',
]
BOE_KEYS = [
    "item_id", "identificador", "title", "titulo", "summary", "buyer",
    "department", "publication_date", "url", "unused",
]
BASES = {
    "ted": (TED_PAYLOAD, TED_KEYS),
    "placsp": (PLACSP_PAYLOAD, PLACSP_KEYS),
    "boe": (BOE_PAYLOAD, BOE_KEYS),
}


def _outcome(fn, records, tombstones) -> tuple[str, object]:
    try:
        return "OK", fn(records, tombstones)
    except Exception as exc:  # noqa: BLE001
        return "RAISE", f"{type(exc).__name__}: {exc}"


def _nested(levels: int):
    value = "leaf"
    for _ in range(levels):
        value = [value]
    return value


def run(iterations: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    divergences: list[dict] = []
    eligible = 0
    for index in range(iterations):
        source = rng.choice(list(BASES))
        base, keys = BASES[source]
        payload = dict(base)
        for _ in range(rng.randint(1, 4)):
            payload[rng.choice(keys)] = rng.choice(VALUES)
        if rng.random() < 0.15:
            payload["unused"] = _nested(rng.randint(60, 140))
        rows = [record_row(payload, source=source, source_file=f"{source}/f.jsonl")]
        tombstones = [tombstone_row("https://example.invalid/t")]
        records, tomb = bronze_frame(rows), tombstone_frame(tombstones)
        verdict = assess_native_eligibility(records, tomb)
        expected = _outcome(reference, records, tomb)
        actual = _outcome(public, records, tomb)
        if expected[0] != actual[0] or (expected[0] == "RAISE" and expected[1] != actual[1]):
            divergences.append({
                "seed": seed, "index": index, "kind": "public_status_or_error",
                "payload": payload, "reference": expected, "public": actual,
            })
            continue
        if verdict.eligible:
            eligible += 1
            native_outcome = _outcome(native, records, tomb)
            if native_outcome[0] != expected[0]:
                divergences.append({
                    "seed": seed, "index": index, "kind": "native_status",
                    "payload": payload, "reference": expected, "native": native_outcome,
                })
            elif expected[0] == "RAISE" and native_outcome[1] != expected[1]:
                divergences.append({
                    "seed": seed, "index": index, "kind": "native_error",
                    "payload": payload, "reference": expected[1], "native": native_outcome[1],
                })
            elif expected[0] == "OK":
                try:
                    assert_silver_parity(native_outcome[1], expected[1])
                except AssertionError as exc:
                    divergences.append({
                        "seed": seed, "index": index, "kind": "native_frame",
                        "payload": payload, "detail": str(exc)[:200],
                    })
        elif expected[0] == "OK":
            try:
                assert_silver_parity(actual[1], expected[1])
            except AssertionError as exc:
                divergences.append({
                    "seed": seed, "index": index, "kind": "fallback_frame",
                    "payload": payload, "detail": str(exc)[:200],
                })
    print(json.dumps({
        "seed": seed, "iterations": iterations, "eligible": eligible,
        "divergences": len(divergences),
    }, ensure_ascii=False))
    for item in divergences[:10]:
        print(json.dumps(item, ensure_ascii=False, default=str))
    return divergences


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--iterations", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=20260917)
    arguments = parser.parse_args()
    return 1 if run(arguments.iterations, arguments.seed) else 0


if __name__ == "__main__":
    raise SystemExit(main())
