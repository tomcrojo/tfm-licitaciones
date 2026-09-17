"""R4 guard regressions through Raw + retrieval evidence -> Bronze -> public API.

The native kernel is deliberately not repaired for out-of-domain inputs.
These tests require the complete original frames to reach the frozen reference
before native runs, and retain native controls for canonical input shapes.
"""
from __future__ import annotations

import itertools
import unittest
from decimal import Decimal
from unittest import mock

import polars as pl

from test_silver import BOE_PAYLOAD, PLACSP_PAYLOAD, TED_PAYLOAD, tombstone_row
from test_silver_native import RawBronzeHarness
from tfm_licitaciones import silver
from tfm_licitaciones.bronze import tombstone_frame
from tfm_licitaciones.silver_guard import (
    REASON_AMOUNT,
    REASON_COUNTRY,
    REASON_KEY_DOMAIN,
    assess_native_eligibility,
)
from tfm_licitaciones.silver_parity import assert_silver_parity
from tfm_licitaciones.silver_reference import build_procurement_events_reference

TED = {"_source": "ted", "ND": "N1", "TI": "Title"}
PLACSP = {"_source": "placsp", "tender_no": "1", "atom_id": "P1", "title": "Title"}
AMOUNT = 123456789012345.67


class R4GuardTests(RawBronzeHarness, unittest.TestCase):
    @staticmethod
    def _run_engine(fn, records, tombstones):
        try:
            return "frame", fn(records, tombstones)
        except Exception as exc:
            return "error", (type(exc), str(exc))

    def _check_frames(self, records, tombstones, reason=None):
        eligibility = assess_native_eligibility(records, tombstones)
        self.assertEqual(eligibility.eligible, reason is None)
        self.assertEqual(eligibility.reason, reason)
        expected = self._run_engine(build_procurement_events_reference, records, tombstones)
        with mock.patch.object(
            silver, "build_procurement_events_native", wraps=silver.build_procurement_events_native,
        ) as native, mock.patch.object(
            silver, "build_procurement_events_reference", wraps=build_procurement_events_reference,
        ) as reference:
            actual = self._run_engine(silver.build_procurement_events, records, tombstones)
        selected, excluded = (native, reference) if reason is None else (reference, native)
        selected.assert_called_once()
        excluded.assert_not_called()
        self.assertIs(selected.call_args.args[0], records)
        self.assertIs(selected.call_args.args[1], tombstones)
        self.assertEqual(actual[0], expected[0])
        if expected[0] == "frame":
            assert_silver_parity(actual[1], expected[1])
        else:
            self.assertEqual(actual[1], expected[1])
        return actual[1]

    def _check(self, payloads, reason=None):
        records, tombstones = self._frames(payloads[0]["_source"], payloads)
        self.assertEqual(records.height, len(payloads))
        return self._check_frames(records, tombstones, reason)

    def test_r4_1_quote_key_does_not_collapse_two_identities(self):
        payloads = [{**TED, "ND": [], "notice_id": n, '"ND': "unused"} for n in ("N1", "N2")]
        for order in (payloads, list(reversed(payloads))):
            with self.subTest(order=[p["notice_id"] for p in order]):
                result = self._check(order, REASON_KEY_DOMAIN)
                self.assertEqual(result["event_id"].to_list(), ["ted:notice:N1", "ted:notice:N2"])

    def test_r4_1_quote_keys_preserve_text_codes_and_amount_presence(self):
        cases = [
            ({**TED, "TI": ["Real"], '"TI': "unused"}, "title", "Real"),
            ({**TED, "PC": [], '"PC': "unused"}, "cpv_codes", []),
            ({**PLACSP, "amount_tax_exclusive": 12, '"amount_estimated_overall': None},
             "estimated_value", Decimal("12.00")),
            ({**TED, "TI": {'"spa': "X", "spa": ["Y"]}}, "title", "Y"),
        ]
        for payload, field, value in cases:
            with self.subTest(payload=payload):
                self.assertEqual(self._check([payload], REASON_KEY_DOMAIN).to_dicts()[0][field], value)

    def test_r4_2_colon_keys_preserve_text_and_url(self):
        cases = [
            ({**TED, "TI": {"x:": "Y"}}, "title", "Y"),
            ({**TED, "links": {"html": {"x:": "https://x.invalid"}}}, "source_url", "https://x.invalid"),
            ({**BOE_PAYLOAD, "summary": {"x: ": ["", "Y"]}}, "description", "Y"),
        ]
        for payload, field, value in cases:
            with self.subTest(payload=payload):
                self.assertEqual(self._check([payload], REASON_KEY_DOMAIN)[field][0], value)

    def test_r4_2_equivalent_titles_do_not_create_a_false_collision(self):
        payloads = [{**TED, "TI": {"x:": "Y"}}, {**TED, "TI": "Y"}]
        for order in itertools.permutations(payloads):
            result = self._check(list(order), REASON_KEY_DOMAIN)
            self.assertEqual(result.height, 1)
            self.assertEqual(result["title"][0], "Y")

    def test_r4_2_distinct_titles_still_raise_a_collision(self):
        payloads = [{**TED, "TI": {"x:": title}} for title in ("Y", "Z")]
        for order in itertools.permutations(payloads):
            error_type, message = self._check(list(order), REASON_KEY_DOMAIN)
            self.assertIs(error_type, ValueError)
            self.assertEqual(message, "Conflicting canonical mappings for event_id 'ted:notice:N1'; differing fields: title")

    def test_r4_3_floats_without_retained_text_preserve_exact_amount(self):
        for payload in [
            {**TED, "estimated-value-lot": AMOUNT},
            {**PLACSP, "amount_estimated_overall": AMOUNT},
            {**PLACSP, "amount_estimated_overall": AMOUNT, "amount_estimated_overall_raw": None},
            {**PLACSP, "amount_tax_exclusive": AMOUNT},
        ]:
            with self.subTest(payload=payload):
                result = self._check([payload], REASON_AMOUNT)
                self.assertEqual(result["estimated_value"][0], Decimal("123456789012345.67"))

    def test_r4_3_float_and_text_do_not_create_a_false_collision(self):
        payloads = [{**TED, "estimated-value-lot": value} for value in (AMOUNT, str(AMOUNT))]
        for order in itertools.permutations(payloads):
            result = self._check(list(order), REASON_AMOUNT)
            self.assertEqual(result.height, 1)
            self.assertEqual(result["estimated_value"][0], Decimal(str(AMOUNT)))

    def test_r4_3_even_unchosen_float_amounts_require_fallback(self):
        for payload in [
            {**TED, "estimated-value-lot": "1.00", "amount": AMOUNT},
            {**PLACSP_PAYLOAD, "amount_tax_exclusive": AMOUNT, "amount_tax_exclusive_raw": None},
        ]:
            with self.subTest(payload=payload):
                self._check([payload], REASON_AMOUNT)

    def test_r4_4_non_ascii_countries_fall_back_in_both_directions(self):
        for value in ("A中", "ÉS", "\u1c89A", "A\ua7cb", "\U00010d50A", "\ua7daA", "é", "\xa0ES"):
            for key in ("CY", "country"):
                with self.subTest(key=key, value=value):
                    self._check([{**TED, key: value}], REASON_COUNTRY)

    def test_key_alphabet_applies_at_every_depth_including_ignored_fields(self):
        for key in ('"ND', "x:", "x: ", "", "a b", "a.b", "a/b", "a\\b", "clé", "x\n"):
            for override in ({key: "unused"}, {"unused": [{"safe": [{key: "unused"}]}]}):
                with self.subTest(key=key, override=override):
                    result = self._check([{**TED, **override}], REASON_KEY_DOMAIN)
                    self.assertEqual(result["event_id"][0], "ted:notice:N1")

    def test_canonical_amount_and_key_controls_still_execute_native(self):
        # Punctuation/Unicode in VALUES is unchanged; only KEYS are restricted.
        controls = [
            {**TED_PAYLOAD, "unused_0-A": {"_0-Z": 'clé: "ND": "not structure"'}},
            {**TED, "estimated-value-lot": 999999999999999999},
            {**TED, "estimated-value-lot": "999999999999999999.99"},
            PLACSP_PAYLOAD,
            {**PLACSP, "amount_estimated_overall": AMOUNT, "amount_estimated_overall_raw": str(AMOUNT)},
            {**PLACSP, "amount_tax_exclusive": AMOUNT, "amount_tax_exclusive_raw": "1.25"},
            BOE_PAYLOAD,
        ]
        for payload in controls:
            with self.subTest(payload=payload):
                self._check([payload])
        result = self._check([controls[4]])
        self.assertEqual(result["estimated_value"][0], Decimal(str(AMOUNT)))
        result = self._check([controls[5]])
        self.assertEqual(result["estimated_value"][0], Decimal("1.25"))

    def test_ascii_countries_keep_native_values_and_nulls(self):
        for value, expected in ((None, None), ("", None), (" ESP ", "ES"), ("ES", "ES"),
                                ("AB", "AB"), ("es", None), ("A1", None), ("Spain", None)):
            with self.subTest(value=value):
                result = self._check([{**TED, "CY": value}])
                self.assertEqual(result["country"][0], expected)

    def test_one_ineligible_row_routes_whole_mixed_batch_and_tombstones(self):
        for bad, reason in [
            ({**TED, "TI": ["Real"], '"TI': "unused"}, REASON_KEY_DOMAIN),
            ({**TED, "TI": {"x:": "Y"}}, REASON_KEY_DOMAIN),
            ({**TED, "estimated-value-lot": AMOUNT}, REASON_AMOUNT),
            ({**TED, "CY": "\u1c89A"}, REASON_COUNTRY),
        ]:
            with self.subTest(reason=reason, payload=bad):
                frames = [self._frames(p["_source"], [p])[0] for p in (bad, PLACSP_PAYLOAD, BOE_PAYLOAD)]
                records = pl.concat(frames)
                tombstones = tombstone_frame([tombstone_row("P1")])
                for ordered in (records, records.reverse()):
                    result = self._check_frames(ordered, tombstones, reason)
                    self.assertEqual(result.height, 4)
                    self.assertIn("placsp:tombstone:P1", result["event_id"].to_list())

    def test_fallback_preserves_first_error_ahead_of_a_hidden_collision(self):
        payloads = [{**TED, "TI": {"x:": title}} for title in ("Y", "Z")]
        payloads += [{**TED, "ND": "A", "estimated-value-lot": " inf "}]
        for order in itertools.permutations(payloads):
            # The first ineligible row may be the key or the amount. Either
            # way reference must see every row in the original order.
            records, tombstones = self._frames("ted", list(order))
            reason = assess_native_eligibility(records, tombstones).reason
            error_type, message = self._check_frames(records, tombstones, reason)
            self.assertIs(error_type, ValueError)
            self.assertEqual(message, "ted:notice:A: amount 'inf' is not representable as decimal(20,2)")
