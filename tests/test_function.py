"""
Offline tests for the Rossum -> FinancialDocDesc -> PostBin hook.

No network: the two Rossum calls and the sink POST are patched. Run with

    python -m unittest discover -s tests -v
"""
import base64
import json
import os
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

import requests

import function
from function import (
    Control,
    EInvoice,
    Line,
    _is_zero_rate,
    map_document,
    parse_export,
    read_config,
    resolve_queue_id,
    rossum_hook_request_handler,
    to_xml,
)

SAMPLES = os.path.join(os.path.dirname(__file__), os.pardir, "samples")
ANNOTATION_ID = "12345678"


def load_sample():
    with open(os.path.join(SAMPLES, "sample_export.json"), encoding="utf-8") as handle:
        return json.load(handle)


def datapoint(schema_id, value):
    return {"category": "datapoint", "schema_id": schema_id, "value": value}


def tuple_of(*datapoints):
    return {"category": "tuple", "schema_id": "row", "children": list(datapoints)}


def multivalue(schema_id, *tuples):
    return {"category": "multivalue", "schema_id": schema_id, "children": list(tuples)}


def export_with(content, annotation_id=ANNOTATION_ID):
    """Wrap content nodes in a minimal but realistic export envelope."""
    return {
        "results": [
            {
                "url": f"https://example.rossum.app/api/v1/annotations/{annotation_id}",
                "content": content,
            }
        ]
    }


def base_payload(**overrides):
    payload = {
        "rossum_authorization_token": "test-token",
        "base_url": "https://example.rossum.app",
        "annotationId": ANNOTATION_ID,
        "settings": {"postbin_url": "https://www.postb.in/BIN"},
    }
    payload.update(overrides)
    return payload


class ParseExportTests(unittest.TestCase):
    def test_parses_sample_into_fields_and_rows(self):
        fields, rows = parse_export(load_sample(), ANNOTATION_ID)

        self.assertEqual(fields["document_id"], "INV-2024-0042")
        self.assertEqual(fields["sender_vat_id"], "CZ12345678")
        self.assertEqual(fields["amount_total"], "332.50")
        self.assertEqual(len(rows["line_items"]), 3)
        self.assertEqual(len(rows["tax_details"]), 2)

    def test_row_values_never_leak_into_header_fields(self):
        """item_* ids live only in rows; a header lookup must not see them."""
        fields, rows = parse_export(load_sample(), ANNOTATION_ID)

        for leaked in ("item_code", "item_amount", "tax_detail_rate"):
            self.assertNotIn(leaked, fields)
        self.assertEqual(rows["line_items"][0]["item_code"], "ACM-1001")

    def test_empty_results_is_a_clear_error(self):
        with self.assertRaises(ValueError) as caught:
            parse_export({"results": []}, ANNOTATION_ID)
        self.assertIn("exportable", str(caught.exception))

    def test_missing_results_key_is_a_clear_error(self):
        with self.assertRaises(ValueError):
            parse_export({}, ANNOTATION_ID)

    def test_export_for_a_different_annotation_is_rejected(self):
        with self.assertRaises(ValueError) as caught:
            parse_export(export_with([], annotation_id="99999999"), ANNOTATION_ID)
        self.assertIn("expected annotation", str(caught.exception))

    def test_identity_check_compares_path_segment_not_substring(self):
        """'123' is a substring of '.../1234' but must not be accepted."""
        with self.assertRaises(ValueError):
            parse_export(export_with([], annotation_id="1234"), "123")

    def test_first_non_empty_value_wins_for_a_repeated_schema_id(self):
        fields, _ = parse_export(
            export_with(
                [
                    {
                        "category": "section",
                        "children": [
                            datapoint("document_id", ""),
                            datapoint("document_id", "FIRST"),
                            datapoint("document_id", "SECOND"),
                        ],
                    }
                ]
            ),
            ANNOTATION_ID,
        )
        self.assertEqual(fields["document_id"], "FIRST")

    def test_all_blank_rows_are_dropped(self):
        _, rows = parse_export(
            export_with(
                [
                    multivalue(
                        "line_items",
                        tuple_of(datapoint("item_code", ""), datapoint("item_amount", "")),
                        tuple_of(datapoint("item_code", "REAL")),
                    )
                ]
            ),
            ANNOTATION_ID,
        )
        self.assertEqual(rows["line_items"], [{"item_code": "REAL"}])

    def test_values_are_normalised_to_text(self):
        fields, _ = parse_export(
            export_with(
                [
                    {
                        "category": "section",
                        "children": [
                            datapoint("currency", None),
                            datapoint("amount_total", 42.0),
                            datapoint("document_id", "  spaced  "),
                        ],
                    }
                ]
            ),
            ANNOTATION_ID,
        )
        self.assertEqual(fields["currency"], "")
        self.assertEqual(fields["amount_total"], "42")  # not "42.0"
        self.assertEqual(fields["document_id"], "spaced")


class MappingTests(unittest.TestCase):
    def map_sample(self, **kwargs):
        fields, rows = parse_export(load_sample(), ANNOTATION_ID)
        kwargs.setdefault("annotation_id", ANNOTATION_ID)
        kwargs.setdefault("today", "2024-05-20")
        return map_document(fields, rows, **kwargs)

    def test_header_fields_map_from_declared_schema_ids(self):
        einvoice, _, _ = self.map_sample()

        self.assertEqual(einvoice.InvoiceNumber, "INV-2024-0042")
        self.assertEqual(einvoice.InvoiceDate, "2024-05-14")
        self.assertEqual(einvoice.DueDate, "2024-06-13")
        self.assertEqual(einvoice.OrderNumber, "PO-77123")
        self.assertEqual(einvoice.Currency, "czk")
        self.assertEqual(einvoice.Total, "332.50")
        self.assertEqual(einvoice.CustormerID, "87654321")

    def test_vat_country_code_is_derived_from_the_vat_id_prefix(self):
        einvoice, _, _ = self.map_sample()
        self.assertEqual(einvoice.VatID, "CZ12345678")
        self.assertEqual(einvoice.VatCountryCode, "CZ")

    def test_numeric_vat_id_yields_no_country_code(self):
        einvoice, _, _ = map_document(
            {"sender_vat_id": "12345678"}, {}, annotation_id="1", today="2024-01-01"
        )
        self.assertEqual(einvoice.VatCountryCode, "")

    def test_one_letter_vat_prefix_is_not_a_country_code(self):
        einvoice, _, _ = map_document(
            {"sender_vat_id": "C1234"}, {}, annotation_id="1", today="2024-01-01"
        )
        self.assertEqual(einvoice.VatCountryCode, "")

    def test_tax_buckets_split_zero_rated_from_rated(self):
        einvoice, _, _ = self.map_sample()

        self.assertEqual(einvoice.NetAmount0, "30.00")  # the zero-rated base
        self.assertEqual(einvoice.NetAmount1, "250.00")
        self.assertEqual(einvoice.TaxAmount1, "52.50")
        self.assertEqual(einvoice.TaxRate1, "21")
        self.assertEqual(einvoice.NetAmount2, "")  # only one rated row present

    def test_blank_tax_row_does_not_consume_a_bucket(self):
        rows = {
            "tax_details": [
                {"tax_detail_rate": "", "tax_detail_base": "", "tax_detail_tax": ""},
                {"tax_detail_rate": "21", "tax_detail_base": "100", "tax_detail_tax": "21"},
            ]
        }
        einvoice, _, _ = map_document({}, rows, annotation_id="1", today="2024-01-01")

        self.assertEqual(einvoice.NetAmount1, "100")
        self.assertEqual(einvoice.TaxRate1, "21")

    def test_only_three_rated_buckets_are_emitted(self):
        rows = {
            "tax_details": [
                {"tax_detail_rate": str(r), "tax_detail_base": str(r * 10),
                 "tax_detail_tax": str(r)}
                for r in (5, 10, 15, 20)
            ]
        }
        einvoice, _, _ = map_document({}, rows, annotation_id="1", today="2024-01-01")

        self.assertEqual(einvoice.TaxRate3, "15")
        self.assertFalse(hasattr(einvoice, "TaxRate4"))

    def test_header_totals_are_the_fallback_when_no_tax_rows_exist(self):
        fields = {"amount_total_base": "280.00", "amount_total_tax": "52.50"}
        einvoice, _, _ = map_document(fields, {}, annotation_id="1", today="2024-01-01")

        self.assertEqual(einvoice.NetAmount1, "280.00")
        self.assertEqual(einvoice.TaxAmount1, "52.50")
        self.assertEqual(einvoice.TaxRate1, "")  # never invented

    def test_line_items_map_in_order(self):
        _, _, lines = self.map_sample()

        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0].ArticleCode, "ACM-1001")
        self.assertEqual(lines[0].Quantity, "2")
        self.assertEqual(lines[0].UnitPrice, "100.00")
        self.assertEqual(lines[0].TotalPrice, "200.00")
        self.assertEqual(lines[2].Description, "Printed manual (zero-rated)")

    def test_control_block_is_derived_not_extracted(self):
        _, control, _ = self.map_sample()

        self.assertEqual(control.Origin, "EMAIL")
        self.assertEqual(control.CurrentDate, "2024-05-20")
        self.assertEqual(control.Barcode, ANNOTATION_ID)

    def test_barcode_prefers_an_extracted_barcode_field(self):
        _, control, _ = map_document(
            {"barcode": "BC-9"}, {}, annotation_id=ANNOTATION_ID, today="2024-01-01"
        )
        self.assertEqual(control.Barcode, "BC-9")

    def test_fields_without_a_schema_id_stay_empty(self):
        """Discount and the routing addresses have no Rossum source."""
        _, control, lines = self.map_sample()

        self.assertEqual(control.EmailTo, "")
        self.assertEqual(control.EmailFrom, "")
        self.assertEqual(lines[0].Discount, "")

    def test_missing_fields_map_to_empty_strings_not_errors(self):
        einvoice, control, lines = map_document(
            {}, {}, annotation_id="1", today="2024-01-01"
        )
        self.assertEqual(einvoice.InvoiceNumber, "")
        self.assertEqual(lines, [])
        self.assertEqual(control.Barcode, "1")


class ZeroRateTests(unittest.TestCase):
    def test_recognises_zero_in_common_formats(self):
        for text in ("0", "0.00", "0,00", " 0 %", "0%"):
            self.assertTrue(_is_zero_rate(text), text)

    def test_non_zero_and_unparseable_are_not_zero(self):
        for text in ("21", "0.5", "", "n/a", "-"):
            self.assertFalse(_is_zero_rate(text), text)


class SerializationTests(unittest.TestCase):
    def test_document_structure_and_element_order(self):
        xml = to_xml(EInvoice(), Control(), [Line()])
        root = ET.fromstring(xml)

        self.assertEqual(root.tag, "FinancialDocDesc")
        self.assertEqual([child.tag for child in root], ["EInvoice", "Control", "Detail"])
        # Declaration order in the dataclass is the contract for element order.
        self.assertEqual(
            [e.tag for e in root.find("EInvoice")][:5],
            ["InvoiceType", "VatCountryCode", "VatID", "CustormerID", "InvoiceNumber"],
        )
        self.assertEqual(
            [e.tag for e in root.find("Detail/Lines")][:3],
            ["ArticleCode", "Description", "Quantity"],
        )

    def test_customer_id_keeps_the_typo_from_the_target_schema(self):
        root = ET.fromstring(to_xml(EInvoice(), Control(), []))
        self.assertIsNotNone(root.find("EInvoice/CustormerID"))
        self.assertIsNone(root.find("EInvoice/CustomerID"))

    def test_a_skeleton_lines_block_is_always_emitted(self):
        root = ET.fromstring(to_xml(EInvoice(), Control(), []))
        self.assertEqual(len(root.findall("Detail/Lines")), 1)

    def test_declaration_is_utf8(self):
        xml = to_xml(EInvoice(), Control(), [])
        self.assertTrue(xml.startswith(b"<?xml"))
        self.assertIn(b"utf-8", xml[:60].lower())

    def test_control_characters_are_stripped_rather_than_crashing(self):
        einvoice = EInvoice()
        einvoice.InvoiceNumber = "A\x0bB"  # vertical tab is illegal in XML 1.0
        xml = to_xml(einvoice, Control(), [])

        self.assertEqual(ET.fromstring(xml).findtext("EInvoice/InvoiceNumber"), "AB")

    def test_sanitisation_also_covers_derived_values(self):
        """Values set after parsing must pass through the same gate."""
        control = Control()
        control.Barcode = "B\x00C"
        xml = to_xml(EInvoice(), control, [])

        self.assertEqual(ET.fromstring(xml).findtext("Control/Barcode"), "BC")

    def test_markup_in_a_value_is_escaped_not_injected(self):
        einvoice = EInvoice()
        einvoice.InvoiceNumber = "<x>&</x>"
        root = ET.fromstring(to_xml(einvoice, Control(), []))

        self.assertEqual(root.findtext("EInvoice/InvoiceNumber"), "<x>&</x>")
        self.assertIsNone(root.find("EInvoice/InvoiceNumber/x"))


class ReadConfigTests(unittest.TestCase):
    def test_reads_a_manual_invoke_payload(self):
        config = read_config(base_payload())

        self.assertEqual(config.annotation_id, ANNOTATION_ID)
        self.assertEqual(config.sink_url, "https://www.postb.in/BIN")
        self.assertEqual(config.base_url, "https://example.rossum.app")

    def test_missing_fields_are_all_named_at_once(self):
        with self.assertRaises(ValueError) as caught:
            read_config({})
        message = str(caught.exception)

        for name in ("rossum_authorization_token", "base_url", "annotationId", "postbin_url"):
            self.assertIn(name, message)

    def test_postbin_url_in_the_body_overrides_the_stored_setting(self):
        config = read_config(base_payload(postbin_url="https://www.postb.in/FRESH"))
        self.assertEqual(config.sink_url, "https://www.postb.in/FRESH")

    def test_annotation_id_can_come_from_a_nested_annotation_object(self):
        payload = base_payload()
        del payload["annotationId"]
        payload["annotation"] = {"id": 555}
        config = read_config(payload)

        self.assertEqual(config.annotation_id, "555")

    def test_a_non_http_sink_is_rejected_before_any_api_call(self):
        for bad in ("file:///etc/passwd", "ftp://host/x", "not-a-url", "https://"):
            with self.assertRaises(ValueError, msg=bad):
                read_config(base_payload(postbin_url=bad))

    def test_token_is_kept_out_of_the_repr(self):
        config = read_config(base_payload())
        self.assertNotIn("test-token", repr(config))


class ResolveQueueTests(unittest.TestCase):
    def test_the_queue_id_is_the_last_segment_of_the_looked_up_url(self):
        api = mock.Mock(spec=function.RossumAPI)
        api.queue_url_of.return_value = "https://example.rossum.app/api/v1/queues/77/"

        self.assertEqual(resolve_queue_id(ANNOTATION_ID, api), "77")
        api.queue_url_of.assert_called_once_with(ANNOTATION_ID)

    def test_an_unusable_queue_url_is_a_clear_error(self):
        api = mock.Mock(spec=function.RossumAPI)
        api.queue_url_of.return_value = ""

        with self.assertRaises(ValueError) as caught:
            resolve_queue_id(ANNOTATION_ID, api)
        self.assertIn(ANNOTATION_ID, str(caught.exception))


class HandlerTests(unittest.TestCase):
    """The handler must never raise: every path returns hook messages."""

    def setUp(self):
        self.posted = []

        def record(url, body):
            self.posted.append((url, body))
            return 201

        patcher = mock.patch.object(function, "post_to_sink", side_effect=record)
        self.post = patcher.start()
        self.addCleanup(patcher.stop)

        # The queue is always looked up; give every handler test one to find.
        lookup = mock.patch.object(
            function.RossumAPI,
            "queue_url_of",
            return_value="https://example.rossum.app/api/v1/queues/87654321",
        )
        self.queue_url_of = lookup.start()
        self.addCleanup(lookup.stop)

    def patch_api(self, export=None, side_effect=None):
        api = mock.patch.object(
            function.RossumAPI,
            "export",
            side_effect=side_effect,
            return_value=export if side_effect is None else None,
        )
        self.addCleanup(api.stop)
        return api.start()

    def only_message(self, result):
        self.assertEqual(list(result), ["messages"])
        self.assertEqual(len(result["messages"]), 1)
        return result["messages"][0]

    def test_happy_path_posts_the_expected_envelope(self):
        export = self.patch_api(export=load_sample())
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "info")
        # The queue always comes from the annotation lookup, never from settings.
        self.queue_url_of.assert_called_once_with(ANNOTATION_ID)
        export.assert_called_once_with("87654321", ANNOTATION_ID)
        url, body = self.posted[0]
        self.assertEqual(url, "https://www.postb.in/BIN")
        self.assertEqual(set(body), {"annotationId", "content"})
        self.assertEqual(body["annotationId"], ANNOTATION_ID)

        xml = base64.b64decode(body["content"])
        root = ET.fromstring(xml)
        self.assertEqual(root.tag, "FinancialDocDesc")
        self.assertEqual(root.findtext("EInvoice/InvoiceNumber"), "INV-2024-0042")
        self.assertEqual(len(root.findall("Detail/Lines")), 3)

    def test_content_is_base64_of_the_utf8_xml_bytes(self):
        self.patch_api(export=load_sample())
        payload = base_payload()

        rossum_hook_request_handler(payload)
        _, body = self.posted[0]

        self.assertTrue(base64.b64decode(body["content"]).startswith(b"<?xml"))

    def test_an_export_with_no_datapoints_warns_instead_of_reporting_success(self):
        self.patch_api(export=export_with([]))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "warning")
        self.assertIn("No datapoints were extracted", message["content"])

    def test_a_missing_field_fails_before_any_network_call(self):
        export = self.patch_api(export=load_sample())

        message = self.only_message(rossum_hook_request_handler({}))

        self.assertEqual(message["type"], "error")
        self.queue_url_of.assert_not_called()
        export.assert_not_called()
        self.assertEqual(self.posted, [])

    def test_http_error_reports_status_and_body(self):
        response = mock.Mock(status_code=404, url="https://example.rossum.app/x")
        response.text = "not found"
        self.patch_api(side_effect=requests.HTTPError(response=response))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("404", message["content"])
        self.assertIn("not found", message["content"])

    def test_http_error_without_a_response_still_returns_a_message(self):
        """exc.response is None for some failures; reading it must not escape."""
        self.patch_api(side_effect=requests.HTTPError("boom"))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("boom", message["content"])

    def test_connect_timeout_explains_the_egress_restriction(self):
        """ConnectTimeout subclasses Timeout, so ordering decides the message."""
        request = mock.Mock(url="https://www.postb.in/BIN")
        self.patch_api(side_effect=requests.ConnectTimeout("refused", request=request))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("outbound internet", message["content"])
        self.assertIn("postb.in", message["content"])

    def test_read_timeout_mentions_the_hook_deadline(self):
        request = mock.Mock(url="https://example.rossum.app/api/v1/queues/1/export")
        self.patch_api(side_effect=requests.ReadTimeout("slow", request=request))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("30s", message["content"])

    def test_an_unexpected_error_is_still_reported_as_a_message(self):
        self.patch_api(side_effect=RuntimeError("kaboom"))
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("RuntimeError", message["content"])

    def test_a_failing_sink_is_reported(self):
        self.patch_api(export=load_sample())
        self.post.side_effect = requests.HTTPError(
            response=mock.Mock(status_code=500, url="https://www.postb.in/BIN", text="down")
        )
        payload = base_payload()

        message = self.only_message(rossum_hook_request_handler(payload))

        self.assertEqual(message["type"], "error")
        self.assertIn("500", message["content"])


if __name__ == "__main__":
    unittest.main()
