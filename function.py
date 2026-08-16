"""
Rossum serverless hook: export an annotation, map it to FinancialDocDesc XML,
and POST it to a sink (PostBin) as base64 inside a JSON envelope.

Entry point: rossum_hook_request_handler(payload) -> {"messages": [...]}

Runtime notes
-------------
* Rossum's python3.12 runtime ships a default third-party library pack that
  includes `requests` and `jmespath`, so we use them instead of hand-rolling
  urllib and a recursive tree walk. The hook's `third_party_library_pack` must
  be "default" (which it is unless changed).
* Outbound internet is disabled by default for Rossum functions. The calls to
  the Rossum API work out of the box; the POST to an external sink requires
  egress to be enabled for the organization (contact product@rossum.ai).
* POST /v1/hooks/{id}/invoke forces a 30s wall clock that cannot be raised, and
  the injected token is valid for 10 minutes, so every request is short-timed.
"""

import base64
import dataclasses
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import jmespath
import requests

# Both endpoints are fixed on purpose: the hook only ever talks to this org's
# Rossum API and to postb.in, so a caller cannot point it anywhere else. Only
# the annotation id and the bin id vary per invocation.
ROSSUM_BASE_URL = "https://foobar.rossum.app"
POSTBIN_ORIGIN = "https://www.postb.in"
# At most 2 sequential calls (export + sink) against the hook's hard 30s
# ceiling, leaving room to return an error instead of being killed. Note this
# bounds each socket operation, not total wall clock.
REQUEST_TIMEOUT = 8.0
TAX_BUCKETS = 3  # the target schema exposes Net/Tax/Rate 1..3

# XML 1.0 forbids most control characters; OCR output occasionally contains one.
# Strip them rather than fail serialization on an otherwise valid document.
_ILLEGAL_XML = re.compile("[^\x09\x0a\x0d\x20-퟿-�\U00010000-\U0010ffff]")


def _src(schema_id: str = "") -> Any:
    """Declare an element sourced from `schema_id`; "" means derived in code."""
    return field(default="", metadata={"schema_id": schema_id})


@dataclass
class EInvoice:
    InvoiceType: str = _src("document_type")
    VatCountryCode: str = _src()  # derived from VatID
    VatID: str = _src("sender_vat_id")
    CustormerID: str = _src("recipient_ic")  # sic: the typo is the target contract
    InvoiceNumber: str = _src("document_id")
    InvoiceDate: str = _src("date_issue")
    OrderNumber: str = _src("order_id")
    Currency: str = _src("currency")
    DueDate: str = _src("date_due")
    NetAmount0: str = _src()  # zero-rated base, from tax_details
    NetAmount1: str = _src()
    TaxAmount1: str = _src()
    TaxRate1: str = _src()
    NetAmount2: str = _src()
    TaxAmount2: str = _src()
    TaxRate2: str = _src()
    NetAmount3: str = _src()
    TaxAmount3: str = _src()
    TaxRate3: str = _src()
    Total: str = _src("amount_total")
    VATExemptionReasonCode: str = _src("vat_exemption_reason_code")


@dataclass
class Control:
    Origin: str = _src()
    CurrentDate: str = _src()
    Barcode: str = _src()
    # Routing addresses live on the payload's email object, not in extracted
    # content, so these stay empty.
    EmailTo: str = _src()
    EmailFrom: str = _src()


@dataclass
class Line:
    ArticleCode: str = _src("item_code")
    Description: str = _src("item_description")
    Quantity: str = _src("item_quantity")
    UnitMeasure: str = _src("item_uom")
    # "Unit Price"; item_amount_base is the excl.-tax variant. Paired with
    # TotalPrice below so both sides of the line are on the same tax basis.
    UnitPrice: str = _src("item_amount")
    Discount: str = _src()  # no standard Rossum field; left empty
    TaxRate: str = _src("item_rate")
    TaxAmount: str = _src("item_tax")
    TotalPrice: str = _src("item_amount_total")


@dataclass
class Config:
    token: str = field(repr=False)  # keep the token out of tracebacks and logs
    annotation_id: str
    sink_url: str


# --- parsing ---------------------------------------------------------------


def _text(value: Any) -> str:
    """Render a datapoint value as text, exactly as extracted.

    No reformatting: rewriting 42.0 to "42" would be inventing a number's
    format, which is the one thing this mapper promises not to do.
    """
    return "" if value is None else str(value).strip()


# Rossum's content tree is sections at the top with datapoints and multivalues
# one level below (the tests also place them at the top level directly), so the
# node stream is a union of both depths: every top-level node, then every child.
# jmespath has no recursive descent — anything nested deeper would be dropped.
# The `|` stops the flatten projection so the filter sees the whole node list.
_NODES = "[content, content[].children[]][] | "
_FIELDS_QUERY = jmespath.compile(_NODES + "[?category=='datapoint']")
# For each multivalue: its schema_id and one datapoint-list per tuple row.
# `[?...][]` turns the filter projection into a plain list so the next step
# maps tuple -> children instead of flattening every row into one.
_ROWS_QUERY = jmespath.compile(
    _NODES + "[?category=='multivalue'].{id: schema_id,"
    " tuples: children[?category=='tuple'][].children[?category=='datapoint']}"
)


def _first_non_empty(datapoints: Any) -> dict[str, str]:
    """Flatten datapoint nodes; first non-empty value per schema_id wins."""
    out: dict[str, str] = {}
    for node in datapoints:
        schema_id = node.get("schema_id")
        if schema_id and not out.get(schema_id):
            out[schema_id] = _text(node.get("value"))
    return out


def parse_export(
    export: Mapping[str, Any], annotation_id: str
) -> tuple[dict[str, str], dict[str, list[dict[str, str]]]]:
    """Validate the export envelope and split its content into fields + rows."""
    results = export.get("results")
    if not results:
        raise ValueError(
            f"Export returned no results for annotation {annotation_id}; "
            "it is probably not in an exportable state yet."
        )
    annotation = results[0]
    fields = _first_non_empty(_FIELDS_QUERY.search(annotation))
    rows: dict[str, list[dict[str, str]]] = {}
    for multivalue in _ROWS_QUERY.search(annotation) or ():
        collected = [
            row
            for row in map(_first_non_empty, multivalue["tuples"] or ())
            if any(row.values())  # an all-blank row is noise, not data
        ]
        if collected:
            rows.setdefault(multivalue["id"], []).extend(collected)
    return fields, rows


# --- mapping ---------------------------------------------------------------


def _is_zero_rate(text: str) -> bool:
    """True for a rate like '0', '0,00' or '0 %'. Unparseable is not zero."""
    cleaned = text.strip().rstrip("%").strip().replace(" ", "").replace(",", ".")
    try:
        return float(cleaned) == 0.0
    except ValueError:
        return False


def _populate(cls: type, source: Mapping[str, str]):
    """Build a model, reading each field from its declared schema_id."""
    obj = cls()
    for fld in dataclasses.fields(cls):
        schema_id = fld.metadata["schema_id"]
        if schema_id:
            setattr(obj, fld.name, source.get(schema_id, ""))
    return obj


def _apply_tax(einvoice: EInvoice, tax_rows: Sequence[Mapping[str, str]]) -> None:
    """Fill NetAmount0 (zero-rated) plus the NetAmount1..3 rate buckets."""
    zero_rated: list[tuple[str, str, str]] = []
    rated: list[tuple[str, str, str]] = []
    for row in tax_rows:
        rate = row.get("tax_detail_rate", "")
        base, tax = row.get("tax_detail_base", ""), row.get("tax_detail_tax", "")
        if not (base or tax):
            continue  # an all-blank row must not consume a bucket
        (zero_rated if _is_zero_rate(rate) else rated).append((rate, base, tax))
    if zero_rated:
        einvoice.NetAmount0 = zero_rated[0][1]
    for index, (rate, base, tax) in enumerate(rated[:TAX_BUCKETS], start=1):
        setattr(einvoice, f"NetAmount{index}", base)
        setattr(einvoice, f"TaxAmount{index}", tax)
        setattr(einvoice, f"TaxRate{index}", rate)


def map_document(
    fields: Mapping[str, str],
    rows: Mapping[str, list[dict[str, str]]],
    *,
    annotation_id: str,
    today: str,
) -> tuple[EInvoice, Control, list[Line]]:
    einvoice: EInvoice = _populate(EInvoice, fields)

    prefix = einvoice.VatID[:2]  # e.g. CZ12345678 -> CZ
    if len(prefix) == 2 and prefix.isalpha():
        einvoice.VatCountryCode = prefix.upper()

    tax_rows = rows.get("tax_details", [])
    if tax_rows:
        _apply_tax(einvoice, tax_rows)
    else:  # no per-rate breakdown extracted; fall back to the header totals
        einvoice.NetAmount1 = fields.get("amount_total_base", "")
        einvoice.TaxAmount1 = fields.get("amount_total_tax", "")

    control = Control()  # every Control element is derived, none is extracted
    control.Origin = "EMAIL"
    control.CurrentDate = today
    control.Barcode = fields.get("barcode") or annotation_id

    lines = [_populate(Line, row) for row in rows.get("line_items", [])]
    return einvoice, control, lines


# --- serialization ---------------------------------------------------------


def _append_fields(parent: ET.Element, model: Any) -> None:
    """Emit one element per declared field, stripping XML-illegal characters.

    Sanitizing here rather than at parse time means every value reaches the
    document through this one gate, including derived ones.
    """
    for fld in dataclasses.fields(model):
        text = getattr(model, fld.name)
        ET.SubElement(parent, fld.name).text = _ILLEGAL_XML.sub("", text)


def to_xml(einvoice: EInvoice, control: Control, lines: Sequence[Line]) -> bytes:
    """Serialize the model; field declaration order is the element order."""
    root = ET.Element("FinancialDocDesc")
    _append_fields(ET.SubElement(root, "EInvoice"), einvoice)
    _append_fields(ET.SubElement(root, "Control"), control)
    detail = ET.SubElement(root, "Detail")
    for line in lines or [Line()]:  # always emit a Lines skeleton
        _append_fields(ET.SubElement(detail, "Lines"), line)
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


# --- I/O -------------------------------------------------------------------


def export_annotation(token: str, annotation_id: str) -> Any:
    response = requests.get(
        f"{ROSSUM_BASE_URL}/api/v1/annotations/export",
        params={"format": "json", "id": annotation_id},
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "rossum-export-hook/1.0",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(
            f"{response.url} returned HTTP {response.status_code} but not "
            f"JSON ({exc}). First bytes: {response.text[:200]!r}"
        ) from exc


def post_to_sink(url: str, body: Mapping[str, str]) -> int:
    """POST the JSON envelope to the unauthenticated sink; return its status."""
    response = requests.post(url, json=body, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.status_code


# --- entry point -----------------------------------------------------------


def read_config(payload: Mapping[str, Any]) -> Config:
    """Validate the payload up front so a failure names the missing field."""
    required = ("rossum_authorization_token", "annotationId", "postbin_id")
    missing = [name for name in required if not payload.get(name)]
    if missing:
        raise ValueError(f"Missing required payload field(s): {', '.join(missing)}.")

    token, annotation_id, bin_id = (str(payload[name]) for name in required)
    return Config(token, annotation_id, f"{POSTBIN_ORIGIN}/{bin_id}")


def rossum_hook_request_handler(payload: dict) -> dict:
    """Rossum hook entry point. Never raises; always returns hook messages."""
    try:
        config = read_config(payload)
        fields, rows = parse_export(
            export_annotation(config.token, config.annotation_id),
            config.annotation_id,
        )
        xml = to_xml(
            *map_document(
                fields,
                rows,
                annotation_id=config.annotation_id,
                today=datetime.now(timezone.utc).date().isoformat(),
            )
        )
        status = post_to_sink(
            config.sink_url,
            {
                "annotationId": config.annotation_id,
                "content": base64.b64encode(xml).decode("ascii"),
            },
        )
        summary = (
            f"Annotation {config.annotation_id}: mapped "
            f"{len(fields)} field(s) and {len(rows.get('line_items', []))} line "
            f"item(s) to FinancialDocDesc, posted {len(xml)} bytes (HTTP {status})."
        )
        if not fields:
            # Valid but empty XML was delivered; say so rather than report success.
            return _message(
                "warning", f"{summary} No datapoints were extracted - check the "
                "annotation's schema ids against the ones this hook declares."
            )
        return _message("info", summary)
    except ValueError as exc:
        return _message("error", str(exc))
    except requests.HTTPError as exc:
        response = exc.response  # None unless raise_for_status() raised this
        if response is None:
            return _message("error", f"HTTP error: {exc}")
        body = response.text[:300] or exc
        return _message("error", f"HTTP {response.status_code} from {response.url}: {body}")
    except requests.ConnectionError as exc:
        # ConnectTimeout subclasses both ConnectionError and Timeout, so this
        # clause must stay above `except requests.Timeout` - otherwise a firewall
        # DROP, the usual shape of disabled egress, reports as a plain timeout.
        return _message(
            "error",
            f"Could not reach {_host_of(exc)}: {exc}. Rossum functions have no "
            "outbound internet access unless it is enabled for the organization.",
        )
    except requests.Timeout as exc:
        return _message(
            "error",
            f"{_host_of(exc)} did not respond within {REQUEST_TIMEOUT}s; "
            "the hook must finish within 30s.",
        )
    except requests.RequestException as exc:
        return _message("error", f"Network error: {exc}")
    except Exception as exc:  # pragma: no cover - last resort, keeps the log useful
        return _message("error", f"Unexpected {type(exc).__name__}: {exc}")


def _host_of(exc: requests.RequestException) -> str:
    """Which host failed — the Rossum API or the sink."""
    url = getattr(exc.request, "url", "") or ""
    return urlparse(url).hostname or "the remote host"


def _message(msg_type: str, content: str) -> dict:
    """Wrap one hook log message; type is one of info, warning, error."""
    return {"messages": [{"type": msg_type, "content": content}]}
