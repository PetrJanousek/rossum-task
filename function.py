"""
Rossum serverless hook: export an annotation, map it to FinancialDocDesc XML,
and POST it to a sink (PostBin) as base64 inside a JSON envelope.

Entry point: rossum_hook_request_handler(payload) -> {"messages": [...]}
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

import requests

ROSSUM_BASE_URL = "https://foobar.rossum.app"
POSTBIN_ORIGIN = "https://www.postb.in"
# At most 2 sequential calls (export + sink) against the hook's hard 30s
# ceiling, leaving room to return an error instead of being killed. Note this
# bounds each socket operation, not total wall clock.
REQUEST_TIMEOUT = 8.0

# XML 1.0 forbids most control characters; OCR output occasionally contains one.
# Strip them rather than fail serialization on an otherwise valid document.
_ILLEGAL_XML = re.compile("[^\x09\x0a\x0d\x20-퟿-�\U00010000-\U0010ffff]")


def _src(schema_id: str = "") -> Any:
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
    Origin: str = ""
    CurrentDate: str = ""
    Barcode: str = ""
    EmailTo: str = ""
    EmailFrom: str = ""


@dataclass
class Line:
    ArticleCode: str = _src("item_code")
    Description: str = _src("item_description")
    Quantity: str = _src("item_quantity")
    UnitMeasure: str = _src("item_uom")
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
    """Render a datapoint value as text, exactly as extracted."""
    return "" if value is None else str(value).strip()


def _nodes(annotation: Mapping[str, Any]):
    """Yield top-level content nodes, then their children (two levels)."""
    for node in annotation.get("content") or []:
        yield node
        yield from node.get("children") or []


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
    fields = _first_non_empty(
        n for n in _nodes(annotation) if n.get("category") == "datapoint"
    )
    rows: dict[str, list[dict[str, str]]] = {}
    for node in _nodes(annotation):
        if node.get("category") != "multivalue":
            continue
        collected = []
        for child in node.get("children") or ():
            if child.get("category") != "tuple":
                continue
            row = _first_non_empty(
                n
                for n in (child.get("children") or ())
                if n.get("category") == "datapoint"
            )
            if any(row.values()):
                collected.append(row)
        if collected:
            rows.setdefault(node.get("schema_id"), []).extend(collected)
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
    rated: list[tuple[str, str, str]] = []
    for row in tax_rows:
        rate = row.get("tax_detail_rate", "")
        base = row.get("tax_detail_base", "")
        tax = row.get("tax_detail_tax", "")
        if not (base or tax):
            continue
        if _is_zero_rate(rate):
            if not einvoice.NetAmount0:
                einvoice.NetAmount0 = base
            continue
        rated.append((base, tax, rate))

    if rated:
        einvoice.NetAmount1, einvoice.TaxAmount1, einvoice.TaxRate1 = rated[0]
    if len(rated) > 1:
        einvoice.NetAmount2, einvoice.TaxAmount2, einvoice.TaxRate2 = rated[1]
    if len(rated) > 2:
        einvoice.NetAmount3, einvoice.TaxAmount3, einvoice.TaxRate3 = rated[2]


def map_document(
    fields: Mapping[str, str],
    rows: Mapping[str, list[dict[str, str]]],
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

    control = Control()
    control.Origin = "EMAIL"
    control.CurrentDate = today
    control.Barcode = fields.get("barcode") or annotation_id

    lines = [_populate(Line, row) for row in rows.get("line_items", [])]
    return einvoice, control, lines


# --- serialization ---------------------------------------------------------


def _append_fields(parent: ET.Element, model: Any) -> None:
    """Emit one element per declared field, stripping XML-illegal characters."""
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
        if not fields and not rows:
            return _message(
                "warning",
                f"Annotation {config.annotation_id}: no datapoints were extracted - "
                "check the annotation's schema ids against the ones this hook declares.",
            )
        einvoice, control, lines = map_document(
            fields,
            rows,
            annotation_id=config.annotation_id,
            today=datetime.now(timezone.utc).date().isoformat(),
        )
        xml = to_xml(einvoice, control, lines)
        status = post_to_sink(
            config.sink_url,
            {
                "annotationId": config.annotation_id,
                "content": base64.b64encode(xml).decode("ascii"),
            },
        )
        return _message(
            "info",
            f"Annotation {config.annotation_id}: mapped "
            f"{len(fields)} field(s) and {len(rows.get('line_items', []))} line "
            f"item(s) to FinancialDocDesc, posted {len(xml)} bytes (HTTP {status}).",
        )
    except ValueError as exc:
        return _message("error", str(exc))
    except requests.HTTPError as exc:
        response = exc.response
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
