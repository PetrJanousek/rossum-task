"""
Rossum serverless hook: export an annotation, map it to FinancialDocDesc XML,
and POST it to a sink (PostBin) as base64 inside a JSON envelope.

Entry point: rossum_hook_request_handler(payload) -> {"messages": [...]}

Runtime notes
-------------
* Rossum's python3.12 runtime ships a default third-party library pack that
  includes `requests`, so we use it instead of hand-rolling urllib. The hook's
  `third_party_library_pack` must be "default" (which it is unless changed).
* Outbound internet is disabled by default for Rossum functions. The calls to
  the Rossum API work out of the box; the POST to an external sink requires
  egress to be enabled for the organization (contact product@rossum.ai).
* POST /v1/hooks/{id}/invoke forces a 30s wall clock that cannot be raised, and
  the injected token is valid for 10 minutes, so every request is short-timed.

Field mapping
-------------
Each XML element is declared exactly once: as a dataclass field whose name is
the tag, whose position is the document order, and whose metadata names the one
Rossum schema_id it reads. Nothing is guessed from a list of candidate aliases;
a queue with a customised schema supplies `settings.schema_overrides` instead.
"""

from __future__ import annotations

import base64
import dataclasses
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests

API_PREFIX = "/api/v1"  # payload base_url is the org origin, without this
# At most 3 sequential calls against the hook's hard 30s ceiling, leaving room
# to return an error instead of being killed. Note this bounds each socket
# operation, not total wall clock, which is why redirects are refused below.
REQUEST_TIMEOUT = 8.0
LINE_ITEMS_ID = "line_items"
TAX_DETAILS_ID = "tax_details"
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
    # content, so these stay empty unless pointed at a field by an override.
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
    Discount: str = _src()  # no standard Rossum field; override-only
    TaxRate: str = _src("item_rate")
    TaxAmount: str = _src("item_tax")
    TotalPrice: str = _src("item_amount_total")


@dataclass
class Config:
    token: str = field(repr=False)  # keep the token out of tracebacks and logs
    base_url: str
    annotation_id: str
    sink_url: str
    queue_id: str = ""
    queue_url: str = ""
    schema_overrides: Mapping[str, str] = dataclasses.field(default_factory=dict)


# --- parsing ---------------------------------------------------------------


def _text(value: Any) -> str:
    """Render a datapoint value as XML-safe text; never invent a number."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def _keep_first(target: dict[str, str], schema_id: str, value: str) -> None:
    """First non-empty value for a schema_id wins — the rule everywhere here."""
    if schema_id and not target.get(schema_id):
        target[schema_id] = value


def _row(node: Mapping[str, Any]) -> dict[str, str]:
    """Flatten one tuple of datapoints; {} when every cell is blank."""
    row: dict[str, str] = {}
    for child in node.get("children") or ():
        if isinstance(child, dict) and child.get("category") == "datapoint":
            _keep_first(row, child.get("schema_id") or "", _text(child.get("value")))
    return row if any(row.values()) else {}


def _walk(
    nodes: Any, fields: dict[str, str], rows: dict[str, list[dict[str, str]]]
) -> None:
    """
    Split the content tree into header datapoints and multivalue row sets.

    Rows are keyed by their multivalue's schema_id and collected structurally,
    so a queue that names its rows something unexpected still yields rows —
    and row data can never leak into the header.
    """
    for node in nodes if isinstance(nodes, list) else ():
        if not isinstance(node, dict):
            continue
        category = node.get("category")
        schema_id = node.get("schema_id") or ""
        if category == "multivalue":
            collected = [
                row
                for row in (
                    _row(child)
                    for child in node.get("children") or ()
                    if isinstance(child, dict) and child.get("category") == "tuple"
                )
                if row
            ]
            if collected:
                rows.setdefault(schema_id, []).extend(collected)
        elif category == "datapoint":
            _keep_first(fields, schema_id, _text(node.get("value")))
        else:
            _walk(node.get("children"), fields, rows)


def parse_export(
    export: Mapping[str, Any], annotation_id: str
) -> tuple[dict[str, str], dict[str, list[dict[str, str]]]]:
    """Validate the export envelope and split its content into fields + rows."""
    results = export.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(
            f"Export returned no results for annotation {annotation_id}; "
            "it is probably not in an exportable state yet."
        )
    annotation = results[0]
    if not isinstance(annotation, Mapping):
        raise ValueError(f"Export result is not an object: {annotation!r:.100}")
    url = str(annotation.get("url", ""))
    # Compare the id path segment, not a substring: "123" is in ".../1234".
    if url and urlparse(url).path.rstrip("/").rsplit("/", 1)[-1] != annotation_id:
        raise ValueError(f"Export returned {url!r}, expected annotation {annotation_id}.")
    fields: dict[str, str] = {}
    rows: dict[str, list[dict[str, str]]] = {}
    _walk(annotation.get("content"), fields, rows)
    return fields, rows


# --- mapping ---------------------------------------------------------------


def _is_zero_rate(text: str) -> bool:
    """True for a rate like '0', '0,00' or '0 %'. Unparseable is not zero."""
    cleaned = text.strip().rstrip("%").strip().replace(" ", "").replace(",", ".")
    try:
        return float(cleaned) == 0.0
    except ValueError:
        return False


def _populate(cls: type, source: Mapping[str, str], overrides: Mapping[str, str]):
    """Build a model, reading each field from its declared (or overridden) id."""
    obj = cls()
    for fld in dataclasses.fields(cls):
        schema_id = overrides.get(fld.name) or fld.metadata.get("schema_id", "")
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
    overrides: Mapping[str, str] | None = None,
) -> tuple[EInvoice, Control, list[Line]]:
    """Map parsed export data onto the FinancialDocDesc model."""
    overrides = overrides or {}
    einvoice: EInvoice = _populate(EInvoice, fields, overrides)

    prefix = einvoice.VatID[:2]  # e.g. CZ12345678 -> CZ
    if len(prefix) == 2 and prefix.isalpha():
        einvoice.VatCountryCode = prefix.upper()

    tax_rows = rows.get(TAX_DETAILS_ID, [])
    if tax_rows:
        _apply_tax(einvoice, tax_rows)
    else:  # no per-rate breakdown extracted; fall back to the header totals
        einvoice.NetAmount1 = fields.get("amount_total_base", "")
        einvoice.TaxAmount1 = fields.get("amount_total_tax", "")

    control: Control = _populate(Control, fields, overrides)
    control.Origin = "EMAIL"
    control.CurrentDate = today
    control.Barcode = fields.get("barcode") or annotation_id

    lines = [_populate(Line, row, overrides) for row in rows.get(LINE_ITEMS_ID, [])]
    return einvoice, control, lines


# --- serialization ---------------------------------------------------------


def _append_fields(parent: ET.Element, model: Any) -> None:
    """Emit one element per declared field, stripping XML-illegal characters.

    Sanitizing here rather than at parse time means every value reaches the
    document through this one gate, including derived and overridden ones.
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


class RossumAPI:
    """Thin authenticated client for the Rossum REST API."""

    def __init__(self, base_url: str, token: str):
        self._base = base_url.rstrip("/") + API_PREFIX
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {token}", "User-Agent": "rossum-export-hook/1.0"}
        )

    def _get(self, path: str, **params: str) -> Any:
        # No redirects: neither endpoint should redirect, and following one
        # would both re-arm the timeout and forward the token to another host.
        response = self._session.get(
            self._base + path,
            params=params,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
        )
        response.raise_for_status()
        if response.is_redirect:
            raise ValueError(
                f"{response.url} redirected to "
                f"{response.headers.get('Location', 'elsewhere')}; check base_url."
            )
        try:
            return response.json()
        except ValueError as exc:
            # Name the endpoint: a bare "Expecting value: line 1 column 1" tells
            # an operator nothing about which call returned a non-JSON page.
            raise ValueError(
                f"{response.url} returned HTTP {response.status_code} but not "
                f"JSON ({exc}). First bytes: {response.text[:200]!r}"
            ) from exc

    def queue_url_of(self, annotation_id: str) -> str:
        return str(self._get(f"/annotations/{annotation_id}").get("queue", ""))

    def export(self, queue_id: str, annotation_id: str) -> Any:
        # No status filter: the export must work for any exportable annotation.
        return self._get(f"/queues/{queue_id}/export", format="json", id=annotation_id)


def post_to_sink(url: str, body: Mapping[str, str]) -> int:
    """POST the JSON envelope to the unauthenticated sink; return its status."""
    response = requests.post(url, json=body, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.status_code


# --- entry point -----------------------------------------------------------


def read_config(payload: Mapping[str, Any]) -> Config:
    """Validate the payload up front so a failure names the missing field."""
    if not isinstance(payload, Mapping):
        raise ValueError("Payload must be a JSON object.")
    settings = payload.get("settings")
    settings = settings if isinstance(settings, Mapping) else {}
    annotation = payload.get("annotation")
    annotation = annotation if isinstance(annotation, Mapping) else {}

    # invocation.manual merges the custom body at the top level; annotation
    # events instead carry a nested annotation object with id and queue.
    annotation_id = payload.get("annotationId") or annotation.get("id")
    # A postbin_url in the invoke body overrides the stored setting, so a caller
    # can target a fresh bin per invocation without editing the hook.
    sink_url = payload.get("postbin_url") or settings.get("postbin_url")
    missing = [
        name
        for name, value in (
            ("rossum_authorization_token", payload.get("rossum_authorization_token")),
            ("base_url", payload.get("base_url")),
            ("annotationId", annotation_id),
            ("postbin_url", sink_url),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required payload field(s): {', '.join(missing)}.")

    # Validate the sink up front: otherwise a typo is only discovered after two
    # API calls have already run, and the hook would POST invoice data anywhere.
    sink = str(sink_url).strip()
    parsed = urlparse(sink)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(
            f"postbin_url must be an http(s) URL with a host, got {sink!r}."
        )

    overrides = settings.get("schema_overrides") or {}
    if not isinstance(overrides, Mapping):
        raise ValueError("settings.schema_overrides must be an object of field: schema_id.")

    return Config(
        token=str(payload["rossum_authorization_token"]),
        base_url=str(payload["base_url"]),
        annotation_id=str(annotation_id),
        sink_url=sink,
        queue_id=str(settings.get("queue_id") or ""),
        queue_url=str(annotation.get("queue") or ""),
        schema_overrides=overrides,
    )


def resolve_queue_id(config: Config, api: RossumAPI) -> str:
    """
    Prefer the queue the annotation actually belongs to.

    A static settings.queue_id is only a fallback: on a hook wired to several
    queues a stale value would export against the wrong one and report the
    annotation as unexportable, which reads as a data problem, not config.
    """
    if config.queue_url:
        queue_url = config.queue_url
    elif config.queue_id:
        return config.queue_id
    else:
        queue_url = api.queue_url_of(config.annotation_id)
    queue_id = urlparse(queue_url).path.rstrip("/").rsplit("/", 1)[-1]
    if not queue_id:
        raise ValueError(
            "Could not determine the queue for this annotation; set settings.queue_id."
        )
    return queue_id


def rossum_hook_request_handler(payload: dict) -> dict:
    """Rossum hook entry point. Never raises; always returns hook messages."""
    try:
        config = read_config(payload)
        api = RossumAPI(config.base_url, config.token)

        queue_id = resolve_queue_id(config, api)
        fields, rows = parse_export(
            api.export(queue_id, config.annotation_id), config.annotation_id
        )
        xml = to_xml(
            *map_document(
                fields,
                rows,
                annotation_id=config.annotation_id,
                today=datetime.now(timezone.utc).date().isoformat(),
                overrides=config.schema_overrides,
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
            f"Annotation {config.annotation_id} (queue {queue_id}): mapped "
            f"{len(fields)} field(s) and {len(rows.get(LINE_ITEMS_ID, []))} line "
            f"item(s) to FinancialDocDesc, posted {len(xml)} bytes (HTTP {status})."
        )
        if not fields:
            # Valid but empty XML was delivered; say so rather than report success.
            return _message(
                "warning", f"{summary} No datapoints were extracted - check the "
                "annotation's schema ids against settings.schema_overrides."
            )
        return _message("info", summary)
    except ValueError as exc:
        return _message("error", str(exc))
    except requests.HTTPError as exc:
        # .response can be None, and an AttributeError raised in here would
        # escape the handler entirely — read defensively.
        response = exc.response
        status = getattr(response, "status_code", "?")
        url = getattr(response, "url", "unknown URL")
        body = (getattr(response, "text", "") or "")[:300]
        return _message("error", f"HTTP {status} from {url}: {body or exc}")
    except (requests.ConnectTimeout, requests.ConnectionError) as exc:
        # ConnectTimeout subclasses Timeout as well, so it must be caught first
        # or a firewall DROP - the usual shape of disabled egress - would be
        # reported as a plain timeout instead of the hint below.
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
