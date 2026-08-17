# Rossum → FinancialDocDesc → PostBin

A Rossum serverless hook. It exports an annotation, maps it to FinancialDocDesc
XML, and POSTs `{ "annotationId", "content" }` to PostBin (`content` is base64 XML).

Entry point: `rossum_hook_request_handler`

Invoke body:

```json
{
  "rossum_authorization_token": "<injected by Rossum>",
  "annotationId": "12345678",
  "postbin_id": "<BIN_ID>"
}
```

## Run

1. Create a Python serverless hook (`default` library pack).
2. Set `ROSSUM_BASE_URL` in `function.py` to your org, then paste the file.
3. Set a `token_owner` so Rossum injects `rossum_authorization_token`.
4. Enable outbound internet for the org — the Rossum API works without it; PostBin does not.

```bash
curl -X POST "https://<org>.rossum.app/api/v1/hooks/<HOOK_ID>/invoke" \
  -H "Authorization: Bearer <TOKEN>" -H "Content-Type: application/json" \
  -d '{"annotationId": "12345678", "postbin_id": "<BIN_ID>"}'
```

Pass the bin id only. The hook always POSTs to `https://www.postb.in/<BIN_ID>`.
The browser UI is `/b/<BIN_ID>`.

The annotation must be in an exportable state.

## Mapping

XML tag names are the dataclass field names. Values are copied as extracted —
no rounding or locale conversion. `CustormerID` keeps that spelling (target schema).

| XML | Source |
|---|---|
| `InvoiceType` | `document_type` |
| `VatID` | `sender_vat_id` |
| `VatCountryCode` | first two letters of `VatID` |
| `CustormerID` | `recipient_ic` |
| `InvoiceNumber` | `document_id` |
| `InvoiceDate` / `DueDate` | `date_issue` / `date_due` |
| `OrderNumber` | `order_id` |
| `Currency` | `currency` |
| `Total` | `amount_total` |
| `VATExemptionReasonCode` | `vat_exemption_reason_code` |
| `NetAmount0` | zero-rate base from `tax_details` |
| `NetAmount1..3`, `TaxAmount1..3`, `TaxRate1..3` | rated `tax_details` rows, first three |
| `Control.Origin` | always `EMAIL` |
| `Control.CurrentDate` | UTC date of the run |
| `Control.Barcode` | `barcode`, or the annotation id |
| `Lines/*` | `line_items` (`item_code`, `item_description`, `item_quantity`, `item_uom`, `item_amount`, `item_rate`, `item_tax`, `item_amount_total`) |
| `Lines/Discount`, `EmailTo`, `EmailFrom` | always empty |

If there are no `tax_details` rows, `amount_total_base` / `amount_total_tax` fill
bucket 1 and `TaxRate1` stays empty.

## Test

```bash
uv run python -m unittest discover -s tests -v
uv run python scripts/local_smoke.py
```

Against a live org (creates a PostBin, prints the decoded XML). `.env` needs
`TOKEN=` and, for the remote script, `HOOK_ID=`:

```bash
uv run python scripts/run_local.py 52915487
uv run python scripts/run_rossum.py 52915487 52915505
```
