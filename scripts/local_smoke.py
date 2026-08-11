"""
Offline smoke check: map the sample export and print the resulting XML.

    python scripts/local_smoke.py

No network and no Rossum account needed — useful for eyeballing the mapping.
"""
import base64
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from function import map_document, parse_export, to_xml  # noqa: E402

SAMPLE = os.path.join(os.path.dirname(__file__), os.pardir, "samples", "sample_export.json")
ANNOTATION_ID = "12345678"


def main():
    with open(SAMPLE, encoding="utf-8") as handle:
        export = json.load(handle)

    fields, rows = parse_export(export, ANNOTATION_ID)
    print(f"parsed {len(fields)} header field(s), "
          f"{len(rows.get('line_items', []))} line item(s), "
          f"{len(rows.get('tax_details', []))} tax row(s)\n")

    xml = to_xml(
        *map_document(fields, rows, annotation_id=ANNOTATION_ID, today="2024-05-20")
    )
    print(xml.decode("utf-8"))

    envelope = {
        "annotationId": ANNOTATION_ID,
        "content": base64.b64encode(xml).decode("ascii"),
    }
    print(f"\nenvelope keys: {sorted(envelope)}")
    print(f"xml bytes: {len(xml)}, base64 chars: {len(envelope['content'])}")


if __name__ == "__main__":
    main()
