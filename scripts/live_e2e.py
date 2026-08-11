"""
Live end-to-end check against a real Rossum organization.

    export ROSSUM_DOMAIN=https://<org>.rossum.app
    export TOKEN=<api key>
    export HOOK_ID=<hook id>
    export ANNOTATION_ID=<exportable annotation id>
    python scripts/live_e2e.py

It creates a fresh PostBin bin, invokes the hook with that bin's URL in the
invoke body, reads the request back out of the bin, then decodes and validates
the XML. The hook's stored settings are left untouched.

Requires `requests` locally and outbound internet enabled for the organization.
"""
import base64
import json
import os
import sys
from xml.etree import ElementTree as ET

import requests

POSTBIN = "https://www.postb.in"
TIMEOUT = 30


def main():
    try:
        domain = os.environ["ROSSUM_DOMAIN"].rstrip("/")
        token = os.environ["TOKEN"]
        hook_id = os.environ["HOOK_ID"]
        annotation_id = os.environ["ANNOTATION_ID"]
    except KeyError as missing:
        sys.exit(f"Missing environment variable: {missing}")

    api = f"{domain}/api/v1"
    auth = {"Authorization": f"Bearer {token}"}

    bin_id = requests.post(f"{POSTBIN}/api/bin", timeout=TIMEOUT).json()["binId"]
    sink = f"{POSTBIN}/{bin_id}"
    print(f"1. created bin {bin_id} (inspect at {POSTBIN}/b/{bin_id})")

    # The function prefers a postbin_url in the invoke body over its stored
    # setting, so a test run never has to mutate the deployed hook.
    body = {"annotationId": annotation_id, "postbin_url": sink}
    invoke = requests.post(
        f"{api}/hooks/{hook_id}/invoke", headers=auth, json=body, timeout=60
    )
    print(f"2. invoke -> HTTP {invoke.status_code}")
    try:
        print("   hook messages:", json.dumps(invoke.json(), indent=2)[:600])
    except ValueError:
        print("   non-JSON response:", invoke.text[:300])
    if invoke.status_code >= 400:
        return 1

    received = requests.get(f"{POSTBIN}/api/bin/{bin_id}/req/shift", timeout=TIMEOUT)
    if received.status_code == 404:
        print("3. NOTHING REACHED THE BIN — outbound internet is most likely")
        print("   disabled for this organization (it is off by default).")
        return 1
    request = received.json()
    print(f"3. bin received {request.get('method')}")

    payload = request["body"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    failures = []

    def check(condition, description):
        print(("   PASS " if condition else "   FAIL ") + description)
        if not condition:
            failures.append(description)

    print("4. validating:")
    check(set(payload) == {"annotationId", "content"}, "envelope is exactly annotationId + content")
    check(str(payload.get("annotationId")) == str(annotation_id), f"annotationId is {annotation_id}")
    check(
        "authorization" not in {key.lower() for key in request.get("headers", {})},
        "no Authorization header leaked to the sink",
    )

    xml = base64.b64decode(payload["content"])
    check(xml.startswith(b"<?xml"), "content base64-decodes to XML")
    root = ET.fromstring(xml)
    check(root.tag == "FinancialDocDesc", "root element is FinancialDocDesc")
    for section in ("EInvoice", "Control", "Detail"):
        check(root.find(section) is not None, f"has <{section}>")
    check(root.find("EInvoice/CustormerID") is not None, "CustormerID typo preserved")
    check(root.findtext("Control/Barcode") == str(annotation_id), "Control/Barcode is the annotation id")

    populated = [e.tag for e in root.find("EInvoice") if (e.text or "").strip()]
    print(f"   populated EInvoice elements: {', '.join(populated) or '(none)'}")
    print(f"   line items: {len(root.findall('Detail/Lines'))}")
    print("\n--- decoded XML ---")
    print(xml.decode("utf-8"))

    print("\nRESULT:", "PASS" if not failures else f"FAIL ({len(failures)})")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
