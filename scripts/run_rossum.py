"""
Invoke the deployed Rossum hook.

Creates a fresh PostBin, POSTs each annotation id to the hook, then
prints the bin URL and the decoded XML.

    uv run python scripts/run_rossum.py 52915487
    uv run python scripts/run_rossum.py 52915487 52915505 52915509

Reads TOKEN and HOOK_ID from the repo-root .env (or the environment).
Optional: HOOK_URL (full hook URL, with or without /invoke) instead of HOOK_ID.
"""
import base64
import json
import os
import re
import sys

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from function import ROSSUM_BASE_URL  # noqa: E402

POSTBIN = "https://www.postb.in"
TIMEOUT = 15


def load_dotenv(root: str) -> None:
    path = os.path.join(root, ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value


def create_bin() -> str:
    response = requests.post(f"{POSTBIN}/api/bin", data="", timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()["binId"]


def invoke_url() -> str:
    url = os.environ.get("HOOK_URL", "").rstrip("/")
    if url:
        return url if url.endswith("/invoke") else f"{url}/invoke"
    hook_id = os.environ.get("HOOK_ID")
    if not hook_id:
        raise SystemExit(
            "Missing HOOK_ID or HOOK_URL. Put HOOK_ID=... in .env or export it."
        )
    return f"{ROSSUM_BASE_URL}/api/v1/hooks/{hook_id}/invoke"


def read_posts(bin_id: str) -> dict[str, str]:
    """Read every request in the bin without removing it."""
    page = requests.get(f"{POSTBIN}/b/{bin_id}", timeout=TIMEOUT)
    page.raise_for_status()
    posts: dict[str, str] = {}
    for req_id in re.findall(r"Req '([^']+)'", page.text):
        response = requests.get(
            f"{POSTBIN}/api/bin/{bin_id}/req/{req_id}", timeout=TIMEOUT
        )
        response.raise_for_status()
        body = response.json()["body"]
        if isinstance(body, str):
            body = json.loads(body)
        posts[str(body["annotationId"])] = base64.b64decode(body["content"]).decode(
            "utf-8"
        )
    return posts


def main() -> int:
    load_dotenv(ROOT)
    ids = sys.argv[1:]
    if not ids:
        print("usage: uv run python scripts/run_rossum.py ANNOTATION_ID [ANNOTATION_ID ...]")
        return 2
    token = os.environ.get("TOKEN")
    if not token:
        print("Missing TOKEN. Put TOKEN=... in .env or export it.")
        return 2

    url = invoke_url()
    bin_id = create_bin()
    bin_url = f"{POSTBIN}/b/{bin_id}"
    print(f"bin:  {bin_url}")
    print(f"hook: {url}")
    print()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    for annotation_id in ids:
        response = requests.post(
            url,
            headers=headers,
            json={"annotationId": annotation_id, "postbin_id": bin_id},
            timeout=60,
        )
        print(f"===== {annotation_id}  HTTP {response.status_code} =====")
        try:
            payload = response.json()
        except ValueError:
            print(response.text[:400])
            print()
            continue
        messages = payload.get("messages") or []
        if messages:
            print(f"{messages[0].get('type')}: {messages[0].get('content')}")
        else:
            print(json.dumps(payload)[:400])
        print()

    posts = read_posts(bin_id)
    for annotation_id in ids:
        xml = posts.get(annotation_id)
        print(f"===== XML {annotation_id} =====")
        print(xml if xml else "(nothing in the bin for this annotation)")
        print()
    print(f"bin: {bin_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
