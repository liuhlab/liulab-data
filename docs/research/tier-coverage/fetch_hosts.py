"""GET each sampled host pdf_url once and classify the reply by its first bytes.

Usage: python fetch_hosts.py <hosts_sample.json> <out.json>
One request a second, generic User-Agent, 30 s timeout, redirects followed, no retries.
Keeps only the first 1 KB of each body in memory; saves nothing.
"""

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

UA = {"User-Agent": "labdata/0.1"}
BOT_MARKS = (
    "captcha",
    "cf-chl",
    "challenge-platform",
    "just a moment",
    "are you a robot",
    "access denied",
    "perfdrive",
    "radware",
    "verify you are human",
    "cloudflare",
)


def classify(status: int, head: bytes) -> str:
    """Name the kind of reply from its status and first bytes."""
    if head.startswith(b"%PDF"):
        return "pdf"
    low = head.lower().decode("latin-1")
    if any(m in low for m in BOT_MARKS):
        return "bot-check"
    if status >= 400:
        return f"http-{status}"
    if "<html" in low or "<!doctype" in low:
        return "html"
    return "other"


res = []
for c in json.loads(Path(sys.argv[1]).read_text()):
    t0 = time.time()
    status, head, final, err = 0, b"", c["pdf_url"], None
    try:
        req = urllib.request.Request(c["pdf_url"], headers=UA)
        with urllib.request.urlopen(req, timeout=30) as r:
            status, final, head = r.status, r.url, r.read(1024)
    except urllib.error.HTTPError as e:
        status, final, head = e.code, e.url, e.read(4096)
    except Exception as e:
        err = type(e).__name__
    kind = classify(status, head) if err is None else f"error-{err}"
    res.append({**c, "status": status, "final_host": final.split("/")[2], "kind": kind})
    print(kind, status, c["pdf_url"][:90], flush=True)
    time.sleep(max(0.0, 1 - (time.time() - t0)))
Path(sys.argv[2]).write_text(json.dumps(res, indent=1))
