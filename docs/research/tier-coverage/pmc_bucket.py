"""Stratum A: list each sampled article in the public PMC bucket; read its JSON record.

Usage: python pmc_bucket.py <sample_A.json> <out.json>
"""

import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

BUCKET = "https://pmc-oa-opendata.s3.amazonaws.com"
NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
UA = {"User-Agent": "labdata/0.1"}


def get(url: str) -> bytes:
    """GET with backoff on S3's 503 Slow Down."""
    for attempt in range(6):
        time.sleep(0.2)
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code != 503 or attempt == 5:
                raise
            time.sleep(2**attempt)
    raise RuntimeError(url)


def probe(rec: dict) -> dict:
    """List one article's folder and read its newest JSON record."""
    pmc = rec["pmcid"] if rec["pmcid"].startswith("PMC") else f"PMC{rec['pmcid']}"
    xml = get(f"{BUCKET}/?list-type=2&prefix={pmc}.")
    keys = [k.text for k in ET.fromstring(xml).findall(".//s:Contents/s:Key", NS)]
    versions = sorted({int(m.group(1)) for k in keys if (m := re.match(rf"{pmc}\.(\d+)/", k))})
    out = {**rec, "versions": versions, "n_keys": len(keys)}
    if not versions:
        return out
    v = versions[-1]
    pref = f"{pmc}.{v}/"
    vkeys = [k for k in keys if k.startswith(pref)]
    out["latest_has_pdf"] = any(k.endswith(".pdf") for k in vkeys)
    out["any_version_has_pdf"] = any(k.endswith(".pdf") for k in keys)
    meta = json.loads(get(f"{BUCKET}/{pref}{pmc}.{v}.json"))
    for f in ("is_pmc_openaccess", "is_manuscript", "is_historical_ocr", "license_code"):
        out[f] = meta.get(f)
    out["json_pdf_url"] = bool(meta.get("pdf_url"))
    return out


recs = json.loads(Path(sys.argv[1]).read_text())
res = [probe(r) for r in recs]
Path(sys.argv[2]).write_text(json.dumps(res, indent=1))
print(len(res))
