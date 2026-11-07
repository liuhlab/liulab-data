"""Strata B and C: batch-look up sampled PMIDs in OpenAlex (keyless, 100 per call).

Usage: python openalex_lookup.py <sample.json> <out.json>
Never touches content.openalex.org; sends no email.
"""

import json
import sys
import time
import urllib.request
from pathlib import Path

API = "https://api.openalex.org/works"
FIELDS = "id,ids,doi,publication_year,open_access,has_content,locations,best_oa_location,type"

recs = json.loads(Path(sys.argv[1]).read_text())
pmids = [str(r["pmid"]) for r in recs]
works = []
for i in range(0, len(pmids), 100):
    chunk = "|".join(pmids[i : i + 100])
    url = f"{API}?filter=ids.pmid:{chunk}&per_page=100&select={FIELDS}"
    req = urllib.request.Request(url, headers={"User-Agent": "labdata/0.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = json.load(r)
        print(i, r.headers.get("x-ratelimit-remaining-usd"), body["meta"]["count"])
    works.extend(body["results"])
    time.sleep(1)
Path(sys.argv[2]).write_text(json.dumps({"sample": recs, "works": works}))
