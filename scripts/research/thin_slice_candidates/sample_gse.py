"""Sample small GEO sequencing Series per year through E-utilities esummary.

Writes two TSVs: GSM_GSE (gsm, gse) and GSE_META (gse, n_samples, taxon, pdat, bioproject,
pmids, is_superseries, gdstype, title). Uses curl; set NCBI_API_KEY to raise the rate limit.

Usage: python3 sample_gse.py FIRST_YEAR LAST_YEAR PER_YEAR MAX_SAMPLES GSM_GSE GSE_META
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"


def call(tool: str, **params: str) -> dict:
    """POST one E-utilities request and return its JSON, retrying on failure."""
    params["retmode"] = "json"
    if os.environ.get("NCBI_API_KEY"):
        params["api_key"] = os.environ["NCBI_API_KEY"]
    body = urllib.parse.urlencode(params)
    for attempt in range(4):
        time.sleep(0.4)
        r = subprocess.run(
            ["curl", "-s", "-f", "-m", "90", "--data-binary", "@-", f"{EUTILS}{tool}.fcgi"],
            input=body,
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode == 0 and r.stdout:
            return json.loads(r.stdout)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{tool} failed")


def main(first: int, last: int, per_year: int, max_samples: int, gsm_out: str, meta_out: str):
    """Write the two TSVs."""
    with open(gsm_out, "w") as gfh, open(meta_out, "w") as mfh:
        for year in range(first, last + 1):
            term = (
                f'"high throughput sequencing"[PTYP] AND gse[ETYP] AND {year}[PDAT]'
                f" AND 2:{max_samples}[NSAM]"
            )
            ids = call("esearch", db="gds", term=term, retmax=str(per_year))["esearchresult"]
            for i in range(0, len(ids["idlist"]), 200):
                res = call("esummary", db="gds", id=",".join(ids["idlist"][i : i + 200]))
                for uid in res["result"]["uids"]:
                    v = res["result"][uid]
                    row = [
                        v["accession"],
                        str(v["n_samples"]),
                        v["taxon"],
                        v["pdat"],
                        v["bioproject"],
                        ",".join(v["pubmedids"]),
                        str(v["summary"].startswith("This SuperSeries")),
                        v["gdstype"],
                        " ".join(v["title"].split()),
                    ]
                    mfh.write("\t".join(row) + "\n")
                    for s in v["samples"]:
                        gfh.write(f"{s['accession']}\t{v['accession']}\n")


if __name__ == "__main__":
    a = sys.argv[1:]
    main(int(a[0]), int(a[1]), int(a[2]), int(a[3]), a[4], a[5])
