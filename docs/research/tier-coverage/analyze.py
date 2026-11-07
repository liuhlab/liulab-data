"""Summarise the bucket and OpenAlex results; draw the host-link sample for fetch_hosts.py.

Usage: python analyze.py <out_dir>
Reads strata.json, res_A.json, res_B.json, res_C.json; writes summary.json and hosts_sample.json.
"""

import json
import math
import random
import sys
from collections import Counter
from pathlib import Path

SEED = 20261107
PMC_HOSTS = ("ncbi.nlm.nih.gov", "europepmc.org")
out = Path(sys.argv[1])
strata = json.loads((out / "strata.json").read_text())


def wilson(k: int, n: int) -> tuple[float, float, float]:
    """Return the fraction and its 95% Wilson interval."""
    if n == 0:
        return (0.0, 0.0, 0.0)
    z, p = 1.96, k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, c - h, c + h)


def row(name: str, k: int, n: int, total: int) -> dict:
    """Return one measure with its interval and stratum-scaled count."""
    p, lo, hi = wilson(k, n)
    return {
        "measure": name,
        "k": k,
        "n": n,
        "frac": round(p, 3),
        "lo": round(lo, 3),
        "hi": round(hi, 3),
        "scaled": round(p * total),
        "scaled_lo": round(lo * total),
        "scaled_hi": round(hi * total),
    }


def host_class(loc: dict) -> str:
    """Class a location's host: preprint, publisher or repository."""
    dom = loc["pdf_url"].split("/")[2].lower()
    if "biorxiv.org" in dom or "medrxiv.org" in dom:
        return "preprint"
    t = (loc.get("source") or {}).get("type")
    return {"journal": "publisher", "repository": "repository"}.get(t, t or "unknown")


def non_pmc_pdfs(w: dict) -> list[dict]:
    """Return locations with a non-PMC pdf_url, best OA copy first."""
    locs = [loc for loc in w["locations"] if loc.get("pdf_url")]
    locs = [loc for loc in locs if not any(h in loc["pdf_url"] for h in PMC_HOSTS)]
    best = w.get("best_oa_location")
    if best and best in locs:  # the tier tries the best copy first
        locs.remove(best)
        locs.insert(0, best)
    return locs


def year_bucket(y: int | None) -> str:
    """Bucket a publication year."""
    if y is None:
        return "unknown"
    return "pre-2010" if y < 2010 else "2010-2017" if y <= 2017 else "2018+"


summary: dict = {"strata": strata}

a = json.loads((out / "res_A.json").read_text()) if (out / "res_A.json").exists() else []
n, total = len(a), strata["A"]
summary["A"] = [
    row("listed in bucket", sum(bool(r["versions"]) for r in a), n, total),
    row("newest version has PDF", sum(bool(r.get("latest_has_pdf")) for r in a), n, total),
    row("any version has PDF", sum(bool(r.get("any_version_has_pdf")) for r in a), n, total),
    row("is_manuscript", sum(bool(r.get("is_manuscript")) for r in a), n, total),
    row("is_pmc_openaccess", sum(bool(r.get("is_pmc_openaccess")) for r in a), n, total),
    row(
        "manuscript and newest has PDF",
        sum(bool(r.get("is_manuscript") and r.get("latest_has_pdf")) for r in a),
        n,
        total,
    ),
    row(
        "OA and newest has no PDF",
        sum(bool(r.get("is_pmc_openaccess") and not r.get("latest_has_pdf")) for r in a),
        n,
        total,
    ),
    row("several versions", sum(len(r["versions"]) > 1 for r in a), n, total),
]
summary["A_license"] = Counter(str(r.get("license_code")) for r in a).most_common()

cands = []
for s in ("B", "C"):
    d = json.loads((out / f"res_{s}.json").read_text())
    sample = {str(r["pmid"]) for r in d["sample"]}
    works = {}
    for w in d["works"]:
        pm = (w["ids"].get("pmid") or "").rsplit("/", 1)[-1]
        if pm in sample:
            works[pm] = w
    n, total = len(sample), strata[s]
    ws = list(works.values())
    with_pdf = [w for w in ws if non_pmc_pdfs(w)]
    first_class = Counter(host_class(non_pmc_pdfs(w)[0]) for w in with_pdf)
    rows = [
        row("found in OpenAlex", len(ws), n, total),
        row("has DOI", sum(bool(w.get("doi")) for w in ws), n, total),
        row("has PMCID in OpenAlex", sum(bool(w["ids"].get("pmcid")) for w in ws), n, total),
        row("is_oa", sum(w["open_access"]["is_oa"] for w in ws), n, total),
        row("non-PMC pdf_url", len(with_pdf), n, total),
        row(
            "has_content.pdf",
            sum(bool((w.get("has_content") or {}).get("pdf")) for w in ws),
            n,
            total,
        ),
        row(
            "no non-PMC pdf_url but has_content.pdf",
            sum(bool(not non_pmc_pdfs(w) and (w.get("has_content") or {}).get("pdf")) for w in ws),
            n,
            total,
        ),
        row(
            "neither",
            sum(
                bool(not non_pmc_pdfs(w) and not (w.get("has_content") or {}).get("pdf"))
                for w in ws
            )
            + n
            - len(ws),
            n,
            total,
        ),
    ]
    rows += [row(f"first non-PMC pdf_url host: {k}", v, n, total) for k, v in first_class.items()]
    summary[s] = rows
    summary[f"{s}_oa_status"] = Counter(w["open_access"]["oa_status"] for w in ws).most_common()
    years = Counter(year_bucket(w.get("publication_year")) for w in ws)
    yp = Counter(year_bucket(w.get("publication_year")) for w in with_pdf)
    summary[f"{s}_years"] = {
        k: {"works": v, "non_pmc_pdf": yp.get(k, 0)} for k, v in sorted(years.items())
    }
    summary[f"{s}_types"] = Counter(w.get("type") for w in ws).most_common()
    for w in with_pdf:
        loc = non_pmc_pdfs(w)[0]
        cands.append(
            {
                "stratum": s,
                "work": w["id"],
                "pdf_url": loc["pdf_url"],
                "host_class": host_class(loc),
            }
        )

summary["host_candidates"] = len(cands)
pick = random.Random(SEED).sample(cands, min(150, len(cands)))
(out / "hosts_sample.json").write_text(json.dumps(pick, indent=1))
(out / "summary.json").write_text(json.dumps(summary, indent=1))
print(json.dumps(summary, indent=1))
