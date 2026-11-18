"""Fetch the live E-utilities values that the bulk check compares against.

Takes the GSM rows written by ``scripts/research/gsm_srx_join.py`` plus extra SRA studies,
fetches every SRX with ``efetch db=sra``, every BioSample and every BioProject, and writes
three files: ``live_runs.tsv`` (one row per run), ``live_biosamples.json`` and
``live_bioprojects.json``.

Credentials come from labdata's resolver (``NCBI_EMAIL``/``NCBI_API_KEY`` or the labdata
config file); without them it runs keyless.

Run::

    pixi run python scripts/research/field_sources/live_reference.py \
        --gsm-jsonl gsm.jsonl --study PRJNA1027859 --study SRP045534 --out live/
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from labdata.exceptions import CredentialsError
from labdata.ncbi import load_credentials

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
BATCH = 100
RUN_FIELDS = [
    "study_key", "srp", "bioproject", "srx", "srx_alias", "srs", "sample_alias", "sample_geo",
    "biosample", "taxid", "strategy", "source", "selection", "layout", "instrument",
    "library_name", "nominal_length", "srr", "srr_alias", "spots", "bases", "size", "published", "is_public",
]  # fmt: skip


class Ncbi:
    """A rate-limited GET client for E-utilities."""

    def __init__(self) -> None:
        try:
            creds = load_credentials(allow_prompt=False)
            self.auth = {"email": creds.email}
            if creds.api_key:
                self.auth["api_key"] = creds.api_key
        except CredentialsError:
            self.auth = {}
        self.gap = 0.12 if "api_key" in self.auth else 0.4
        self.last = 0.0

    def eu(self, tool: str, **params: str) -> str:
        """Call one E-utility with the credentials attached, retrying failures."""
        query = urllib.parse.urlencode({**params, **self.auth, "tool": "labdata-research"})
        url = f"{EUTILS}{tool}.fcgi?{query}"
        for attempt in range(5):
            time.sleep(max(0.0, self.gap - (time.time() - self.last)))
            self.last = time.time()
            try:
                with urllib.request.urlopen(url, timeout=300) as resp:
                    text = resp.read().decode("utf-8", "replace")
                if "API rate limit exceeded" not in text[:300]:
                    return text
            except OSError:
                pass
            time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"gave up on {url[:200]}")


def study_srx(ncbi: Ncbi, study: str) -> list[str]:
    """Any study accession -> its SRX accessions, through an SRA search."""
    reply = ncbi.eu("esearch", db="sra", term=f"{study}[All Fields]", retmode="json", retmax="5000")
    uids = json.loads(reply)["esearchresult"]["idlist"]
    srx: list[str] = []
    for i in range(0, len(uids), BATCH):
        reply = ncbi.eu("esummary", db="sra", id=",".join(uids[i : i + BATCH]), retmode="json")
        result = json.loads(reply)["result"]
        for uid in result.get("uids", []):
            exp = ElementTree.fromstring(f"<x>{result[uid]['expxml']}</x>").find("Experiment")
            if exp is not None and exp.get("acc"):
                srx.append(str(exp.get("acc")))
    return sorted(srx)


def ext(node: ElementTree.Element | None, namespace: str) -> str:
    """Return the first EXTERNAL_ID in ``namespace`` under ``node``, or an empty string."""
    if node is None:
        return ""
    for ident in node.iter("EXTERNAL_ID"):
        if ident.get("namespace") == namespace:
            return (ident.text or "").strip()
    return ""


def runs_of(pkg: ElementTree.Element, key: str) -> list[dict[str, Any]]:
    """Flatten one EXPERIMENT_PACKAGE into one dict per run."""
    exp, sample, study = pkg.find("EXPERIMENT"), pkg.find("SAMPLE"), pkg.find("STUDY")
    lib = exp.find("DESIGN/LIBRARY_DESCRIPTOR") if exp is not None else None
    layout = lib.find("LIBRARY_LAYOUT") if lib is not None else None

    def txt(node: ElementTree.Element | None, path: str) -> str:
        return " ".join(((node.findtext(path) if node is not None else "") or "").split())

    base = {
        "study_key": key,
        "srp": study.get("accession") if study is not None else "",
        "bioproject": ext(study, "BioProject"),
        "srx": exp.get("accession") if exp is not None else "",
        "srx_alias": exp.get("alias") if exp is not None else "",
        "srs": sample.get("accession") if sample is not None else "",
        "sample_alias": sample.get("alias") if sample is not None else "",
        "sample_geo": ext(sample, "GEO"),
        "biosample": ext(sample, "BioSample"),
        "taxid": txt(sample, "SAMPLE_NAME/TAXON_ID"),
        "strategy": txt(lib, "LIBRARY_STRATEGY"),
        "source": txt(lib, "LIBRARY_SOURCE"),
        "selection": txt(lib, "LIBRARY_SELECTION"),
        "layout": layout[0].tag if layout is not None and len(layout) else "",
        "instrument": txt(exp, ".//INSTRUMENT_MODEL"),
        "library_name": txt(lib, "LIBRARY_NAME"),
        "nominal_length": layout[0].get("NOMINAL_LENGTH", "")
        if layout is not None and len(layout)
        else "",
    }
    return [
        {
            **base,
            "srr": run.get("accession"),
            "srr_alias": run.get("alias"),
            "spots": run.get("total_spots"),
            "bases": run.get("total_bases"),
            "size": run.get("size"),
            "published": run.get("published"),
            "is_public": run.get("is_public"),
        }
        for run in pkg.iter("RUN")
    ]


def main() -> None:
    """Fetch, flatten and write the live reference files."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--gsm-jsonl", type=Path, required=True)
    ap.add_argument("--study", action="append", default=[])
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    ncbi = Ncbi()
    args.out.mkdir(parents=True, exist_ok=True)

    wanted: dict[str, str] = {}
    biosamples: set[str] = set()
    for line in args.gsm_jsonl.read_text().splitlines():
        row = json.loads(line)
        for srx in row["srx_soft"]:
            wanted[srx] = row["gsm"]
        if row["biosample_soft"]:
            biosamples.add(row["biosample_soft"])
    for study in args.study:
        for srx in study_srx(ncbi, study):
            wanted.setdefault(srx, study)

    rows: list[dict[str, Any]] = []
    todo = sorted(wanted)
    for _round in range(3):
        for i in range(0, len(todo), BATCH):
            root = ElementTree.fromstring(
                ncbi.eu("efetch", db="sra", id=",".join(todo[i : i + BATCH]))
            )
            for pkg in root.iter("EXPERIMENT_PACKAGE"):
                exp = pkg.find("EXPERIMENT")
                srx = exp.get("accession", "") if exp is not None else ""
                rows.extend(runs_of(pkg, wanted.get(srx, "")))
        seen = {r["srx"] for r in rows}
        todo = [x for x in todo if x not in seen]
        if not todo:
            break
    with (args.out / "live_runs.tsv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, RUN_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["srx"], r["srr"] or "")))

    biosamples |= {r["biosample"] for r in rows if r["biosample"]}
    bs_out: dict[str, Any] = {}
    ids = sorted(biosamples)
    for i in range(0, len(ids), BATCH):
        reply = ncbi.eu("efetch", db="biosample", id=",".join(ids[i : i + BATCH]), rettype="full")
        for bs in ElementTree.fromstring(reply).iter("BioSample"):
            org = bs.find("Description/Organism")
            bs_out[bs.get("accession") or ""] = {
                "title": " ".join((bs.findtext("Description/Title") or "").split()),
                "taxid": org.get("taxonomy_id") if org is not None else None,
                "package": bs.findtext("Package"),
                "last_update": bs.get("last_update"),
                "attributes": [
                    [a.get("attribute_name"), a.get("harmonized_name"), (a.text or "").strip()]
                    for a in bs.iter("Attribute")
                ],
            }
    (args.out / "live_biosamples.json").write_text(json.dumps(bs_out, indent=1, sort_keys=True))

    bp_out: dict[str, Any] = {}
    for acc in sorted({r["bioproject"] for r in rows if r["bioproject"]}):
        reply = ncbi.eu("esearch", db="bioproject", term=f"{acc}[PRJA]", retmode="json")
        uids = json.loads(reply)["esearchresult"]["idlist"]
        if not uids:
            continue
        doc = ElementTree.fromstring(ncbi.eu("efetch", db="bioproject", id=uids[0]))
        sub = doc.find(".//Submission")
        bp_out[acc] = {
            "title": doc.findtext(".//ProjectDescr/Title"),
            "description": " ".join((doc.findtext(".//ProjectDescr/Description") or "").split()),
            "publications": sorted(p.get("id") or "" for p in doc.iter("Publication")),
            "data_types": sorted(d.text or "" for d in doc.iter("DataType")),
            "submitted": sub.get("submitted") if sub is not None else None,
            "last_update": sub.get("last_update") if sub is not None else None,
        }
    (args.out / "live_bioprojects.json").write_text(json.dumps(bp_out, indent=1, sort_keys=True))
    print(f"runs {len(rows)}, experiments {len({r['srx'] for r in rows})}, missing {todo}")
    print(f"biosamples {len(bs_out)} of {len(biosamples)}, bioprojects {len(bp_out)}")


if __name__ == "__main__":
    main()
