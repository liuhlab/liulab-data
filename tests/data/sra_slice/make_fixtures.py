"""Write the fixtures of thin-slice records 5 and 6 from live NCBI (run once; output committed).

Run from the repo root::

    python -I tests/data/sra_slice/make_fixtures.py

Records 5 (``PRJNA436361``) and 6 (``PRJNA530347``) are SRA-only BioProjects. Each keeps
the two experiments in ``KEEP``. Writes, next to this script:

- ``packages/<SRX>.xml``: each kept experiment's ``EXPERIMENT_PACKAGE`` from
  ``efetch db=sra rettype=full``.
- ``pubmed_si.json``: the live ``<accession>[si]`` PMIDs of each project and each study
  the packages name.
- ``snapshots/bioproject/2026-10-14/bioproject.xml``: both projects' ``efetch db=bioproject``
  records rewrapped in the bulk file's ``<Package>`` shape. Each ``AuthorSet`` keeps its
  first three authors.
- ``snapshots/biosample/2026-10-14/biosample_set.xml.gz``: the BioSamples the packages name.
- ``snapshots/sra-accessions/2026-10-14/SRA_Accessions.tab``: the real header, then rows for
  the kept experiments written from the package XML, sorted (status ``live``, every date the run's
  ``published`` date). They are not copied from the bulk file.
- ``snapshots/geo-series/2026-10-14/gds_gse_esummary.jsonl.gz``: copied from the
  ``prjna1027859`` fixture; this slice holds no GEO record.
"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DATE = "2026-10-14"
#: Each project and the SRA UIDs of the two experiments kept from it.
KEEP = {"PRJNA436361": ["5174026", "5174027"], "PRJNA530347": ["7688771", "7688772"]}
TAB_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy"
)


def get(url: str) -> str:
    """Fetch one URL as text with curl, pausing to stay under NCBI's anonymous rate."""
    time.sleep(0.5)
    return subprocess.run(["curl", "-sSf", url], capture_output=True, check=True, text=True).stdout


def one(pattern: str, text: str, group: int) -> str:
    """Return one group of the first match of ``pattern``, failing loudly when it is absent."""
    match = re.search(pattern, text, re.S)
    if match is None:
        raise SystemExit(f"no match for {pattern!r}")
    return match[group]


def write(path: Path, text: str) -> None:
    """Write text, gzipped when the name ends in ``.gz``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        path.write_bytes(gzip.compress(text.encode(), mtime=0))
    else:
        path.write_text(text, encoding="utf-8")


def trim_authors(text: str) -> str:
    """Keep the first three authors of every ``AuthorSet``."""

    def keep(match: re.Match[str]) -> str:
        authors = re.findall(r"\s*<Author>.*?</Author>", match[2], re.S)
        return match[1] + "".join(authors[:3]) + match[3]

    return re.sub(r"(<AuthorSet>)(.*?)(\s*</AuthorSet>)", keep, text, flags=re.S)


def main() -> None:
    """Fetch and write every fixture file."""
    rows: list[str] = []
    biosamples: set[str] = set()
    queried = set(KEEP)
    for project, uids in KEEP.items():
        reply = get(f"{EUTILS}/efetch.fcgi?db=sra&id={','.join(uids)}&rettype=full&retmode=xml")
        for pkg in re.findall(r"<EXPERIMENT_PACKAGE>.*?</EXPERIMENT_PACKAGE>", reply, re.S):
            srx = one(r'<EXPERIMENT [^>]*?\baccession="([^"]+)"', pkg, 1)
            write(
                HERE / "packages" / f"{srx}.xml",
                '<?xml version="1.0" encoding="UTF-8"  ?>\n<EXPERIMENT_PACKAGE_SET>\n'
                f"{pkg}</EXPERIMENT_PACKAGE_SET>\n",
            )
            sra = one(r'<SUBMISSION [^>]*?\baccession="([^"]+)"', pkg, 1)
            srp = one(r'<STUDY [^>]*?\baccession="([^"]+)"', pkg, 1)
            srs = one(r'<SAMPLE [^>]*?\baccession="([^"]+)"', pkg, 1)
            samn = one(r'namespace="BioSample">([^<]+)<', pkg, 1)
            run = one(r"<RUN [^>]*>", pkg, 0)
            srr = one(r'accession="([^"]+)"', run, 1)
            day = one(r'published="([^"]+)"', run, 1).replace(" ", "T") + "Z"
            spots = one(r'total_spots="(\d+)"', run, 1)
            bases = one(r'total_bases="(\d+)"', run, 1)
            common = f"{sra}\tlive\t{day}\t{day}\t{day}"
            rows += [
                f"{srx}\t{common}\tEXPERIMENT\t-\tpublic\t{srx}\t-\t{srs}\t{srp}\t-\t-\t-\t-"
                f"\t{samn}\t{project}\t-",
                f"{srr}\t{common}\tRUN\t-\tpublic\t{srr}\t{srx}\t{srs}\t{srp}\t1\t{spots}"
                f"\t{bases}\t-\t{samn}\t{project}\t-",
                f"{srs}\t{common}\tSAMPLE\t-\tpublic\t{srs}\t-\t-\t-\t-\t-\t-\t-\t{samn}\t-\t-",
                f"{srp}\t{common}\tSTUDY\t-\tpublic\t{project}\t-\t-\t-\t-\t-\t-\t-\t-"
                f"\t{project}\t-",
            ]
            biosamples.add(samn)
            queried.add(srp)
    write(
        HERE / "snapshots" / "sra-accessions" / DATE / "SRA_Accessions.tab",
        "\n".join([TAB_HEADER, *sorted({r.split("\t", 1)[0]: r for r in rows}.values())]) + "\n",
    )

    si = {}
    for accession in sorted(queried):
        text = get(f"{EUTILS}/esearch.fcgi?db=pubmed&term={accession}%5Bsi%5D&retmax=100")
        si[accession] = re.findall(r"<Id>(\d+)</Id>", text)
    write(HERE / "pubmed_si.json", json.dumps(si, indent=2) + "\n")

    bulk = '<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n'
    for project in KEEP:
        text = get(f"{EUTILS}/efetch.fcgi?db=bioproject&id={project[5:]}&retmode=xml")
        inner = one(r"<DocumentSummary[^>]*>\n(.*)</DocumentSummary>", text, 1)
        bulk += "  <Package>\n    <Project>\n" + trim_authors(inner).rstrip()
        bulk += "\n    </Project>\n  </Package>\n"
    write(HERE / "snapshots" / "bioproject" / DATE / "bioproject.xml", bulk + "</PackageSet>\n")

    ids = ",".join(sorted(biosamples))
    text = get(f"{EUTILS}/efetch.fcgi?db=biosample&id={ids}")
    elements = re.findall(r"<BioSample .*?</BioSample>", text, re.S)
    out = '<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
    out += "".join(element + "\n" for element in elements) + "</BioSampleSet>\n"
    write(HERE / "snapshots" / "biosample" / DATE / "biosample_set.xml.gz", out)

    geo = HERE / "snapshots" / "geo-series" / DATE / "gds_gse_esummary.jsonl.gz"
    geo.parent.mkdir(parents=True, exist_ok=True)
    source = HERE.parent / "prjna1027859" / "snapshots" / "geo-series" / DATE / geo.name
    shutil.copyfile(source, geo)


if __name__ == "__main__":
    main()
