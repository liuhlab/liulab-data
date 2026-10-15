"""Write the GEO thin-slice test fixtures from live NCBI (run once; the output is committed).

Run from the repo root::

    python -I tests/data/geo_slice/make_fixtures.py

Writes, next to this script:

- ``soft/<GSE>.soft``: each Series' decompressed family SOFT, trimmed to the samples in
  ``KEEP``. Each data table keeps its header and first two rows. A dropped sample also loses
  its ``!Series_sample_id`` line, so every file stays consistent with itself.
- ``packages/<SRX>.xml``: the ``EXPERIMENT_PACKAGE`` of every experiment the kept samples
  name in SOFT or through ``elink``.
- ``entrez.json``: the live ``elink`` answers (sample to sra, Series to bioproject), the
  accession behind each linked UID, and the PubMed ``[si]`` answers.
- ``snapshots/``: ``bioproject.xml`` with every BioProject named, rewrapped in the bulk file's
  ``<Package>`` shape; a ``biosample_set.xml.gz`` and an ``SRA_Accessions.tab`` that hold
  no record of this slice.
"""

from __future__ import annotations

import gzip
import json
import re
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DATE = "2026-10-14"
TAB_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy"
)
#: Each Series and the samples kept from its family SOFT; ``None`` keeps them all.
KEEP: dict[str, list[str] | None] = {
    "GSE246307": ["GSM8571270"],
    "GSE246308": [],
    "GSE246309": [],
    "GSE246310": [],
    "GSE153830": [],
    "GSE19456": None,
    "GSE233931": [],
    "GSE233685": [],
    "GSE20301": ["GSM517436", "GSM517440"],
    "GSE28770": None,
}
#: Series whose first sample is kept, and the SuperSeries that keeps its SubSeries' samples.
FIRST = ("GSE246308", "GSE246309", "GSE153830", "GSE233931", "GSE233685")
SUPER = "GSE246310"


def get(url: str) -> bytes:
    """Fetch one URL with curl, pausing to stay under NCBI's anonymous rate."""
    time.sleep(0.4)
    return subprocess.run(["curl", "-sSf", url], capture_output=True, check=True).stdout


def blocks(text: str) -> list[tuple[str, str, str]]:
    """Split family SOFT into (entity, accession, block text)."""
    out: list[tuple[str, str, list[str]]] = []
    for line in text.splitlines(keepends=True):
        if line.startswith("^"):
            entity, _, accession = line[1:].partition(" = ")
            out.append((entity, accession.strip(), [line]))
        elif out:
            out[-1][2].append(line)
    return [(e, a, "".join(lines)) for e, a, lines in out]


def trim_tables(block: str) -> str:
    """Keep each data table's header and first two rows."""
    out, row = [], -1
    for line in block.splitlines(keepends=True):
        if line.startswith(("!sample_table_begin", "!platform_table_begin")):
            row = 0
        elif line.startswith(("!sample_table_end", "!platform_table_end")):
            row = -1
        elif row >= 0:
            row += 1
            if row > 3:
                continue
        out.append(line)
    return "".join(out)


def trim(text: str, keep: set[str] | None) -> str:
    """Keep the Series, its platforms and the kept samples."""
    out = []
    for entity, accession, block in blocks(text):
        if entity == "SAMPLE" and keep is not None and accession not in keep:
            continue
        if entity == "SERIES" and keep is not None:
            block = "".join(
                line
                for line in block.splitlines(keepends=True)
                if not line.startswith("!Series_sample_id = ")
                or line.split(" = ", 1)[1].strip() in keep
            )
        out.append(trim_tables(block))
    return "".join(out)


def first_with_four_srx(text: str) -> str:
    """Return the first sample with four SRA relation lines."""
    for entity, accession, block in blocks(text):
        if entity == "SAMPLE" and block.count("!Sample_relation = SRA:") == 4:
            return accession
    raise SystemExit("no sample with 4 SRX")


def summaries(db: str, uids: list[str]) -> dict[str, str]:
    """Map each UID to its accession through one ``esummary``."""
    if not uids:
        return {}
    text = get(f"{EUTILS}/esummary.fcgi?db={db}&id={','.join(uids)}").decode()
    if db == "sra":
        pairs = re.findall(r'<Id>(\d+)</Id>.*?Experiment acc="([^"]+)"', text, re.S)
    else:
        pairs = re.findall(r'<DocumentSummary uid="(\d+)">.*?<Project_Acc>([^<]+)<', text, re.S)
    return dict(pairs)


def elink(dbfrom: str, db: str, uid: str) -> list[str]:
    """Return the UIDs one ``elink`` gives."""
    text = get(f"{EUTILS}/elink.fcgi?dbfrom={dbfrom}&db={db}&id={uid}").decode()
    linkset = text.split("<LinkSetDb>", 1)
    return re.findall(r"<Id>(\d+)</Id>", linkset[1]) if len(linkset) == 2 else []


def si(accession: str) -> list[str]:
    """Return the PMIDs of one ``<accession>[si]`` search."""
    text = get(f"{EUTILS}/esearch.fcgi?db=pubmed&term={accession}%5Bsi%5D&retmax=100").decode()
    return re.findall(r"<Id>(\d+)</Id>", text)


def main() -> None:
    """Fetch and write every fixture file."""
    soft: dict[str, str] = {}
    for gse in KEEP:
        url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/soft/{gse}_family.soft.gz"
        soft[gse] = gzip.decompress(get(url)).decode()
    keep = {gse: None if gsms is None else set(gsms) for gse, gsms in KEEP.items()}
    for gse in FIRST:
        keep[gse] = {next(a for e, a, _ in blocks(soft[gse]) if e == "SAMPLE")}
    keep["GSE20301"] = {*(KEEP["GSE20301"] or []), first_with_four_srx(soft["GSE20301"])}
    keep[SUPER] = set().union(*(keep[s] or set() for s in ("GSE246307", "GSE246308", "GSE246309")))
    (HERE / "soft").mkdir(exist_ok=True)
    gsms: dict[str, None] = {}
    for gse, text in soft.items():
        trimmed = trim(text, keep[gse])
        (HERE / "soft" / f"{gse}.soft").write_text(trimmed, encoding="utf-8")
        gsms.update((a, None) for e, a, _ in blocks(trimmed) if e == "SAMPLE")
    all_soft = "".join((HERE / "soft" / f"{gse}.soft").read_text() for gse in soft)

    links: dict[str, list[str]] = {}
    for gsm in gsms:
        uid = str(300_000_000 + int(gsm[3:]))
        links[f"gds sra {uid}"] = elink("gds", "sra", uid)
    for gse in soft:
        uid = str(200_000_000 + int(gse[3:]))
        links[f"gds bioproject {uid}"] = elink("gds", "bioproject", uid)
    sra_uids = sorted({u for k, v in links.items() if " sra " in k for u in v})
    bp_uids = sorted({u for k, v in links.items() if " bioproject " in k for u in v})
    accessions = {"sra": summaries("sra", sra_uids), "bioproject": summaries("bioproject", bp_uids)}

    srx = set(re.findall(r"^!Sample_relation = SRA: \S*?term=([SED]RX\d+)", all_soft, re.M))
    srx |= set(accessions["sra"].values())
    (HERE / "packages").mkdir(exist_ok=True)
    reply = get(f"{EUTILS}/efetch.fcgi?db=sra&id={','.join(sorted(srx))}&rettype=full&retmode=xml")
    studies: set[str] = set()
    for package in re.findall(
        r"<EXPERIMENT_PACKAGE>.*?</EXPERIMENT_PACKAGE>", reply.decode(), re.S
    ):
        accession = re.search(r'<EXPERIMENT [^>]*?\baccession="([^"]+)"', package)
        assert accession is not None
        (HERE / "packages" / f"{accession[1]}.xml").write_text(package, encoding="utf-8")
        studies |= set(re.findall(r'<STUDY [^>]*?\baccession="([^"]+)"', package))

    pubmed = {acc: si(acc) for acc in sorted({*soft, *studies})}
    (HERE / "entrez.json").write_text(
        json.dumps({"elink": links, "accessions": accessions, "pubmed_si": pubmed}, indent=1)
        + "\n",
        encoding="utf-8",
    )

    projects = set(re.findall(r"bioproject/(PRJ[A-Z]+\d+)", all_soft))
    projects |= set(accessions["bioproject"].values())
    for package in (HERE / "packages").glob("*.xml"):
        projects |= set(re.findall(r'namespace="BioProject">(PRJ[A-Z]+\d+)<', package.read_text()))
    ids = ",".join(sorted(p[5:] for p in projects))
    bp = get(f"{EUTILS}/efetch.fcgi?db=bioproject&id={ids}&retmode=xml").decode()
    bulk = '<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n'
    for inner in re.findall(r"<DocumentSummary[^>]*>\n(.*?)</DocumentSummary>", bp, re.S):
        bulk += "  <Package>\n    <Project>\n" + inner.rstrip() + "\n    </Project>\n  </Package>\n"
    bulk += "</PackageSet>\n"
    snap = HERE / "snapshots"
    for folder, name, data in (
        ("bioproject", "bioproject.xml", bulk.encode()),
        (
            "biosample",
            "biosample_set.xml.gz",
            gzip.compress(b'<?xml version="1.0"?>\n<BioSampleSet>\n</BioSampleSet>\n', mtime=0),
        ),
        ("sra-accessions", "SRA_Accessions.tab", (TAB_HEADER + "\n").encode()),
    ):
        out = snap / folder / DATE / name
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)


if __name__ == "__main__":
    main()
