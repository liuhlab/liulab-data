"""Write the PRJNA1027859 test fixtures from live NCBI (run once; the output is committed).

Run from the repo root::

    python -I tests/data/prjna1027859/make_fixtures.py

Reads the six packages from ``tests/data/sra_package/``. Writes, next to this script:

- ``snapshots/bioproject/2026-10-14/bioproject.xml``: the project's ``efetch db=bioproject``
  record rewrapped in the bulk file's ``<PackageSet><Package><Project>`` shape, after one
  unrelated project.
- ``snapshots/biosample/2026-10-14/biosample_set.xml.gz``: the six BioSamples from
  ``efetch db=biosample``, one element per line, after one unrelated BioSample.
- ``snapshots/sra-accessions/2026-10-14/SRA_Accessions.tab``: the real header and five real
  DRA rows, then rows for this project written from the package XML (status ``live``, every
  date the run's ``published`` date). They are not copied from the bulk file.
"""

from __future__ import annotations

import gzip
import re
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
DATE = "2026-10-14"
SRX = [f"SRX2428313{n}" for n in range(6)]

TAB_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\tType\tCenter\tVisibility\t"
    "Alias\tExperiment\tSample\tStudy\tLoaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\t"
    "ReplacedBy"
)
DRA_ROWS = """\
DRA000001	DRA000001	live	2026-10-09T20:49:39Z	2010-03-24T03:10:22Z	2009-06-20T02:48:01Z	SUBMISSION	KEIO	public	DRA000001	-	-	-	-	-	-	0499ee6dee910c59ad38c636fd6aa739	-	-	-
DRP000001	DRA000001	live	2026-10-09T17:42:28Z	2015-07-31T15:20:44Z	2009-06-20T02:48:02Z	STUDY	KEIO	public	DRP000001	-	-	-	-	-	-	b39d9865d1216be6221a0cdd6198a347	-	PRJDA38027	-
DRR000001	DRA000001	live	2026-10-09T20:49:39Z	2010-03-24T03:10:22Z	2009-06-20T02:48:04Z	RUN	KEIO	public	DRR000001	DRX000001	DRS000001	DRP000001	1	10148174	730668528	4cb340809f8c728990c54e023a885a2a	SAMD00016353	PRJDA38027	-
DRS000001	DRA000001	live	2026-10-09T17:57:25Z	2010-03-24T03:10:22Z	2009-06-20T02:48:03Z	SAMPLE	KEIO	public	DRS000001	-	-	-	-	-	-	31018cf693b795bb21e6409ed8a86546	SAMD00016353	-	-
DRX000001	DRA000001	live	2026-10-09T18:51:36Z	2010-03-24T03:10:22Z	2009-06-20T02:48:03Z	EXPERIMENT	KEIO	public	DRX000001	-	DRS000001	DRP000001	-	-	-	e16cdcbe96cc71c1dbc8d11f2595e7d7	SAMD00016353	PRJDA38027	-
"""

OTHER_PROJECT = """\
  <Package>
    <Project>
      <Project>
        <ProjectID>
          <ArchiveID accession="PRJNA3" archive="NCBI" id="3"/>
        </ProjectID>
        <ProjectDescr>
          <Name>Borreliella burgdorferi B31</Name>
          <Title>Causes Lyme disease</Title>
        </ProjectDescr>
      </Project>
      <Submission submitted="2003-02-25">
        <Description>
          <Access>public</Access>
        </Description>
      </Submission>
    </Project>
  </Package>
"""

OTHER_BIOSAMPLE = (
    '<BioSample submission_date="2008-04-04T08:44:24.950" last_update="2026-08-30T02:00:02.453" '
    'publication_date="2008-04-04T00:00:00.000" access="public" id="2" '
    'accession="SAMN00000002"><Ids><Id db="BioSample" is_primary="1">SAMN00000002</Id></Ids>'
    '<Status status="live" when="2008-04-04T08:44:24.950"/></BioSample>\n'
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


def main() -> None:
    """Fetch and write every fixture file."""
    folder = HERE.parent / "sra_package"
    packages = "".join((folder / f"{srx}.xml").read_text(encoding="utf-8") for srx in SRX)

    rows = [TAB_HEADER, DRA_ROWS.rstrip("\n")]
    day = ""
    for pkg in re.findall(r"<EXPERIMENT_PACKAGE>.*?</EXPERIMENT_PACKAGE>", packages, re.S):
        srx = one(r'<EXPERIMENT [^>]*?\baccession="([^"]+)"', pkg, 1)
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
        tail = "Princeton University\tpublic"
        rows += [
            f"{srx}\t{common}\tEXPERIMENT\t{tail}\t{srx}\t-\t{srs}\t{srp}\t-\t-\t-\t-\t{samn}\tPRJNA1027859\t-",
            f"{srr}\t{common}\tRUN\t{tail}\t{srr}\t{srx}\t{srs}\t{srp}\t1\t{spots}\t{bases}\t-\t{samn}\tPRJNA1027859\t-",
            f"{srs}\t{common}\tSAMPLE\t{tail}\t{srs}\t-\t-\t-\t-\t-\t-\t-\t{samn}\t-\t-",
        ]
    rows.append(
        f"SRP502277\tSRA1847283\tlive\t{day}\t{day}\t{day}\tSTUDY\tPrinceton University\tpublic"
        "\tPRJNA1027859\t-\t-\t-\t-\t-\t-\t-\t-\tPRJNA1027859\t-"
    )
    tab = HERE / "snapshots" / "sra-accessions" / DATE / "SRA_Accessions.tab"
    tab.parent.mkdir(parents=True, exist_ok=True)
    tab.write_text("\n".join(rows) + "\n", encoding="utf-8")

    bp = get(f"{EUTILS}/efetch.fcgi?db=bioproject&id=1027859&retmode=xml")
    inner = one(r"<DocumentSummary[^>]*>\n(.*)</DocumentSummary>", bp, 1)
    bulk = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n'
        + OTHER_PROJECT
        + "  <Package>\n    <Project>\n"
        + inner.rstrip()
        + "\n    </Project>\n  </Package>\n</PackageSet>\n"
    )
    out = HERE / "snapshots" / "bioproject" / DATE / "bioproject.xml"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(bulk, encoding="utf-8")

    ids = ",".join(f"SAMN4093561{n}" for n in range(6, 10)) + ",SAMN40935620,SAMN40935621"
    bs = get(f"{EUTILS}/efetch.fcgi?db=biosample&id={ids}")
    elements = re.findall(r"<BioSample .*?</BioSample>", bs, re.S)
    text = '<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n' + OTHER_BIOSAMPLE
    text += "".join(element + "\n" for element in elements) + "</BioSampleSet>\n"
    out = HERE / "snapshots" / "biosample" / DATE / "biosample_set.xml.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(gzip.compress(text.encode(), mtime=0))


if __name__ == "__main__":
    main()
