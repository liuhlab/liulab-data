"""Compare tarball XML with Entrez EXPERIMENT_PACKAGE XML for some submissions.

Args: extracted_dir out_dir SUB [SUB ...]
For each submission, efetch every experiment's package and compare per record
(experiment, run, sample, study) the set of element/attribute paths and their values.
"""

import collections
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def paths(el, prefix=""):
    """Yield (path, value) for every element text and attribute under el (el included)."""
    p = f"{prefix}/{el.tag}" if prefix else el.tag
    for k, v in el.attrib.items():
        yield f"{p}@{k}", v
    t = (el.text or "").strip()
    if t:
        yield p, t
    for c in el:
        yield from paths(c, p)


def pathset(el):
    d = collections.defaultdict(list)
    for p, v in paths(el):
        d[p].append(v)
    return d


def efetch(ids):
    q = urllib.parse.urlencode({"db": "sra", "id": ",".join(ids), "rettype": "full",
                                "retmode": "xml", "tool": "labdata-research"})
    import subprocess
    return subprocess.run(["curl", "-s", "--max-time", "120", f"{EUTILS}?{q}"],
                          check=True, capture_output=True).stdout


src, out = Path(sys.argv[1]), Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
only_tar = collections.Counter()
only_pkg = collections.Counter()
diff_val = collections.Counter()
n_records = collections.Counter()
for sub in sys.argv[3:]:
    d = src / sub
    tar = {}
    for kind, tag in [("experiment", "EXPERIMENT"), ("run", "RUN"), ("sample", "SAMPLE"),
                      ("study", "STUDY")]:
        f = d / f"{sub}.{kind}.xml"
        if f.exists():
            for el in ET.parse(f).getroot().iter(tag):
                if el.get("accession"):
                    tar[(tag, el.get("accession"))] = el
    srx = [a for (t, a) in tar if t == "EXPERIMENT"]
    raw = efetch(srx)
    (out / f"{sub}.efetch.xml").write_bytes(raw)
    time.sleep(0.5)
    root = ET.fromstring(raw)
    pkg = {}
    for p in root.iter("EXPERIMENT_PACKAGE"):
        for tag in ("EXPERIMENT", "SAMPLE", "STUDY"):
            for el in p.iter(tag):
                if el.get("accession") and el.tag == tag:
                    pkg.setdefault((tag, el.get("accession")), el)
        for el in p.iter("RUN"):
            if el.get("accession"):
                pkg.setdefault(("RUN", el.get("accession")), el)
    print(sub, "tar records", collections.Counter(t for t, _ in tar),
          "pkg records", collections.Counter(t for t, _ in pkg))
    missing_in_pkg = [k for k in tar if k not in pkg]
    missing_in_tar = [k for k in pkg if k not in tar]
    if missing_in_pkg or missing_in_tar:
        print("  only in tar:", missing_in_pkg[:5], " only in pkg:", missing_in_tar[:5])
    for key in tar.keys() & pkg.keys():
        tag = key[0]
        n_records[tag] += 1
        a, b = pathset(tar[key]), pathset(pkg[key])
        for p in a.keys() - b.keys():
            only_tar[(tag, p)] += 1
        for p in b.keys() - a.keys():
            only_pkg[(tag, p)] += 1
        for p in a.keys() & b.keys():
            if sorted(a[p]) != sorted(b[p]):
                diff_val[(tag, p)] += 1
                if diff_val[(tag, p)] == 1:
                    print("  value differs", tag, p, a[p][:2], "|", b[p][:2])


def show(title, c):
    print(f"\n## {title}")
    for (tag, p), n in sorted(c.items()):
        print(f"{tag}\t{n}/{n_records[tag]}\t{p}")


print("\nrecords compared", dict(n_records))
show("only in tarball", only_tar)
show("only in EXPERIMENT_PACKAGE", only_pkg)
show("value differs", diff_val)
