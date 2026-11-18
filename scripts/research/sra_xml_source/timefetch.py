"""Time efetch of SRA packages for GEO-linked UIDs at several batch sizes. Arg: out_dir."""

import random
import re
import subprocess
import sys
import time
from pathlib import Path

E = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)


def get(url, data=None):
    cmd = ["curl", "-s", "--max-time", "300", url]
    if data:
        cmd += ["--data", data]
    return subprocess.run(cmd, check=True, capture_output=True).stdout


random.seed(48)
uids = []
for start in random.sample(range(0, 5_600_000), 6):
    r = get(f"{E}esearch.fcgi?db=sra&term=sra_gds%5BFilter%5D&retmax=200&retstart={start}")
    uids += re.findall(rb"<Id>(\d+)</Id>", r)
    time.sleep(0.4)
uids = [u.decode() for u in uids]
print("uids", len(uids))
pos = 0
for size in (100, 100, 200, 500):
    batch = uids[pos : pos + size]
    pos += size
    t = time.time()
    raw = get(f"{E}efetch.fcgi", "db=sra&rettype=full&retmode=xml&id=" + ",".join(batch))
    dt = time.time() - t
    n = raw.count(b"<EXPERIMENT_PACKAGE>")
    runs = len(re.findall(rb"<RUN [^>]*accession=", raw))
    (out / f"batch{size}_{pos}.xml").write_bytes(raw)
    print(f"batch {size}: {dt:.1f} s, {len(raw)/1e6:.2f} MB, packages {n}, runs {runs}")
    time.sleep(0.5)
