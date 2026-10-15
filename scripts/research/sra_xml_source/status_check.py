"""Which SRA_Accessions statuses appear in the tarball? Args: acc_chunk extracted_dir.

For every row in the chunk whose Submission folder was extracted, check whether the
accession appears in that folder's XML, grouped by Type, Status and Visibility.
"""

import collections
import sys
from pathlib import Path

chunk, root = Path(sys.argv[1]), Path(sys.argv[2])
subs = {p.name for p in root.iterdir() if p.is_dir()}
cache = {}


def text(sub):
    if sub not in cache:
        cache[sub] = "".join(f.read_text(errors="replace") for f in (root / sub).iterdir())
    return cache[sub]


tally = collections.Counter()
types = collections.Counter()
examples = {}
rows = 0
with chunk.open(errors="replace") as fh:
    next(fh)  # partial first line
    for line in fh:
        f = line.rstrip("\n").split("\t")
        if len(f) < 20:
            continue
        rows += 1
        acc, sub, status, typ, vis = f[0], f[1], f[2], f[6], f[8]
        types[(typ, status, vis)] += 1
        if sub not in subs:
            continue
        found = f'accession="{acc}"' in text(sub)
        key = (typ, status, vis, found)
        tally[key] += 1
        examples.setdefault(key, (acc, sub, f[7]))
print("rows", rows, "first/last types", types.most_common(12))
for k, n in sorted(tally.items()):
    print(k, n, examples[k])
