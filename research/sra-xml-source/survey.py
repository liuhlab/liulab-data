"""Survey an extracted tarball folder: GEO-brokered submissions and file kinds. Arg: dir."""

import collections
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
subs = sorted(p for p in root.iterdir() if p.is_dir())
lens = collections.Counter((p.name[:3], len(p.name)) for p in subs)
print("prefix/len", sorted(lens.items()))
geo = []
kinds = collections.Counter()
for d in subs:
    files = {f.name.split(".")[-2] for f in d.iterdir()}
    kinds[tuple(sorted(files))] += 1
    s = d / f"{d.name}.submission.xml"
    if s.exists() and 'center_name="GEO"' in s.read_text(errors="replace")[:3000]:
        geo.append((d.name, tuple(sorted(files))))
print("kinds", kinds.most_common(10))
print("geo submissions", len(geo))
print(collections.Counter(k for _, k in geo).most_common())
# biggest numbers among SRA: lexicographic order check
nums = [int(re.sub(r"\D", "", p.name)) for p in subs if p.name.startswith("SRA")]
print("SRA min/max number", min(nums), max(nums))
print("geo sample", [g for g, _ in geo[-15:]])
