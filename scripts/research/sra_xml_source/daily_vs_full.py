"""Compare a daily's extracted folders with the monthly's. Args: daily_dir full_dir."""

import collections
import re
import sys
from pathlib import Path

daily, full = Path(sys.argv[1]), Path(sys.argv[2])
dsubs = sorted(p.name for p in daily.iterdir() if p.is_dir())
print("daily folders", len(dsubs), collections.Counter(s[:3] for s in dsubs))
nums = sorted(int(re.sub(r"\D", "", s)) for s in dsubs if s.startswith("SRA"))
print("SRA numbers: min", nums[0], "median", nums[len(nums) // 2], "max", nums[-1])
sizes = sorted(sum(f.stat().st_size for f in (daily / s).iterdir()) for s in dsubs)
print("folder bytes median", sizes[len(sizes) // 2], "max", sizes[-1], "sum", sum(sizes))
both = [s for s in dsubs if (full / s).is_dir()]
same = diff = 0
for s in both:
    a = {f.name: f.read_bytes() for f in (daily / s).iterdir()}
    b = {f.name: f.read_bytes() for f in (full / s).iterdir()}
    if a == b:
        same += 1
    else:
        diff += 1
print("in both", len(both), "identical", same, "different", diff)
geo = sum(
    'center_name="GEO"' in (daily / s / f"{s}.submission.xml").read_text(errors="replace")
    for s in dsubs
    if (daily / s / f"{s}.submission.xml").exists()
)
print("daily GEO submissions", geo)
