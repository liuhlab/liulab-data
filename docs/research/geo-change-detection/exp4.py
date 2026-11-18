# ruff: noqa
"""Pilot 4: each released-on-10/15 Series not updated that day is in UDAT for its SOFT date."""

import json
import sys
from datetime import datetime

sys.path.insert(0, sys.argv[1])
from es import search  # noqa: E402

r = json.load(open(sys.argv[1] + "/out2.json"))["R"]
hit = 0
by = {}
for x in r:
    day = datetime.strptime(x["last_update"].strip(), "%b %d %Y").strftime("%Y/%m/%d")
    by.setdefault(day, []).append(x["gse"])
for day, gses in by.items():
    _, idl, _ = search(f"gse[ETYP] AND {day}[UDAT]", retmax=10000)
    got = {"GSE" + str(int(u) - 200000000) for u in idl}
    n = sum(g in got for g in gses)
    hit += n
    print(day, len(gses), n)
print("total", len(r), "found", hit)
