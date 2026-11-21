# ruff: noqa
"""Pilot 3: does a new 'Reanalysis of' Series move the original's last-update date."""

import json
import re
import sys
from datetime import datetime

sys.path.insert(0, sys.argv[1])
from es import fetch, search  # noqa: E402

ACC = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}&targ=self&form=text&view=brief"


def brief(acc):
    t = fetch(ACC.format(acc))
    return t.decode("utf-8", "replace") if t else ""


def field(text, name):
    return [v.strip() for v in re.findall(r"^!\w+?_" + name + r" = (.*)$", text, re.M)]


def d(s):
    return datetime.strptime(s.strip(), "%b %d %Y").date()


out = []
for day in ["2026/10/15", "2026/10/14", "2026/10/13"]:
    _, idl, _ = search(f"gse[ETYP] AND {day}[PDAT]", retmax=10000)
    for u in idl:
        g = "GSE" + str(int(u) - 200000000)
        t = brief(g)
        for rel in field(t, "relation"):
            m = re.match(r"Reanalysis of: (GS[EM]\d+)", rel)
            if not m:
                continue
            pt = brief(m.group(1))
            plu = field(pt, "last_update_date")
            back = [x for x in field(pt, "relation") if g in x]
            out.append(
                {
                    "new": g,
                    "public": field(t, "status")[0],
                    "orig": m.group(1),
                    "orig_last_update": plu[0] if plu else None,
                    "back_link": back,
                }
            )
    print(day, len(out), flush=True)
json.dump(out, open(sys.argv[1] + "/out3.json", "w"), indent=1)
