# ruff: noqa
"""Pilot 2: look for changes that leave a Series' last-update date behind."""

import json
import random
import re
import sys
from datetime import datetime, date

sys.path.insert(0, sys.argv[1])
from es import fetch, search  # noqa: E402

D = sys.argv[1]
random.seed(1099)
ACC = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}&targ=self&form=text&view=brief"
DIR = "https://ftp.ncbi.nlm.nih.gov/geo/series/{}nnn/{}/"
_brief = {}


def brief(acc):
    if acc not in _brief:
        t = fetch(ACC.format(acc))
        _brief[acc] = t.decode("utf-8", "replace") if t else ""
    return _brief[acc]


def field(text, name):
    return [v.strip() for v in re.findall(r"^!\w+?_" + name + r" = (.*)$", text, re.M)]


def d(s):
    return datetime.strptime(s.strip(), "%b %d %Y").date()


def listing(url):
    t = fetch(url)
    if not t:
        return None
    rows = re.findall(r'href="([^"/?][^"]*)">[^<]*</a>\s+(\d{4}-\d\d-\d\d) (\d\d:\d\d)', t.decode())
    return [(n, date.fromisoformat(dd)) for n, dd, _ in rows]


res = json.load(open(f"{D}/out-20261015.json"))
# R: Series released on the day but not updated that day
r = []
for g in res["pdat_not_in_udat"]:
    t = brief(g)
    lu, st = field(t, "last_update_date"), field(t, "status")
    r.append({"gse": g, "last_update": lu[0] if lu else None, "status": st[0] if st else None})
print("R done", len(r))

# S: random public Series across the whole list
total, _, _ = search("gse[ETYP]")
uids = []
for off in random.sample(range(0, total - 10000), 3):
    _, idl, _ = search("gse[ETYP]", retmax=10000, retstart=off)
    uids += idl
picks = ["GSE" + str(int(u) - 200000000) for u in random.sample(uids, 120)]
s = []
for g in picks:
    t = brief(g)
    lu = field(t, "last_update_date")
    if not lu:
        continue
    row = {
        "gse": g,
        "last_update": lu[0],
        "status": field(t, "status")[0],
        "types": field(t, "type"),
        "suppl_after": [],
        "rel_after": [],
    }
    last = d(lu[0])
    files = listing(DIR.format(g[:-3], g) + "suppl/")
    row["n_suppl"] = None if files is None else len(files)
    for n, fd in files or []:
        if fd > last:
            row["suppl_after"].append([n, fd.isoformat()])
    rels = field(t, "relation")
    row["relations"] = rels
    for rel in rels:
        m = re.match(
            r"(Reanalyzed by|SubSeries of|SuperSeries of|Alternative to|Reanalysis of)"
            r": (GSE\d+)",
            rel,
        )
        if not m:
            continue
        pt = brief(m.group(2))
        pst = field(pt, "status")
        if pst and pst[0].startswith("Public on "):
            pd = d(pst[0][len("Public on ") :])
            if pd > last:
                row["rel_after"].append([rel, pd.isoformat()])
    s.append(row)
print("S done", len(s))
json.dump({"R": r, "S": s}, open(f"{D}/out2.json", "w"), indent=1)
