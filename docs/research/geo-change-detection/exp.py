# ruff: noqa
"""Pilot: does a Series' Entrez UDAT match its SOFT last-update date, and what moves it."""

import json
import random
import re
import sys
from datetime import datetime

sys.path.insert(0, sys.argv[1])
from es import fetch, search  # noqa: E402

OUT = sys.argv[1] + "/out"
random.seed(99)
ACC = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={}&targ=self&form=text&view=brief"


def brief(acc):
    t = fetch(ACC.format(acc))
    return t.decode("utf-8", "replace") if t else ""


def field(text, name):
    return re.findall(r"^!\w+_" + name + r" = (.*)$", text, re.M)


def d(s):
    return datetime.strptime(s.strip(), "%b %d %Y").date()


def ids(term, n):
    _, idl, _ = search(term, retmax=n)
    return idl


def gse_of(uid):
    return "GSE" + str(int(uid) - 200000000)


res = {}
day = sys.argv[2]  # e.g. 2026/10/15
# A: GSE with UDAT=day: SOFT last_update_date
udat = [gse_of(u) for u in ids(f"gse[ETYP] AND {day}[UDAT]", 10000)]
pdat = [gse_of(u) for u in ids(f"gse[ETYP] AND {day}[PDAT]", 10000)]
res["udat_n"], res["pdat_n"] = len(udat), len(pdat)
res["pdat_not_in_udat"] = sorted(set(pdat) - set(udat))
sample = random.sample(udat, min(40, len(udat)))
a = []
for g in sample:
    t = brief(g)
    lu = field(t, "last_update_date")
    st = field(t, "status")
    sub = field(t, "submission_date")
    a.append(
        {
            "gse": g,
            "last_update": lu[0] if lu else None,
            "status": st[0] if st else None,
            "submitted": sub[0] if sub else None,
        }
    )
res["A"] = a
json.dump(res, open(f"{OUT}-{day.replace('/', '')}.json", "w"), indent=1)
print("A done", len(a))

# B: GSM with UDAT=day: does its Series also carry UDAT=day?
gsm_uids = ids(f"gsm[ETYP] AND {day}[UDAT]", 10000)
res["gsm_udat_n"] = len(gsm_uids)
udat_set = set(udat)
b = []
for u in random.sample(gsm_uids, min(40, len(gsm_uids))):
    gsm = "GSM" + str(int(u) - 300000000)
    t = brief(gsm)
    series = field(t, "series_id")
    lu = field(t, "last_update_date")
    rows = {"gsm": gsm, "last_update": lu[0] if lu else None, "series": []}
    for s in series:
        ts = brief(s.strip())
        slu = field(ts, "last_update_date")
        rows["series"].append(
            {
                "gse": s.strip(),
                "in_udat_day": s.strip() in udat_set,
                "last_update": slu[0] if slu else None,
            }
        )
    b.append(rows)
res["B"] = b
json.dump(res, open(f"{OUT}-{day.replace('/', '')}.json", "w"), indent=1)
print("B done", len(b))
