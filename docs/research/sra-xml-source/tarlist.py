"""List member order and mtimes of a truncated .tar.gz. Arg: part."""

import collections
import datetime as dt
import sys
import tarfile

order, mt = [], collections.Counter()
newest = []
try:
    with tarfile.open(sys.argv[1], "r|gz") as tf:
        for m in tf:
            top = m.name.lstrip("./").split("/")[0]
            if not order or order[-1] != top:
                order.append(top)
            d = dt.datetime.utcfromtimestamp(m.mtime)
            mt[d.strftime("%Y")] += 1
            newest.append((m.mtime, m.name))
except Exception as e:
    print("stopped:", type(e).__name__)
print("folders in order:", len(order))
for i in range(0, len(order), max(1, len(order) // 25)):
    print(i, order[i])
print("last", order[-5:])
print("mtime years", sorted(mt.items()))
newest.sort()
for t, n in newest[-5:]:
    print(dt.datetime.utcfromtimestamp(t).isoformat(), n)
