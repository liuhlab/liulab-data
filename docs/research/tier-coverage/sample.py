"""Draw the seeded stratum samples from the PMID table.

Usage: python sample.py <pmid-pmcid-doi.parquet> <out_dir>
"""

import json
import random
import sys
from pathlib import Path

import duckdb

SEED = 20261107
SIZES = {"A": 300, "B": 500, "C": 500}

src, out = sys.argv[1], Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
strata = {
    "A": "pmcid IS NOT NULL AND in_pmc_cloud",
    "B": "pmcid IS NOT NULL AND NOT in_pmc_cloud",
    "C": "pmcid IS NULL",
}
counts = {}
for name, where in strata.items():
    rows = con.execute(
        f"SELECT pmid, pmcid, doi FROM read_parquet(?) WHERE {where} ORDER BY pmid", [src]
    ).fetchall()
    counts[name] = len(rows)
    pick = random.Random(SEED).sample(rows, SIZES[name])
    recs = [{"pmid": r[0], "pmcid": r[1], "doi": r[2]} for r in pick]
    (out / f"sample_{name}.json").write_text(json.dumps(recs))
(out / "strata.json").write_text(json.dumps(counts))
print(counts)
