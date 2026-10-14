"""Compare the SRA metadata Parquet with live SRA XML values, run by run.

Reads the Parquet parts under ``<bulk-root>/sra-metadata-parquet/<date>/`` with DuckDB, pulls
the rows for every run in ``live_runs.tsv`` (from ``live_reference.py``) and reports which
fields agree. It also counts GEO aliases across the whole table. Writes
``parquet_rows.tsv`` and ``parquet_summary.json`` to ``--out``.

Run (DuckDB is not a labdata dependency)::

    pixi exec --spec python=3.12 --spec python-duckdb python \
        scripts/research/field_sources/parquet_check.py \
        --bulk-root "$BULK" --date 2026-10-14 --live live/ --out out/
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb  # pyright: ignore[reportMissingImports]

COLUMNS = [
    "acc", "experiment", "sample_acc", "biosample", "sra_study", "bioproject", "assay_type",
    "librarysource", "libraryselection", "librarylayout", "instrument", "library_name",
    "sample_name", "organism", "center_name", "consent", "releasedate", "mbases", "mbytes",
    "avgspotlen", "attributes",
]  # fmt: skip

# live_runs.tsv field -> Parquet column, compared ignoring case and spaces.
PAIRS = {
    "srx": "experiment",
    "srs": "sample_acc",
    "biosample": "biosample",
    "srp": "sra_study",
    "bioproject": "bioproject",
    "strategy": "assay_type",
    "source": "librarysource",
    "selection": "libraryselection",
    "layout": "librarylayout",
    "instrument": "instrument",
    "library_name": "library_name",
    "sample_alias": "sample_name",
}


def norm(value: Any) -> str:
    """Case- and space-insensitive text."""
    return " ".join(str(value or "").lower().split())


def main() -> None:
    """Query, compare and write the summary."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--bulk-root", type=Path, required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--live", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    parts = str(args.bulk_root / "sra-metadata-parquet" / args.date / "*.parquet")

    with (args.live / "live_runs.tsv").open() as fh:
        live = {r["srr"]: r for r in csv.DictReader(fh, delimiter="\t")}
    biosamples = json.loads((args.live / "live_biosamples.json").read_text())

    con = duckdb.connect()
    con.execute(f"CREATE VIEW m AS SELECT * FROM read_parquet('{parts}')")
    accs = ", ".join(f"'{a}'" for a in live)
    rows = con.execute(f"SELECT {', '.join(COLUMNS)} FROM m WHERE acc IN ({accs})").fetchall()
    found = {r[0]: dict(zip(COLUMNS, r, strict=True)) for r in rows}

    with (args.out / "parquet_rows.tsv").open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(COLUMNS)
        for acc in sorted(found):
            writer.writerow([json.dumps(found[acc][c], default=str) for c in COLUMNS])

    agree: Counter[str] = Counter()
    differ: dict[str, Counter[str]] = {k: Counter() for k in PAIRS}
    numeric: Counter[str] = Counter()
    attrs: Counter[str] = Counter()
    for srr, run in live.items():
        pq = found.get(srr)
        if pq is None:
            continue
        for lf, pc in PAIRS.items():
            if norm(run[lf]) == norm(pq[pc]):
                agree[lf] += 1
            else:
                differ[lf][f"{run[lf]!s} | {pq[pc]!s}"] += 1
        bases, spots = int(run["bases"] or 0), int(run["spots"] or 0)
        numeric["mbases_eq_round"] += pq["mbases"] == round(bases / 1e6)
        numeric["mbases_eq_floor"] += pq["mbases"] == bases // 1_000_000
        numeric["avgspotlen_eq"] += bool(spots) and pq["avgspotlen"] == round(bases / spots)
        numeric["releasedate_eq_published_day"] += str(pq["releasedate"]) == run["published"][:10]
        bs = biosamples.get(run["biosample"])
        if bs is not None:
            pq_pairs = {(norm(a["k"]), norm(a["v"])) for a in pq["attributes"] or []}
            bs_pairs = {(norm(n), norm(v)) for n, _h, v in bs["attributes"]}
            bs_harm = {(norm(h), norm(v)) for _n, h, v in bs["attributes"] if h}
            attrs["runs_with_biosample"] += 1
            attrs["bs_attr_pairs"] += len(bs_pairs)
            attrs["bs_raw_pairs_in_parquet"] += len(bs_pairs & pq_pairs)
            attrs["bs_harmonized_pairs_in_parquet"] += len(bs_harm & pq_pairs)
            attrs["parquet_pairs"] += len(pq_pairs)
            keys = {k for k, _v in pq_pairs}
            attrs["parquet_keys_with_sam_suffix"] += sum(k.endswith("_sam") for k in keys)

    totals = con.execute(
        "SELECT count(*), count(DISTINCT experiment), "
        "count(*) FILTER (WHERE sample_name LIKE 'GSM%'), "
        "count(*) FILTER (WHERE library_name LIKE 'GSM%'), "
        "count(*) FILTER (WHERE center_name = 'GEO'), "
        "count(DISTINCT sample_name) FILTER (WHERE sample_name LIKE 'GSM%') "
        "FROM m"
    ).fetchone()
    consent = con.execute("SELECT consent, count(*) FROM m GROUP BY 1 ORDER BY 2 DESC").fetchall()
    attr_keys = con.execute(
        f"SELECT a.k, count(*) FROM (SELECT unnest(attributes) AS a FROM m WHERE acc IN ({accs})) "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 25"
    ).fetchall()
    summary = {
        "live_runs": len(live),
        "found_in_parquet": len(found),
        "missing": sorted(set(live) - set(found)),
        "agree": dict(agree),
        "differ": {k: dict(v) for k, v in differ.items() if v},
        "numeric": dict(numeric),
        "attributes": dict(attrs),
        "attribute_keys_sample": attr_keys,
        "table": dict(
            zip(
                [
                    "runs",
                    "experiments",
                    "runs_sample_name_gsm",
                    "runs_library_name_gsm",
                    "runs_center_geo",
                    "distinct_gsm_sample_names",
                ],
                totals or [],
                strict=True,
            )
        ),
        "consent": consent,
    }
    (args.out / "parquet_summary.json").write_text(json.dumps(summary, indent=1, default=str))
    print(json.dumps(summary, indent=1, default=str))


if __name__ == "__main__":
    main()
