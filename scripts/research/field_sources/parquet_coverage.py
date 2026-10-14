"""Count the live runs in ``SRA_Accessions.tab`` that the SRA metadata Parquet lacks.

Joins the RUN rows of ``SRA_Accessions.tab`` with the Parquet ``acc`` column in DuckDB and
breaks the gap down by visibility, archive prefix, the ``Loaded`` flag and release year.
Writes ``parquet_coverage.json`` to ``--out``.

Run (DuckDB is not a labdata dependency)::

    pixi exec --spec python=3.12 --spec python-duckdb python \
        scripts/research/field_sources/parquet_coverage.py \
        --bulk-root "$BULK" --date 2026-10-14 --out out/
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb  # pyright: ignore[reportMissingImports]


def main() -> None:
    """Join, count and write the summary."""
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--bulk-root", type=Path, required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    parts = args.bulk_root / "sra-metadata-parquet" / args.date / "*.parquet"
    tab = args.bulk_root / "sra-accessions" / args.date / "SRA_Accessions.tab"

    con = duckdb.connect()
    con.execute(f"CREATE TABLE pq AS SELECT DISTINCT acc FROM read_parquet('{parts}')")
    con.execute(
        "CREATE TABLE runs AS SELECT Accession AS acc, Status AS status, Visibility AS vis, "
        "Loaded AS loaded, substr(Published, 1, 4) AS year "
        f"FROM read_csv('{tab}', delim='\\t', header=true, all_varchar=true, quote='') "
        "WHERE Type = 'RUN'"
    )
    query = (
        "SELECT r.status, r.vis, substr(r.acc, 1, 3) AS prefix, r.loaded, "
        "count(*) AS runs, count(p.acc) AS in_parquet "
        "FROM runs r LEFT JOIN pq p USING (acc) GROUP BY ALL ORDER BY runs DESC"
    )
    by_kind = con.execute(query).fetchall()
    missing_years = con.execute(
        "SELECT r.year, count(*) FROM runs r ANTI JOIN pq p USING (acc) "
        "WHERE r.status = 'live' AND r.vis = 'public' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    only_parquet = con.execute("SELECT count(*) FROM pq ANTI JOIN runs USING (acc)").fetchone()
    summary = {
        "parquet_runs": con.execute("SELECT count(*) FROM pq").fetchone(),
        "by_status_visibility_prefix_loaded": [
            dict(zip(["status", "visibility", "prefix", "loaded", "runs", "in_parquet"], r))
            for r in by_kind
        ],
        "live_public_missing_by_year": missing_years,
        "parquet_runs_not_in_accessions": only_parquet,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "parquet_coverage.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
