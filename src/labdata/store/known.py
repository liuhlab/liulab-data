"""Build ``known.parquet``: every accession the snapshots list, with its snapshot's date.

The accessions come from each snapshot's extract (``labdata store extract``), deduped per
source. DuckDB sorts with a memory cap and spills to ``<root>/work/tmp``.
"""

from __future__ import annotations

import json
from pathlib import Path

from labdata.store import extract, snapshots

#: The snapshot sources ``known.parquet`` draws from.
SOURCES = (
    snapshots.GEO_SERIES,
    snapshots.SRA_ACCESSIONS,
    snapshots.BIOPROJECT,
    snapshots.BIOSAMPLE,
)

#: The Parquet key-value metadata key holding each source's snapshot date, as JSON.
DATES_KEY = "snapshot_dates"


def write(root: Path, out: Path) -> dict[str, str]:
    """Write ``out`` from the extract of each source's newest snapshot.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    out : pathlib.Path
        The ``known.parquet`` path.

    Returns
    -------
    dict of str to str
        Each source's snapshot date, also stored in the file's metadata under ``DATES_KEY``.

    Raises
    ------
    StoreBuildError
        If a source's snapshot is missing or not extracted.
    """
    folders = {source: extract.done_dir(root, source) for source in SOURCES}
    dates = {source: date for source, (_, date) in folders.items()}
    selects = [
        f"SELECT DISTINCT accession, {extract.sql_string(source)} AS source, "
        f"{extract.sql_string(date)} AS snapshot_date FROM read_parquet({extract.parts(folder)})"
        for source, (folder, date) in folders.items()
    ]
    meta = extract.sql_string(json.dumps(dates, sort_keys=True))
    con = extract.connect(root)
    try:
        con.execute(
            f"COPY (SELECT * FROM ({' UNION ALL '.join(selects)}) ORDER BY accession, source) "
            f"TO {extract.sql_string(str(out))} (FORMAT parquet, KV_METADATA {{{DATES_KEY}: {meta}}})"
        )
    finally:
        con.close()
    return dates
