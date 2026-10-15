"""Build ``known.parquet``: every accession the snapshots list, with its snapshot's date.

The accessions come from each snapshot's extract (``labdata store extract``), deduped per
source. The table depends only on the snapshot dates, so it is sorted once per set of dates
into ``<root>/work/known/`` and each build links that file.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from labdata.store import extract, snapshots
from labdata.store._files import temp_beside

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

    ``out`` is a hard link to (or, across file systems, a copy of) the cached table for these
    snapshot dates, which is written first if missing.

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
    key = "_".join(f"{source}-{date}" for source, date in dates.items())
    cached = root / "work" / "known" / f"known-{key}.parquet"
    if not cached.is_file():
        cached.parent.mkdir(parents=True, exist_ok=True)
        tmp = temp_beside(cached)
        try:
            _sort(root, folders, dates, tmp)
            tmp.replace(cached)
        finally:
            tmp.unlink(missing_ok=True)
    try:
        os.link(cached, out)
    except OSError:
        shutil.copyfile(cached, out)
    return dates


def _sort(
    root: Path, folders: dict[str, tuple[Path, str]], dates: dict[str, str], out: Path
) -> None:
    selects = [
        f"SELECT DISTINCT accession, {extract.sql_string(source)} AS source, "
        f"{extract.sql_string(date)} AS snapshot_date FROM read_parquet({extract.parts(folder)})"
        for source, (folder, date) in folders.items()
    ]
    meta = extract.sql_string(json.dumps(dates, sort_keys=True))
    with extract.connect(root) as con:
        con.execute(
            f"COPY (SELECT * FROM ({' UNION ALL '.join(selects)}) ORDER BY accession, source) "
            f"TO {extract.sql_string(str(out))} (FORMAT parquet, KV_METADATA {{{DATES_KEY}: {meta}}})"
        )
