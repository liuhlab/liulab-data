"""Build ``known.parquet``: every public accession the snapshots list, with its snapshot's date.

The accessions come from each snapshot's extract (``labdata store extract``), deduped per
source. Only ``live`` rows of ``SRA_Accessions.tab`` and BioSample count, so a withdrawn or
suppressed record is unknown, not merely unheld. The table depends only on the snapshot
dates, so it is sorted once per set of dates into ``<root>/work/known/`` and each build links
that file. A sample build writes its own
share straight into the build.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq

from labdata.exceptions import StoreBuildError
from labdata.store import extract, layout, snapshots, targets
from labdata.store._files import temp_beside
from labdata.store.layout import sql_string

logger = logging.getLogger(__name__)

#: The snapshot sources ``known.parquet`` draws from.
SOURCES = (
    snapshots.GEO_SERIES,
    snapshots.SRA_ACCESSIONS,
    snapshots.BIOPROJECT,
    snapshots.BIOSAMPLE,
)

#: The Parquet key-value metadata key holding each source's snapshot date, as JSON.
DATES_KEY = "snapshot_dates"

#: The sources whose rows carry a status; only ``live`` rows count.
_STATUSED = (snapshots.SRA_ACCESSIONS, snapshots.BIOSAMPLE)


def write(
    root: Path,
    out: Path,
    *,
    source: targets.BuildSource | None = None,
    held: Path | None = None,
) -> dict[str, str]:
    """Write ``out`` from the extract of each source's newest snapshot.

    ``out`` is a hard link to (or, across file systems, a copy of) the cached table for these
    snapshot dates, which is written first if missing. When ``source`` samples, ``out`` is
    written directly and keeps only the accessions in that seeded sample or in ``held``.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    out : pathlib.Path
        The ``known.parquet`` path.
    source : labdata.store.targets.BuildSource or None
        Where the extracts are read, and the sample kept; defaults to all of ``root``.
    held : pathlib.Path or None
        A sample build's ``holdings.parquet``; its accessions are kept too.

    Returns
    -------
    dict of str to str
        Each source's snapshot date, also stored in the file's metadata under ``DATES_KEY``.

    Raises
    ------
    StoreBuildError
        If a source's snapshot is missing, not extracted, or extracted without a status column.
    """
    read = root if source is None else source.root
    folders = {name: extract.done_dir(read, name) for name in SOURCES}
    for name in _STATUSED:
        _require_status(name, *folders[name])
    dates = {name: date for name, (_, date) in folders.items()}
    if source is not None and source.sample is not None:
        _sort(root, folders, dates, out, source.sample, held)
        return dates
    key = "_".join(f"{source}-{date}" for source, date in dates.items())
    cached = root / "work" / "known" / f"known-public-{key}.parquet"
    if not cached.is_file():
        cached.parent.mkdir(parents=True, exist_ok=True)
        tmp = temp_beside(cached)
        try:
            _sort(root, folders, dates, tmp, None, None)
            tmp.replace(cached)
        finally:
            tmp.unlink(missing_ok=True)
    try:
        os.link(cached, out)
    except OSError:
        shutil.copyfile(cached, out)
    return dates


def _sort(
    root: Path,
    folders: dict[str, tuple[Path, str]],
    dates: dict[str, str],
    out: Path,
    sample: float | None,
    held: Path | None,
) -> None:
    selects = [
        f"SELECT DISTINCT accession, {sql_string(source)} AS source, "
        f"{sql_string(date)} AS snapshot_date FROM read_parquet({extract.parts(folder)})"
        + (" WHERE status = 'live'" if source in _STATUSED else "")
        for source, (folder, date) in folders.items()
    ]
    tally = None
    with extract.connect(root) as con:
        if sample is not None:
            tally = targets.add_in_sample(con, sample)
            kept = "in_sample(accession)"
            if held is not None:
                listed = f"read_parquet({sql_string(str(held))})"
                kept += f" OR accession IN (SELECT accession FROM {listed})"
            selects = [f"SELECT * FROM ({s}) WHERE {kept}" for s in selects]
        layout.write(
            con,
            layout.KNOWN,
            out,
            " UNION ALL ".join(selects),
            {DATES_KEY: json.dumps(dates, sort_keys=True)},
        )
        if tally is not None:
            logger.info(
                "known: the sample kept %d accessions and dropped %d, then added the held ones",
                tally.kept,
                tally.dropped,
            )


def _require_status(source: str, folder: Path, date: str) -> None:
    part = next(folder.glob("part-*.parquet"), None)
    if part is not None and "status" not in pq.read_schema(part).names:
        raise StoreBuildError(
            f"the {source} extract of {date} has no status column; delete {folder} "
            f"and run `labdata store extract --source {source}` again"
        )
