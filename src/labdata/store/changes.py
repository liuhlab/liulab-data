"""A refresh's re-fetch list: the GEO Series to fetch again though ``raw/`` may hold them.

The list holds the target Series GEO updated since the last successful refresh, less
:data:`OVERLAP_DAYS`; the target Series not yet held; the held target Series whose last SOFT
fetch failed; and every Series a relation line in their saved SOFT names. The fetch lanes take it as their ``refetch`` Series.

The store modules are imported where they are used, so the CLI reads :data:`OVERLAP_DAYS`
without loading pyarrow.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from labdata.store._files import write_atomic

if TYPE_CHECKING:
    from labdata.ncbi.entrez import EntrezClient

#: Days before the last successful refresh the change search starts, so a late update
#: GEO dates back a little is still found.
OVERLAP_DAYS = 3
#: UIDs per ``esearch`` call.
PAGE = 10_000
#: The relation lines whose Series are fetched again with the Series that names them.
RELATION = re.compile(
    r"^!Series_relation = (?:Reanalysis of|SuperSeries of|SubSeries of): (GSE\d+)", re.M
)
#: The experiment a Sample's SRA relation line names.
SRA_RELATION = re.compile(r"^!Sample_relation\s*=\s*SRA:\s*\S*?[?&]term=([SED]RX\d+)", re.M)


@dataclass(frozen=True, slots=True)
class ChangeList:
    """The re-fetch list written, and how many Series each way found."""

    path: Path
    updated: int
    new: int
    failed: int
    related: int
    total: int


def updated_series(client: EntrezClient, since: dt.date) -> set[str]:
    """Return every GEO Series whose newest update is on or after ``since``.

    The search is open-ended, as ``[UDAT]`` keeps only a Series' newest update date.
    """
    from labdata.store.fetch import GDS_OFFSET

    term = f"gse[ETYP] AND {since:%Y/%m/%d}:3000[UDAT]"
    offset = GDS_OFFSET["GSE"]
    uids = (int(uid) for uid in client.esearch_all("gds", term, page=PAGE))
    return {f"GSE{uid - offset}" for uid in uids if offset < uid < GDS_OFFSET["GSM"]}


def related(root: Path, series: Iterable[str]) -> set[str]:
    """Return ``series`` and every Series their saved SOFT names by a relation line, in turn."""
    from labdata.store import raw

    found = set(series)
    frontier = set(found)
    while frontier:
        frontier = raw.scan(root, raw.GEO_SOFT, frontier, RELATION) - found
        found |= frontier
    return found


def refetch_experiments(root: Path, series: Iterable[str]) -> set[str]:
    """Return the experiments the saved SOFT of ``series`` and their related Series name."""
    from labdata.store import raw

    return raw.scan(root, raw.GEO_SOFT, related(root, series), SRA_RELATION)


def change_list(
    root: Path,
    last_refresh: dt.date,
    out: Path,
    *,
    client: EntrezClient,
    snapshot_date: str | None = None,
) -> ChangeList:
    """Write the re-fetch list, one Series per line, and return it with its counts.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    last_refresh : datetime.date
        The date of the last successful refresh.
    out : pathlib.Path
        The file to write; it appears whole.
    client : EntrezClient
        The NCBI seam, for the ``gds`` search.
    snapshot_date : str or None
        The ``geo-series`` snapshot of the target list; defaults to the newest.

    Returns
    -------
    ChangeList
        The file and its counts; a Series found two ways counts once, the first way.
    """
    import pyarrow.parquet as pq

    from labdata.store import raw, targets

    path = targets.target_list(root, snapshot_date)
    wanted = {
        gse
        for batch in pq.ParquetFile(path).iter_batches(columns=["accession"])
        for gse in batch.column(0).to_pylist()
    }
    since = last_refresh - dt.timedelta(days=OVERLAP_DAYS)
    updated = updated_series(client, since) & wanted
    unheld = wanted - updated
    unheld -= raw.held(root, raw.GEO_SOFT, unheld) | raw.held(root, raw.GEO_GAPS, unheld)
    failed = soft_failed(root, wanted) - updated
    listed = related(root, updated | unheld | failed)
    out.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(out, "".join(f"{gse}\n" for gse in sorted(listed, key=_number)))
    rest = listed - updated - unheld - failed
    return ChangeList(out, len(updated), len(unheld), len(failed), len(rest), len(listed))


def soft_failed(root: Path, series: Iterable[str]) -> set[str]:
    """Return which of ``series`` hold a SOFT older than their ``soft-missing`` gap row.

    Such a Series was held when a re-fetch of its SOFT failed: no later fetch asks for it
    unless GEO updates it again.
    """
    from labdata.store import gaps, raw

    gap_rows = raw.saved(root, raw.GEO_GAPS, series)
    missing = {gse for gse, text in gap_rows.items() if text.split("\n", 1)[0] == gaps.SOFT_MISSING}
    gap_dates = raw.fetch_dates(root, raw.GEO_GAPS, missing)
    soft = raw.fetch_dates(root, raw.GEO_SOFT, missing)
    return {gse for gse in missing if gse in soft and gap_dates[gse] > soft[gse]}


def _number(accession: str) -> tuple[str, int]:
    return accession[:3], int(accession[3:])
