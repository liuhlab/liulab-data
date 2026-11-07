"""Removals: confirm which held GEO Series are withdrawn, and write removal parts for them.

A removal candidate is a Series ``raw/`` holds that the newest ``geo-series`` snapshot no
longer lists, or that this refresh's fetch got no record for. It is removed only when a fetch
of it alone confirms it is gone: a record-level "cannot get document summary" in a 200 reply,
or NCBI's "ID list is empty". Any other answer keeps it; the next refresh finds it again. A
gone Series takes its rows in every Series-keyed source with it. SRA records are never
candidates. Removal parts are applied by pack (see :mod:`labdata.store.raw`).
"""

from __future__ import annotations

import gzip
import json
import logging
import re
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import pyarrow.parquet as pq

from labdata.exceptions import AccessionError, EntrezError
from labdata.ncbi.entrez import EntrezClient, ids_empty
from labdata.store import gaps, raw, snapshots
from labdata.store.fetch import gds_uid
from labdata.store.model import kind_of

#: The raw sources keyed by GEO Series: a gone Series' rows leave each of them.
SERIES_KEYED = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.GEO_GAPS, raw.ENTREZ_LINKS, raw.PUBMED_SI)
#: Confirmation fetches in flight; the client's limiter sets the rate.
INFLIGHT = 4
#: Candidates confirmed between writes of removal parts: small enough that a cut-off run
#: loses little work, large enough that it writes few parts.
BATCH = 100

#: A Series' "not served" answer in a 200 ``esummary`` reply; the uid fills in.
_GONE = r"<ERROR>UID={uid}: cannot get document summary</ERROR>"
logger = logging.getLogger(__name__)


class _State(Enum):
    GONE = "gone"
    SERVED = "served"
    FAILED = "failed"


@dataclass
class ConfirmReport:
    """What one confirmation run found: candidates asked, confirmed gone, kept, failed.

    ``removals`` counts the accessions each source's removal parts list.
    """

    asked: int = 0
    gone: int = 0
    kept: int = 0
    failed: int = 0
    removals: dict[str, int] = field(default_factory=dict)


def candidates(root: Path, extra: Iterable[str] = ()) -> list[str]:
    """Return the held Series that may be withdrawn, sorted.

    A candidate is a held Series the newest ``geo-series`` snapshot does not list, a Series
    this refresh saved as ``soft-missing``, or one of ``extra``.
    """
    found = set(extra)
    listed = _gds_series(root)
    found |= {gse for gse in _held_series(root) if gse not in listed}
    for path in raw.part_files(root, raw.GEO_GAPS):
        for gse, text in _part_rows(path, "raw_text"):
            if text.split("\n", 1)[0] == gaps.SOFT_MISSING:
                found.add(gse)
    return sorted(found)


def confirm(root: Path, accessions: Iterable[str], *, client: EntrezClient) -> ConfirmReport:
    """Ask NCBI about each candidate alone and write removal parts for those confirmed gone.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    accessions : iterable of str
        GEO Series from :func:`candidates`. A Series a pending removal part already lists is
        not asked again; any other accession is skipped with a warning.
    client : EntrezClient
        The NCBI seam.

    Returns
    -------
    ConfirmReport
        Counts for this run.
    """
    pending = _pending_removals(root)
    series = set()
    for accession in accessions:
        if _is_series(accession):
            series.add(accession)
        else:
            logger.warning("not confirmed, as only GEO Series are: %s", accession)
    todo = sorted(series - pending)
    report = ConfirmReport(asked=len(todo))
    logger.info("confirming %d removal candidates", len(todo))
    with ThreadPoolExecutor(INFLIGHT, thread_name_prefix="confirm") as pool:
        for start in range(0, len(todo), BATCH):
            batch = todo[start : start + BATCH]
            states = dict(zip(batch, pool.map(lambda a: _state(client, a), batch), strict=True))
            _apply(root, states, report)
            logger.info(
                "confirmed %d of %d: %d gone, %d served, %d failed",
                start + len(batch),
                len(todo),
                report.gone,
                report.kept,
                report.failed,
            )
    return report


def _is_series(accession: str) -> bool:
    try:
        return kind_of(accession) == "GEO Series"
    except AccessionError:
        return False


def _state(client: EntrezClient, accession: str) -> _State:
    """Ask for one Series alone; return whether GEO serves it."""
    uid = str(gds_uid(accession))
    try:
        text = client.eutil_text("esummary", {"db": "gds", "id": uid})
    except EntrezError as err:
        if ids_empty(err):
            return _State.GONE
        logger.warning("could not confirm %s: %s", accession, err)
        return _State.FAILED
    if re.search(rf"<Id>{uid}</Id>", text):
        return _State.SERVED
    if re.search(_GONE.format(uid=uid), text):
        return _State.GONE
    logger.warning(
        "could not confirm %s: an answer that is neither a record nor not-found", accession
    )
    return _State.FAILED


def _apply(root: Path, states: dict[str, _State], report: ConfirmReport) -> None:
    """Write one batch's removal parts and count its answers."""
    gone = {a for a, state in states.items() if state is _State.GONE}
    report.gone += len(gone)
    report.failed += sum(state is _State.FAILED for state in states.values())
    report.kept += sum(state is _State.SERVED for state in states.values())
    if not gone:
        return
    for source in SERIES_KEYED:
        held = raw.held(root, source, gone)
        if held:
            raw.write_removals(root, source, held)
            report.removals[source] = report.removals.get(source, 0) + len(held)


def _pending_removals(root: Path) -> set[str]:
    return {
        accession for path in raw.part_files(root, raw.GEO_SOFT) for accession in raw.removed(path)
    }


def _part_rows(path: Path, column: str) -> Iterator[tuple[str, str]]:
    """Yield a part's accessions with one more column, a batch at a time."""
    for batch in pq.ParquetFile(path).iter_batches(columns=["accession", column]):
        values = batch.to_pydict()
        yield from zip(values["accession"], values[column], strict=True)


def _held_series(root: Path) -> set[str]:
    return {a for source in SERIES_KEYED for a in raw.held(root, source) if a.startswith("GSE")}


def _gds_series(root: Path) -> set[str]:
    """Return every Series the newest ``geo-series`` snapshot lists."""
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        return {json.loads(line)["accession"] for line in fh if line.strip()}
