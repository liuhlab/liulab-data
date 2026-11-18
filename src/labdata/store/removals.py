"""Removals: confirm which held GEO Series are withdrawn, and write removal parts for them.

A removal candidate is a Series ``raw/`` holds that the newest ``geo-series`` snapshot no
longer lists, or that this refresh's fetch got no record for. It is removed only when a fetch
of it alone confirms it is gone: a record-level "cannot get document summary" in a 200 reply,
or NCBI's "ID list is empty". Any other answer keeps it; the next refresh finds it again. A
gone Series takes its rows in every Series-keyed source with it. One the newest snapshot lists
may be a target Series, so its ``geo-gaps`` row becomes a ``withdrawn`` gap instead, which
makes it no candidate and no fetch again. SRA records are never candidates. Every read of
``raw/`` and pack apply removal parts (see :mod:`labdata.store.raw`).
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import re
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from labdata.exceptions import AccessionError
from labdata.ncbi.entrez import EntrezClient
from labdata.store import gaps, raw, snapshots
from labdata.store._removal import INFLIGHT, State, ask_alone, write_removals
from labdata.store.fetch import gds_uid
from labdata.store.model import kind_of

#: The raw sources keyed by GEO Series: a gone Series' rows leave each of them.
SERIES_KEYED = (raw.GEO_SOFT, raw.GEO_FILELIST, raw.GEO_GAPS, raw.ENTREZ_LINKS, raw.PUBMED_SI)
#: Candidates confirmed between writes of removal parts: small enough that a cut-off run
#: loses little work, large enough that it writes few parts.
BATCH = 100

#: A Series' "not served" answer in a 200 ``esummary`` reply; the uid fills in.
_GONE = r"<ERROR>UID={uid}: cannot get document summary</ERROR>"
logger = logging.getLogger(__name__)


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
    for gse, text in raw.saved(root, raw.GEO_GAPS, since_pack=True):
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
        GEO Series from :func:`candidates`; any other accession is skipped with a warning.
    client : EntrezClient
        The NCBI seam.

    Returns
    -------
    ConfirmReport
        Counts for this run.
    """
    listed = _gds_series(root)
    series = set()
    for accession in accessions:
        if _is_series(accession):
            series.add(accession)
        else:
            logger.warning("not confirmed, as only GEO Series are: %s", accession)
    todo = sorted(series)
    report = ConfirmReport(asked=len(todo))
    logger.info("confirming %d removal candidates", len(todo))
    with ThreadPoolExecutor(INFLIGHT, thread_name_prefix="confirm") as pool:
        for start in range(0, len(todo), BATCH):
            batch = todo[start : start + BATCH]
            states = dict(zip(batch, pool.map(lambda a: _state(client, a), batch), strict=True))
            _apply(root, states, report, listed)
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


def _state(client: EntrezClient, accession: str) -> State:
    """Ask for one Series alone; return whether GEO serves it."""
    uid = str(gds_uid(accession))
    state, _ = ask_alone(
        client,
        accession,
        "esummary",
        {"db": "gds", "id": uid},
        served=lambda text: text if re.search(rf"<Id>{uid}</Id>", text) else None,
        gone=lambda text: re.search(_GONE.format(uid=uid), text) is not None,
    )
    return state


def _apply(root: Path, states: dict[str, State], report: ConfirmReport, listed: set[str]) -> None:
    """Write one batch's withdrawn gap rows and removal parts, and count its answers."""
    gone = {a for a, state in states.items() if state is State.GONE}
    report.gone += len(gone)
    report.failed += sum(state is State.FAILED for state in states.values())
    report.kept += sum(state is State.SERVED for state in states.values())
    if not gone:
        return
    # Saved before the removal parts, which a rerun reads as this batch done.
    today = dt.datetime.now(dt.UTC).date().isoformat()
    withdrawn = gone & listed
    raw.save(root, raw.GEO_GAPS, [gaps.gap_row(a, gaps.WITHDRAWN, today) for a in withdrawn])
    for source in SERIES_KEYED:
        drop = raw.held(root, source, gone)
        if source == raw.GEO_GAPS:
            drop -= withdrawn
        write_removals(root, source, drop, report.removals)


def _held_series(root: Path) -> set[str]:
    """Return the Series held in a Series-keyed source, a ``withdrawn`` gap row aside."""
    keyed = [source for source in SERIES_KEYED if source != raw.GEO_GAPS]
    found = {a for source in keyed for a in raw.held(root, source)}
    found |= {
        a for a, text in raw.saved(root, raw.GEO_GAPS) if text.split("\n", 1)[0] != gaps.WITHDRAWN
    }
    return {a for a in found if a.startswith("GSE")}


def _gds_series(root: Path) -> set[str]:
    """Return every Series the newest ``geo-series`` snapshot lists."""
    snap = snapshots.snapshot(root, snapshots.GEO_SERIES)
    with gzip.open(snap.path, "rt", encoding="utf-8") as fh:
        return {json.loads(line)["accession"] for line in fh if line.strip()}
