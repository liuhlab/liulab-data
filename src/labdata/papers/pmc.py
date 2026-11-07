"""The PMC bucket tier: a paper's XML, PDF and supplements from ``pmc-oa-opendata``."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Collection
from pathlib import Path

from labdata._http import Transport
from labdata.papers.tables import PaperTables


def fetch(
    state: PaperTables,
    papers: Collection[str],
    *,
    root: Path,
    transport: Transport,
    today: dt.date,
    log: Callable[[str], None],
) -> None:
    """Fetch what the PMC bucket holds for ``papers`` into the store, changing ``state``.

    It adds or renews ``files`` rows, writes the files under each paper's folder and sets
    ``xml_tier`` and ``pdf_tier`` on ``papers`` rows; the caller recomputes status and saves.
    Papers with no PMCID are passed over. Not built yet: it changes nothing.

    Parameters
    ----------
    state : PaperTables
        The paper store's tables, loaded.
    papers : collection of str
        The papers to try: every linked paper the PMC sync holds.
    root : pathlib.Path
        The paper store root.
    transport : Transport
        Sends the bucket's HTTPS requests.
    today : datetime.date
        The UTC date of the pass.
    log : callable
        Takes one progress line.
    """
