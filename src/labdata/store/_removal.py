"""What the removal steps share: asking NCBI about one record alone, and writing removals."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection
from enum import Enum
from pathlib import Path

from labdata.exceptions import EntrezError
from labdata.ncbi.entrez import EntrezClient, ids_empty
from labdata.store import raw

#: Confirmation fetches in flight; the client's limiter sets the rate.
INFLIGHT = 4
logger = logging.getLogger(__name__)


class State(Enum):
    """NCBI's answer about one record asked alone."""

    GONE = "gone"
    SERVED = "served"
    FAILED = "failed"


def ask_alone(
    client: EntrezClient,
    accession: str,
    eutil: str,
    params: dict[str, str],
    *,
    served: Callable[[str], str | None],
    gone: Callable[[str], bool],
) -> tuple[State, str]:
    """Ask NCBI about one record alone and classify the answer.

    Parameters
    ----------
    client : EntrezClient
        The NCBI seam.
    accession : str
        The record asked about, for the log.
    eutil : str
        The E-utility to call.
    params : dict of str to str
        Its query parameters.
    served : callable
        Returns the record from a reply that serves it, else ``None`` or empty.
    gone : callable
        Whether a reply that does not serve the record says it is gone.

    Returns
    -------
    tuple of (State, str)
        The state and, when served, what ``served`` returned. "ID list is empty" is gone;
        any other error or unclear reply is failed.
    """
    try:
        text = client.eutil_text(eutil, params)
    except EntrezError as err:
        if ids_empty(err):
            return State.GONE, ""
        logger.warning("could not confirm %s: %s", accession, err)
        return State.FAILED, ""
    if record := served(text):
        return State.SERVED, record
    if gone(text):
        return State.GONE, ""
    logger.warning(
        "could not confirm %s: an answer that is neither a record nor not-found", accession
    )
    return State.FAILED, ""


def write_removals(
    root: Path, source: str, accessions: Collection[str], counts: dict[str, int]
) -> None:
    """Write a removal part for ``accessions`` in ``source`` and add them to ``counts``."""
    if not accessions:
        return
    raw.write_removals(root, source, accessions)
    counts[source] = counts.get(source, 0) + len(accessions)


def pending_removals(root: Path, source: str) -> set[str]:
    """Return the accessions the pending removal parts of ``source`` list."""
    return {a for path in raw.part_files(root, source) for a in raw.removed(path)}
