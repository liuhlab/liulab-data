"""The update: bring each declared vocabulary's current release into the store.

One process under the store's run lock. Per vocabulary it reads the release from the file's
header, downloads the file only when that release has no snapshot yet, reads it and writes
the snapshot. Then it switches every vocabulary that succeeded at once.
"""

from __future__ import annotations

import logging
import os
import urllib.parse
from collections.abc import Sequence
from pathlib import Path

from labdata._files import utc_stamp
from labdata._http import HttpTransport, Transport
from labdata.exceptions import LabdataError
from labdata.ontology import fetch, lifecycle, obo, paths
from labdata.ontology.lifecycle import Outcome, Result, UpdateStatus, VocabularyResult
from labdata.ontology.root import find_root, switch
from labdata.ontology.tables import TABLE_LAYOUT, layout_of, snapshot_name, write_snapshot
from labdata.ontology.vocabularies import VOCABULARIES, Vocabulary, release_of

logger = logging.getLogger(__name__)


def update(
    root: str | os.PathLike[str] | None = None,
    *,
    vocabularies: Sequence[Vocabulary] = VOCABULARIES,
    transport: Transport | None = None,
    owner_gone: bool = False,
) -> UpdateStatus:
    """Update every vocabulary to its current release, under the store's run lock.

    A vocabulary that fails keeps its current snapshot, and the others still update.

    Parameters
    ----------
    root : str or os.PathLike, optional
        The ontology store root; default ``LABDATA_ONTOLOGIES``, then the pointer.
    vocabularies : Sequence of Vocabulary
        The vocabularies to update; default every declared one.
    transport : Transport, optional
        The HTTP transport every request goes through.
    owner_gone : bool
        The update holding the run lock has ended, though this host cannot check it.

    Returns
    -------
    UpdateStatus
        The status record as written; its outcome is ``failed`` if any vocabulary failed.

    Raises
    ------
    OntologyBusyError
        If another update holds the store's run lock.
    """
    path = find_root(root)
    web = transport if transport is not None else HttpTransport()
    with lifecycle.run_lock(path, "update", owner_gone=owner_gone):
        start = utc_stamp()
        lifecycle.write_status(path, UpdateStatus(start=start, outcome=Outcome.RUNNING))
        try:
            rows = {vocab.name: _update_one(path, vocab, web) for vocab in vocabularies}
            done = {name: row.snapshot for name, row in rows.items() if row.snapshot is not None}
            switch(path, done)
        except BaseException:
            lifecycle.write_status(
                path, UpdateStatus(start=start, end=utc_stamp(), outcome=Outcome.FAILED)
            )
            raise
        failed = any(row.result == Result.FAILED for row in rows.values())
        outcome = Outcome.FAILED if failed else Outcome.BUILT
        status = UpdateStatus(start, utc_stamp(), outcome, rows)
        lifecycle.write_status(path, status)
    return status


def _update_one(root: Path, vocab: Vocabulary, transport: Transport) -> VocabularyResult:
    release: str | None = None
    try:
        head = fetch.read_head(vocab.url, transport=transport)
        release = release_of(obo.header_version(head))
        name = snapshot_name(vocab.name, release)
        if layout_of(root, name) == TABLE_LAYOUT:
            return VocabularyResult(release, name, Result.UNCHANGED)
        source = paths.DOWNLOADS.at(root, _file_name(vocab))
        try:
            fetch.download(vocab.url, source, transport=transport)
            data = obo.read_obo(source, vocab)
            name = write_snapshot(root, data, sources=[source])
        finally:
            source.unlink(missing_ok=True)
        return VocabularyResult(data.release, name, Result.UPDATED)
    except LabdataError as err:
        logger.warning("%s: %s", vocab.name, err)
        return VocabularyResult(release, None, Result.FAILED, str(err))


def _file_name(vocab: Vocabulary) -> str:
    return Path(urllib.parse.urlsplit(vocab.url).path).name or f"{vocab.name}.obo"
