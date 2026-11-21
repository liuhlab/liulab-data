"""The update: bring each declared vocabulary's current release into the store.

One process under the store's run lock. Per vocabulary it reads the release with no full
download, from a header or the file's date, downloads the files only when that release has no
snapshot yet, reads them and writes the snapshot. Then it switches every vocabulary that succeeded at once.
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
from labdata.ontology import cellosaurus, fetch, lifecycle, obo, paths, taxdump
from labdata.ontology.lifecycle import Outcome, Result, UpdateStatus, VocabularyResult
from labdata.ontology.root import find_root, switch
from labdata.ontology.tables import (
    TABLE_LAYOUT,
    SnapshotData,
    layout_of,
    snapshot_name,
    write_snapshot,
)
from labdata.ontology.vocabularies import (
    VOCABULARIES,
    ReaderKind,
    VersionRule,
    Vocabulary,
    file_date,
    owl_version,
    release_of,
)

logger = logging.getLogger(__name__)


def update(
    root: str | os.PathLike[str] | None = None,
    *,
    vocabularies: Sequence[Vocabulary] | None = None,
    transport: Transport | None = None,
    owner_gone: bool = False,
) -> UpdateStatus:
    """Update every vocabulary to its current release, under the store's run lock.

    A vocabulary that fails keeps its current snapshot, and the others still update.

    Parameters
    ----------
    root : str or os.PathLike, optional
        The ontology store root; default ``LABDATA_ONTOLOGIES``, then the pointer.
    vocabularies : Sequence of Vocabulary, optional
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
    chosen = VOCABULARIES if vocabularies is None else vocabularies
    with lifecycle.run_lock(path, "update", owner_gone=owner_gone):
        start = utc_stamp()
        lifecycle.write_status(path, UpdateStatus(start=start, outcome=Outcome.RUNNING))
        try:
            rows = {vocab.name: _update_one(path, vocab, web) for vocab in chosen}
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
        release = current_release(vocab, transport=transport)
        name = snapshot_name(vocab.name, release)
        if layout_of(root, name) == TABLE_LAYOUT:
            return VocabularyResult(release, name, Result.UNCHANGED)
        sources = [paths.DOWNLOADS.at(root, _file_name(url)) for url in vocab.downloads]
        try:
            for url, source in zip(vocab.downloads, sources, strict=True):
                fetch.download(url, source, transport=transport)
            data = _read(vocab, sources, release, transport)
            name = write_snapshot(root, data, sources=sources)
        finally:
            for source in sources:
                source.unlink(missing_ok=True)
        return VocabularyResult(data.release, name, Result.UPDATED)
    except LabdataError as err:
        logger.warning("%s: %s", vocab.name, err)
        return VocabularyResult(release, None, Result.FAILED, str(err))


def current_release(vocab: Vocabulary, *, transport: Transport) -> str:
    """Return the release a vocabulary's source states now, read with no full download.

    Raises
    ------
    LabdataError
        If the source cannot be reached, or states no usable release.
    """
    if vocab.version is VersionRule.FILE_DATE:
        return file_date(fetch.last_modified(vocab.version_url, transport=transport))
    head = fetch.read_head(vocab.version_url, transport=transport)
    if vocab.version is VersionRule.OWL_VERSION_INFO:
        return release_of(owl_version(head))
    return release_of(obo.header_version(head))


def _read(
    vocab: Vocabulary, sources: list[Path], release: str, transport: Transport
) -> SnapshotData:
    if vocab.reader is ReaderKind.TAXDUMP:
        listed = fetch.read_head(f"{vocab.url}.md5", transport=transport)
        taxdump.check_md5(sources[0], listed)
        return taxdump.read_taxdump(sources[0], vocab, release)
    if vocab.reader is ReaderKind.CELLOSAURUS:
        return cellosaurus.read_cellosaurus(sources[0], sources[1], vocab)
    stated = None if vocab.version is VersionRule.DATA_VERSION else release
    return obo.read_obo(sources[0], vocab, release=stated)


def _file_name(url: str) -> str:
    return Path(urllib.parse.urlsplit(url).path).name
