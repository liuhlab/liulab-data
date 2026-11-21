"""The update: bring each declared vocabulary's current release into the store.

One process under the store's run lock. It first remakes, from their kept source files, the
tables of current snapshots an older table layout wrote. Per vocabulary it then reads the
release with no full download, from a header or the file's date, downloads the files only when
that release has no snapshot yet, reads them, checks the own-term count against the current
snapshot's and writes the snapshot. Then it switches every vocabulary that succeeded at once,
and prunes.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import os
import urllib.parse
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path

from labdata._files import utc_stamp
from labdata._http import HttpTransport, Transport
from labdata.exceptions import LabdataError, OntologyError
from labdata.ontology import cellosaurus, fetch, lifecycle, obo, paths, tables, taxdump
from labdata.ontology import vocabularies as declared
from labdata.ontology.lifecycle import Outcome, Result, UpdateStatus, VocabularyResult
from labdata.ontology.root import find_root, prune, read_current, switch
from labdata.ontology.tables import (
    SnapshotData,
    connect,
    layout_of,
    read_meta,
    snapshot_name,
    tables_path,
    write_snapshot,
)
from labdata.ontology.vocabularies import (
    ReaderKind,
    VersionRule,
    Vocabulary,
    file_date,
    owl_version,
    release_of,
)

logger = logging.getLogger(__name__)

#: The least share of the current snapshot's own terms a new release must hold. Vocabularies
#: mark retired terms obsolete rather than delete them, so a release that loses more is cut.
MIN_TERM_SHARE = 0.95


def update(
    root: str | os.PathLike[str] | None = None,
    *,
    vocabularies: Sequence[Vocabulary] | None = None,
    transport: Transport | None = None,
    owner_gone: bool = False,
) -> UpdateStatus:
    """Update every vocabulary to its current release, under the store's run lock, and prune.

    A vocabulary that fails keeps its current snapshot, and the others still update. A new
    release with fewer than :data:`MIN_TERM_SHARE` of the current snapshot's own terms fails.

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
    chosen = declared.VOCABULARIES if vocabularies is None else vocabularies
    with lifecycle.run_lock(path, "update", owner_gone=owner_gone):
        start = utc_stamp()
        lifecycle.write_status(path, UpdateStatus(start=start, outcome=Outcome.RUNNING))
        try:
            _remake(path, chosen)
            rows = {vocab.name: _update_one(path, vocab, web) for vocab in chosen}
            switch(
                path,
                {
                    name: row.snapshot
                    for name, row in rows.items()
                    if row.result != Result.FAILED and row.snapshot is not None
                },
            )
            current = read_current(path)
            rows = {name: _described(path, row, current.get(name)) for name, row in rows.items()}
            prune(path)
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
        if layout_of(root, name) == tables.TABLE_LAYOUT:
            return VocabularyResult(release, name, Result.UNCHANGED)
        sources = [paths.DOWNLOADS.at(root, _file_name(url)) for url in vocab.downloads]
        try:
            for url, source in zip(vocab.downloads, sources, strict=True):
                fetch.download(url, source, transport=transport)
            if vocab.reader is ReaderKind.TAXDUMP:
                listed = fetch.read_head(f"{vocab.url}.md5", transport=transport)
                taxdump.check_md5(sources[0], listed)
            data = _read(vocab, sources, release)
            _check_terms(root, data)
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


def _read(vocab: Vocabulary, sources: Sequence[Path], release: str) -> SnapshotData:
    """Read a vocabulary's files, in the order of its downloads, with its reader."""
    if vocab.reader is ReaderKind.TAXDUMP:
        return taxdump.read_taxdump(sources[0], vocab, release)
    if vocab.reader is ReaderKind.CELLOSAURUS:
        return cellosaurus.read_cellosaurus(sources[0], sources[1], vocab)
    stated = None if vocab.version is VersionRule.DATA_VERSION else release
    return obo.read_obo(sources[0], vocab, release=stated)


def _file_name(url: str) -> str:
    return Path(urllib.parse.urlsplit(url).path).name


def _remake(root: Path, vocabularies: Sequence[Vocabulary]) -> None:
    """Remake from its kept source files each current snapshot an older table layout wrote.

    A remake that fails is logged, and the vocabulary's own step then downloads its release.
    """
    current = read_current(root)
    for vocab in vocabularies:
        name = current.get(vocab.name)
        if name is None or layout_of(root, name) in (None, tables.TABLE_LAYOUT):
            continue
        try:
            data = _read_kept(root, vocab, name)
            if snapshot_name(vocab.name, data.release) != name:
                raise OntologyError(f"its source file states release {data.release}")
            write_snapshot(root, data)
        except LabdataError as err:
            logger.warning("%s: cannot remake %s from its source: %s", vocab.name, name, err)


def _read_kept(root: Path, vocab: Vocabulary, snapshot: str) -> SnapshotData:
    # A release not stated in the source files, such as a file date, is the snapshot's own.
    kept = paths.SNAPSHOTS.at(root, snapshot, paths.SOURCE_DIR)
    files = [kept / _file_name(url) for url in vocab.downloads]
    missing = [f.name for f in files if not f.is_file()]
    if missing:
        raise OntologyError(f"{snapshot} keeps no {', '.join(missing)}")
    with closing(connect(tables_path(root, snapshot))) as db:
        release = read_meta(db)["release"]
    return _read(vocab, files, release)


def _check_terms(root: Path, data: SnapshotData) -> None:
    """Refuse a release holding far fewer own terms than the current snapshot."""
    current = read_current(root).get(data.vocabulary)
    if current is None:
        return
    held = _term_count(root, current)
    if len(data.terms) < MIN_TERM_SHARE * held:
        raise OntologyError(
            f"release {data.release} holds {len(data.terms)} own terms, under "
            f"{MIN_TERM_SHARE:.0%} of the {held} in {current}"
        )


def _described(root: Path, row: VocabularyResult, current: str | None) -> VocabularyResult:
    """Return ``row`` naming the current snapshot, its own-term count and source file date."""
    snapshot = current if row.result == Result.FAILED else row.snapshot
    if snapshot is None:
        return row
    files = list(paths.SNAPSHOTS.at(root, snapshot, paths.SOURCE_DIR).glob("*"))
    newest = max((f.stat().st_mtime for f in files), default=None)
    date = None if newest is None else f"{dt.datetime.fromtimestamp(newest, dt.UTC):%Y-%m-%d}"
    return dataclasses.replace(
        row, snapshot=snapshot, terms=_term_count(root, snapshot), source_date=date
    )


def _term_count(root: Path, snapshot: str) -> int:
    with closing(tables.connect(tables_path(root, snapshot))) as db:
        return int(db.execute("select count(*) from terms").fetchone()[0])
