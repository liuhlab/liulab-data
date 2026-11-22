"""The update: bring each declared vocabulary's current release into the store.

One process under the store's run lock. It first remakes, from their kept source files, the
tables of current snapshots an older table layout wrote. Per vocabulary it then reads the
release with no full download, from a header or the file's date, downloads the files only when
that release has no snapshot yet, reads them, checks the own-term count against the current
snapshot's and writes the snapshot. Then it switches every vocabulary that succeeded at once,
prunes, and copies the store to GCS (:mod:`labdata.ontology.lifecycle`). A vocabulary given a
file on disk is read from that file in place of the web.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import os
import shutil
import urllib.parse
from collections.abc import Mapping, Sequence
from contextlib import closing
from pathlib import Path

from labdata._files import temp_beside, utc_stamp
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
    term_count,
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
    sources: Mapping[str, str | os.PathLike[str]] | None = None,
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
    sources : Mapping of str to path, optional
        A file on disk per vocabulary name, read in place of the web and copied into the
        snapshot; the file itself is left as it is. Only a vocabulary whose one file states
        its release takes one.
    transport : Transport, optional
        The HTTP transport every request goes through.
    owner_gone : bool
        The update holding the run lock has ended, though this host cannot check it.

    Returns
    -------
    UpdateStatus
        The status record as written; its outcome is ``failed`` if any vocabulary failed,
        and ``copy_pending`` is set if the copy to GCS failed or was skipped.

    Raises
    ------
    OntologyError
        If a source names no vocabulary to update, names no file, or is given for a
        vocabulary that cannot take one; nothing is changed.
    OntologyBusyError
        If another update holds the store's run lock.
    OntologyCopyError
        If the work disk is not in step with GCS, or a pending copy fails, before any download.
    """
    path = find_root(root)
    web = transport if transport is not None else HttpTransport()
    chosen = declared.VOCABULARIES if vocabularies is None else vocabularies
    given = _given(chosen, sources or {})
    with lifecycle.run_lock(path, "update", owner_gone=owner_gone):
        lifecycle.copy_first(path)
        # A copy still pending stays so until one works, whatever this update does.
        pending = lifecycle.read_status(path).copy_pending
        start = utc_stamp()
        lifecycle.write_status(
            path, UpdateStatus(start=start, outcome=Outcome.RUNNING, copy_pending=pending)
        )
        try:
            _remake(path, chosen)
            rows = {v.name: _update_one(path, v, web, given.get(v.name)) for v in chosen}
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
            stopped = UpdateStatus(start, utc_stamp(), Outcome.FAILED, copy_pending=pending)
            lifecycle.write_status(path, stopped)
            raise
        failed = any(row.result == Result.FAILED for row in rows.values())
        outcome = Outcome.FAILED if failed else Outcome.BUILT
        lifecycle.write_status(path, UpdateStatus(start, utc_stamp(), outcome, rows, pending))
        lifecycle.copy_last(path)
    return lifecycle.read_status(path)


def _given(
    vocabularies: Sequence[Vocabulary], sources: Mapping[str, str | os.PathLike[str]]
) -> dict[str, Path]:
    """Return each given source file by vocabulary name; refuse one the update cannot take."""
    by_name = {vocab.name: vocab for vocab in vocabularies}
    given: dict[str, Path] = {}
    for name, file in sources.items():
        vocab = by_name.get(name)
        if vocab is None:
            known = ", ".join(by_name)
            raise OntologyError(f"no vocabulary named {name!r} to update; it updates {known}")
        if vocab.version is not VersionRule.DATA_VERSION:
            raise OntologyError(
                f"{name}: its release comes from the web ({vocab.version}), not from its file"
            )
        if len(vocab.downloads) > 1:
            raise OntologyError(f"{name}: it is read from {len(vocab.downloads)} files, not one")
        given[name] = Path(file)
        if not given[name].is_file():
            raise OntologyError(f"{name}: no file at {given[name]}")
    return given


def _update_one(
    root: Path, vocab: Vocabulary, transport: Transport, given: Path | None = None
) -> VocabularyResult:
    release: str | None = None
    try:
        if given is None:
            release = current_release(vocab, transport=transport)
        else:
            release = _stated_release(vocab, _head_of(given))
        name = snapshot_name(vocab.name, release)
        if layout_of(root, name) == tables.TABLE_LAYOUT:
            # A kept snapshot that is not current, as when a source goes back a release.
            current = read_current(root).get(vocab.name) == name
            return VocabularyResult(release, name, Result.UNCHANGED if current else Result.UPDATED)
        sources = [paths.DOWNLOADS.at(root, _file_name(url)) for url in vocab.downloads]
        try:
            if given is None:
                release = _download(vocab, sources, transport) or release
            else:
                _copy_in(given, sources[0])
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
    return _stated_release(vocab, fetch.read_head(vocab.version_url, transport=transport))


def _stated_release(vocab: Vocabulary, head: bytes) -> str:
    """Return the release a file's first bytes state, by the vocabulary's version rule."""
    if vocab.version is VersionRule.OWL_VERSION_INFO:
        return release_of(owl_version(head))
    return release_of(obo.header_version(head))


def _head_of(path: Path) -> bytes:
    """Return a file's first bytes, as many as a header read from the web asks for."""
    try:
        with path.open("rb") as fh:
            return fh.read(fetch.HEAD_BYTES)
    except OSError as err:
        raise OntologyError(f"cannot read {path}: {err}") from err


def _copy_in(path: Path, dest: Path) -> None:
    """Copy a given source file to ``dest``, written whole and dated as the file is."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_beside(dest)
    try:
        shutil.copyfile(path, tmp)
        stat = path.stat()
        os.utime(tmp, (stat.st_atime, stat.st_mtime))
        tmp.replace(dest)
    except OSError as err:
        raise OntologyError(f"cannot copy {path} in: {err}") from err
    finally:
        tmp.unlink(missing_ok=True)


def _download(vocab: Vocabulary, sources: Sequence[Path], transport: Transport) -> str | None:
    """Download a vocabulary's files; return the release the download names, if it names one."""
    if vocab.reader is ReaderKind.TAXDUMP:
        return _download_taxdump(vocab, sources[0], transport)
    for url, source in zip(vocab.downloads, sources, strict=True):
        fetch.download(url, source, transport=transport)
    return None


def _download_taxdump(vocab: Vocabulary, dest: Path, transport: Transport) -> str:
    """Download the taxonomy dump, checked against its ``.md5`` read first; return its date.

    NCBI rebuilds the dump daily, so a pair that does not match is fetched once more.
    """
    md5 = f"{vocab.url}.md5"
    listed = fetch.read_head(md5, transport=transport)
    modified = fetch.download(vocab.url, dest, transport=transport)
    try:
        taxdump.check_md5(dest, listed)
    except OntologyError as err:
        logger.warning("%s: %s; fetching both again", vocab.name, err)
        listed = fetch.read_head(md5, transport=transport)
        modified = fetch.download(vocab.url, dest, transport=transport)
        taxdump.check_md5(dest, listed)
    return file_date(modified)


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
    """Refuse a release holding no own terms, or far fewer than the current snapshot."""
    if not data.terms:
        raise OntologyError(f"release {data.release} holds 0 own terms")
    current = read_current(root).get(data.vocabulary)
    if current is None:
        return
    held = term_count(root, current)
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
        row, snapshot=snapshot, terms=term_count(root, snapshot), source_date=date
    )
