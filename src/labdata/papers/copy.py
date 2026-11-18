"""Copy the paper store to GCS, which holds it for good: the changes since the last copy.

This is the paper store's adapter of :mod:`labdata._gcs`. The held set is the ``files``
table's paper files, each with its MD5, size and fetch date, and the state files
:mod:`labdata.papers.paths` declares at the top of the store, which go up every time. The copy
record, :data:`COPIED`, is Parquet. A routine copy lists only the top level of the store. With no copy record on the work disk or in GCS, it is skipped
and asks for ``labdata papers copy --full``, which uploads every file. A full copy fetches the
copy record in GCS first, and refuses when the disk's table lacks more of its paths than a
routine copy deletes: the disk does not hold the whole store.

A copy refuses, sending nothing, when the ``files`` table is missing or empty, or when it would
delete more of the copied paper files than :func:`labdata._gcs.delete_limit` allows, unless
the deletes are accepted by hand, or when the work disk is not in step with GCS
(:func:`check`).

Hidden files stay behind, :data:`COPIED` aside: the lock, the daily pass's journal and every
temp file. The status record goes up last, after the copy record, as
:mod:`labdata.papers.lifecycle` wrote it, which runs every copy under the paper store's lock.
:func:`restore` fills a work disk from GCS.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata import _gcs
from labdata.exceptions import LabdataError
from labdata.papers import paths
from labdata.papers.root import MARKER, PaperStore, open_paper_store, paper_lock
from labdata.papers.status import STATUS
from labdata.papers.tables import FILES

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The copy record: the files GCS holds, as the last successful copy left them, each paper
#: file with its MD5, size and fetch date. Kept here on the disk and in GCS.
COPIED = paths.COPY_RECORD.rel

#: The state files at the top of the store the copy sends with the paper files; the two
#: records go up after them.
_TOP = [
    d.rel
    for d in paths.LAYOUT
    if d.kind is paths.STATE
    and not d.rel.endswith("/")
    and d not in (paths.STATUS, paths.COPY_RECORD)
]

_COLUMNS = ["path", "md5", "size", "fetched"]
_SCHEMA = pa.schema([FILES.schema.field(c) for c in _COLUMNS])

#: Paths to their ``(md5, size, fetched)``; None for a file at the top of the store.
_Held = dict[str, tuple[object, ...] | None]


class PaperCopyError(LabdataError):
    """The paper store cannot be copied: no destination, a suspect files table, or rclone failed."""


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped, with a message saying why."""

    destination: str
    skipped: bool = False
    message: str = ""


def destination(store: PaperStore) -> str:
    """Return the marker's ``copy_to``; empty when the paper store has no copy."""
    return str(store.settings.get(COPY_TO_KEY) or "")


def send(
    store: PaperStore, dest: str, status: bytes, *, full: bool = False, accept_deletes: int = 0
) -> CopyResult:
    """Send the paper store's changes since the last successful copy to ``dest``.

    Takes no lock: :func:`labdata.papers.lifecycle.copy` runs it.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    dest : str
        An rclone path, ``<remote>:<bucket>/<path>``.
    status : bytes
        The status record, sent last, to GCS and then the work disk.
    full : bool
        Upload every file instead, and delete nothing. It lists every file on both sides; for
        a first copy or a repair.
    accept_deletes : int
        Delete up to this many paths that left the ``files`` table, even past the usual
        bound: for files moved or removed on purpose.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's login has expired, or when a
        routine copy finds no copy record, on the disk or in GCS.

    Raises
    ------
    PaperCopyError
        If the ``files`` table is missing or empty, or more of the copied paths left it than
        this copy deletes; or if rclone fails for another reason.
    """
    held = _held(store.root)
    adapter = _Adapter(store.root, status)
    skipped = _gcs.copy(adapter, dest, held, full=full, accept_deletes=accept_deletes)
    return CopyResult(dest, skipped=skipped is not None, message=skipped or "")


def check(store: PaperStore, dest: str) -> bool:
    """Check the work disk against GCS at ``dest``, as :func:`labdata._gcs.check` does.

    Returns
    -------
    bool
        False when rclone's login has expired, so nothing was checked.

    Raises
    ------
    PaperCopyError
        If the work disk's copy record is not the one in GCS, or GCS holds none and it does.
    """
    return _gcs.check(_Adapter(store.root, b""), dest)


def restore(
    root: Path, source: str | None = None, *, approve: Callable[[int, int], bool]
) -> str | None:
    """Fill the work disk at ``root`` from ``source``, as :func:`labdata._gcs.restore` says.

    Holds the paper store's lock, so a restore never runs beside a pass or a copy.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root; it may be empty or missing.
    source : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
    approve : callable
        Called with the number of files and bytes to download, before any; False stops it.

    Returns
    -------
    str or None
        Why it stopped: rclone's login has expired or ``approve`` said no; None when done.

    Raises
    ------
    PaperCopyError
        If no source is set, GCS holds no copy record or lacks a file it names, or rclone fails.
    """
    if not source and (root / MARKER).is_file():
        source = destination(open_paper_store(root))
    if not source:
        raise PaperCopyError("no source: pass --from <remote>:<bucket>/<path>")
    root.mkdir(parents=True, exist_ok=True)
    with paper_lock(root):
        return _gcs.restore(_Adapter(root, b""), source, approve=approve)


class _Adapter(_gcs.Adapter):
    record = COPIED
    status = STATUS
    what = "paper store"
    command = "labdata papers copy"
    restorer = "labdata papers restore"
    error = PaperCopyError
    full_filters = ("--filter=- .*", "--filter=- .*/**", "--filter=- /*")
    full_checks = True

    def __init__(self, root: Path, status: bytes) -> None:
        super().__init__(root)
        self.status_record = status

    def finalized(self) -> bytes:
        return self.status_record

    def state(self, path: str) -> bool:
        try:
            return paths.kind(path) is paths.STATE
        except ValueError:
            return False

    def load(self, data: bytes) -> _Held:
        return _signatures(pq.read_table(pa.BufferReader(data), columns=_COLUMNS, schema=_SCHEMA))

    def dump(self, files: _gcs.Files) -> bytes:
        rows = []
        for path, sig in sorted(files.items()):
            md5, size, fetched = sig if isinstance(sig, tuple) else (None, None, None)
            rows.append({"path": path, "md5": md5, "size": size, "fetched": fetched})
        sink = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), sink, compression="zstd")
        return sink.getvalue().to_pybytes()

    def check_deletes(
        self, held: _gcs.Files, copied: _gcs.Files, accepted: int, *, full: bool
    ) -> None:
        recorded = sum(sig is not None for sig in copied.values())
        gone = sum(copied[p] is not None for p in self.gone(held, copied))
        limit = _gcs.delete_limit(recorded, accepted)
        if gone > limit and full:
            raise PaperCopyError(
                f"{gone} of the {recorded} files the bucket's copy record lists are missing "
                f"from {FILES.path(self.root)}, more than the {limit} a full copy allows, so "
                "nothing was copied: this work disk does not hold the whole store, and a full "
                "copy would replace the bucket's tables with its own. Restore the store from "
                "the bucket first. If the files left the store on purpose, run the copy "
                "without --full."
            )
        if gone > limit:
            raise PaperCopyError(
                f"{gone} of the {recorded} files the last copy sent have left "
                f"{FILES.path(self.root)}, more than the {limit} this copy deletes, so nothing "
                "was copied: the table may be damaged, or this work disk was not restored from "
                "the bucket. If the files left the store on purpose, "
                f"`labdata papers copy --accept-deletes {gone}` deletes them in the bucket."
            )


def _held(root: Path) -> _Held:
    """Return the ``files`` table's paper files, and the files at the top of the store."""
    path = FILES.path(root)
    held = {}
    if path.is_file():
        held = _signatures(pq.read_table(path, columns=_COLUMNS, schema=_SCHEMA))
    if not held:
        raise PaperCopyError(
            f"{path} is missing or empty, so nothing was copied: the copy would replace the "
            "bucket's tables with it and delete the bucket's papers"
        )
    top = (rel for rel in _TOP if (root / rel).is_file() and not (root / rel).is_symlink())
    return held | dict.fromkeys(top)


def _signatures(table: pa.Table) -> _Held:
    """Return each named path with its signature; a path at the top of the store has none."""
    return {
        r["path"]: (r["md5"], r["size"], r["fetched"]) if "/" in r["path"] else None
        for r in table.to_pylist()
        if r["path"]
    }
