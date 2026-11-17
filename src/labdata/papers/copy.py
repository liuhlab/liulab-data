"""Copy the paper store to GCS, which holds it for good: the changes since the last copy.

This is the paper store's adapter of :mod:`labdata._gcs`. The held set is the ``files``
table's paper files, each with its MD5, size and fetch date, and the files at the top of the
store, which go up every time. The copy record, :data:`COPIED`, is Parquet. A routine copy lists
only the top level of the store. With no copy record on the work disk or in GCS, it is skipped
and asks for ``labdata papers copy --full``, which uploads every file. A full copy fetches the
copy record in GCS first, and refuses when the disk's table lacks more of its paths than a
routine copy deletes: the disk does not hold the whole store.

A copy refuses, sending nothing, when the ``files`` table is missing or empty, or when it would
delete more of the copied paper files than :func:`labdata._gcs.delete_limit` allows, unless
the deletes are accepted by hand.

Hidden files stay behind, :data:`COPIED` aside: the lock, the daily pass's journal and every
temp file; so does ``paper-status.json``, which the copy itself updates. A skipped or refused
copy sets ``copy_pending``; a later success clears it.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata import _gcs
from labdata.exceptions import LabdataError
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import STATUS, read_status, write_status
from labdata.papers.tables import FILES

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The copy record: the files GCS holds, as the last successful copy left them, each paper
#: file with its MD5, size and fetch date. Kept here on the disk and in GCS.
COPIED = ".copied-files.parquet"

#: Left out of the copy: hidden files and folders, and the status record, which the copy
#: itself changes.
_LEFT_OUT = ["--filter=- .*", "--filter=- .*/**", f"--filter=- /{STATUS}"]

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


def copy(
    store: PaperStore, to: str | None = None, *, full: bool = False, accept_deletes: int = 0
) -> CopyResult:
    """Send the paper store's changes since the last successful copy to ``to``.

    Holds the paper store's lock, so a copy never runs beside a pass.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    to : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
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
        If no destination is set; if ``full`` comes with ``accept_deletes``; if the ``files``
        table is missing or empty, or more of the copied paths left it than this copy deletes;
        or if rclone fails for another reason. Any failure past the first two checks sets
        ``copy_pending``.
    """
    root = store.root
    dest = to or destination(store)
    if not dest:
        raise PaperCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {root / MARKER}"
        )
    if full and accept_deletes:
        raise PaperCopyError("--full deletes nothing, so it takes no --accept-deletes")
    with paper_lock(root):
        start = dt.datetime.now(dt.UTC)
        try:
            held = _held(root)
            skipped = _gcs.copy(
                _Adapter(root), dest, held, full=full, accept_deletes=accept_deletes
            )
        except BaseException:
            _pending(root, True)
            raise
        if skipped:
            _pending(root, True)
            return CopyResult(dest, skipped=True, message=skipped)
        stamp = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        write_status(root, replace(read_status(root), copy_pending=False, last_copy_success=stamp))
    return CopyResult(dest)


class _Adapter(_gcs.Adapter):
    record = COPIED
    what = "paper store"
    command = "labdata papers copy"
    error = PaperCopyError
    full_filters = (*_LEFT_OUT, "--filter=- /*")
    full_checks = True

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
    top = (
        p.name
        for p in root.iterdir()
        if p.is_file() and not p.is_symlink() and not p.name.startswith(".") and p.name != STATUS
    )
    return held | dict.fromkeys(top)


def _signatures(table: pa.Table) -> _Held:
    """Return each named path with its signature; a path at the top of the store has none."""
    return {
        r["path"]: (r["md5"], r["size"], r["fetched"]) if "/" in r["path"] else None
        for r in table.to_pylist()
        if r["path"]
    }


def _pending(root: Path, pending: bool) -> None:
    write_status(root, replace(read_status(root), copy_pending=pending))
