"""Copy the paper store to GCS: the changes since the last copy, or on request a full mirror.

A routine copy lists only the top level of the store and the bucket. It compares the ``files``
table with :data:`COPIED`, the paths, MD5s, sizes and fetch dates the last successful copy
sent: a new or changed row is uploaded by name, a path gone from the table is deleted in the
bucket, and the small top-level files go up every time. With no :data:`COPIED`, a routine copy
is skipped and asks for a full mirror, ``labdata papers copy --full``, which writes it.

Hidden files stay behind: the lock, the daily pass's journal, :data:`COPIED` and every temp
file; so does ``paper-status.json``, which the copy itself updates. A skipped copy, on an
expired login or with no earlier copy, sets ``copy_pending``; a later success clears it.
"""

from __future__ import annotations

import datetime as dt
import os
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata import _rclone
from labdata.exceptions import LabdataError
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import STATUS, read_status, write_status
from labdata.papers.tables import FILES
from labdata.store._files import temp_beside

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The ``files`` rows the bucket holds, as the last successful copy left them: each held
#: path with its MD5, size and fetch date.
COPIED = ".copied-files.parquet"

#: Left out of the copy, and deleted from the bucket: hidden files and folders, and the
#: status record, which the copy itself changes.
_LEFT_OUT = ["--filter=- .*", "--filter=- .*/**", f"--filter=- /{STATUS}"]

_COLUMNS = ["path", "md5", "size", "fetched"]
_SCHEMA = pa.schema([FILES.schema.field(c) for c in _COLUMNS])

#: Paths to their ``(md5, size, fetched)``.
_Held = dict[str, tuple[object, ...]]


class PaperCopyError(LabdataError):
    """The paper store cannot be copied: no destination is set, or rclone failed."""


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped, with a message saying why."""

    destination: str
    skipped: bool = False
    message: str = ""


def destination(store: PaperStore) -> str:
    """Return the marker's ``copy_to``; empty when the paper store has no copy."""
    return str(store.settings.get(COPY_TO_KEY) or "")


def copy(store: PaperStore, to: str | None = None, *, full: bool = False) -> CopyResult:
    """Send the paper store's changes since the last successful copy to ``to``.

    Holds the paper store's lock, so a copy never runs beside a pass.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    to : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
    full : bool
        Mirror the whole store instead: upload new files, then delete objects the store no
        longer holds. It lists every file on both sides; for a first copy or a repair.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's login has expired, or when a
        routine copy finds no earlier successful copy to start from.

    Raises
    ------
    PaperCopyError
        If no destination is set, or rclone fails for another reason; when rclone ran,
        ``copy_pending`` is then set, as the bucket is behind.
    """
    root = store.root
    dest = to or destination(store)
    if not dest:
        raise PaperCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {root / MARKER}"
        )
    with paper_lock(root):
        start = dt.datetime.now(dt.UTC)
        held = _held(root)
        copied = None if full else _copied(root)
        if not full and copied is None:
            _pending(root, True)
            message = (
                "no earlier copy to send changes from, so the bucket is behind the paper "
                "store. Run `labdata papers copy --full` once; later copies send changes."
            )
            return CopyResult(dest, skipped=True, message=message)
        try:
            if copied is None:
                done = _rclone.mirror(
                    str(root),
                    dest,
                    [*_LEFT_OUT, f"--filter=- /{MARKER}"],
                    _LEFT_OUT,
                    run=_run_rclone,
                    error=PaperCopyError,
                )
            else:
                done = _changes(root, dest, held, copied)
        except PaperCopyError:
            _pending(root, True)
            raise
        except OSError as err:
            _pending(root, True)
            raise PaperCopyError(
                f"cannot run rclone: {err}; run inside the pixi environment"
            ) from err
        if not done:
            _pending(root, True)
            message = _rclone.expired_message(dest, "paper store", "labdata papers copy")
            return CopyResult(dest, skipped=True, message=message)
        _write_copied(root, held)
        stamp = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        write_status(root, replace(read_status(root), copy_pending=False, last_copy_success=stamp))
    return CopyResult(dest)


def _held(root: Path) -> _Held:
    path = FILES.path(root)
    if not path.is_file() and (root / COPIED).is_file():
        # Read as empty, it would delete every copied file in the bucket.
        raise PaperCopyError(f"{path} is missing; the copy would delete the bucket's papers")
    return _signatures(path)


def _copied(root: Path) -> _Held | None:
    path = root / COPIED
    return _signatures(path) if path.is_file() else None


def _signatures(path: Path) -> _Held:
    if not path.is_file():
        return {}
    data = pq.read_table(path, columns=_COLUMNS, schema=_SCHEMA).to_pylist()
    return {r["path"]: (r["md5"], r["size"], r["fetched"]) for r in data if r["path"]}


def _write_copied(root: Path, held: _Held) -> None:
    path = root / COPIED
    rows = [dict(zip(_COLUMNS, (p, *sig), strict=True)) for p, sig in sorted(held.items())]
    tmp = temp_beside(path)
    try:
        pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), tmp, compression="zstd")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _changes(root: Path, dest: str, held: _Held, copied: _Held) -> bool:
    """Upload new and changed files by name, then the top level, then delete dropped files.

    Returns False when rclone's login has expired.
    """
    probe = _run_rclone(["rclone", "lsjson", dest, "--max-depth", "1"])
    if probe.returncode and _rclone.login_expired(probe.stderr):
        return False
    put = sorted(p for p, sig in held.items() if copied.get(p) != sig)
    gone = sorted(set(copied) - set(held))
    steps: list[tuple[list[str], list[str]]] = []
    if put:
        steps.append((["copy", str(root), dest, "--no-traverse"], put))
    # The tables go up after the files they name and before the files they dropped go.
    steps.append((["copy", str(root), dest, "--max-depth=1", *_LEFT_OUT], []))
    if gone:
        steps.append((["delete", dest, "--no-traverse"], gone))
    for args, paths in steps:
        with _file_list(root, paths) as listed:
            done = _run_rclone(["rclone", *args, *listed, *_rclone.PROGRESS])
        if done.returncode == 0:
            continue
        if _rclone.login_expired(done.stderr):
            return False
        raise PaperCopyError(f"rclone {args[0]} failed ({done.returncode}): {done.stderr}")
    return True


@contextmanager
def _file_list(root: Path, paths: list[str]) -> Iterator[list[str]]:
    """Yield the rclone flag naming a hidden file that lists ``paths``; none for no paths."""
    if not paths:
        yield []
        return
    fd, name = tempfile.mkstemp(prefix=".copy-list.", dir=root)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.writelines(p + "\n" for p in paths)
        yield [f"--files-from-raw={name}"]
    finally:
        Path(name).unlink(missing_ok=True)


def _pending(root: Path, pending: bool) -> None:
    write_status(root, replace(read_status(root), copy_pending=pending))


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner, which tests replace."""
    return _rclone.run(args)
