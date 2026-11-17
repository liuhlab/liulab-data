"""Copy the paper store to GCS, which holds it for good: the changes since the last copy.

A routine copy lists only the top level of the store and the bucket. It compares the ``files``
table with the copy record, :data:`COPIED`, the paths, MD5s, sizes and fetch dates the last
successful copy sent: a new or changed row is uploaded by name, a path gone from the table is
deleted in the bucket, and the small top-level files go up every time. The copy record goes up
last; a work disk without it fetches the bucket's. With neither, a routine copy is skipped and
asks for ``labdata papers copy --full``, which uploads every file. A full copy fetches the
bucket's copy record first, and refuses when the disk's table lacks more of its paths than a
routine copy deletes: the disk does not hold the whole store.

No copy deletes by comparing the bucket with the disk. A copy refuses, sending nothing, when the
``files`` table is missing or empty, or when it would delete more of the copied paths than
:func:`labdata._rclone.delete_limit` allows, unless the deletes are accepted by hand.

Hidden files stay behind, :data:`COPIED` aside: the lock, the daily pass's journal and every
temp file; so does ``paper-status.json``, which the copy itself updates. A skipped or refused
copy sets ``copy_pending``; a later success clears it.
"""

from __future__ import annotations

import datetime as dt
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labdata import _rclone
from labdata.exceptions import LabdataError
from labdata.papers import paths
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import STATUS, read_status, write_status
from labdata.papers.tables import FILES
from labdata.store._files import temp_beside

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The copy record: the ``files`` rows the bucket holds, as the last successful copy left
#: them, each held path with its MD5, size and fetch date. Kept here on the disk and in the
#: bucket.
COPIED = paths.COPY_RECORD.rel

#: Left out of the copy: hidden files and folders, and the status record, which the copy
#: itself changes.
_LEFT_OUT = ["--filter=- .*", "--filter=- .*/**", f"--filter=- /{STATUS}"]

_COLUMNS = ["path", "md5", "size", "fetched"]
_SCHEMA = pa.schema([FILES.schema.field(c) for c in _COLUMNS])

#: Paths to their ``(md5, size, fetched)``.
_Held = dict[str, tuple[object, ...]]


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
        routine copy finds no copy record, on the disk or in the bucket.

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
            skipped = _send(root, dest, full=full, accept_deletes=accept_deletes)
        except OSError as err:
            _pending(root, True)
            raise PaperCopyError(
                f"cannot run rclone: {err}; run inside the pixi environment"
            ) from err
        except BaseException:
            _pending(root, True)
            raise
        if skipped:
            _pending(root, True)
            return CopyResult(dest, skipped=True, message=skipped)
        stamp = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        write_status(root, replace(read_status(root), copy_pending=False, last_copy_success=stamp))
    return CopyResult(dest)


def _send(root: Path, dest: str, *, full: bool, accept_deletes: int) -> str | None:
    """Copy the store to ``dest``; return why the copy was skipped, or None when it is done."""
    expired = _rclone.expired_message(dest, "paper store", "labdata papers copy")
    held = _held(root)
    path = root / COPIED
    fetch = ["copy", dest, str(root), "--no-traverse"]
    # The bucket's copy record is never older than the disk's: it goes up first.
    if (full or not path.is_file()) and not _rclone.run_on_paths(
        fetch, [COPIED], folder=root, run=_run_rclone, error=PaperCopyError
    ):
        return expired
    if not full and not path.is_file():
        return (
            "no copy record, on the disk or in the bucket, so the bucket is behind the "
            "paper store. Run `labdata papers copy --full` once: it uploads "
            "every file and deletes nothing. Later copies send changes."
        )
    copied = _signatures(path)
    _check_deletes(root, held, copied, accept_deletes, full=full)
    if _changes(root, dest, held, None if full else copied) and _put_copied(root, dest, held):
        return None
    return expired


def _held(root: Path) -> _Held:
    path = FILES.path(root)
    held = _signatures(path)
    if not held:
        raise PaperCopyError(
            f"{path} is missing or empty, so nothing was copied: the copy would replace the "
            "bucket's tables with it and delete the bucket's papers"
        )
    return held


def _check_deletes(root: Path, held: _Held, copied: _Held, accepted: int, *, full: bool) -> None:
    gone = len(copied.keys() - held.keys())
    limit = _rclone.delete_limit(len(copied), accepted)
    if gone > limit and full:
        raise PaperCopyError(
            f"{gone} of the {len(copied)} files the bucket's copy record lists are missing "
            f"from {FILES.path(root)}, more than the {limit} a full copy allows, so nothing "
            "was copied: this work disk does not hold the whole store, and a full copy would "
            "replace the bucket's tables with its own. Restore the store from the bucket "
            "first. If the files left the store on purpose, run the copy without --full."
        )
    if gone > limit:
        raise PaperCopyError(
            f"{gone} of the {len(copied)} files the last copy sent have left "
            f"{FILES.path(root)}, more than the {limit} this copy deletes, so nothing was "
            "copied: the table may be damaged, or this work disk was not restored from the "
            "bucket. If the files left the store on purpose, "
            f"`labdata papers copy --accept-deletes {gone}` deletes them in the bucket."
        )


def _signatures(path: Path) -> _Held:
    if not path.is_file():
        return {}
    data = pq.read_table(path, columns=_COLUMNS, schema=_SCHEMA).to_pylist()
    return {r["path"]: (r["md5"], r["size"], r["fetched"]) for r in data if r["path"]}


def _changes(root: Path, dest: str, held: _Held, copied: _Held | None) -> bool:
    """Upload new and changed files, then the top level, then delete dropped files.

    With no ``copied``, upload every file and delete none. Returns False when rclone's login
    has expired.
    """
    if not _rclone.logged_in(dest, run=_run_rclone):
        return False
    steps: list[tuple[list[str], list[str]]] = []
    gone: list[str] = []
    if copied is None:
        steps.append((["copy", str(root), dest, *_LEFT_OUT, "--filter=- /*"], []))
    else:
        put = sorted(p for p, sig in held.items() if copied.get(p) != sig)
        gone = sorted(set(copied) - set(held))
        if put:
            steps.append((["copy", str(root), dest, "--no-traverse"], put))
    # The tables go up after the files they name and before the files they dropped go.
    steps.append((["copy", str(root), dest, "--max-depth=1", *_LEFT_OUT], []))
    if gone:
        steps.append((["delete", dest, "--no-traverse"], gone))
    return all(
        _rclone.run_on_paths(
            [*args, *_rclone.PROGRESS], paths, folder=root, run=_run_rclone, error=PaperCopyError
        )
        for args, paths in steps
    )


def _put_copied(root: Path, dest: str, held: _Held) -> bool:
    """Save ``held`` as :data:`COPIED`, in the bucket first, then on the disk.

    Returns False when rclone's login has expired.
    """
    path = root / COPIED
    rows = [dict(zip(_COLUMNS, (p, *sig), strict=True)) for p, sig in sorted(held.items())]
    tmp = temp_beside(path)
    try:
        pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), tmp, compression="zstd")
        to = f"{dest.rstrip('/')}/{COPIED}"
        if not _rclone.run_on_paths(
            ["copyto", str(tmp), to], [], folder=root, run=_run_rclone, error=PaperCopyError
        ):
            return False
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def _pending(root: Path, pending: bool) -> None:
    write_status(root, replace(read_status(root), copy_pending=pending))


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner, which tests replace."""
    return _rclone.run(args)
