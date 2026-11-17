"""The GCS protocol both stores share: a copy sends a store's changes to GCS, which holds it.

Each store has an :class:`Adapter`, which supplies its held set: the files GCS should hold,
from the store root, each with a signature that changes with the file, or None for a file
sent every time. The copy record is the held set as the last successful copy left it, at the
store root on the work disk and in GCS.

After one listing call checks rclone's login, :func:`copy` sends, in order: the new and
changed files by name; the files at the top of the root, the marker among them; the deletes,
by name, of the recorded files that left the held set; and the copy record, to GCS first and
then the work disk. No copy deletes by comparing GCS with the work disk.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from pathlib import Path

from labdata import _rclone
from labdata.store._files import temp_beside

#: The largest share of a copy record's paths one copy deletes in GCS, unless one path is
#: more or more are accepted by hand; past it the copy refuses.
MAX_DELETE_SHARE = 0.05

#: Paths from the store root to their signatures; None for a file sent every time.
Files = Mapping[str, object]

_Run = Callable[[list[str]], subprocess.CompletedProcess[str]]


class Adapter(ABC):
    """One store's side of the protocol: its root, its copy record and its rules."""

    #: The copy record's file name at the store root.
    record: str
    #: The store's name and its copy command, for messages.
    what: str
    command: str
    #: Raised when rclone fails for a reason other than an expired login.
    error: type[Exception]
    #: rclone filters that pick, out of the whole root, what a full copy sends below the top
    #: level; None sends the held files by name.
    full_filters: tuple[str, ...] | None = None
    #: Whether a full copy reads the copy record in GCS, for :meth:`check_deletes`.
    full_checks: bool = False
    #: Fetched from GCS with a missing copy record, for :meth:`seed`.
    seed_from: tuple[str, ...] = ()

    def __init__(self, root: Path) -> None:
        self.root = root

    @abstractmethod
    def load(self, data: bytes) -> Files:
        """Return the files a copy record's bytes name."""

    @abstractmethod
    def dump(self, files: Files) -> bytes:
        """Return the bytes of a copy record naming ``files``."""

    @abstractmethod
    def check_deletes(self, held: Files, copied: Files, accepted: int, *, full: bool) -> None:
        """Raise :attr:`error` when the copy would delete more than :func:`delete_limit` allows."""

    def holder(self, path: str) -> str:
        """Return the folder that holds ``path`` in the store."""
        return path.rpartition("/")[0]

    def gone(self, held: Files, copied: Files) -> list[str]:
        """Return the recorded paths that left the held set, which a copy deletes in GCS.

        A file with no signature whose holder is still held stays: it is missing on the work
        disk, not gone from the store.
        """
        holders = {self.holder(p) for p in held}
        return sorted(
            p
            for p, sig in copied.items()
            if p not in held and (sig is not None or self.holder(p) not in holders)
        )

    def seed(self, fetched: Mapping[str, bytes]) -> Files | None:
        """Return a copy record for a store with none, from the :attr:`seed_from` GCS holds."""
        return None


def delete_limit(recorded: int, accepted: int = 0) -> int:
    """Return how many of ``recorded`` paths one copy may delete in GCS.

    It is the largest of one, :data:`MAX_DELETE_SHARE` of ``recorded`` and ``accepted``.
    """
    return int(max(1, MAX_DELETE_SHARE * recorded, accepted))


def copy(
    adapter: Adapter, dest: str, held: Files, *, full: bool = False, accept_deletes: int = 0
) -> str | None:
    """Send ``held`` to ``dest``: the changes since the last successful copy.

    Parameters
    ----------
    adapter : Adapter
        The store.
    dest : str
        An rclone path, ``<remote>:<bucket>/<path>``.
    held : Mapping
        The held set.
    full : bool
        Send every file instead, delete nothing and save ``held`` as the copy record.
    accept_deletes : int
        Passed to :meth:`Adapter.check_deletes`.

    Returns
    -------
    str or None
        Why the copy was skipped: rclone's login has expired, or a routine copy found no copy
        record on the work disk or in GCS and the adapter seeded none. None when it is done.

    Raises
    ------
    Exception
        :meth:`Adapter.check_deletes`'s refusal, before anything is sent; the adapter's
        :attr:`~Adapter.error` when rclone cannot run or fails for another reason.
    """
    run = _runner(adapter)
    expired = _expired(adapter, dest)
    if not _rclone.logged_in(dest, run=run):
        return expired
    copied = None
    if not full or adapter.full_checks:
        disk = adapter.root / adapter.record
        if not full and disk.is_file():
            copied = adapter.load(disk.read_bytes())
        else:
            names = [adapter.record, *([] if full else adapter.seed_from)]
            fetched = _fetch(adapter, dest, names, run)
            if fetched is None:
                return expired
            if adapter.record in fetched:
                copied = adapter.load(fetched[adapter.record])
            elif full:
                copied = adapter.load(disk.read_bytes()) if disk.is_file() else None
            else:
                copied = adapter.seed(fetched)
                if copied is None:
                    return (
                        f"no copy record, on the work disk or in GCS, so GCS is behind the "
                        f"{adapter.what}. Run `{adapter.command} --full` once: it uploads the "
                        f"{adapter.what} and deletes nothing. Later copies send changes."
                    )
                # Up first, so a copy cut off after it never has to seed one again.
                if not _put_record(adapter, dest, copied, run):
                    return expired
        if copied is not None:
            adapter.check_deletes(held, copied, accept_deletes, full=full)
    if full:
        copied = None
    gone = [] if copied is None else adapter.gone(held, copied)
    if not _send(adapter, dest, held, copied, gone, run):
        return expired
    # A file missing on the work disk but not gone stays in GCS, so the record keeps it.
    left = set(gone)
    kept = {p: sig for p, sig in (copied or {}).items() if p not in held and p not in left}
    return None if _put_record(adapter, dest, {**kept, **held}, run) else expired


def _send(
    adapter: Adapter, dest: str, held: Files, copied: Files | None, gone: list[str], run: _Run
) -> bool:
    """Send the files, then the top of the root, then the deletes; False on an expired login.

    With no ``copied``, send every file.
    """
    root = str(adapter.root)
    files = sorted(p for p in held if "/" in p)
    by_name = ["copy", root, dest, "--no-traverse"]
    first: tuple[list[str], list[str] | None]
    if copied is None and adapter.full_filters is not None:
        first = (["copy", root, dest, *adapter.full_filters], None)
    elif copied is None:
        first = (["copy", root, dest], files)
    else:
        first = (by_name, [p for p in files if held[p] is None or copied.get(p) != held[p]])
    steps = [
        first,
        (by_name, sorted(p for p in held if "/" not in p)),
        (["delete", dest, "--no-traverse"], gone),
    ]
    # An empty list is left out: rclone given no paths would act on the whole root.
    return all(
        _rclone.run_on_paths(
            [*args, *_rclone.PROGRESS],
            paths or [],
            folder=adapter.root,
            run=run,
            error=adapter.error,
        )
        for args, paths in steps
        if paths is None or paths
    )


def _fetch(adapter: Adapter, dest: str, names: list[str], run: _Run) -> dict[str, bytes] | None:
    """Return each of ``names`` GCS holds; None when rclone's login has expired."""
    folder = Path(tempfile.mkdtemp(prefix=".copy-fetch.", dir=adapter.root))
    try:
        fetch = ["copy", dest, str(folder), "--no-traverse"]
        if not _rclone.run_on_paths(
            fetch, names, folder=adapter.root, run=run, error=adapter.error
        ):
            return None
        return {n: (folder / n).read_bytes() for n in names if (folder / n).is_file()}
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _put_record(adapter: Adapter, dest: str, files: Files, run: _Run) -> bool:
    """Save ``files`` as the copy record, in GCS first, then on the work disk.

    Returns False when rclone's login has expired.
    """
    path = adapter.root / adapter.record
    tmp = temp_beside(path)
    try:
        tmp.write_bytes(adapter.dump(files))
        to = f"{dest.rstrip('/')}/{adapter.record}"
        if not _rclone.run_on_paths(
            ["copyto", str(tmp), to], [], folder=adapter.root, run=run, error=adapter.error
        ):
            return False
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def _expired(adapter: Adapter, dest: str) -> str:
    """Return what to do when rclone's login has expired: reconnect, then copy again."""
    remote = dest.split(":", 1)[0]
    return (
        f"rclone's Google login has expired, so GCS is behind the {adapter.what}. "
        f"Run `rclone config reconnect {remote}:`, then `{adapter.command}`."
    )


def _runner(adapter: Adapter) -> _Run:
    """Return the rclone seam, with a missing rclone raised as the adapter's error."""

    def run(args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return _rclone.run(args)
        except OSError as err:
            raise adapter.error(
                f"cannot run rclone: {err}; run inside the pixi environment"
            ) from err

    return run
