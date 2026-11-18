"""The GCS protocol both stores share: a copy sends a store's changes to GCS, which holds it.

Each store has an :class:`Adapter`, which supplies its held set: the files GCS should hold,
from the store root, each with a signature that changes with the file, or None for a file
sent every time. The copy record is the held set as the last successful copy left it, at the
store root on the work disk and in GCS.

After one listing call checks rclone's login, :func:`copy` checks the work disk against GCS
(:func:`check`), then sends, in order: the new and changed files by name; the files at the
top of the root, the marker among them; the deletes, by name, of the recorded files that left
the held set; the copy record; and last the status record, marked finalized. Each record goes
to GCS first, then the work disk. The copy record waits on the disk under its pending name
until it lands in GCS, and is then renamed over the old one; a copy cut between the two leaves
a pending record :func:`check` adopts. No copy deletes by comparing GCS with the work disk.

:func:`restore` runs the copy the other way: it fills a work disk with the state files the
copy record in GCS names, and saves that record and the status record on the disk last.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from labdata import _paths, _rclone
from labdata._files import temp_beside

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The largest share of a copy record's paths one copy deletes in GCS, unless one path is
#: more or more are accepted by hand; past it the copy refuses.
MAX_DELETE_SHARE = 0.05

#: A restore asks before it downloads more than this many bytes.
ASK_ABOVE = 1_000_000_000

#: Paths from the store root to their signatures; None for a file sent every time.
Files = Mapping[str, object]

_Run = Callable[[list[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped, with a message saying why."""

    destination: str
    skipped: bool = False
    message: str = ""


class Adapter(ABC):
    """One store's side of the protocol: its root, its copy record and its rules."""

    #: The copy record's, its pending copy's and the status record's file names at the root.
    record: str
    pending: str
    status: str
    #: The store's name and its copy and restore commands, for messages.
    what: str
    command: str
    restorer: str
    #: Raised when rclone fails for a reason other than an expired login.
    error: type[Exception]
    #: The store's declared paths; the state files among them are what a restore brings.
    layout: tuple[_paths.Declared, ...]
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

    def state(self, path: str) -> bool:
        """Return whether ``path``, from the store root, is a state file: one a restore brings."""
        try:
            return _paths.kind(path, self.layout) is _paths.STATE
        except ValueError:
            return False

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
        """Return a copy record for a store with none, from the :attr:`seed_from` GCS holds.

        The copy uses it only when GCS holds each file it lets the copy leave out.
        """
        return None


def delete_limit(recorded: int, accepted: int = 0) -> int:
    """Return how many of ``recorded`` paths one copy may delete in GCS.

    It is the largest of one, :data:`MAX_DELETE_SHARE` of ``recorded`` and ``accepted``.
    """
    return int(max(1, MAX_DELETE_SHARE * recorded, accepted))


def copy(
    adapter: Adapter,
    dest: str,
    held: Files,
    status: bytes | None,
    *,
    full: bool = False,
    accept_deletes: int = 0,
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
    status : bytes or None
        The status record marked finalized, sent last; None leaves the one in GCS as it stands.
    full : bool
        Send every file instead, delete nothing and save ``held`` as the copy record.
    accept_deletes : int
        Passed to :meth:`Adapter.check_deletes`.

    Returns
    -------
    str or None
        Why the copy was skipped: rclone's login has expired, or a routine copy found no copy
        record on the work disk or in GCS and the adapter seeded none, or seeded one naming a
        file GCS lacks or holds with another size. None when it is done, the status record
        included.

    Raises
    ------
    Exception
        :func:`check`'s or :meth:`Adapter.check_deletes`'s refusal, before anything is sent;
        the adapter's :attr:`~Adapter.error` when rclone cannot run or fails for another reason.
    """
    run = _runner(adapter)
    expired = _expired(adapter, dest)
    if not _rclone.logged_in(dest, run=run) or not _check(adapter, dest, run, full=full):
        return expired
    copied = None
    if not full or adapter.full_checks:
        disk = adapter.root / adapter.record
        if not full and disk.is_file():
            copied = adapter.load(disk.read_bytes())
        else:
            # A routine copy gets here only when GCS holds no copy record either.
            names = [adapter.record] if full else list(adapter.seed_from)
            fetched = _fetch(adapter, dest, names, run) if names else {}
            if fetched is None:
                return expired
            if adapter.record in fetched:
                copied = adapter.load(fetched[adapter.record])
            elif full:
                copied = adapter.load(disk.read_bytes()) if disk.is_file() else None
            else:
                copied = adapter.seed(fetched)
                lacking = [] if copied is None else _lacking(adapter, dest, held, copied, run)
                if lacking is None:
                    return expired
                if copied is None or lacking:
                    why = f"; GCS lacks {lacking[0]} or holds another size" if lacking else ""
                    return (
                        f"no copy record, on the work disk or in GCS, so GCS is behind the "
                        f"{adapter.what}{why}. Run `{adapter.command} --full` once: it uploads "
                        f"the {adapter.what} and deletes nothing. Later copies send changes."
                    )
                # Up first, so a copy cut off after it never has to seed one again.
                if not _put(adapter, dest, adapter.record, adapter.dump(copied), run):
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
    if not _put(adapter, dest, adapter.record, adapter.dump({**kept, **held}), run):
        return expired
    if status is not None and not _put(adapter, dest, adapter.status, status, run):
        return expired
    return None


def check(adapter: Adapter, dest: str) -> bool:
    """Check that the work disk's copy record is the one in GCS, by MD5, downloading nothing.

    GCS's MD5 comes from the object's metadata. A pending copy record with GCS's MD5, which a
    copy cut after its upload leaves, first becomes the disk's. A disk with no copy record passes
    only when GCS holds none either: the copy's seeding rules handle it.

    Returns
    -------
    bool
        False when rclone's login has expired, so nothing was checked.

    Raises
    ------
    Exception
        The adapter's :attr:`~Adapter.error`, naming the restore command, when the records
        differ or only GCS holds one; or, naming a full copy, when only the disk holds one.
    """
    run = _runner(adapter)
    return _rclone.logged_in(dest, run=run) and _check(adapter, dest, run, full=False)


def _check(adapter: Adapter, dest: str, run: _Run, *, full: bool) -> bool:
    """Run :func:`check` after the login probe; a full copy may send to GCS with no record."""
    listed = _listing(adapter, dest, [adapter.record], run)
    if listed is None:
        return False
    disk = adapter.root / adapter.record
    pending = adapter.root / adapter.pending
    if adapter.record in listed and _matches(pending, *listed[adapter.record]):
        pending.replace(disk)
    if not disk.is_file():
        if adapter.record not in listed:
            return True
        raise adapter.error(
            f"the work disk has no copy record, {adapter.record}, but GCS at {dest} holds one, "
            f"so nothing was sent: this disk was not restored from GCS. Run "
            f"`{adapter.restorer} --from {dest}`; it downloads only the files this disk lacks "
            "or holds with another size or MD5."
        )
    if adapter.record not in listed:
        if full:
            return True
        raise adapter.error(
            f"GCS at {dest} holds no copy record, but the work disk's names what an earlier "
            f"copy sent there, so nothing was sent: GCS is new or was emptied. "
            f"`{adapter.command} --full` sends the whole {adapter.what}."
        )
    if listed[adapter.record][1] != hashlib.md5(disk.read_bytes()).hexdigest():
        raise adapter.error(
            f"the work disk's copy record, {adapter.record}, is not the one in GCS at {dest}, so "
            f"nothing was sent: this disk is behind GCS, or another disk sent to it since. "
            f"Run `{adapter.restorer} --from {dest}` to bring the work disk in step with GCS."
        )
    return True


def restore(adapter: Adapter, source: str, *, approve: Callable[[int, int], bool]) -> str | None:
    """Fill the work disk from ``source``: the state files its copy record names, and no other.

    It downloads the files whose entries differ between the copy record in GCS and the disk's,
    or every file when the disk has none, leaving out a file present with GCS's size and MD5.
    Then it deletes the state files the disk's copy record names and GCS's does not, or, on a
    disk with no copy record, every state file on it GCS's does not name. It saves GCS's copy
    record and status record on the disk last. Scratch and rebuildable files stay.

    Parameters
    ----------
    adapter : Adapter
        The store.
    source : str
        An rclone path, ``<remote>:<bucket>/<path>``.
    approve : callable
        Called with the number of files and bytes to download, before any; False stops the
        restore there.

    Returns
    -------
    str or None
        Why the restore stopped: rclone's login has expired, or ``approve`` said no. None when
        it is done.

    Raises
    ------
    Exception
        The adapter's :attr:`~Adapter.error` when GCS holds no copy record or lacks a file it
        names, or when rclone cannot run or fails for another reason.
    """
    run = _runner(adapter)
    root = adapter.root
    expired = _expired(adapter, source, "the restore again")
    if not _rclone.logged_in(source, run=run):
        return expired
    root.mkdir(parents=True, exist_ok=True)
    fetched = _fetch(adapter, source, [adapter.record, adapter.status], run)
    if fetched is None:
        return expired
    if adapter.record not in fetched:
        raise adapter.error(f"{source} holds no copy record, {adapter.record}: nothing to restore")
    held = {p: s for p, s in adapter.load(fetched[adapter.record]).items() if adapter.state(p)}
    disk = root / adapter.record
    had = adapter.load(disk.read_bytes()) if disk.is_file() else None
    wanted = [
        p for p in held if had is None or p not in had or held[p] is None or had[p] != held[p]
    ]
    listed = _listing(adapter, source, wanted, run)
    if listed is None:
        return expired
    lacking = [p for p in wanted if p not in listed]
    if lacking:
        raise adapter.error(
            f"{source} lacks {len(lacking)} files its copy record names, such as {lacking[0]}: "
            f"run `{adapter.command} --full` from a disk that holds the {adapter.what}"
        )
    get = [p for p in wanted if not _matches(root / p, *listed[p])]
    if not approve(len(get), sum(listed[p][0] for p in get)):
        return "nothing was downloaded"
    pull = ["copy", source, str(root), "--no-traverse", "--ignore-times", *_rclone.PROGRESS]
    # The files first, then the top of the root, so the marker never names a missing build.
    for names in ([p for p in get if "/" in p], [p for p in get if "/" not in p]):
        if names and not _rclone.run_on_paths(
            pull, names, folder=root, run=run, error=adapter.error
        ):
            return expired
    named = _state_files(adapter) if had is None else had
    for path in sorted(p for p in named if p not in held and adapter.state(p)):
        _remove(root, path)
    for name in (adapter.record, adapter.status):
        if name in fetched:
            tmp = temp_beside(root / name)
            tmp.write_bytes(fetched[name])
            tmp.replace(root / name)
    return None


def _listing(
    adapter: Adapter, source: str, paths: list[str], run: _Run
) -> dict[str, tuple[int, str]] | None:
    """Return each of ``paths`` GCS holds with its size and MD5; None on an expired login."""
    if not paths:
        return {}
    out: list[str] = []
    args = ["lsjson", source, "--recursive", "--files-only", "--hash"]
    if not _rclone.run_on_paths(
        args, paths, folder=adapter.root, run=run, error=adapter.error, out=out
    ):
        return None
    return {e["Path"]: (e["Size"], e.get("Hashes", {}).get("md5", "")) for e in json.loads(out[0])}


def _lacking(
    adapter: Adapter, dest: str, held: Files, seeded: Files, run: _Run
) -> list[str] | None:
    """Return the files a seeded record would leave out of the copy that GCS lacks.

    A file counts as lacking when GCS holds it with a size other than the disk's. One listing,
    no download; None on an expired login.
    """
    skipped = [p for p, sig in held.items() if sig is not None and seeded.get(p) == sig]
    listed = _listing(adapter, dest, skipped, run)
    if listed is None:
        return None
    return [
        p for p in skipped if p not in listed or listed[p][0] != (adapter.root / p).stat().st_size
    ]


def _matches(path: Path, size: int, md5: str) -> bool:
    """Return whether ``path`` is a file of ``size`` bytes with ``md5``."""
    if not path.is_file() or path.stat().st_size != size:
        return False
    digest = hashlib.md5()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest() == md5


def _state_files(adapter: Adapter) -> list[str]:
    """Return the state files on the work disk, the two records aside, as paths from the root."""
    root = adapter.root
    found = []
    for rel in _paths.declared(_paths.STATE, adapter.layout):
        path = root / rel
        files = path.rglob("*") if rel.endswith("/") else [path]
        found += [p.relative_to(root).as_posix() for p in files if p.is_file()]
    return [p for p in found if p not in (adapter.record, adapter.status)]


def _remove(root: Path, path: str) -> None:
    """Delete ``path`` under ``root``, and the folders it leaves empty."""
    (root / path).unlink(missing_ok=True)
    folder = (root / path).parent
    while folder != root and folder.is_dir() and not any(folder.iterdir()):
        folder.rmdir()
        folder = folder.parent


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


def _put(adapter: Adapter, dest: str, name: str, data: bytes, run: _Run) -> bool:
    """Save ``data`` as the file ``name`` at the top of the root, in GCS first, then on the disk.

    The copy record goes up from its pending name, which a cut leaves for :func:`check`.
    Returns False when rclone's login has expired.
    """
    path = adapter.root / name
    record = name == adapter.record
    tmp = adapter.root / adapter.pending if record else temp_beside(path)
    try:
        tmp.write_bytes(data)
        to = f"{dest.rstrip('/')}/{name}"
        if not _rclone.run_on_paths(
            ["copyto", str(tmp), to], [], folder=adapter.root, run=run, error=adapter.error
        ):
            return False
        tmp.replace(path)
    finally:
        if not record:
            tmp.unlink(missing_ok=True)
    return True


def _expired(adapter: Adapter, dest: str, then: str = "") -> str:
    """Return what to do when rclone's login has expired: reconnect, then copy again."""
    remote = dest.split(":", 1)[0]
    if then:
        return (
            f"rclone's Google login has expired. Run `rclone config reconnect {remote}:`, "
            f"then run {then}."
        )
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
