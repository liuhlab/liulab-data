"""Copy the store's current state to GCS, which holds it for good: the changes since the last copy.

The current state is the current build and the raw files and snapshot folders it read, as its
manifest names them, with the README and manifest beside each, the marker and the root
``README.md``. ``work/``, ``support/``, the status record and older builds stay behind.

A routine copy compares it with the copy record, :data:`COPIED`, the files the last successful
copy sent with their sha256. A new or changed file goes up by name, as does every file with no
sha256; then the marker; then the files the record names that the current state left are
deleted in the bucket by name; then the record goes up, and onto the disk. A work disk without it fetches the bucket's. With neither, a routine copy seeds it
from the build the bucket's marker names, when the bucket is known to hold exactly that build;
otherwise it is skipped and asks for ``labdata store copy --full``, which uploads the current
state and deletes nothing.

No copy deletes by comparing the bucket with the disk. A copy refuses, sending nothing, when it
would delete more of the copy record's raw files than :func:`labdata._rclone.delete_limit`
allows, unless the deletes are accepted by hand. One listing call first checks rclone's login.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

from labdata import _rclone
from labdata.exceptions import LabdataError, NotAStoreError
from labdata.store._files import temp_beside
from labdata.store.build import BUILD_KEY, build_lock
from labdata.store.layout import MANIFEST, manifest_path
from labdata.store.provenance import README
from labdata.store.reader import build_path
from labdata.store.root import MARKER, open_store

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: The copy record: each file the bucket holds, as the last successful copy left it, with its
#: sha256, or null for a file that goes up every time. Kept here on the disk and in the bucket.
COPIED = ".copied-files.json"

#: Sent after the rest: the marker, so the bucket's never names a build it lacks.
_TOP = [README, MARKER]

#: Paths from the store root to their sha256; None for a file that goes up every time.
_Files = dict[str, str | None]


class StoreCopyError(LabdataError):
    """The store cannot be copied: it does not open, has no destination, or rclone failed."""


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped, with a message saying why."""

    destination: str
    skipped: bool = False
    message: str = ""


def copy(
    root: Path,
    destination: str | None = None,
    *,
    full: bool = False,
    accept_deletes: int = 0,
    in_step: bool = False,
) -> CopyResult:
    """Send the store's changes since the last successful copy to ``destination``.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    destination : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
    full : bool
        Upload every file of the current state instead, and delete nothing. It lists every
        file on both sides; for a first copy or a repair.
    accept_deletes : int
        Delete up to this many raw files the current build no longer reads, even past the
        usual bound: for files removed on purpose.
    in_step : bool
        Whether the bucket holds exactly the build its marker names and what that build read.
        A routine copy with no copy record, on the disk or in the bucket, then seeds one from
        that build, if it is on the disk.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's Google login has expired, or when a
        routine copy finds no copy record and cannot seed one. Past the destination checks,
        the status record then marks the copy pending, as it does on any failure; a success
        clears it.

    Raises
    ------
    StoreCopyError
        If the store does not open, its current build is incomplete, no destination is set,
        ``full`` comes with ``accept_deletes``, more of the copied raw files would be deleted
        than this copy deletes, or rclone fails for another reason. rclone is not called when
        the store is refused.
    StoreBuildError
        If a build, prune or copy of this store is running.
    """
    try:
        store = open_store(root)
    except NotAStoreError as err:
        raise StoreCopyError(f"copy refused: {err}") from None
    dest = str(destination or store.settings.get(COPY_TO_KEY) or "")
    if not dest:
        raise StoreCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {root / MARKER}"
        )
    if full and accept_deletes:
        raise StoreCopyError("--full deletes nothing, so it takes no --accept-deletes")
    # Imported here: the refresh imports this module.
    from labdata.store.refresh import note_copy

    try:
        with build_lock(root):
            held = _current(root, store.settings.get(BUILD_KEY))
            skipped = _send(
                root, dest, held, full=full, accept_deletes=accept_deletes, in_step=in_step
            )
    except BaseException:
        note_copy(root, pending=True)
        raise
    note_copy(root, pending=skipped is not None)
    if skipped:
        return CopyResult(dest, skipped=True, message=skipped)
    return CopyResult(dest)


def _send(
    root: Path, dest: str, held: _Files, *, full: bool, accept_deletes: int, in_step: bool
) -> str | None:
    """Copy the current state to ``dest``; return why the copy was skipped, or None when done."""
    expired = _rclone.expired_message(dest, "store", "labdata store copy")
    if not _rclone.logged_in(dest, run=_run_rclone):
        return expired
    copied = None
    if not full:
        path = root / COPIED
        if path.is_file():
            copied = json.loads(path.read_text(encoding="utf-8"))
        else:
            fetched = _fetch(root, dest, [COPIED, MARKER])
            if fetched is None:
                return expired
            if COPIED in fetched:
                copied = json.loads(fetched[COPIED])
            else:
                copied = _seed(root, fetched.get(MARKER)) if in_step else None
                if copied is None:
                    return (
                        "no copy record, on the disk or in the bucket, so the bucket is behind "
                        "the store. Run `labdata store copy --full` once: it uploads the current "
                        "state and deletes nothing. Later copies send changes."
                    )
                # Up first, so a copy cut off after it never has to trust the marker again.
                if not _put_copied(root, dest, copied):
                    return expired
        _check_deletes(held, copied, accept_deletes)
    if not _changes(root, dest, held, copied):
        return expired
    # A side file missing on the disk stays in the bucket, so the record keeps naming it.
    kept = {} if copied is None else {p: copied[p] for p in copied if p not in _gone(held, copied)}
    if _put_copied(root, dest, kept | held):
        return None
    return expired


def _fetch(root: Path, dest: str, names: list[str]) -> dict[str, str] | None:
    """Return the text of each of ``names`` the bucket holds; None when the login has expired."""
    folder = Path(tempfile.mkdtemp(prefix=".copy-fetch.", dir=root))
    try:
        if not _ok(root, ["copy", dest, str(folder), "--no-traverse"], names):
            return None
        return {
            n: (folder / n).read_text(encoding="utf-8") for n in names if (folder / n).is_file()
        }
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _seed(root: Path, marker: str | None) -> _Files | None:
    """Return the files of the build the bucket's ``marker`` names, if it is on the disk.

    The caller vouches that the bucket holds exactly that build and what it read.
    """
    build = tomllib.loads(marker).get(BUILD_KEY) if marker else None
    if not build or not manifest_path(build_path(root, str(build))).is_file():
        return None
    return _state(root, str(build))[0]


def _changes(root: Path, dest: str, held: _Files, copied: _Files | None) -> bool:
    """Upload new and changed files, then the marker, then delete what left the current state.

    With no ``copied``, upload every file and delete none. Returns False when rclone's login
    has expired.
    """
    files = sorted(set(held) - set(_TOP))
    if copied is None:
        first, put, gone = ["copy", str(root), dest], files, []
    else:
        first = ["copy", str(root), dest, "--no-traverse"]
        put = [p for p in files if held[p] is None or copied.get(p) != held[p]]
        gone = _gone(held, copied)
    steps = [
        (first, put),
        (["copy", str(root), dest, "--no-traverse"], [p for p in _TOP if p in held]),
        (["delete", dest, "--no-traverse"], gone),
    ]
    # A step with no paths is left out: rclone given none would act on the whole store.
    return all(_ok(root, [*args, *_rclone.PROGRESS], paths) for args, paths in steps if paths)


def _gone(held: _Files, copied: _Files) -> list[str]:
    """Return the recorded paths the current state left, as its manifest says.

    A path goes when what holds it, a build, a raw source or a snapshot, is no longer read,
    or when it carries a sha256 the manifest no longer names. A file with no sha256 under a
    folder still read is a side file that is missing on the disk: it stays.
    """
    units = {_unit(p) for p in held}
    return sorted(
        p
        for p, sha in copied.items()
        if p not in held and (_unit(p) not in units or sha is not None)
    )


def _unit(path: str) -> str:
    """Return the folder that holds ``path`` in the current state, ``""`` for the top level."""
    parts = path.split("/")
    return "/".join(parts[: {"builds": 2, "raw": 2, "snapshots": 3}.get(parts[0], 0)])


def _check_deletes(held: _Files, copied: _Files, accepted: int) -> None:
    raw = {p for p, sha in copied.items() if p.startswith("raw/") and sha}
    gone = [p for p in _gone(held, copied) if p in raw]
    limit = _rclone.delete_limit(len(raw), accepted)
    if len(gone) > limit:
        raise StoreCopyError(
            f"copy refused: {len(gone)} of the {len(raw)} raw files the last copy sent are "
            f"not read by the current build, more than the {limit} this copy deletes, so "
            "nothing was copied: its manifest may be damaged. If the files left on purpose, "
            f"`labdata store copy --accept-deletes {len(gone)}` deletes them in the bucket."
        )


def _put_copied(root: Path, dest: str, held: _Files) -> bool:
    """Save ``held`` as :data:`COPIED`, in the bucket first, then on the disk.

    Returns False when rclone's login has expired.
    """
    path = root / COPIED
    tmp = temp_beside(path)
    try:
        tmp.write_text(json.dumps(held, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        if not _ok(root, ["copyto", str(tmp), f"{dest.rstrip('/')}/{COPIED}"], []):
            return False
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def _current(root: Path, build: object) -> _Files:
    """Return the current state's files with their sha256, after checking it is all there."""
    if not build:
        raise StoreCopyError(f"copy refused: {root / MARKER} names no build")
    folder = build_path(root, str(build))
    if not manifest_path(folder).is_file():
        rel = folder.relative_to(root).as_posix()
        raise StoreCopyError(f"copy refused: {rel} is missing or has no {MANIFEST}")
    held, missing = _state(root, str(build))
    if missing:
        raise StoreCopyError(
            "copy refused: the current build is incomplete; missing " + ", ".join(missing)
        )
    return held


def _state(root: Path, build: str) -> tuple[_Files, list[str]]:
    """Return a build's files and what it read, with their sha256, and those not on the disk."""
    folder = build_path(root, build)
    rel = folder.relative_to(root).as_posix()
    data = json.loads(manifest_path(folder).read_text(encoding="utf-8"))
    missing = [
        f"{rel}/{f['name']}"
        for f in data.get("files", [])
        if not (folder / f["name"]).is_file()
        or (folder / f["name"]).stat().st_size != f.get("bytes")
    ]
    held: _Files = {f"{rel}/{f['name']}": f.get("sha256") for f in data.get("files", [])}
    inputs = data.get("inputs", {})
    for source, files in inputs.get("raw", {}).items():
        named = {f"raw/{source}/{f['name']}": f.get("sha256") for f in files}
        missing += [p for p in named if not (root / p).is_file()]
        held |= named | _small(root, f"raw/{source}")
    for source, snap in sorted(inputs.get("snapshots", {}).items()):
        sub = f"snapshots/{source}/{snap['date']}"
        if (root / sub).is_dir():
            held |= {f"{sub}/{p}": None for p in _listing(root / sub)}
        held[f"{sub}/{snap['name']}"] = snap.get("sha256")
        if not (root / sub / snap["name"]).is_file():
            missing.append(f"{sub}/{snap['name']}")
    held |= _small(root, rel) | {p: None for p in _TOP if (root / p).is_file()}
    return held, missing


def _small(root: Path, folder: str) -> _Files:
    """Return a folder's README and manifest, those present, to go up every time."""
    return {f"{folder}/{n}": None for n in (README, MANIFEST) if (root / folder / n).is_file()}


def _listing(folder: Path) -> list[str]:
    """Return every file under ``folder``, hidden ones aside, as paths from it."""
    return sorted(
        p.relative_to(folder).as_posix()
        for p in folder.rglob("*")
        if p.is_file() and not any(part.startswith(".") for part in p.relative_to(folder).parts)
    )


def _ok(root: Path, args: list[str], paths: list[str]) -> bool:
    return _rclone.on_paths(args, paths, folder=root, run=_run_rclone, error=StoreCopyError)


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner; a missing rclone is a ``StoreCopyError``."""
    try:
        return _rclone.run(args)
    except OSError as err:
        raise StoreCopyError(f"cannot run rclone: {err}; run inside the pixi environment") from err
