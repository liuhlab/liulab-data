"""Copy the store's current state to GCS, which holds it for good: what changed since the last.

This is the GEO/SRA store's adapter of :mod:`labdata._gcs`. The held set is the current state:
the current build and the raw files and snapshot folders it read, as its manifest names them,
with the README and manifest beside each, the marker and the root ``README.md``. ``work/``,
``support/`` and older builds stay behind. The copy record, :data:`COPIED`, is JSON: each file
with its sha256, or null for a file sent every time. :func:`labdata.store.lifecycle.finalize`
runs each copy and gives it the status record to send last, marked finalized.

A routine copy with no copy record on the work disk or in GCS seeds one from the build the
marker in GCS names, when GCS is known to hold exactly that build; otherwise it is skipped and
asks for ``labdata store copy --full``, which uploads the current state and deletes nothing. A
copy refuses, sending nothing, when it would delete more of the copy record's raw files than
:func:`labdata._gcs.delete_limit` allows, unless the deletes are accepted by hand, or when the
work disk is not in step with GCS (:func:`check`). :func:`restore` fills a work disk from GCS.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path

from labdata import _gcs
from labdata.exceptions import LabdataError, NotAStoreError
from labdata.store import paths
from labdata.store.build import build_lock
from labdata.store.layout import MANIFEST, manifest_path
from labdata.store.provenance import README
from labdata.store.reader import build_path
from labdata.store.root import open_store

#: The copy record: each file GCS holds, as the last successful copy left it, with its
#: sha256, or null for a file that goes up every time. Kept here on the disk and in GCS.
COPIED = paths.COPY_RECORD.rel

#: The files at the top of the root: they go up after the rest, so the marker in GCS never
#: names a build GCS lacks.
_TOP = [paths.README.rel, paths.MARKER.rel]

#: Paths from the store root to their sha256; None for a file that goes up every time.
_Files = dict[str, str | None]


class StoreCopyError(LabdataError):
    """The store cannot be copied: it does not open, has no destination, or rclone failed."""


def destination(root: Path, to: str | None = None) -> str:
    """Return ``to``, or else the marker's ``copy_to``: where the store's copy goes.

    Raises
    ------
    StoreCopyError
        If ``root`` is not a store, or neither names a destination.
    """
    try:
        store = open_store(root)
    except NotAStoreError as err:
        raise StoreCopyError(f"copy refused: {err}") from None
    dest = str(to or store.settings.get(_gcs.COPY_TO_KEY) or "")
    if not dest:
        raise StoreCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{_gcs.COPY_TO_KEY} in {paths.MARKER.at(root)}"
        )
    return dest


def send(
    root: Path,
    dest: str,
    status: bytes | None,
    *,
    full: bool = False,
    accept_deletes: int = 0,
    bucket_matches_marker: bool = False,
) -> _gcs.CopyResult:
    """Send the store's changes since the last successful copy to ``dest``, then ``status``.

    Holds the build lock; :func:`labdata.store.lifecycle.finalize` runs it.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    dest : str
        An rclone path, ``<remote>:<bucket>/<path>``.
    status : bytes or None
        The status record marked finalized, sent last; None sends none.
    full : bool
        Upload every file of the current state instead, and delete nothing. It lists every
        file on both sides; for a first copy or a repair.
    accept_deletes : int
        Delete up to this many raw files the current build no longer reads, even past the
        usual bound: for files removed on purpose.
    bucket_matches_marker : bool
        Whether GCS holds exactly the build its marker names and what that build read. A
        routine copy with no copy record, on the disk or in GCS, then seeds one from that
        build, if it is on the disk.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's Google login has expired, or when a
        routine copy finds no copy record and cannot seed one.

    Raises
    ------
    StoreCopyError
        If the current build is incomplete, the work disk is not in step with GCS, more of
        the copied raw files would be deleted than this copy deletes, or rclone fails for
        another reason. rclone is not called when the build is incomplete.
    StoreBuildError
        If a build, prune or copy of this store is running.
    """
    with build_lock(root):
        held = _current(root, paths.read_marker(root).get(paths.BUILD_KEY))
        adapter = _Adapter(root, seeds=bucket_matches_marker)
        skipped = _gcs.copy(adapter, dest, held, status, full=full, accept_deletes=accept_deletes)
    return _gcs.CopyResult(dest, skipped=skipped is not None, message=skipped or "")


def check(root: Path) -> bool:
    """Check the work disk against the marker's destination, as :func:`labdata._gcs.check` does.

    A store with no destination passes.

    Returns
    -------
    bool
        False when rclone's login has expired, so nothing was checked.

    Raises
    ------
    StoreCopyError
        If the work disk's copy record is not the one in GCS, or GCS holds none and it does.
    """
    dest = str(paths.read_marker(root).get(_gcs.COPY_TO_KEY) or "")
    return not dest or _gcs.check(_Adapter(root, seeds=False), dest)


def restore(
    root: Path, source: str | None = None, *, approve: Callable[[int, int], bool]
) -> str | None:
    """Fill the work disk at ``root`` from ``source``, as :func:`labdata._gcs.restore` says.

    Holds the store's build lock, so a restore never runs beside a build, prune or copy.

    Parameters
    ----------
    root : pathlib.Path
        The store root; it may be empty or missing.
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
    StoreCopyError
        If no source is set, GCS holds no copy record or lacks a file it names, or rclone fails.
    StoreBuildError
        If a build, prune or copy of this store is running.
    """
    if not source and paths.MARKER.at(root).is_file():
        source = str(paths.read_marker(root).get(_gcs.COPY_TO_KEY) or "")
    if not source:
        raise StoreCopyError("no source: pass --from <remote>:<bucket>/<path>")
    with build_lock(root):
        return _gcs.restore(_Adapter(root, seeds=False), source, approve=approve)


class _Adapter(_gcs.Adapter):
    record = COPIED
    status = paths.STATUS.rel
    what = "store"
    command = "labdata store copy"
    restorer = "labdata store restore"
    error = StoreCopyError
    layout = paths.LAYOUT
    seed_from = (paths.MARKER.rel,)

    def __init__(self, root: Path, *, seeds: bool) -> None:
        super().__init__(root)
        self.seeds = seeds

    def load(self, data: bytes) -> _gcs.Files:
        return json.loads(data)

    def dump(self, files: _gcs.Files) -> bytes:
        return (json.dumps(files, indent=1, sort_keys=True) + "\n").encode()

    def holder(self, path: str) -> str:
        """Return the build, raw source or snapshot that holds ``path``; ``""`` at the top."""
        parts = path.split("/")
        depth = {paths.BUILDS.name: 2, paths.RAW.name: 2, paths.SNAPSHOTS.name: 3}
        return "/".join(parts[: depth.get(parts[0], 0)])

    def check_deletes(
        self, held: _gcs.Files, copied: _gcs.Files, accepted: int, *, full: bool
    ) -> None:
        raw = {p for p, sha in copied.items() if p.startswith(paths.RAW.rel) and sha}
        gone = [p for p in self.gone(held, copied) if p in raw]
        limit = _gcs.delete_limit(len(raw), accepted)
        if len(gone) > limit:
            raise StoreCopyError(
                f"copy refused: {len(gone)} of the {len(raw)} raw files the last copy sent are "
                f"not read by the current build, more than the {limit} this copy deletes, so "
                "nothing was copied: its manifest may be damaged. If the files left on purpose, "
                f"`labdata store copy --accept-deletes {len(gone)}` deletes them in the bucket."
            )

    def seed(self, fetched: Mapping[str, bytes]) -> _Files | None:
        """Return the files of the build the marker in GCS names, if it is on the disk.

        Only when the caller vouches that GCS holds exactly that build and what it read.
        """
        marker = fetched.get(paths.MARKER.rel)
        if not self.seeds or not marker:
            return None
        build = paths.parse_marker(marker.decode()).get(paths.BUILD_KEY)
        if not build:
            return None
        if not manifest_path(build_path(self.root, str(build))).is_file():
            return None
        return _build_files(self.root, str(build))[0]


def _current(root: Path, build: object) -> _Files:
    """Return the current state's files with their sha256, after checking it is all there."""
    if not build:
        raise StoreCopyError(f"copy refused: {paths.MARKER.at(root)} names no build")
    folder = build_path(root, str(build))
    if not manifest_path(folder).is_file():
        rel = folder.relative_to(root).as_posix()
        raise StoreCopyError(f"copy refused: {rel} is missing or has no {MANIFEST}")
    held, missing = _build_files(root, str(build))
    if missing:
        raise StoreCopyError(
            "copy refused: the current build is incomplete; missing " + ", ".join(missing)
        )
    return held


def _build_files(root: Path, build: str) -> tuple[_Files, list[str]]:
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
        named = {f"{paths.RAW.rel}{source}/{f['name']}": f.get("sha256") for f in files}
        missing += [p for p in named if not (root / p).is_file()]
        held |= named | _side_files(root, f"{paths.RAW.rel}{source}")
    for source, snap in sorted(inputs.get("snapshots", {}).items()):
        sub = f"{paths.SNAPSHOTS.rel}{source}/{snap['date']}"
        if (root / sub).is_dir():
            held |= {f"{sub}/{p}": None for p in _listing(root / sub)}
        held[f"{sub}/{snap['name']}"] = snap.get("sha256")
        if not (root / sub / snap["name"]).is_file():
            missing.append(f"{sub}/{snap['name']}")
    held |= _side_files(root, rel) | {p: None for p in _TOP if (root / p).is_file()}
    return held, missing


def _side_files(root: Path, folder: str) -> _Files:
    """Return a folder's README and manifest, those present, to go up every time."""
    return {f"{folder}/{n}": None for n in (README, MANIFEST) if (root / folder / n).is_file()}


def _listing(folder: Path) -> list[str]:
    """Return every file under ``folder``, hidden ones aside, as paths from it."""
    return sorted(
        p.relative_to(folder).as_posix()
        for p in folder.rglob("*")
        if p.is_file() and not any(part.startswith(".") for part in p.relative_to(folder).parts)
    )
