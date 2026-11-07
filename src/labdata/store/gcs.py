"""Copy the store's current state to a bucket with ``rclone``.

The copy holds the marker, the root ``README.md``, the snapshots the current build read,
``raw/`` and the current build; ``work/``, ``support/``, the status record and older builds
are left out, and the bucket's copies of them are deleted.
One listing call first checks that rclone's login has not expired.
A first ``rclone copy`` uploads everything but the marker, then an ``rclone sync`` uploads
the marker and deletes the rest, so the bucket's marker never names a build it lacks. rclone
skips files already uploaded, so a cut-off copy resumes when rerun.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from labdata import _rclone
from labdata.exceptions import LabdataError, NotAStoreError
from labdata.store.build import build_lock
from labdata.store.layout import MANIFEST, manifest_path
from labdata.store.provenance import README
from labdata.store.reader import build_path
from labdata.store.root import MARKER, open_store

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"


class StoreCopyError(LabdataError):
    """The store cannot be copied: it does not open, has no destination, or rclone failed."""


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped because rclone's login expired."""

    destination: str
    skipped: bool = False
    message: str = ""


def copy(root: Path, destination: str | None = None) -> CopyResult:
    """Copy the store's current state to ``destination`` with rclone.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    destination : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's Google login has expired.

    Raises
    ------
    StoreCopyError
        If the store does not open, its current build is incomplete, no destination is set,
        or rclone fails for another reason. rclone is not called when the store is refused.
    StoreBuildError
        If a build, prune or copy of this store is running.
    """
    try:
        store = open_store(root)
    except NotAStoreError as err:
        raise StoreCopyError(f"copy refused: {err}") from None
    dest = destination or store.settings.get(COPY_TO_KEY)
    if not dest:
        raise StoreCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {root / MARKER}"
        )
    with build_lock(root):
        selected = _selected(root, store.settings.get("build"))
        done = _rclone.mirror(
            str(root),
            str(dest),
            [*selected, "--filter=- **"],
            [*selected, f"--filter=+ /{MARKER}", "--filter=- **"],
            run=_run_rclone,
            error=StoreCopyError,
        )
    if not done:
        message = _rclone.expired_message(str(dest), "store", "labdata store copy")
        return CopyResult(str(dest), skipped=True, message=message)
    return CopyResult(str(dest))


def _selected(root: Path, build: object) -> list[str]:
    """Return the include filters for the current state, after checking it is all there."""
    if not build:
        raise StoreCopyError(f"copy refused: {root / MARKER} names no build")
    folder = build_path(root, str(build))
    rel = folder.relative_to(root).as_posix()
    manifest = manifest_path(folder)
    if not manifest.is_file():
        raise StoreCopyError(f"copy refused: {rel} is missing or has no {MANIFEST}")
    data = json.loads(manifest.read_text(encoding="utf-8"))
    missing = [
        f"{rel}/{f['name']}"
        for f in data.get("files", [])
        if not (folder / f["name"]).is_file()
        or (folder / f["name"]).stat().st_size != f.get("bytes")
    ]
    inputs = data.get("inputs", {})
    for source, files in inputs.get("raw", {}).items():
        missing += [
            f"raw/{source}/{f['name']}"
            for f in files
            if not (root / "raw" / source / f["name"]).is_file()
        ]
    snaps = sorted((s, v["date"], v["name"]) for s, v in inputs.get("snapshots", {}).items())
    missing += [
        f"snapshots/{s}/{date}/{name}"
        for s, date, name in snaps
        if not (root / "snapshots" / s / date / name).is_file()
    ]
    if missing:
        raise StoreCopyError(
            "copy refused: the current build is incomplete; missing " + ", ".join(missing)
        )
    return [
        "--filter=- .*",
        "--filter=- .*/**",
        *(f"--filter=+ /snapshots/{s}/{date}/**" for s, date, _ in snaps),
        "--filter=+ /raw/**",
        f"--filter=+ /{rel}/**",
        f"--filter=+ /{README}",
    ]


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner; a missing rclone is a ``StoreCopyError``."""
    try:
        return _rclone.run(args)
    except OSError as err:
        raise StoreCopyError(f"cannot run rclone: {err}; run inside the pixi environment") from err
