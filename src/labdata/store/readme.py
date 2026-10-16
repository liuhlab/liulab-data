"""The store's root ``README.md``, filled from the store's current state.

The text is the packaged ``store_readme.md``; this module fills in the current build, its
switch time, the snapshots it reads, the commit and version that made it, and folder sizes.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import os
import stat
import string
import tomllib
from pathlib import Path

from labdata.exceptions import NotAStoreError
from labdata.store._files import write_atomic
from labdata.store.provenance import MANIFEST, README
from labdata.store.root import MARKER

#: The top-level folders whose sizes the README gives, after the current build.
_SIZED = ("raw", "snapshots", "support", "work")

logger = logging.getLogger(__name__)


def write(root: Path) -> Path:
    """Write ``<root>/README.md`` for the build the marker names.

    Parameters
    ----------
    root : pathlib.Path
        The store root.

    Returns
    -------
    pathlib.Path
        The README written.

    Raises
    ------
    NotAStoreError
        If the root has no marker, or the marker names no build.
    """
    marker = root / MARKER
    if not marker.is_file():
        raise NotAStoreError(f"{root} is not a labdata store: it has no {MARKER}")
    with marker.open("rb") as fh:
        settings = tomllib.load(fh)
    build = settings.get("build")
    if not build:
        raise NotAStoreError(f"{marker} names no build; run `labdata store build`")
    folder = root / "builds" / str(build)
    manifest_path = folder / MANIFEST
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
    )
    made = manifest.get("produced_by")
    snaps = manifest.get("inputs", {}).get("snapshots", {})
    sizes = [(f"builds/{build}/", _size(folder))] + [
        (f"{name}/", _size(root / name)) for name in _SIZED if (root / name).is_dir()
    ]
    text = string.Template(_template()).substitute(
        build=build,
        switched=settings.get("switched", "an unrecorded time"),
        made_by=_made_by(made),
        snapshot_table=_table(
            ("Source", "Date", "File"),
            [(f"`{s}`", v["date"], f"`{v['name']}`") for s, v in sorted(snaps.items())],
        ),
        size_table=_table(("Folder", "Size"), [(f"`{n}`", _human(b)) for n, b in sizes]),
    )
    out = root / README
    write_atomic(out, text)
    return out


def try_write(root: Path) -> Path | None:
    """Write the README as ``write`` does, but log a warning instead of raising.

    Returns
    -------
    pathlib.Path or None
        The README written, or ``None`` if it could not be.
    """
    try:
        return write(root)
    except Exception as err:
        logger.warning(
            "could not write %s: %s; run `labdata store readme` to write it", root / README, err
        )
        return None


def _made_by(made: dict[str, str | None] | None) -> str:
    if not made:
        return "an unrecorded command, at an unrecorded commit."
    version = f", labdata {made['version']}." if made.get("version") else "."
    return f"`{made.get('command')}` at commit `{made.get('commit') or 'unknown'}`{version}"


def _template() -> str:
    return importlib.resources.files("labdata.store").joinpath("store_readme.md").read_text("utf-8")


def _table(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    lines = [header, tuple("---" for _ in header), *rows]
    return "\n".join("| " + " | ".join(cells) + " |" for cells in lines)


def _size(folder: Path) -> int:
    """Return the summed sizes of the files under ``folder``, skipping symlinks.

    A file deleted while the walk runs counts as gone.
    """
    total = 0
    for dirpath, _, names in os.walk(folder):
        for name in names:
            try:
                info = (Path(dirpath) / name).lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISLNK(info.st_mode):
                total += info.st_size
    return total


def _human(n: int) -> str:
    """Return a byte count in decimal units, such as ``2.5 kB`` or ``12.8 GB``."""
    size, unit = float(n), "B"
    for bigger in ("kB", "MB", "GB", "TB"):
        if size < 1000:
            break
        size, unit = size / 1000, bigger
    return f"{n} B" if unit == "B" else f"{size:.1f} {unit}"
