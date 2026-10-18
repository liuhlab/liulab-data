"""Delete the builds and snapshots the store no longer reads, after a grace period.

A non-current build goes once the store switched away from it more than the grace period
ago, ``GRACE`` by default. A snapshot goes once it is older than the one the current build
reads and every build that read it went out of use more than the grace period ago; its extract
parts under ``work/`` go with it. A snapshot newer than the current build's is waiting for the
next build and stays. Neither the current build nor what it reads is ever deleted.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from labdata.store import extract, readme
from labdata.store.build import (
    BUILD_KEY,
    SWITCHED_AWAY_KEY,
    SWITCHED_KEY,
    build_lock,
    write_marker,
)
from labdata.store.provenance import MANIFEST
from labdata.store.reader import build_folders, build_path
from labdata.store.root import MARKER, read_marker

#: How long a build stays readable after the store switched away from it, by default.
GRACE = dt.timedelta(hours=24)

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True, slots=True)
class PruneReport:
    """What one prune deleted: build folders first, then snapshot folders."""

    deleted: list[Path] = field(default_factory=list)


def prune(
    root: Path, *, now: dt.datetime | None = None, grace: dt.timedelta = GRACE
) -> PruneReport:
    """Delete old builds and unread snapshots, keeping everything inside the grace period.

    A build switched away from before switch times were recorded counts as switched away
    when the current build was switched to. After deleting anything, it rewrites the root
    ``README.md`` so its sizes stay current.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    now : datetime.datetime or None
        The time to measure the grace period from; defaults to the current UTC time.
    grace : datetime.timedelta
        How long a build stays after the store switched away from it: ``GRACE``, 24 hours,
        by default. Zero deletes at once.

    Returns
    -------
    PruneReport
        The folders deleted.

    Raises
    ------
    StoreBuildError
        If a build, prune or copy of this store is running.
    """
    now = now or dt.datetime.now(dt.UTC)
    with build_lock(root):
        marker = root / MARKER
        settings = read_marker(root)
        current = settings.get(BUILD_KEY)
        if not current:
            return PruneReport()
        switched = _time(settings.get(SWITCHED_KEY)) or dt.datetime.fromtimestamp(
            marker.stat().st_mtime, dt.UTC
        )
        away: dict[str, str] = settings.get(SWITCHED_AWAY_KEY, {})
        builds = build_folders(root)
        reads = {b.name: _reads(b) for b in builds}
        unused_since = {
            b.name: _time(away.get(b.name)) or switched for b in builds if b.name != current
        }
        old = [build_path(root, name) for name, t in unused_since.items() if now - t > grace]
        latest = dict(reads.get(str(current), set()))
        snaps = []
        for folder in sorted((root / "snapshots").glob("*/*")):
            source, date = folder.parent.name, folder.name
            if not (folder.is_dir() and _DATE.match(date)) or date >= latest.get(source, date):
                continue
            readers = [n for n, t in unused_since.items() if (source, date) in reads[n]]
            if now - max((unused_since[n] for n in readers), default=switched) > grace:
                snaps.append(folder)
        for folder in [*old, *snaps]:
            shutil.rmtree(folder)
        for folder in snaps:
            shutil.rmtree(
                extract.extract_dir(root, folder.parent.name, folder.name), ignore_errors=True
            )
        if any(p.name in away for p in old):
            settings[SWITCHED_AWAY_KEY] = {k: v for k, v in away.items() if not _gone(root, k)}
            write_marker(root, settings)
        if old or snaps:
            readme.try_write(root)
        return PruneReport([*old, *snaps])


def _reads(build: Path) -> set[tuple[str, str]]:
    """Return the ``(source, date)`` of each snapshot a build's manifest says it read."""
    manifest = build / MANIFEST
    if not manifest.is_file():
        return set()
    inputs = json.loads(manifest.read_text(encoding="utf-8")).get("inputs", {})
    return {(source, s["date"]) for source, s in inputs.get("snapshots", {}).items()}


def _time(value: object) -> dt.datetime | None:
    return dt.datetime.fromisoformat(value) if isinstance(value, str) else None


def _gone(root: Path, build_id: str) -> bool:
    return not build_path(root, build_id).is_dir()
