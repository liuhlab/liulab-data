"""Copy the ontology store to GCS, which holds it for good: the changes since the last copy.

This is the ontology store's adapter of :mod:`labdata._gcs`. The held set is the marker, the
README, the current-snapshots record, which go up every time, and each kept snapshot's tables
file and source files, each with its sha256. The copy record, :data:`COPIED`, is JSON. Hidden
files and ``work/`` stay behind. :mod:`labdata.ontology.lifecycle` runs each copy under the
run lock and gives it the status record to send last.

A copy refuses, sending nothing, when a current snapshot has no tables, when the work disk is
not in step with GCS (:func:`check`), or when it would delete more snapshots than it adds and
more than :func:`labdata._gcs.delete_limit` allows, unless the deletes are accepted by hand.
:func:`restore` fills a work disk from GCS.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from labdata import _gcs
from labdata._files import read_toml_marker
from labdata.exceptions import LabdataError
from labdata.ontology import paths
from labdata.ontology.root import read_current

#: The copy record: each file GCS holds, as the last successful copy left it, with its
#: sha256, or null for a file that goes up every time. Kept here on the disk and in GCS.
COPIED = paths.COPY_RECORD.rel

#: The files at the top of the root; they go up after the snapshots.
_TOP = [paths.README.rel, paths.CURRENT.rel, paths.MARKER.rel]

#: Paths from the store root to their sha256; None for a file that goes up every time.
_Files = dict[str, str | None]


class OntologyCopyError(LabdataError):
    """The ontology store cannot be copied: no destination, a missing snapshot, or rclone failed."""


def destination(root: Path) -> str:
    """Return the marker's ``copy_to``; empty when the store has no copy."""
    marker = read_toml_marker(root, paths.MARKER.rel, "an ontology store")
    return str(marker.get(_gcs.COPY_TO_KEY) or "")


def send(
    root: Path, dest: str, status: bytes | None, *, full: bool = False, accept_deletes: int = 0
) -> _gcs.CopyResult:
    """Send the ontology store's changes since the last successful copy to ``dest``.

    Takes no lock: :func:`labdata.ontology.lifecycle.finalize` runs it.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.
    dest : str
        An rclone path, ``<remote>:<bucket>/<path>``.
    status : bytes or None
        The status record, sent last, to GCS and then the work disk; None sends none.
    full : bool
        Upload every held file instead, and delete nothing: a first copy or a repair.
    accept_deletes : int
        Delete up to this many snapshots that left the store, even past the usual bound.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's login has expired, or when a
        routine copy finds no copy record, on the disk or in GCS.

    Raises
    ------
    OntologyCopyError
        If a current snapshot has no tables, the work disk is not in step with GCS, the copy
        would delete too many snapshots, or rclone fails for another reason.
    """
    held = _held(root)
    adapter = _Adapter(root)
    skipped = _gcs.copy(adapter, dest, held, status, full=full, accept_deletes=accept_deletes)
    return _gcs.CopyResult(dest, skipped=skipped is not None, message=skipped or "")


def check(root: Path, dest: str) -> bool:
    """Check the work disk against GCS at ``dest``, as :func:`labdata._gcs.check` does.

    Returns
    -------
    bool
        False when rclone's login has expired, so nothing was checked.

    Raises
    ------
    OntologyCopyError
        If the work disk's copy record is not the one in GCS, or only one side holds one.
    """
    return _gcs.check(_Adapter(root), dest)


def restore(root: Path, source: str | None, *, approve: Callable[[int, int], bool]) -> str | None:
    """Fill the work disk at ``root`` from ``source``, as :func:`labdata._gcs.restore` says.

    Takes no lock: :func:`labdata.ontology.lifecycle.restore` runs it.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root; it may be empty.
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
    OntologyCopyError
        If no source is set, GCS holds no copy record or lacks a file it names, or rclone fails.
    """
    if not source and paths.MARKER.at(root).is_file():
        source = destination(root)
    if not source:
        raise OntologyCopyError("no source: pass --from <remote>:<bucket>/<path>")
    return _gcs.restore(_Adapter(root), source, approve=approve)


class _Adapter(_gcs.Adapter):
    record = COPIED
    pending = paths.PENDING_RECORD.rel
    status = paths.STATUS.rel
    what = "ontology store"
    command = "labdata ontology copy"
    restorer = "labdata ontology restore"
    error = OntologyCopyError
    layout = paths.LAYOUT

    def load(self, data: bytes) -> _gcs.Files:
        return json.loads(data)

    def dump(self, files: _gcs.Files) -> bytes:
        return (json.dumps(files, indent=1, sort_keys=True) + "\n").encode()

    def holder(self, path: str) -> str:
        """Return the snapshot folder that holds ``path``; ``""`` at the top."""
        parts = path.split("/")
        return "/".join(parts[:2]) if parts[0] == paths.SNAPSHOTS.name else ""

    def check_deletes(
        self, held: _gcs.Files, copied: _gcs.Files, accepted: int, *, full: bool
    ) -> None:
        # Each new snapshot lets the prune drop an older one of its vocabulary.
        recorded = {self.holder(p) for p, sig in copied.items() if sig is not None}
        kept = {self.holder(p) for p in held}
        gone = len(recorded - kept)
        added = len(kept - recorded - {""})
        limit = _gcs.delete_limit(len(recorded), accepted + added)
        if gone > limit:
            raise OntologyCopyError(
                f"copy refused: {gone} of the {len(recorded)} snapshots the last copy sent have "
                f"left {paths.SNAPSHOTS.at(self.root)}, more than the {limit} this copy "
                "deletes, so nothing was copied: this work disk may have lost them, or was not "
                "restored from the bucket. If they left on purpose, "
                f"`labdata ontology copy --accept-deletes {gone}` deletes them in the bucket."
            )


def _held(root: Path) -> _Files:
    """Return each kept snapshot's files with their sha256, and the files at the top."""
    missing = [
        name
        for name in read_current(root).values()
        if not paths.SNAPSHOTS.at(root, name, paths.TABLES_FILE).is_file()
    ]
    if missing or not paths.CURRENT.at(root).is_file():
        what = f"current snapshot {missing[0]} has no tables" if missing else "no current record"
        raise OntologyCopyError(f"copy refused: {what}, so nothing was copied")
    held: _Files = {}
    for folder in sorted(paths.SNAPSHOTS.at(root).iterdir()):
        if folder.name.startswith(".") or not folder.is_dir():
            continue
        source = folder / paths.SOURCE_DIR
        files = [folder / paths.TABLES_FILE, *(sorted(source.iterdir()) if source.is_dir() else [])]
        for path in files:
            if path.is_file() and not path.name.startswith("."):
                held[path.relative_to(root).as_posix()] = _sha256(path)
    return held | {p: None for p in _TOP if (root / p).is_file()}


def _sha256(path: Path) -> str:
    with path.open("rb") as fh:
        return hashlib.file_digest(fh, "sha256").hexdigest()
