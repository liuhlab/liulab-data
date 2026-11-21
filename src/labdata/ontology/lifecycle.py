"""The ontology store's run lock, its status record, ``ontology-status.json``, and its copy.

An update is finalized once GCS holds it. It first copies when the last update's copy is
pending, or else checks the work disk against GCS, and ends with a copy that sends the status
record last, marked finalized. A copy that fails or is skipped leaves the copy pending, for the
next update or ``labdata ontology copy``. Only this module writes the status record.
"""

from __future__ import annotations

import enum
import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from labdata import _gcs, _lock
from labdata._files import write_atomic
from labdata.exceptions import LabdataError
from labdata.ontology import copy, paths

logger = logging.getLogger(__name__)


class OntologyBusyError(LabdataError):
    """Another update holds the ontology store's run lock."""


class Outcome(enum.StrEnum):
    """How the last update stands."""

    RUNNING = "running"
    #: Ended with a vocabulary failed, or stopped part of the way.
    FAILED = "failed"
    #: Every vocabulary is current on the work disk.
    BUILT = "built"
    #: Built, and GCS holds it.
    FINALIZED = "finalized"


class Result(enum.StrEnum):
    """What the last update did with one vocabulary."""

    UPDATED = "updated"
    UNCHANGED = "unchanged"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class VocabularyResult:
    """One vocabulary's row in the status record.

    ``release`` is ``None`` when the update could not read it; ``reason`` says why a
    vocabulary failed. ``snapshot`` is the vocabulary's current snapshot after the update, so
    a failed vocabulary names the one it kept. ``terms`` is that snapshot's own-term count, and
    ``source_date`` the date of its source file, ``YYYY-MM-DD``.
    """

    release: str | None
    snapshot: str | None
    result: Result
    reason: str | None = None
    terms: int | None = None
    source_date: str | None = None


@dataclass(frozen=True, slots=True)
class UpdateStatus:
    """The status record: the last update's start, end and outcome, and one row per vocabulary.

    ``start`` and ``end`` are UTC, ISO 8601; ``end`` is ``None`` while it runs. ``outcome``
    is ``None`` before the first update. ``copy_pending`` says GCS is behind the work disk.
    """

    start: str | None = None
    end: str | None = None
    outcome: Outcome | None = None
    vocabularies: dict[str, VocabularyResult] = field(default_factory=dict)
    copy_pending: bool = False


@contextmanager
def run_lock(root: Path, holder: str, *, owner_gone: bool = False) -> Iterator[None]:
    """Hold the ontology store's run lock, so two updates never write the store at once.

    Raises
    ------
    OntologyBusyError
        If another update holds it and still runs, or this host cannot tell and
        ``owner_gone`` is False.
    """
    with _lock.run_lock(
        paths.LOCK.at(root), holder, error=OntologyBusyError, owner_gone=owner_gone
    ):
        yield


def read_status(root: Path) -> UpdateStatus:
    """Return the status record; an empty one before the first update.

    Keys the file holds that the record lacks are ignored.
    """
    path = paths.STATUS.at(root)
    if not path.is_file():
        return UpdateStatus()
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    rows = data.pop("vocabularies", {})
    data["outcome"] = Outcome(data["outcome"]) if data.get("outcome") else None
    status = UpdateStatus(**_known(UpdateStatus, data))
    for name, row in rows.items():
        row["result"] = Result(row["result"])
        status.vocabularies[name] = VocabularyResult(**_known(VocabularyResult, row))
    return status


def write_status(root: Path, status: UpdateStatus) -> None:
    """Write the status record whole."""
    write_atomic(paths.STATUS.at(root), _encode(status).decode())


def _known(cls: type[UpdateStatus | VocabularyResult], data: dict[str, Any]) -> dict[str, Any]:
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in names}


def finalize(
    root: Path,
    destination: str | None = None,
    *,
    full: bool = False,
    accept_deletes: int = 0,
    owner_gone: bool = False,
) -> _gcs.CopyResult:
    """Copy the store to GCS under the run lock; see :func:`labdata.ontology.copy.send`.

    Parameters
    ----------
    root : pathlib.Path
        The ontology store root.
    destination : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.
    full, accept_deletes
        Passed to the copy; ``full`` takes no ``accept_deletes``.
    owner_gone : bool
        As for :func:`run_lock`.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when the copy was skipped; it stays pending.

    Raises
    ------
    OntologyCopyError
        If no destination is set, or ``full`` comes with ``accept_deletes``; or the copy's
        own refusals and failures, which leave the copy pending.
    OntologyBusyError
        If an update holds the run lock.
    """
    dest = destination or copy.destination(root)
    if not dest:
        raise copy.OntologyCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{_gcs.COPY_TO_KEY} in {paths.MARKER.at(root)}"
        )
    if full and accept_deletes:
        raise copy.OntologyCopyError("--full deletes nothing, so it takes no --accept-deletes")
    with run_lock(root, "copy", owner_gone=owner_gone):
        return _send(root, dest, full=full, accept_deletes=accept_deletes)


def restore(
    root: Path,
    source: str | None = None,
    *,
    approve: Callable[[int, int], bool],
    owner_gone: bool = False,
) -> str | None:
    """Fill the work disk from GCS under the run lock; see :func:`labdata.ontology.copy.restore`.

    Raises
    ------
    OntologyBusyError
        If an update holds the run lock.
    """
    root.mkdir(parents=True, exist_ok=True)
    with run_lock(root, "restore", owner_gone=owner_gone):
        return copy.restore(root, source, approve=approve)


def copy_first(root: Path) -> None:
    """Start an update: copy when the last copy is pending, or else check the disk against GCS.

    A skipped copy, or a check rclone's expired login stops, only logs.

    Raises
    ------
    OntologyCopyError
        If the work disk is not in step with GCS, or the copy fails.
    """
    dest = copy.destination(root)
    if not dest:
        return
    if read_status(root).copy_pending:
        result = _owed(root, dest)
        if result.skipped:
            logger.warning("the pending GCS copy was skipped: %s", result.message)
    elif not copy.check(root, dest):
        logger.warning("rclone's Google login has expired, so the disk was not checked against GCS")


def copy_last(root: Path) -> None:
    """End an update with a copy; one that fails or is skipped leaves it pending, logged."""
    dest = copy.destination(root)
    if not dest:
        return
    try:
        result = _owed(root, dest)
    except LabdataError as err:
        logger.warning("the GCS copy failed, so it is pending: %s", err)
        return
    if result.skipped:
        logger.warning("the GCS copy was skipped, so it is pending: %s", result.message)


def _owed(root: Path, dest: str) -> _gcs.CopyResult:
    """Send an update's copy; the store's first, with no copy record on the disk, sends all."""
    return _send(root, dest, full=not paths.COPY_RECORD.at(root).is_file())


def _send(root: Path, dest: str, *, full: bool = False, accept_deletes: int = 0) -> _gcs.CopyResult:
    """Mark the copy pending, then copy; a copy that works sends the record marked finalized."""
    status = replace(read_status(root), copy_pending=True)
    write_status(root, status)
    # Under the lock, a record left running was stopped.
    ended = {
        Outcome.RUNNING: Outcome.FAILED,
        Outcome.BUILT: Outcome.FINALIZED,
        None: Outcome.FINALIZED,
    }
    done = replace(status, outcome=ended.get(status.outcome, status.outcome), copy_pending=False)
    return copy.send(root, dest, _encode(done), full=full, accept_deletes=accept_deletes)


def _encode(status: UpdateStatus) -> bytes:
    data = asdict(status)
    data["outcome"] = status.outcome or ""  # the file's form before the first update
    return (json.dumps(data, indent=2, sort_keys=True) + "\n").encode()
