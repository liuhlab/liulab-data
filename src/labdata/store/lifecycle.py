"""The GEO/SRA store's life cycle: every update ends finalized, once GCS holds it.

An update goes through the phases fetched, packed, built, checked, switched and finalized. It
is a refresh or a full run, each a Snakemake DAG, a build by hand, or the yearly SRA removal.
This module owns the status record, the switch and the copy outcome: a build is written
without a switch (:func:`build`), :func:`switch` checks it and only then switches the work
disk to it, and :func:`finalize` copies the store to GCS.

The status record, ``refresh-status.json``, is the store's: the last update (``update``), its
start, end and outcome, and the build it made. The outcome is running; failed, stopped before
its switch; abandoned; built, switched on the work disk and not finalized; or finalized.
``copy_pending`` says GCS is behind the work disk. A refresh or a full run also names its folder
under ``work/``. A refresh keeps the date its change search starts from, its counts and the date
of the last successful refresh, written at its switch.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from labdata.exceptions import StoreBuildError
from labdata.ncbi.entrez import EntrezClient
from labdata.store import build as _build
from labdata.store import gaps, gcs, pack, paths, readme, sra_removal
from labdata.store._files import write_atomic
from labdata.store.gaps import Coverage
from labdata.store.reader import BuildReader, build_path
from labdata.store.sra_removal import SraRemovalReport

logger = logging.getLogger(__name__)

#: The status record's file name in the store root.
STATUS = paths.STATUS.rel
#: The outcomes the status record names.
RUNNING, FAILED, ABANDONED, BUILT, FINALIZED = (
    "running",
    "failed",
    "abandoned",
    "built",
    "finalized",
)
#: The updates the status record names, by the command that runs each.
REFRESH, RUN, BUILD, REMOVE_SRA = "refresh", "run", "build", "remove-sra"
#: The status key keeping ``copy_pending`` as it stood when the update switched builds.
AT_SWITCH = "copy_pending_at_switch"
#: The file in a refresh's folder each pack appends its per-bucket counts to.
PACK_COUNTS = "pack-counts.jsonl"
#: An outcome before the switch: the work disk still reads the build before the update's.
_UNSWITCHED = (RUNNING, FAILED)


class CheckError(StoreBuildError):
    """A new build failed its checks, so the store was not switched to it."""

    def __init__(self, message: str, coverage: Coverage) -> None:
        super().__init__(message)
        self.coverage = coverage


def read_status(root: Path) -> dict[str, Any]:
    """Return the status record, or an empty dict when the store has none."""
    path = paths.STATUS.at(root)
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def write_status(root: Path, status: Mapping[str, Any]) -> None:
    """Write the status record whole."""
    write_atomic(paths.STATUS.at(root), _dump(status))


def start(root: Path, update: str, folder: str | None = None, **fields: Any) -> dict[str, Any]:
    """Begin an update, or carry on the run in ``folder`` the status record names.

    A run first checks the work disk against GCS (:func:`labdata.store.gcs.check`), and only
    warns when rclone's login has expired. A new update keeps the last-success date and
    ``copy_pending``; ``fields`` add to it. A run carried on is running again unless it has
    switched builds.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    update : str
        :data:`REFRESH`, :data:`RUN`, :data:`BUILD` or :data:`REMOVE_SRA`.
    folder : str or None
        A run's folder, from the root; None for an update by hand.
    **fields
        More of the new update's record, such as ``started`` or ``since``.

    Returns
    -------
    dict
        The status record written.

    Raises
    ------
    StoreCopyError
        If a run's work disk is not in step with GCS; the copy is marked pending.
    """
    if folder is not None:
        _check(root)
    status = read_status(root)
    if folder is not None and status.get("folder") == folder:
        if status.get("outcome") in _UNSWITCHED:
            status["outcome"] = RUNNING
        if status.get("outcome") != FINALIZED:
            status["ended"] = None
    else:
        status = {
            "update": update,
            "started": _now(),
            "ended": None,
            "outcome": RUNNING,
            "last_success": status.get("last_success"),
            "copy_pending": bool(status.get("copy_pending")),
            **({} if folder is None else {"folder": folder}),
            **fields,
        }
    write_status(root, status)
    return status


def end(root: Path) -> dict[str, Any]:
    """Record that a run stopped: an update still running failed before its switch."""
    status = read_status(root)
    if status.get("outcome") == FINALIZED:
        return status
    if status.get("outcome") == RUNNING:
        status["outcome"] = FAILED
    status["ended"] = _now()
    write_status(root, status)
    return status


def unfinished(status: Mapping[str, Any], update: str) -> bool:
    """Return whether the status record names an unfinished run of ``update``.

    That is a run stopped before its switch, or one switched with its copy still pending.
    """
    if _update(status) != update or not status.get("folder"):
        return False
    outcome = status.get("outcome")
    return outcome in _UNSWITCHED or (outcome == BUILT and bool(status.get("copy_pending")))


def build(
    root: Path,
    build_id: str | None = None,
    *,
    workers: int | None = None,
    chunk_bytes: int = _build.PARSE_CHUNK_BYTES,
    fraction: float | None = None,
    sample_of: Path | None = None,
    switch: bool = True,
) -> Path:
    """Write a new build; by hand, switch to it at once.

    By hand, the build is an update of its own, switched to with no checks: built, not
    finalized, until a copy works. A run's build step passes ``switch=False``: the build is
    recorded as the running update's, and the run's :func:`switch` step checks it. A sample
    build switches its scratch root and keeps no status record.

    Parameters
    ----------
    root, build_id, workers, chunk_bytes, fraction, sample_of
        As for :func:`labdata.store.build.build`.
    switch : bool
        Switch to the build at once.

    Returns
    -------
    pathlib.Path
        The new build folder.

    Raises
    ------
    StoreBuildError
        As :func:`labdata.store.build.build` raises it.
    """
    made = {"workers": workers, "chunk_bytes": chunk_bytes, "fraction": fraction}
    if sample_of is not None:
        out = _build.build(root, build_id, sample_of=sample_of, **made)
        _switch_marker(root, out.name)
        return out
    if switch:
        start(root, BUILD)
    try:
        out = _build.build(root, build_id, **made)
    except BaseException:
        if switch:
            end(root)
        raise
    status = read_status(root) | {"build": out.name}
    write_status(root, status)
    if switch:
        _switched(root, status)
        end(root)
    return out


def switch(root: Path) -> tuple[dict[str, Any], Coverage]:
    """Check the running update's new build, then switch the store to it.

    The check is coverage (:func:`labdata.store.gaps.coverage`). After the switch the update
    is built, and the copy pending when the store names a destination; a refresh records its
    date as the last success, and its counts.

    Returns
    -------
    tuple of dict and Coverage
        The status record written, and the check's report.

    Raises
    ------
    CheckError
        If the build fails coverage; the store keeps its build.
    StoreBuildError
        If the status record names no new build.
    """
    status = read_status(root)
    build_id = status.get("build")
    if not build_id:
        raise StoreBuildError(
            f"{paths.STATUS.at(root)} names no new build to switch to; build one first"
        )
    report = gaps.coverage(root, BuildReader(build_path(root, str(build_id))))
    if not report.passed:
        raise CheckError(
            f"build {build_id} fails coverage, so the store stays on its build", report
        )
    return _switched(root, status), report


def finalize(
    root: Path, destination: str | None = None, *, full: bool = False, accept_deletes: int = 0
) -> gcs.CopyResult:
    """Copy the store to GCS: the update is finalized once the copy works.

    The copy sends the status record last, marked finalized, unless the update has not
    switched builds yet. A copy skipped or failed marks it pending. The parameters are
    :func:`labdata.store.gcs.copy`'s.

    Returns
    -------
    CopyResult
        As :func:`labdata.store.gcs.copy` returns it.

    Raises
    ------
    StoreCopyError
        As :func:`labdata.store.gcs.copy` raises it.
    """
    return gcs.copy(
        root,
        destination,
        full=full,
        accept_deletes=accept_deletes,
        bucket_matches_marker=_gcs_holds_marker_build(read_status(root)),
    )


def remove_sra(root: Path, *, client: EntrezClient) -> SraRemovalReport:
    """Run the yearly SRA removal, the first step of an update that pack and build apply.

    The update stays running, not finalized, until a build and a copy after it.
    """
    start(root, REMOVE_SRA)
    try:
        return sra_removal.remove(root, client=client)
    except BaseException:
        end(root)
        raise


def finalized(root: Path) -> bytes | None:
    """Return the status record marked finalized, for a copy to send last.

    A store with no record gets one that says only that, and when. None while an update that
    has not switched builds is running, failed or abandoned: the copy leaves its record.
    """
    status = read_status(root)
    if status.get("outcome") in (*_UNSWITCHED, ABANDONED):
        return None
    ended = status.get("ended") or _now()
    return _dump(status | {"outcome": FINALIZED, "copy_pending": False, "ended": ended}).encode()


def note_copy(root: Path, *, pending: bool) -> None:
    """Mark the GCS copy pending or done in the status record, if the store has one."""
    status = read_status(root)
    if status and status.get("copy_pending") != pending:
        write_status(root, status | {"copy_pending": pending})


def _check(root: Path) -> None:
    """Refuse a run on a work disk not in step with GCS, marking the copy pending."""
    try:
        checked = gcs.check(root)
    except gcs.StoreCopyError:
        note_copy(root, pending=True)
        raise
    if not checked:
        logger.warning(
            "rclone's Google login has expired, so the work disk was not checked against GCS"
        )


def _switched(root: Path, status: dict[str, Any]) -> dict[str, Any]:
    """Switch the marker to the update's build and record it built."""
    _switch_marker(root, str(status["build"]))
    status |= {
        "outcome": BUILT,
        "ended": None,
        # Kept when a resumed run records its switch again.
        AT_SWITCH: bool(status.get(AT_SWITCH, status.get("copy_pending"))),
        "copy_pending": bool(paths.read_marker(root).get(gcs.COPY_TO_KEY)),
    }
    if _update(status) == REFRESH:
        status |= {
            "last_success": status["started"][:10],
            "counts": pack.read_counts(root / status["folder"] / PACK_COUNTS),
        }
    write_status(root, status)
    return status


def _switch_marker(root: Path, build_id: str) -> None:
    """Point the marker at a build and write the root README for it, under the build lock."""
    with _build.build_lock(root):
        paths.switch(root, build_id)
        readme.try_write(root)


def _update(status: Mapping[str, Any]) -> str | None:
    """Return the update a record names; one from before records named it is a refresh's."""
    return status.get("update") or (REFRESH if status.get("folder") else None)


def _gcs_holds_marker_build(status: Mapping[str, Any]) -> bool:
    """Return whether GCS held exactly the build its marker names, by the status record.

    That is, the copy was not pending when the last update switched builds, or, with no switch
    recorded, it is not pending now. False when the store has no status record.
    """
    return bool(status) and not status.get(AT_SWITCH, status.get("copy_pending"))


def _dump(status: Mapping[str, Any]) -> str:
    return json.dumps(status, indent=2, sort_keys=True) + "\n"


def _now() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
