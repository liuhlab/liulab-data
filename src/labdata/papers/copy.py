"""Copy the paper store's new and changed files to GCS with ``rclone copy``.

A copy never deletes in the bucket, as the paper store never deletes a paper's files. Hidden
files stay behind: the lock, the daily pass's journal and every temp file. An expired login
skips the copy and sets ``copy_pending`` in ``paper-status.json``; a later copy clears it.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

from labdata import _rclone
from labdata.exceptions import LabdataError
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import read_status, write_status

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"


class PaperCopyError(LabdataError):
    """The paper store cannot be copied: no destination is set, or rclone failed."""


@dataclass(frozen=True, slots=True)
class CopyResult:
    """The outcome of one copy: done, or skipped because rclone's login expired."""

    destination: str
    skipped: bool = False
    message: str = ""


def destination(store: PaperStore) -> str:
    """Return the marker's ``copy_to``; empty when the paper store has no copy."""
    return str(store.settings.get(COPY_TO_KEY) or "")


def copy(store: PaperStore, to: str | None = None) -> CopyResult:
    """Copy the paper store's new and changed files to ``to`` with rclone.

    Holds the paper store's lock, so a copy never runs beside a pass.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    to : str or None
        An rclone path, ``<remote>:<bucket>/<path>``; defaults to the marker's ``copy_to``.

    Returns
    -------
    CopyResult
        ``skipped`` is true, with a message, when rclone's login has expired.

    Raises
    ------
    PaperCopyError
        If no destination is set, or rclone fails for another reason; ``copy_pending`` is
        then set, as the bucket is behind.
    """
    root = store.root
    dest = to or destination(store)
    if not dest:
        raise PaperCopyError(
            f"no destination: pass --to <remote>:<bucket>/<path> or set "
            f"{COPY_TO_KEY} in {root / MARKER}"
        )
    with paper_lock(root):
        try:
            done = _run_rclone(
                ["rclone", "copy", str(root), dest, "--exclude=.*", *_rclone.PROGRESS]
            )
        except OSError as err:
            _pending(root, True)
            raise PaperCopyError(
                f"cannot run rclone: {err}; run inside the pixi environment"
            ) from err
        if done.returncode and _rclone.login_expired(done.stderr):
            _pending(root, True)
            remote = dest.split(":", 1)[0]
            return CopyResult(
                dest,
                skipped=True,
                message=(
                    "rclone's Google login has expired, so the bucket is behind the paper store. "
                    f"Run `rclone config reconnect {remote}:`, then `labdata papers copy`."
                ),
            )
        if done.returncode:
            _pending(root, True)
            raise PaperCopyError(f"rclone copy failed ({done.returncode}): {done.stderr}")
        _pending(root, False)
    return CopyResult(dest)


def _pending(root: Path, pending: bool) -> None:
    write_status(root, replace(read_status(root), copy_pending=pending))


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner, which tests replace."""
    return _rclone.run(args)
