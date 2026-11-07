"""Make the paper store's GCS copy match it, as the GEO/SRA store's copy does.

New and changed files are uploaded, the marker last, then objects the store no longer holds
are deleted. Hidden files stay behind: the lock, the daily pass's journal and every temp
file; so does ``paper-status.json``, which the copy itself updates. An expired login skips the copy and sets ``copy_pending`` in ``paper-status.json``; a
later copy clears it.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

from labdata import _rclone
from labdata.exceptions import LabdataError
from labdata.papers.root import MARKER, PaperStore, paper_lock
from labdata.papers.status import STATUS, read_status, write_status

#: The marker key naming the rclone destination, ``<remote>:<bucket>/<path>``.
COPY_TO_KEY = "copy_to"

#: Left out of the copy, and deleted from the bucket: hidden files and folders, and the
#: status record, which the copy itself changes.
_LEFT_OUT = ["--filter=- .*", "--filter=- .*/**", f"--filter=- /{STATUS}"]


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
    """Make ``to`` match the paper store with rclone: new files first, then deletes.

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
        If no destination is set, or rclone fails for another reason; when rclone ran,
        ``copy_pending`` is then set, as the bucket is behind.
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
            done = _rclone.mirror(
                str(root),
                dest,
                [*_LEFT_OUT, f"--filter=- /{MARKER}"],
                _LEFT_OUT,
                run=_run_rclone,
                error=PaperCopyError,
            )
        except PaperCopyError:
            _pending(root, True)
            raise
        except OSError as err:
            _pending(root, True)
            raise PaperCopyError(
                f"cannot run rclone: {err}; run inside the pixi environment"
            ) from err
        _pending(root, not done)
    if not done:
        message = _rclone.expired_message(dest, "paper store", "labdata papers copy")
        return CopyResult(dest, skipped=True, message=message)
    return CopyResult(dest)


def _pending(root: Path, pending: bool) -> None:
    write_status(root, replace(read_status(root), copy_pending=pending))


def _run_rclone(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone through the shared runner, which tests replace."""
    return _rclone.run(args)
