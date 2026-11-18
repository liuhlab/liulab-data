"""Whether a Slurm job or a process still runs, and :func:`show_job`, the one seam that asks Slurm.

The paper job reads its successors' states and its log folder through it. The GEO/SRA store's
run lock records its :class:`Owner` and asks :func:`alive` whether that owner still runs.
"""

from __future__ import annotations

import os
import re
import socket
import subprocess
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from labdata._files import utc_stamp

#: The states of a job that has ended; any other state counts as running.
ENDED = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "TIMEOUT",
    }
)


def show_job(job_id: str) -> str | None:
    """Return ``scontrol show job`` output; the seam tests replace.

    Returns
    -------
    str or None
        The output; ``""`` when Slurm knows no such job, None when Slurm cannot be asked.
    """
    try:
        done = subprocess.run(
            ["scontrol", "show", "job", job_id], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    if done.returncode == 0:
        return done.stdout
    return "" if "Invalid job id" in done.stderr else None


def job_state(job_id: str) -> str | None:
    """Return a job's state, such as ``RUNNING``: ``""`` when Slurm knows no such job.

    None when Slurm cannot be asked, or does not say.
    """
    shown = show_job(job_id)
    if not shown:
        return shown
    state = re.search(r"\bJobState=(\S+)", shown)
    return state[1] if state else None


@dataclass(frozen=True, slots=True)
class Owner:
    """The process that holds a lock: its Slurm job, if any, its host, its ID and start time."""

    job: str | None
    host: str
    pid: int
    started: str

    @classmethod
    def me(cls) -> Owner:
        """Return this process as an owner."""
        return cls(
            job=os.environ.get("SLURM_JOB_ID") or None,
            host=socket.gethostname(),
            pid=os.getpid(),
            started=utc_stamp(),
        )

    def record(self) -> dict[str, Any]:
        """Return the owner as a JSON object; :meth:`from_record` reads it back."""
        return asdict(self)

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> Owner | None:
        """Return the owner a :meth:`record` names; None when it does not name one whole."""
        try:
            return cls(
                job=record["job"] and str(record["job"]),
                host=str(record["host"]),
                pid=int(record["pid"]),
                started=str(record["started"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def __str__(self) -> str:
        job = f"Slurm job {self.job}, " if self.job else ""
        return f"{job}process {self.pid} on {self.host}, started {self.started}"


def alive(owner: Owner) -> bool | None:
    """Return whether ``owner`` still runs; None when this host cannot tell.

    On the owner's host, its process answers. Elsewhere Slurm answers for its job: a job that
    ended, or that Slurm no longer knows, is gone, and so is this job's own run before a
    requeue.
    """
    if owner.host == socket.gethostname():
        return _running(owner.pid)
    if not owner.job:
        return None
    if owner.job == os.environ.get("SLURM_JOB_ID"):
        return False
    state = job_state(owner.job)
    if state is None:
        return None
    return bool(state) and state not in ENDED


def _running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
