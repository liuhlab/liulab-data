"""The rclone seam: run one rclone command and tell an expired login from other faults.

Every copy to or listing of a remote (a GCS bucket, a Drive folder) goes through :func:`run`,
which tests replace. Both GCS copies also take their delete bound from here.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

#: rclone flags that print a one-line progress summary a minute.
PROGRESS = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]

#: The largest share of a copy record's paths one GCS copy deletes in the bucket, unless one
#: path is more or more are accepted by hand; past it the copy refuses.
MAX_DELETE_SHARE = 0.05

_EXPIRED = re.compile(r"invalid_grant|maybe it has expired|expired or revoked")


def run(args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run rclone, echoing its log lines to stderr as they come and keeping them.

    Parameters
    ----------
    args : list of str
        The command line, ``rclone`` first.

    Returns
    -------
    subprocess.CompletedProcess
        The exit status and the stderr text; stdout is not captured.

    Raises
    ------
    OSError
        If rclone cannot be started.
    """
    proc = subprocess.Popen(args, stderr=subprocess.PIPE, text=True)
    lines = []
    assert proc.stderr is not None
    for line in proc.stderr:
        sys.stderr.write(line)
        lines.append(line)
    return subprocess.CompletedProcess(args, proc.wait(), "", "".join(lines))


def delete_limit(recorded: int, accepted: int = 0) -> int:
    """Return how many of ``recorded`` paths one copy may delete in the bucket.

    It is the largest of one, :data:`MAX_DELETE_SHARE` of ``recorded`` and ``accepted``.
    """
    return int(max(1, MAX_DELETE_SHARE * recorded, accepted))


def login_expired(stderr: str) -> bool:
    """Return whether rclone's log says its OAuth login has expired or was revoked."""
    return bool(_EXPIRED.search(stderr))


def logged_in(dest: str, *, run: Callable[[list[str]], subprocess.CompletedProcess[str]]) -> bool:
    """Return False when one listing call of ``dest`` shows rclone's login has expired.

    Any other fault is left for the next command to meet.
    """
    probe = run(["rclone", "lsjson", dest, "--max-depth", "1"])
    return not (probe.returncode and login_expired(probe.stderr))


def run_on_paths(
    args: list[str],
    paths: list[str],
    *,
    folder: Path,
    run: Callable[[list[str]], subprocess.CompletedProcess[str]],
    error: Callable[[str], Exception],
) -> bool:
    """Run ``rclone <args>`` on ``paths`` alone, or as it is when ``paths`` is empty.

    The paths go to rclone as ``--files-from-raw``, in a hidden list file in ``folder`` that
    is deleted afterwards.

    Returns
    -------
    bool
        False when rclone's login has expired.

    Raises
    ------
    Exception
        ``error(message)`` when rclone fails for another reason.
    """
    name = ""
    try:
        listed = []
        if paths:
            fd, name = tempfile.mkstemp(prefix=".copy-list.", dir=folder)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.writelines(p + "\n" for p in paths)
            listed = [f"--files-from-raw={name}"]
        done = run(["rclone", *args, *listed])
    finally:
        if name:
            Path(name).unlink(missing_ok=True)
    if done.returncode == 0:
        return True
    if login_expired(done.stderr):
        return False
    raise error(f"rclone {args[0]} failed ({done.returncode}): {done.stderr}")


def expired_message(dest: str, what: str, command: str) -> str:
    """Return what to do when rclone's login expired: reconnect, then rerun ``command``."""
    remote = dest.split(":", 1)[0]
    return (
        f"rclone's Google login has expired, so the bucket is behind the {what}. "
        f"Run `rclone config reconnect {remote}:`, then `{command}`."
    )
