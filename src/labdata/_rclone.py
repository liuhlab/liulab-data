"""The rclone seam: run one rclone command and tell an expired login from other faults.

Every copy to or listing of a remote (a GCS bucket, a Drive folder) goes through :func:`run`,
which tests replace.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable

#: rclone flags that print a one-line progress summary a minute.
PROGRESS = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]

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


def login_expired(stderr: str) -> bool:
    """Return whether rclone's log says its OAuth login has expired or was revoked."""
    return bool(_EXPIRED.search(stderr))


def mirror(
    src: str,
    dest: str,
    copy_filters: list[str],
    sync_filters: list[str],
    *,
    run: Callable[[list[str]], subprocess.CompletedProcess[str]],
    error: Callable[[str], Exception],
) -> bool:
    """Make ``dest`` match ``src``: new files first, then deletes.

    One listing call first shows an expired login before any upload. ``rclone copy`` with
    ``copy_filters`` uploads; ``rclone sync --delete-excluded`` with ``sync_filters`` uploads
    the rest and deletes every object those filters leave out. A cut-off run resumes.

    Returns
    -------
    bool
        False when rclone's login has expired, so ``dest`` is behind.

    Raises
    ------
    Exception
        ``error(message)`` when rclone fails for another reason.
    """
    probe = run(["rclone", "lsjson", dest, "--max-depth", "1"])
    if probe.returncode and login_expired(probe.stderr):
        return False
    for args in (
        ["rclone", "copy", src, dest, *copy_filters, *PROGRESS],
        ["rclone", "sync", src, dest, "--delete-excluded", *sync_filters, *PROGRESS],
    ):
        done = run(args)
        if done.returncode == 0:
            continue
        if login_expired(done.stderr):
            return False
        raise error(f"rclone {args[1]} failed ({done.returncode}): {done.stderr}")
    return True


def expired_message(dest: str, what: str, command: str) -> str:
    """Return what to do when rclone's login expired: reconnect, then rerun ``command``."""
    remote = dest.split(":", 1)[0]
    return (
        f"rclone's Google login has expired, so the bucket is behind the {what}. "
        f"Run `rclone config reconnect {remote}:`, then `{command}`."
    )
