"""The rclone seam: run one rclone command and tell an expired login from other faults.

Every copy to or listing of a remote (a GCS bucket, a Drive folder) goes through :func:`run`,
which tests replace.
"""

from __future__ import annotations

import re
import subprocess
import sys

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
