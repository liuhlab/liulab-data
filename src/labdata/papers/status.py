"""``paper-status.json``: what the paper job did last and where the papers stand.

The paper job reads it at start to find what is due: a day with no daily-pass success, a
month with no PMC sync success.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

from labdata.store._files import write_atomic

#: The status file at the paper store root.
STATUS = "paper-status.json"


@dataclass(frozen=True, slots=True)
class PaperStatus:
    """The contents of ``paper-status.json``.

    Parameters
    ----------
    last_pass : dict
        The last pass run: ``kind`` (``daily`` or ``pmc-sync``), ``start`` and ``end`` (UTC,
        ISO 8601) and ``outcome`` (``running``, ``success`` or ``failed``); a PMC sync also
        gives its ``sample`` share, ``None`` for all papers, and on success its PMC bucket
        ``bucket_retries`` by HTTP status or failure and ``bucket_throttled`` by status.
    last_daily_success : str or None
        The UTC date of the last daily pass that succeeded.
    last_pmc_sync_success : str or None
        The UTC date of the last PMC sync over all papers that succeeded.
    fetches : dict
        ``date`` and, under ``by_tier``, the papers fetched that day per tier.
    quota : dict
        ``date`` and, under ``by_key``, the OpenAlex downloads spent that day per key.
    counts : dict
        Papers per status, then per reason.
    paperpile_pending : bool
        The Paperpile step was skipped because the Drive login has expired.
    copy_pending : bool
        The GCS copy was skipped because its login has expired.
    """

    last_pass: dict[str, Any] = field(default_factory=dict)
    last_daily_success: str | None = None
    last_pmc_sync_success: str | None = None
    fetches: dict[str, Any] = field(default_factory=dict)
    quota: dict[str, Any] = field(default_factory=dict)
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    paperpile_pending: bool = False
    copy_pending: bool = False


def read_status(root: Path) -> PaperStatus:
    """Return the paper store's status; an empty one before the first pass.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.

    Returns
    -------
    PaperStatus
        The status; keys the file holds that ``PaperStatus`` lacks are ignored.
    """
    path = root / STATUS
    if not path.is_file():
        return PaperStatus()
    data = json.loads(path.read_text(encoding="utf-8"))
    names = {f.name for f in fields(PaperStatus)}
    return PaperStatus(**{k: v for k, v in data.items() if k in names})


def write_status(root: Path, status: PaperStatus) -> Path:
    """Write ``paper-status.json`` whole, through a temp file and a rename.

    Parameters
    ----------
    root : pathlib.Path
        The paper store root.
    status : PaperStatus
        The status to write.

    Returns
    -------
    pathlib.Path
        The file written.
    """
    path = root / STATUS
    write_atomic(path, json.dumps(asdict(status), indent=2, sort_keys=True) + "\n")
    return path
