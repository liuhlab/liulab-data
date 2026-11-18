"""Refresh the store with one resumable Snakemake run.

The DAG lives in the packaged ``refresh.smk``; :func:`run` hands it to the one driver,
:func:`labdata.store.pipeline.drive`. A refresh's markers, logs and Snakemake state sit in
``work/refresh/<started>/``. The status record (:mod:`labdata.store.lifecycle`) names that
folder until the refresh is finalized, so a rerun within :data:`RESUME_WITHIN` of its start
resumes there: at the copy, when only the copy is left. An older one is not resumed.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import shlex
import sys
from pathlib import Path
from typing import Any

import labdata
from labdata._files import utc_stamp
from labdata.exceptions import LabdataError
from labdata.store import lifecycle, paths, pipeline, provenance
from labdata.store.layout import manifest_path
from labdata.store.reader import build_path
from labdata.store.root import open_store

#: The version of ``refresh.smk``'s markers; raise it when a marker changes meaning, so an
#: unfinished refresh an older DAG opened is left, not resumed.
DAG = 4

#: An unfinished refresh is resumed only within this time of its start.
RESUME_WITHIN = dt.timedelta(days=7)

logger = logging.getLogger(__name__)


class RefreshError(LabdataError):
    """A refresh cannot start: no date to search changes from, or no snapshot downloader."""


def downloader() -> Path:
    """Return the bulk snapshot downloader of this checkout; it is not in the wheel."""
    return Path(labdata.__file__).resolve().parents[2] / provenance.DOWNLOADER


def _first_since(root: Path, build: object) -> str:
    """Return the ``geo-series`` snapshot date the current build read."""
    manifest = manifest_path(build_path(root, str(build))) if build else None
    if manifest is None or not manifest.is_file():
        raise RefreshError(
            "no refresh has succeeded and the store names no build, so there is no date to "
            "search GEO's changes from; pass --since YYYY-MM-DD"
        )
    data = json.loads(manifest.read_text(encoding="utf-8"))
    return str(data["inputs"]["snapshots"]["geo-series"]["date"])


def run(
    root: Path,
    *,
    since: str | None = None,
    cores: int | None = None,
    ncbi: int = pipeline.NCBI_STEPS,
    verbose: bool = True,
    owner_gone: bool = False,
) -> int:
    """Run a refresh, or resume the unfinished one the status record names.

    The unfinished refresh is resumed when it started within :data:`RESUME_WITHIN` and an
    older refresh DAG did not open it. Otherwise a new refresh starts in a new folder; one that
    stopped before its switch is abandoned, and prune removes its folder.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    since : str or None
        The date to search GEO's changes from, ``YYYY-MM-DD``, less the overlap. Defaults
        to the last successful refresh's date, then the ``geo-series`` snapshot date of the
        current build. A resumed refresh keeps its own.
    cores, ncbi, verbose, owner_gone
        As for :func:`labdata.store.pipeline.drive`.

    Returns
    -------
    int
        Snakemake's exit code: ``0`` when every step finished.

    Raises
    ------
    RefreshError
        If there is no date to search from, or a new refresh finds no snapshot downloader.
    StoreBusyError
        If another update holds the run lock.
    """
    root = open_store(root).root
    return pipeline.drive(
        root,
        lifecycle.REFRESH,
        lambda status: _plan(root, status, since),
        snakefile="refresh.smk",
        cores=cores,
        ncbi=ncbi,
        verbose=verbose,
        owner_gone=owner_gone,
    )


def _plan(root: Path, status: dict[str, Any], since: str | None) -> pipeline.Round:
    """Resume the unfinished refresh the status record names, or plan a new one."""
    unfinished = lifecycle.unfinished(status, lifecycle.REFRESH)
    left = None
    if unfinished and status.get("dag", 1) != DAG:
        left = "an older refresh DAG opened it"
    elif unfinished and _age(status["started"]) > RESUME_WITHIN:
        left = f"it started more than {RESUME_WITHIN.days} days ago"
    if left:
        logger.warning("the unfinished refresh in %s is not resumed: %s", status["folder"], left)
        if status["outcome"] != lifecycle.BUILT:
            since = since or status["since"]
        unfinished = False
    if unfinished:
        logger.info("resuming the refresh started %s", status["started"])
        if since is not None and since != status["since"]:
            logger.warning(
                "--since %s is not used: the unfinished refresh keeps %s", since, status["since"]
            )
        started, since, fields = status["started"], status["since"], {}
        work = root / status["folder"]
    else:
        started = utc_stamp()
        since = (
            since
            or status.get("last_success")
            or _first_since(root, paths.read_marker(root).get(paths.BUILD_KEY))
        )
        fields = {"started": started, "since": since, "counts": {}, "dag": DAG}
        work = paths.REFRESHES.at(root, started.replace(":", ""))
    script = downloader()
    if not (work / "snapshots.done").is_file() and not script.is_file():
        raise RefreshError(
            f"no snapshot downloader at {script}: run the refresh from a checkout of liulab-data"
        )
    config = {
        "python": shlex.quote(sys.executable),
        "downloader": str(script),
        "snapshots": str(paths.SNAPSHOTS.at(root)),
        "date": started[:10],
        "since": since,
    }
    return pipeline.Round(work, fields, config)


def _age(started: str) -> dt.timedelta:
    return dt.datetime.now(dt.UTC) - dt.datetime.fromisoformat(started)
