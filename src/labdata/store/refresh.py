"""Refresh the store with one resumable Snakemake run.

The DAG lives in the packaged ``refresh.smk``; :func:`run` hands it to the one driver,
:func:`labdata.store.pipeline.drive`. A refresh's markers, logs and Snakemake state sit in
``work/refresh/<started>/``. The status record (:mod:`labdata.store.lifecycle`) names that
folder until the refresh is finalized, and a rerun goes on as
:func:`labdata.store.lifecycle.plan` decides.
"""

from __future__ import annotations

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

#: The version of ``refresh.smk``'s markers; raise it when a marker changes meaning, so a
#: refresh an older DAG opened is not resumed before its switch.
DAG = 4

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
    """Run a refresh, or go on with the unfinished one the status record names.

    One stopped before its switch is resumed when it started within
    :data:`labdata.store.lifecycle.RESUME_WITHIN` by this refresh DAG; otherwise it is
    abandoned, prune removes its folder, and a new refresh starts in a new folder. One that
    switched with its copy pending gets the copy alone.

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
        lambda status: _round(root, status, since),
        snakefile="refresh.smk",
        cores=cores,
        ncbi=ncbi,
        verbose=verbose,
        owner_gone=owner_gone,
    )


def _round(root: Path, status: dict[str, Any], since: str | None) -> pipeline.Round:
    """Set up the refresh :func:`lifecycle.plan` decides on: its folder, record and config."""
    decided = lifecycle.plan(status, lifecycle.REFRESH, dag=DAG)
    if decided.abandoned:
        logger.warning(
            "the unfinished refresh in %s is not resumed: %s", status["folder"], decided.abandoned
        )
        since = since or status["since"]
    if decided.action == lifecycle.Action.COPY:
        return pipeline.Round(root / str(decided.folder), decided)
    if decided.action == lifecycle.Action.RESUME:
        logger.info("resuming the refresh started %s", status["started"])
        if since is not None and since != status["since"]:
            logger.warning(
                "--since %s is not used: the unfinished refresh keeps %s", since, status["since"]
            )
        started, since, fields = status["started"], status["since"], {}
        work = root / str(decided.folder)
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
    return pipeline.Round(work, decided, fields, config)
