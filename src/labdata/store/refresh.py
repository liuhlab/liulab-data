"""Refresh the store with one resumable Snakemake run.

The DAG lives in the packaged ``refresh.smk``; :func:`run` drives it through the full run's
Snakemake seam, :func:`labdata.store.pipeline._run_snakemake`. A refresh's markers, logs and
Snakemake state sit in ``work/refresh/<started>/``. The status record
(:mod:`labdata.store.lifecycle`) names that folder until the refresh is finalized, so a rerun
resumes there: at the copy, when only the copy is left.
"""

from __future__ import annotations

import datetime as dt
import importlib.resources
import json
import logging
import shlex
import sys
from pathlib import Path

import labdata
from labdata._pipeline import RESILIENCE_FLAGS
from labdata.exceptions import LabdataError
from labdata.store import _limits, extract, gcs, lifecycle, paths, pipeline, provenance
from labdata.store.layout import manifest_path
from labdata.store.reader import build_path
from labdata.store.root import open_store

#: The version of ``refresh.smk``'s markers; raise it when a marker changes meaning, so an
#: unfinished refresh an older DAG opened is left, not resumed.
DAG = 4

logger = logging.getLogger(__name__)


class RefreshError(LabdataError):
    """A refresh cannot start: no date to search changes from, or no snapshot downloader."""


def memory_plan(total_mb: int) -> dict[str, int]:
    """Return each step's memory in MB, so the steps that can run at once fit in ``total_mb``.

    A snapshot extract runs beside the steps from the target list to pack and takes what the
    GEO lane and one other step leave; the build and the switch's check run alone with all
    of it.
    """
    geo, lane = total_mb // 16, total_mb // 8
    plan = dict.fromkeys(
        ("snapshots", "geo_series", "targets", "changes", "eutils", "confirm", "pack"), lane
    )
    plan |= dict.fromkeys(("prune", "copy"), lane)
    return plan | {
        "geo": geo,
        "extract": total_mb - geo - lane,
        "build": total_mb,
        "switch": total_mb,
    }


def downloader() -> Path:
    """Return the bulk snapshot downloader of this checkout; it is not in the wheel."""
    return Path(labdata.__file__).resolve().parents[2] / provenance.DOWNLOADER


def _snakefile() -> Path:
    return Path(str(importlib.resources.files("labdata.store").joinpath("refresh.smk")))


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
) -> int:
    """Run a refresh, or resume the unfinished one the status record names.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    since : str or None
        The date to search GEO's changes from, ``YYYY-MM-DD``, less the overlap. Defaults
        to the last successful refresh's date, then the ``geo-series`` snapshot date of the
        current build. A resumed refresh keeps its own.
    cores : int or None
        Cores the steps may use; defaults to :func:`labdata.store._limits.default_workers`.
    ncbi : int
        Network steps that may run at once.
    verbose : bool
        Show Snakemake's and each step's output.

    Returns
    -------
    int
        Snakemake's exit code: ``0`` when every step finished.

    Raises
    ------
    RefreshError
        If there is no date to search from, or a new refresh finds no snapshot downloader.
    """
    store = open_store(root)
    root = store.root
    status = lifecycle.read_status(root)
    unfinished = lifecycle.unfinished(status, lifecycle.REFRESH)
    if unfinished and status.get("dag", 1) != DAG:
        logger.warning(
            "an older refresh DAG opened the unfinished refresh, so it is left unfinished: %s",
            status["folder"],
        )
        since = since or status["since"]
        unfinished = False
    if unfinished:
        logger.info("resuming the refresh started %s", status["started"])
        if since is not None and since != status["since"]:
            logger.warning(
                "--since %s is not used: the unfinished refresh keeps %s", since, status["since"]
            )
        folder, fields = status["folder"], {}
    else:
        started = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        folder = paths.REFRESHES.rel + started.replace(":", "")
        fields = {
            "started": started,
            "since": since
            or status.get("last_success")
            or _first_since(root, store.settings.get(paths.BUILD_KEY)),
            "counts": {},
            "dag": DAG,
        }
    work = root / folder
    script = downloader()
    if not (work / "snapshots.done").is_file() and not script.is_file():
        raise RefreshError(
            f"no snapshot downloader at {script}: run the refresh from a checkout of liulab-data"
        )
    status = lifecycle.start(root, lifecycle.REFRESH, folder, **fields)
    work.mkdir(parents=True, exist_ok=True)
    cores = cores or _limits.default_workers()
    memory = int(_limits.allocation_mb() * pipeline.STEPS_SHARE)
    config = {
        "labdata": f"{shlex.quote(sys.executable)} -m labdata",
        "python": shlex.quote(sys.executable),
        "downloader": str(script),
        "snapshots": str(paths.SNAPSHOTS.at(root)),
        "root": str(root),
        "date": status["started"][:10],
        "since": status["since"],
        "extracts": list(extract.SOURCES),
        "copy": bool(store.settings.get(gcs.COPY_TO_KEY)),
        "mem_mb": memory_plan(memory),
    }
    config_path = work / "config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    argv = [
        "--snakefile",
        str(_snakefile()),
        "--directory",
        str(work),
        "--cores",
        str(cores),
        "--resources",
        f"ncbi={ncbi}",
        f"mem_mb={memory}",
        "--configfile",
        str(config_path),
        "--printshellcmds",
        *RESILIENCE_FLAGS,
    ]
    code = pipeline._run_snakemake(argv, verbose=verbose)
    lifecycle.end(root)
    return code
