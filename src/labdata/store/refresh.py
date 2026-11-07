"""Refresh the store with one resumable Snakemake run, and keep its status record.

The DAG lives in the packaged ``refresh.smk``; :func:`run` drives it through the full run's
Snakemake seam, :func:`labdata.store.pipeline._run_snakemake`. A refresh's markers, logs and
Snakemake state sit in ``work/refresh/<started>/``. The status record,
``refresh-status.json`` in the store root, names that folder until the refresh succeeds, so
a rerun resumes there.

The status record holds the start, the end, the outcome, the folder, the date the change
search starts from (``since``), the last successful refresh's date, the added, replaced and
removed records per source, and whether the GCS copy is pending. The date and the counts are
written by the step after the switch, the success outcome when the whole DAG has finished.
"""

from __future__ import annotations

import datetime as dt
import importlib.resources
import json
import logging
import shlex
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import labdata
from labdata._pipeline import RESILIENCE_FLAGS
from labdata.exceptions import LabdataError
from labdata.store import _limits, extract, gcs, pipeline, provenance
from labdata.store._files import write_atomic
from labdata.store.layout import manifest_path
from labdata.store.reader import build_path
from labdata.store.root import open_store

#: The status record's file name in the store root.
STATUS = "refresh-status.json"
#: Outcomes the status record names.
RUNNING, SUCCEEDED, FAILED = "running", "succeeded", "failed"
#: The file in a refresh's folder each pack appends its per-bucket counts to.
PACK_COUNTS = "pack-counts.jsonl"

logger = logging.getLogger(__name__)


class RefreshError(LabdataError):
    """A refresh cannot start: no date to search changes from, or no snapshot downloader."""


def memory_plan(total_mb: int) -> dict[str, int]:
    """Return each step's memory in MB, so the steps that can run at once fit in ``total_mb``.

    A snapshot extract runs beside the steps from the target list to pack and takes what the
    GEO lane and one other step leave; the build runs alone with all of it.
    """
    geo, lane = total_mb // 16, total_mb // 8
    plan = dict.fromkeys(
        ("snapshots", "targets", "changes", "eutils", "confirm", "reach", "pack"), lane
    )
    plan |= dict.fromkeys(("status", "prune", "copy"), lane)
    return plan | {"geo": geo, "extract": total_mb - geo - lane, "build": total_mb}


def read_status(root: Path) -> dict[str, Any]:
    """Return the status record, or an empty dict when the store was never refreshed."""
    path = root / STATUS
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def write_status(root: Path, status: dict[str, Any]) -> None:
    """Write the status record whole."""
    write_atomic(root / STATUS, json.dumps(status, indent=2, sort_keys=True) + "\n")


def downloader() -> Path:
    """Return the bulk snapshot downloader of this checkout; it is not in the wheel."""
    return Path(labdata.__file__).resolve().parents[2] / provenance.DOWNLOADER


def _snakefile() -> Path:
    return Path(str(importlib.resources.files("labdata.store").joinpath("refresh.smk")))


def _now() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    status = read_status(root)
    if status.get("outcome") in (RUNNING, FAILED) and status.get("folder"):
        logger.info("resuming the refresh started %s", status["started"])
    else:
        started = _now()
        status = {
            "started": started,
            "ended": None,
            "outcome": RUNNING,
            "folder": f"work/refresh/{started.replace(':', '')}",
            "since": since
            or status.get("last_success")
            or _first_since(root, store.settings.get("build")),
            "last_success": status.get("last_success"),
            "counts": {},
            "copy_pending": bool(status.get("copy_pending")),
        }
    work = root / status["folder"]
    script = downloader()
    if not (work / "snapshots.done").is_file() and not script.is_file():
        raise RefreshError(
            f"no snapshot downloader at {script}: run the refresh from a checkout of liulab-data"
        )
    status |= {"outcome": RUNNING, "ended": None}
    write_status(root, status)
    work.mkdir(parents=True, exist_ok=True)
    cores = cores or _limits.default_workers()
    memory = int(_limits.allocation_mb() * pipeline.STEPS_SHARE)
    config = {
        "labdata": f"{shlex.quote(sys.executable)} -m labdata",
        "python": shlex.quote(sys.executable),
        "downloader": str(script),
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
    status = read_status(root)
    status |= {"ended": _now(), "outcome": FAILED if code else SUCCEEDED}
    write_status(root, status)
    return code


def switched(root: Path) -> dict[str, Any]:
    """Record the running refresh's date and counts once its build is switched to.

    The copy is marked pending when the store names a destination.

    Returns
    -------
    dict
        The status record written.

    Raises
    ------
    RefreshError
        If no refresh is running.
    """
    store = open_store(root)
    status = read_status(store.root)
    if status.get("outcome") not in (RUNNING, FAILED):
        raise RefreshError(f"no refresh is running: {store.root / STATUS} says so")
    counts: dict[str, dict[str, int]] = defaultdict(lambda: dict.fromkeys(_COUNTS, 0))
    path = store.root / status["folder"] / PACK_COUNTS
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    for line in lines:
        row = json.loads(line)
        for key in _COUNTS:
            counts[row["source"]][key] += row[key]
    status |= {
        "last_success": status["started"][:10],
        "counts": dict(sorted(counts.items())),
        "copy_pending": bool(store.settings.get(gcs.COPY_TO_KEY)),
    }
    write_status(store.root, status)
    return status


_COUNTS = ("added", "replaced", "removed")


def note_copy(root: Path, *, pending: bool) -> None:
    """Mark the GCS copy pending or done in the status record, if the store has one."""
    status = read_status(root)
    if status:
        write_status(root, status | {"copy_pending": pending})
