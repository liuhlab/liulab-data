"""Drive one round of the full run with Snakemake: fetch, pack, extract, build, prune, copy.

The DAG lives in the packaged ``pipeline.smk``. Each step is a ``labdata store`` subcommand,
so labdata owns what a step does and Snakemake owns the order and the resume: a step's
marker appears only when the step exits cleanly, and a rerun starts at the first step
    without one. An ``ncbi`` resource caps the network steps apart from ``--cores``, and a
``mem_mb`` resource keeps the steps that run at once inside the job's memory.

A round's markers, logs and Snakemake state sit in ``<root>/work/run/<round>/``, where the
round names the ``geo-series`` snapshot date and the sample fraction, e.g.
``2026-10-14-f0.01``. Every ``snakemake`` call goes through :func:`_run_snakemake`, the seam
tests replace.
"""

from __future__ import annotations

import datetime as dt
import importlib.resources
import json
import logging
import shlex
import sys
from pathlib import Path

from labdata import _pipeline
from labdata._pipeline import RESILIENCE_FLAGS
from labdata.store import extract, gcs, snapshots, targets
from labdata.store.root import open_store

logger = logging.getLogger(__name__)

#: Network steps that may run at once: the GEO lane and the E-utilities lane.
NCBI_STEPS = 2

#: The share of the job's memory the steps may hold at once; the rest is for Snakemake.
STEPS_SHARE = 0.9

#: The lanes whose ``<lane>.failed`` file sends them back to work on the next run, each with
#: the DAG rule that writes that file.
LANES = {"geo": "geo", "eutils": "eutils_after_geo"}


def memory_plan(total_mb: int) -> dict[str, int]:
    """Return each step's memory in MB, so the steps that can run at once fit in ``total_mb``.

    The fetch lanes, the target list and pack run beside a snapshot extract and take a small
    share each; the extract takes the rest. The build and coverage run alone with all of it.
    """
    geo, lane = total_mb // 16, total_mb // 8
    return {
        "targets": lane,
        "geo": geo,
        "eutils": lane,
        "pack": lane,
        "extract": total_mb - geo - lane,
        "build": total_mb,
        "coverage": total_mb,
        "prune": lane,
        "copy": lane,
    }


def round_dir(root: Path, date: str, fraction: float | None) -> Path:
    """Return the folder of one round's markers, logs and Snakemake state."""
    return root / "work" / "run" / f"{date}-{targets.fraction_name(fraction)}"


def _snakefile() -> Path:
    return Path(str(importlib.resources.files("labdata.store").joinpath("pipeline.smk")))


def _run_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Run ``snakemake`` with ``argv``; the seam tests replace."""
    return _pipeline.run_snakemake(argv, verbose=verbose)


def run(
    root: Path,
    *,
    fraction: float | None = None,
    cores: int | None = None,
    ncbi: int = NCBI_STEPS,
    verbose: bool = True,
) -> int:
    """Run one round: the seeded sample of ``fraction`` of the target list, or all of it.

    The round uses the newest snapshot of every source and records their dates in
    ``run.json`` in its folder. A rerun of the same round resumes at the first unfinished
    step; the fetch steps skip every item already saved, by this round or a smaller one. A
    fetch lane that left items failed runs again, and so does every step after it.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    fraction : float or None
        The round's sample of the target list, in (0, 1]; ``None`` or ``1`` for all of it.
    cores : int or None
        Cores the steps may use; defaults to :func:`labdata.store.extract.default_workers`.
    ncbi : int
        Network steps that may run at once.
    verbose : bool
        Show Snakemake's and each step's output.

    Returns
    -------
    int
        Snakemake's exit code: ``0`` when every step finished, coverage included.

    Raises
    ------
    StoreBuildError
        If a snapshot source has no snapshot.
    ValueError
        If the fraction is not in (0, 1].
    """
    fraction = targets.check_fraction(fraction)
    store = open_store(root)
    root = store.root
    dates = {source: snapshots.snapshot(root, source).date for source in extract.SOURCES}
    geo_date = dates[snapshots.GEO_SERIES]
    work = round_dir(root, geo_date, fraction)
    work.mkdir(parents=True, exist_ok=True)
    # --forcerun reruns a rule and every rule after it, even where their markers exist.
    rerun = []
    for lane, rule in LANES.items():
        if (work / f"{lane}.failed").is_file():
            logger.info("the %s lane left items failed last time; it runs again", lane)
            rerun.append(rule)
    cores = cores or extract.default_workers()
    memory = int(extract.allocation_mb() * STEPS_SHARE)
    labdata = f"{shlex.quote(sys.executable)} -m labdata"
    config = {
        "labdata": labdata,
        "root": str(root),
        "fraction": "" if fraction is None else repr(fraction),
        "snapshots": dates,
        "targets": str(targets.list_path(root, geo_date, fraction)),
        "copy": bool(store.settings.get(gcs.COPY_TO_KEY)),
        "mem_mb": memory_plan(memory),
    }
    config_path = work / "config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    started = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = {"fraction": fraction, "snapshots": dates, "started": started}
    (work / "run.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    logger.info("round %s: snapshots %s", work.name, dates)
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
        *(["--forcerun", *rerun] if rerun else []),
    ]
    return _run_snakemake(argv, verbose=verbose)
