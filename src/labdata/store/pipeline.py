"""The one driver of the store's DAGs, and one round of the full run, from the fetch to the copy.

:func:`drive` runs the full run's DAG (``pipeline.smk``) and the refresh's (``refresh.smk``),
which end with the same closing steps (``closing.smk``). Each step is a ``labdata store``
subcommand, so labdata owns what a step does and Snakemake owns the order and the resume: a
step's marker appears only when the step exits cleanly, and a rerun starts at the first step
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
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from labdata import _pipeline
from labdata._pipeline import RESILIENCE_FLAGS
from labdata.store import _limits, extract, gcs, lifecycle, paths, snapshots, targets
from labdata.store.root import open_store

logger = logging.getLogger(__name__)

#: Network steps that may run at once: the GEO lane and the E-utilities lane.
NCBI_STEPS = 2

#: The share of the job's memory the steps may hold at once; the rest is for Snakemake.
STEPS_SHARE = 0.9

#: The lanes whose ``<lane>.failed`` file sends them back to work on the next run, each with
#: the DAG rule that writes that file.
LANES = {"geo": "geo", "eutils": "eutils_after_geo"}


@dataclass(frozen=True, slots=True)
class Round:
    """One start of a DAG, as its caller plans it from the status record.

    Parameters
    ----------
    folder : pathlib.Path
        The run's folder: its markers, logs, config and Snakemake state.
    fields : dict
        The status record of a new update, beside what :func:`lifecycle.start` writes.
    config : dict
        The DAG's own config, beside what every DAG gets.
    rerun : list of str
        Rules to run again, with every rule after them.
    """

    folder: Path
    fields: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    rerun: list[str] = field(default_factory=list)


def memory_plan(total_mb: int) -> dict[str, int]:
    """Return each step's memory in MB, so the steps that can run at once fit in ``total_mb``.

    The download, fetch and pack steps run beside a snapshot extract and take a small share
    each; the extract takes the rest. The build and the switch's check run alone with all of it.
    """
    geo, lane = total_mb // 16, total_mb // 8
    steps = ("snapshots", "geo_series", "targets", "changes", "eutils", "confirm", "pack")
    return dict.fromkeys((*steps, "prune", "copy"), lane) | {
        "geo": geo,
        "extract": total_mb - geo - lane,
        "build": total_mb,
        "switch": total_mb,
    }


def round_dir(root: Path, date: str, fraction: float | None) -> Path:
    """Return the folder of one round's markers, logs and Snakemake state."""
    return paths.RUNS.at(root, f"{date}-{targets.fraction_name(fraction)}")


def _snakefile(name: str) -> Path:
    return Path(str(importlib.resources.files("labdata.store").joinpath(name)))


def _run_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Run ``snakemake`` with ``argv``; the seam tests replace."""
    return _pipeline.run_snakemake(argv, verbose=verbose)


def drive(
    root: Path,
    update: str,
    plan: Callable[[dict[str, Any]], Round],
    *,
    snakefile: str,
    cores: int | None = None,
    ncbi: int = NCBI_STEPS,
    verbose: bool = True,
    owner_gone: bool = False,
) -> int:
    """Run a store DAG under the run lock: the one driver of the full run and the refresh.

    Under the lock, ``plan`` reads the status record and names the run's folder; the update
    starts there, or carries on. The driver writes the config, with the memory plan, runs
    Snakemake and records how the run stopped.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    update : str
        :data:`lifecycle.RUN` or :data:`lifecycle.REFRESH`.
    plan : callable
        Takes the status record and returns the :class:`Round` to run.
    snakefile : str
        The DAG's file in this package.
    cores : int or None
        Cores the steps may use; defaults to :func:`labdata.store._limits.default_workers`.
    ncbi : int
        Network steps that may run at once.
    verbose : bool
        Show Snakemake's and each step's output.
    owner_gone : bool
        The run lock's owner has ended, though this host cannot check it.

    Returns
    -------
    int
        Snakemake's exit code: ``0`` when every step finished.

    Raises
    ------
    StoreBusyError
        If another update holds the run lock.
    """
    with lifecycle.run_lock(root, update, owner_gone=owner_gone):
        chosen = plan(lifecycle.read_status(root))
        work = chosen.folder
        lifecycle.start(root, update, work.relative_to(root).as_posix(), **chosen.fields)
        work.mkdir(parents=True, exist_ok=True)
        memory = int(_limits.allocation_mb() * STEPS_SHARE)
        config = {
            "labdata": f"{shlex.quote(sys.executable)} -m labdata",
            "root": str(root),
            "extracts": list(extract.SOURCES),
            "copy": bool(paths.read_marker(root).get(gcs.COPY_TO_KEY)),
            "mem_mb": memory_plan(memory),
            **chosen.config,
        }
        config_path = work / "config.json"
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        argv = [
            "--snakefile",
            str(_snakefile(snakefile)),
            "--directory",
            str(work),
            "--cores",
            str(cores or _limits.default_workers()),
            "--resources",
            f"ncbi={ncbi}",
            f"mem_mb={memory}",
            "--configfile",
            str(config_path),
            "--printshellcmds",
            *RESILIENCE_FLAGS,
            *(["--forcerun", *chosen.rerun] if chosen.rerun else []),
        ]
        try:
            return _run_snakemake(argv, verbose=verbose)
        finally:
            lifecycle.end(root)


def run(
    root: Path,
    *,
    fraction: float | None = None,
    cores: int | None = None,
    ncbi: int = NCBI_STEPS,
    verbose: bool = True,
    owner_gone: bool = False,
) -> int:
    """Run one round: the seeded sample of ``fraction`` of the target list, or all of it.

    The round uses the newest snapshot of every source and records their dates in
    ``run.json`` in its folder, and the round in the status record. A rerun of the same
    round resumes at the first unfinished step; the fetch steps skip every item already
    saved, by this round or a smaller one. A fetch lane that left items failed runs again,
    and so does every step after it.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    fraction : float or None
        The round's sample of the target list, in (0, 1]; ``None`` or ``1`` for all of it.
    cores, ncbi, verbose, owner_gone
        As for :func:`drive`.

    Returns
    -------
    int
        Snakemake's exit code: ``0`` when every step finished, the coverage check included.

    Raises
    ------
    StoreBuildError
        If a snapshot source has no snapshot, or another update holds the run lock.
    ValueError
        If the fraction is not in (0, 1].
    """
    fraction = targets.check_fraction(fraction)
    root = open_store(root).root
    dates = {source: snapshots.snapshot(root, source).date for source in extract.SOURCES}
    geo_date = dates[snapshots.GEO_SERIES]
    work = round_dir(root, geo_date, fraction)

    def plan(_status: dict[str, Any]) -> Round:
        # --forcerun reruns a rule and every rule after it, even where their markers exist.
        rerun = []
        for lane, rule in LANES.items():
            if (work / f"{lane}.failed").is_file():
                logger.info("the %s lane left items failed last time; it runs again", lane)
                rerun.append(rule)
        work.mkdir(parents=True, exist_ok=True)
        started = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        record = {"fraction": fraction, "snapshots": dates, "started": started}
        (work / "run.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        logger.info("round %s: snapshots %s", work.name, dates)
        config = {
            "fraction": "" if fraction is None else repr(fraction),
            "snapshots": dates,
            "targets": str(targets.list_path(root, geo_date, fraction)),
        }
        return Round(work, config=config, rerun=rerun)

    return drive(
        root,
        lifecycle.RUN,
        plan,
        snakefile="pipeline.smk",
        cores=cores,
        ncbi=ncbi,
        verbose=verbose,
        owner_gone=owner_gone,
    )
