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

import importlib.resources
import json
import logging
import shlex
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from labdata import _gcs, _pipeline
from labdata._files import utc_stamp
from labdata._pipeline import RESILIENCE_FLAGS
from labdata.store import _limits, extract, lifecycle, paths, snapshots, targets
from labdata.store.root import open_store

logger = logging.getLogger(__name__)

#: Network steps that may run at once: the GEO lane and the E-utilities lane.
NCBI_STEPS = 2

#: The share of the job's memory the steps may hold at once; the rest is for Snakemake.
STEPS_SHARE = 0.9

#: The lanes whose ``<lane>.failed`` file sends them back to work on the next run, each with
#: the DAG rule that writes that file.
LANES = {"geo": "geo", "eutils": "eutils_after_geo"}

#: The marker of every store DAG's last step.
DONE = "copy.done"


@dataclass(frozen=True, slots=True)
class Round:
    """One start of a DAG, as its caller sets it up from :func:`lifecycle.plan`.

    Parameters
    ----------
    folder : pathlib.Path
        The run's folder: its markers, logs, config and Snakemake state.
    plan : lifecycle.Plan
        What the driver does.
    fields : dict
        The status record of a new update, beside what :func:`lifecycle.start` writes.
    config : dict
        The DAG's own config, beside what every DAG gets.
    rerun : list of str
        Rules to run again, with every rule after them.
    """

    folder: Path
    plan: lifecycle.Plan
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
    set_up: Callable[[dict[str, Any]], Round],
    *,
    snakefile: str,
    cores: int | None = None,
    ncbi: int = NCBI_STEPS,
    verbose: bool = True,
    owner_gone: bool = False,
) -> int:
    """Run a store DAG under the run lock: the one driver of the full run and the refresh.

    Under the lock, ``set_up`` reads the status record and sets up the run by
    :func:`lifecycle.plan`, which the driver carries out. To resume or start, it writes the
    config, with the memory plan, runs Snakemake and records how the run stopped. A run that
    has switched builds with its copy pending gets the copy alone, as ``labdata store copy``
    does it. A run with nothing to do runs nothing and leaves the status record as it was.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    update : str
        :data:`lifecycle.RUN` or :data:`lifecycle.REFRESH`.
    set_up : callable
        Takes the status record and returns the :class:`Round`.
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
        Snakemake's exit code, or ``1`` for a copy skipped: ``0`` when every step finished.

    Raises
    ------
    StoreBusyError
        If another update holds the run lock.
    StoreCopyError
        As :func:`lifecycle.finalize` raises it, for a run left only its copy.
    """
    with lifecycle.run_lock(root, update, owner_gone=owner_gone):
        chosen = set_up(lifecycle.read_status(root))
        work = chosen.folder
        if chosen.plan.action == lifecycle.Action.NOTHING:
            logger.warning(
                "the run in %s is done or finalized, so nothing runs; to run it "
                "again, delete that folder",
                work,
            )
            return 0
        if chosen.plan.action == lifecycle.Action.COPY:
            logger.info("the run in %s has switched builds, so only its copy is left", work)
            result = lifecycle.finalize(root)
            if result.skipped:
                logger.warning("%s", result.message)
            return int(result.skipped)
        folder = work.relative_to(root).as_posix()
        lifecycle.start(root, update, folder, running=chosen.plan.running, **chosen.fields)
        work.mkdir(parents=True, exist_ok=True)
        memory = int(_limits.allocation_mb() * STEPS_SHARE)
        config = {
            "labdata": f"{shlex.quote(sys.executable)} -m labdata",
            "root": str(root),
            "extracts": list(extract.SOURCES),
            "copy": bool(paths.read_marker(root).get(_gcs.COPY_TO_KEY)),
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
    and so does every step after it. A rerun of a round left only its copy copies, and one of
    a finished or finalized round runs nothing.

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

    def set_up(status: dict[str, Any]) -> Round:
        # --forcerun reruns a rule and every rule after it, even where their markers exist.
        failed = [lane for lane in LANES if (work / f"{lane}.failed").is_file()]
        rerun = [LANES[lane] for lane in failed]
        config = {
            "fraction": "" if fraction is None else repr(fraction),
            "snapshots": dates,
            "targets": str(targets.list_path(root, geo_date, fraction)),
        }
        folder = work.relative_to(root).as_posix()
        done = (work / DONE).is_file()
        decided = lifecycle.plan(status, lifecycle.RUN, folder, done=done, rerun=bool(rerun))
        chosen = Round(work, decided, config=config, rerun=rerun)
        # Only the copy is left: a failed lane waits for the run after it.
        copy = decided.action == lifecycle.Action.COPY
        for lane in failed:
            logger.log(
                logging.WARNING if copy else logging.INFO,
                "the %s lane left items failed; it runs again%s",
                lane,
                " on the next run" if copy else "",
            )
        if decided.action in (lifecycle.Action.RESUME, lifecycle.Action.NEW):
            work.mkdir(parents=True, exist_ok=True)
            record = {"fraction": fraction, "snapshots": dates, "started": utc_stamp()}
            (work / "run.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
            logger.info("round %s: snapshots %s", work.name, dates)
        return chosen

    return drive(
        root,
        lifecycle.RUN,
        set_up,
        snakefile="pipeline.smk",
        cores=cores,
        ncbi=ncbi,
        verbose=verbose,
        owner_gone=owner_gone,
    )
