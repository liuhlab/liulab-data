"""Drive the FASTQ-download DAG with Snakemake (the download's orchestration seam).

The per-run *stages* live in :mod:`labdata.geo.sratools`; this module turns a list
of ``(run, srx_dir)`` tasks into a Snakemake run of the packaged ``pipeline.smk``
workflow and reports which runs finished. Snakemake owns scheduling, DAG
resume, and — the point of the exercise — *resource-aware* concurrency: the
``prefetch`` rule declares ``resources: ncbi=1`` so ``--resources ncbi=<N>`` caps
concurrent NCBI network jobs, while ``--cores`` bounds total CPU across the
(local, CPU-bound) extraction/compression rules. The two therefore overlap: the
link keeps pulling new runs while cores chew through already-fetched ones.

Every ``snakemake`` invocation is funnelled through :func:`_run_snakemake`, the one
place a subprocess is launched here and the seam tests monkeypatch (mirroring
:func:`labdata.geo.sratools._run`).
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import shlex
import shutil
import sys
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import TYPE_CHECKING

from labdata import _pipeline
from labdata._pipeline import CONTROL_DIRNAME, RESILIENCE_FLAGS

if TYPE_CHECKING:
    from labdata.geo.sra_records import Run

logger = logging.getLogger(__name__)

#: Whole-pipeline success flag (under the control dir): written once every requested
#: run has finished. It is the durable, sole record of a completed download — once it
#: exists, Snakemake's internal state (``.snakemake``) is wiped and a rerun of the same
#: (or a subset) request short-circuits without invoking Snakemake at all.
SUCCESS_FILENAME = "success.json"


def _load_completed(output_root: Path) -> dict[str, str]:
    """Return the ``{run: srx}`` map the success flag records, or ``{}`` if absent."""
    return _pipeline.load_completed(output_root, SUCCESS_FILENAME)


def _write_success(output_root: Path, runs: dict[str, str]) -> None:
    """Write the success flag recording every completed ``{run: srx}``."""
    _pipeline.write_success(output_root, runs, SUCCESS_FILENAME)


def _snakefile() -> Path:
    """Return the filesystem path to the packaged ``pipeline.smk`` workflow."""
    return Path(str(importlib.resources.files("labdata.geo").joinpath("pipeline.smk")))


def _run_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Run ``snakemake`` with ``argv`` (the download's seam); return its exit code.

    A thin per-module alias for :func:`labdata._pipeline.run_snakemake` so this
    module's tests can monkeypatch ``pipeline._run_snakemake`` in place.
    """
    return _pipeline.run_snakemake(argv, verbose=verbose)


def run(
    tasks: list[tuple[Run, Path]],
    *,
    output_root: Path,
    ncbi_parallel: int,
    cores: int,
    threads_per_run: int,
    max_size: str,
    retries: int,
    backoff: float,
    keep_sra: bool,
    prefetch_only: bool,
    original: AbstractSet[str] = frozenset(),
    verbose: bool,
) -> dict[str, bool]:
    """Download ``(run, srx_dir)`` tasks by running the Snakemake DAG.

    Writes the config under ``<output_root>/.labdata`` and invokes the packaged
    ``pipeline.smk`` via :func:`_run_snakemake`. The DAG's data-file outputs (the
    ``.sra``/FASTQ, tracked with ``temp()``) drive resume and cleanup; results are
    read back from the per-experiment ``.<SRR>.done`` markers (or the prefetched
    ``.sra`` under ``prefetch_only``) so a run that failed under ``--keep-going`` is
    reported ``False`` without sinking the batch.

    Once every requested run finishes, a durable whole-pipeline success flag
    (``.labdata/success.json``) is written and Snakemake's internal ``.snakemake``
    state is wiped — the flag then becomes the sole record of completion, so a rerun
    of the same (or a subset) request short-circuits without invoking Snakemake.
    ``prefetch_only`` opts out of this (it is a deliberate partial step).

    Parameters
    ----------
    tasks : list of (Run, Path)
        Each run paired with the (already created) experiment directory it belongs
        in; each ``srx_dir`` must be a direct child of ``output_root``. An empty list
        is a no-op that returns ``{}``.
    output_root : Path
        The download's root (the Snakemake workdir); experiment dirs sit directly
        beneath it.
    ncbi_parallel : int
        Cap on concurrent ``prefetch`` jobs (``--resources ncbi=``).
    cores : int
        Total CPU cores the DAG may use (``--cores``).
    threads_per_run : int
        Threads handed to ``fasterq-dump``/``pigz`` per run.
    max_size : str
        Passed through to ``prefetch --max-size``.
    retries, backoff : int, float
        Retry policy for the ``prefetch`` stage (applied inside the stage itself).
    keep_sra : bool
        Keep each run's ``.sra`` next to its FASTQ.
    prefetch_only : bool
        Stop after ``prefetch`` (targets the ``.prefetched`` markers, skipping the
        extract/compress rules).
    original : set of str
        Run accessions to fetch in *original format* (download only, no
        extract/compress). These are routed to the Snakefile's ``original`` rule via
        a separate ``original_runs`` config map and produce a distinct
        ``.<SRR>.original.done`` marker; they are unaffected by ``prefetch_only``.
    verbose : bool
        Show Snakemake's progress.

    Returns
    -------
    dict[str, bool]
        Maps each run accession to whether its data is present on completion.

    Raises
    ------
    DownloadError
        If the ``snakemake`` executable is not installed.
    """
    if not tasks:
        return {}

    output_root = Path(output_root)
    # Map each run to its experiment dir as a single path component relative to the
    # workdir (output_root), so the Snakefile's ``{srx}`` wildcard and the data-file
    # outputs it temp()-tracks resolve cleanly.
    runs = {run_.accession: str(Path(srx_dir).relative_to(output_root)) for run_, srx_dir in tasks}

    # The whole-pipeline success flag is the durable record of a full download. When it
    # already covers every requested run, short-circuit — no Snakemake, no reliance on
    # its (possibly wiped) internal state. Otherwise only the not-yet-done runs are
    # handed to Snakemake; prefetch_only is a deliberate partial step, so it opts out.
    completed = {} if prefetch_only else _load_completed(output_root)
    to_run = {srr: srx for srr, srx in runs.items() if srr not in completed}
    if not to_run:
        return dict.fromkeys(runs, True)

    control_dir = output_root / CONTROL_DIRNAME
    control_dir.mkdir(parents=True, exist_ok=True)
    # Partition the not-yet-done runs by mode: default sra-tools runs drive the
    # prefetch/extract/compress chain; original-format runs drive the download-only
    # ``original`` rule. The two maps are disjoint.
    sra_runs = {srr: srx for srr, srx in to_run.items() if srr not in original}
    original_runs = {srr: srx for srr, srx in to_run.items() if srr in original}
    config = {
        # How the workflow shells back into labdata (env-independent: same Python).
        "labdata": f"{shlex.quote(sys.executable)} -m labdata",
        "runs": sra_runs,
        "original_runs": original_runs,
        "max_size": max_size,
        "retries": retries,
        "backoff": backoff,
        "threads_per_run": threads_per_run,
        "keep_sra": keep_sra,
        "prefetch_only": prefetch_only,
    }
    config_path = control_dir / "config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True))

    argv = [
        "--snakefile",
        str(_snakefile()),
        "--directory",
        str(output_root),
        "--cores",
        str(cores),
        "--resources",
        f"ncbi={ncbi_parallel}",
        "--configfile",
        str(config_path),
        # Resume on marker presence, recover interrupted jobs, keep a walltime/OOM-killed
        # job's partial .sra so prefetch resumes, keep going past one failed run, and take
        # no workdir lock (so an HPC kill leaves nothing to unlock). See RESILIENCE_FLAGS.
        *RESILIENCE_FLAGS,
    ]
    returncode = _run_snakemake(argv, verbose=verbose)
    if returncode != 0:
        logger.warning("snakemake exited %d — some runs may have failed", returncode)

    # Read results back from what the DAG produced: the original-format marker for
    # original runs, else the .done marker (full) or the prefetched .sra
    # (prefetch_only), all under <output_root>/<srx>/.
    def _present(srr: str, srx: str) -> bool:
        if srr in original:
            return (output_root / srx / f".{srr}.original.done").exists()
        if prefetch_only:
            return (output_root / srx / srr / f"{srr}.sra").exists()
        return (output_root / srx / f".{srr}.done").exists()

    ran = {srr: _present(srr, srx) for srr, srx in to_run.items()}
    # Runs already covered by the success flag are complete by definition.
    results = {srr: (True if srr in completed else ran[srr]) for srr in runs}

    # On a fully-successful full download, record the durable success flag and wipe
    # Snakemake's now-redundant internal state — the flag becomes the sole record.
    if not prefetch_only and all(results.values()):
        _write_success(output_root, {**completed, **runs})
        shutil.rmtree(output_root / ".snakemake", ignore_errors=True)
    return results
