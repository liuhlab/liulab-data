"""Drive the cellranger-BAM → FASTQ DAG with Snakemake (the conversion's seam).

The per-run *stage* lives in :mod:`labdata.tenx.bamtofastq`; this module turns a list
of ``(run, srx_dir)`` tasks into a Snakemake run of the packaged ``pipeline.smk``
workflow and reports which runs finished. Snakemake owns scheduling, DAG resume, and
concurrency — ``--cores`` bounds total CPU across the (local, CPU-bound) ``bamtofastq``
jobs. Unlike the download DAG (:mod:`labdata.geo.pipeline`) there is no network step,
so no ``ncbi`` resource is declared.

Every ``snakemake`` invocation is funnelled through :func:`_run_snakemake`, the one
place a subprocess is launched here and the seam tests monkeypatch (mirroring
:func:`labdata.geo.pipeline._run_snakemake`).
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import shlex
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from labdata import _pipeline
from labdata._pipeline import CONTROL_DIRNAME, RESILIENCE_FLAGS

if TYPE_CHECKING:
    from labdata.geo.sra_records import Run

logger = logging.getLogger(__name__)

#: Whole-pipeline success flag (under the control dir): written once every requested
#: run has finished. Once it exists, Snakemake's internal state (``.snakemake``) is
#: wiped and a rerun of the same (or a subset) request short-circuits without invoking
#: Snakemake at all.
SUCCESS_FILENAME = "tenx_success.json"


def _load_completed(output_root: Path) -> dict[str, str]:
    """Return the ``{run: srx}`` map the success flag records, or ``{}`` if absent."""
    return _pipeline.load_completed(output_root, SUCCESS_FILENAME)


def _write_success(output_root: Path, runs: dict[str, str]) -> None:
    """Write the success flag recording every completed ``{run: srx}``."""
    _pipeline.write_success(output_root, runs, SUCCESS_FILENAME)


def _snakefile() -> Path:
    """Return the filesystem path to the packaged ``pipeline.smk`` workflow."""
    return Path(str(importlib.resources.files("labdata.tenx").joinpath("pipeline.smk")))


def _run_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Run ``snakemake`` with ``argv`` (the conversion's seam); return its exit code.

    A thin per-module alias for :func:`labdata._pipeline.run_snakemake` so this
    module's tests can monkeypatch ``pipeline._run_snakemake`` in place.
    """
    return _pipeline.run_snakemake(argv, verbose=verbose)


def run(
    tasks: list[tuple[Run, Path]],
    *,
    output_root: Path,
    cores: int,
    threads_per_run: int,
    reads_per_fastq: int,
    remove_bam: bool = False,
    verbose: bool,
) -> dict[str, bool]:
    """Convert ``(run, srx_dir)`` tasks to FASTQ by running the Snakemake DAG.

    Writes the config under ``<output_root>/.labdata`` and invokes the packaged
    ``pipeline.smk`` via :func:`_run_snakemake`. The DAG's per-run ``.<SRR>.tenx.done``
    marker drives resume; results are read back from those markers so a run that failed
    under ``--keep-going`` is reported ``False`` without sinking the batch.

    Once every requested run finishes, a durable whole-pipeline success flag
    (``.labdata/tenx_success.json``) is written and Snakemake's internal ``.snakemake``
    state is wiped — the flag then becomes the sole record of completion, so a rerun of
    the same (or a subset) request short-circuits without invoking Snakemake.

    Parameters
    ----------
    tasks : list of (Run, Path)
        Each run paired with the (already created) experiment directory it belongs in;
        each ``srx_dir`` must be a direct child of ``output_root``. An empty list is a
        no-op that returns ``{}``.
    output_root : Path
        The conversion's root (the Snakemake workdir); experiment dirs sit directly
        beneath it.
    cores : int
        Total CPU cores the DAG may use (``--cores``).
    threads_per_run : int
        Threads handed to ``bamtofastq`` (``--nthreads``) per run.
    reads_per_fastq : int
        Reads per output FASTQ chunk (``bamtofastq --reads-per-fastq``).
    remove_bam : bool, default False
        Delete each run's source BAM after its FASTQ are produced (reclaims the large
        original once it is no longer needed).
    verbose : bool
        Show Snakemake's progress.

    Returns
    -------
    dict[str, bool]
        Maps each run accession to whether its FASTQ are present on completion.

    Raises
    ------
    DownloadError
        If the ``snakemake`` executable is not installed.
    """
    if not tasks:
        return {}

    output_root = Path(output_root)
    # Map each run to its experiment dir as a single path component relative to the
    # workdir (output_root), so the Snakefile's ``{srx}`` wildcard resolves cleanly.
    runs = {run_.accession: str(Path(srx_dir).relative_to(output_root)) for run_, srx_dir in tasks}

    # The whole-pipeline success flag is the durable record of a full conversion. When
    # it already covers every requested run, short-circuit — no Snakemake, no reliance
    # on its (possibly wiped) internal state. Otherwise only the not-yet-done runs are
    # handed to Snakemake.
    completed = _load_completed(output_root)
    to_run = {srr: srx for srr, srx in runs.items() if srr not in completed}
    if not to_run:
        return dict.fromkeys(runs, True)

    control_dir = output_root / CONTROL_DIRNAME
    control_dir.mkdir(parents=True, exist_ok=True)
    config = {
        # How the workflow shells back into labdata (env-independent: same Python).
        "labdata": f"{shlex.quote(sys.executable)} -m labdata",
        "runs": to_run,
        "threads_per_run": threads_per_run,
        "reads_per_fastq": reads_per_fastq,
        "remove_bam": remove_bam,
    }
    config_path = control_dir / "tenx_config.json"
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True))

    argv = [
        "--snakefile",
        str(_snakefile()),
        "--directory",
        str(output_root),
        "--cores",
        str(cores),
        "--configfile",
        str(config_path),
        # Resume on marker presence, recover interrupted jobs, keep partial outputs so a
        # rerun resumes, keep going past one failed run, and take no workdir lock (so an
        # HPC kill leaves nothing to unlock). See RESILIENCE_FLAGS.
        *RESILIENCE_FLAGS,
    ]
    returncode = _run_snakemake(argv, verbose=verbose)
    if returncode != 0:
        logger.warning("snakemake exited %d — some runs may have failed", returncode)

    # Read results back from the per-run markers under <output_root>/<srx>/.
    ran = {srr: (output_root / srx / f".{srr}.tenx.done").exists() for srr, srx in to_run.items()}
    # Runs already covered by the success flag are complete by definition.
    results = {srr: (True if srr in completed else ran[srr]) for srr in runs}

    # On a fully-successful conversion, record the durable success flag and wipe
    # Snakemake's now-redundant internal state — the flag becomes the sole record.
    if all(results.values()):
        _write_success(output_root, {**completed, **runs})
        shutil.rmtree(output_root / ".snakemake", ignore_errors=True)
    return results
