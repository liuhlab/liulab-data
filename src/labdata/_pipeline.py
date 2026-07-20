"""Shared Snakemake-driver plumbing for labdata's pipelines.

Both the GEO download DAG (:mod:`labdata.geo.pipeline`) and the tenx bamtofastq DAG
(:mod:`labdata.tenx.pipeline`) drive a packaged ``pipeline.smk`` the same way: every
``snakemake`` invocation is funnelled through one subprocess seam, and a durable
whole-pipeline success flag is recorded under a ``.labdata`` control dir so a rerun of a
finished request short-circuits without invoking Snakemake. That common machinery lives
here; each pipeline module keeps only what differs — its Snakefile, its config, and the
name of its success flag — and re-exposes these under module-local names so its tests
can monkeypatch the seam in place.
"""

from __future__ import annotations

import json
import logging
import shlex
import subprocess
from pathlib import Path

from labdata.exceptions import DownloadError

logger = logging.getLogger(__name__)

#: Directory (under a pipeline's output root) holding the generated Snakemake config and
#: the whole-pipeline success flag — kept out of the data output tree.
CONTROL_DIRNAME = ".labdata"


def success_path(output_root: Path, filename: str) -> Path:
    """Path to a pipeline's whole-pipeline success flag under ``output_root``."""
    return Path(output_root) / CONTROL_DIRNAME / filename


def load_completed(output_root: Path, filename: str) -> dict[str, str]:
    """Return the ``{run: unit}`` map a success flag records, or ``{}`` if absent."""
    path = success_path(output_root, filename)
    if not path.exists():
        return {}
    try:
        runs = json.loads(path.read_text()).get("runs", {})
    except (OSError, ValueError):
        return {}
    return runs if isinstance(runs, dict) else {}


def write_success(output_root: Path, runs: dict[str, str], filename: str) -> None:
    """Write a success flag recording every completed ``{run: unit}``."""
    path = success_path(output_root, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"runs": dict(sorted(runs.items()))}, indent=2, sort_keys=True))


def run_snakemake(argv: list[str], *, verbose: bool) -> int:
    """Run ``snakemake`` with ``argv``; return its exit code.

    The shared subprocess seam for every labdata pipeline. Snakemake's own progress is
    streamed to the terminal when ``verbose``; otherwise it is discarded. A non-zero
    exit (individual job failures under ``--keep-going``) is returned to the caller
    rather than raised, so partial results can still be collected — only a *missing*
    ``snakemake`` binary raises.

    Parameters
    ----------
    argv : list of str
        Arguments passed to ``snakemake`` (without the program name).
    verbose : bool
        When ``True`` Snakemake's output is shown; when ``False`` it is suppressed.

    Returns
    -------
    int
        Snakemake's exit code (``0`` on full success).

    Raises
    ------
    DownloadError
        If the ``snakemake`` executable is not installed.
    """
    cmd = ["snakemake", *argv]
    logger.info("running: %s", " ".join(shlex.quote(part) for part in cmd))
    sink = None if verbose else subprocess.DEVNULL
    try:
        completed = subprocess.run(cmd, stdout=sink, stderr=sink, check=False)
    except FileNotFoundError as err:
        raise DownloadError(
            "'snakemake' not found — install snakemake (e.g. via pixi/conda)"
        ) from err
    return completed.returncode


#: The resilience flags shared by every pipeline's ``snakemake`` invocation: resume on
#: marker presence (not param/config churn), recover interrupted jobs, keep partial
#: outputs so a rerun resumes rather than restarts, never abort the batch on one failure,
#: and take no workdir lock (so an HPC walltime/OOM kill leaves nothing to unlock).
RESILIENCE_FLAGS = [
    "--rerun-triggers",
    "mtime",
    "--rerun-incomplete",
    "--keep-incomplete",
    "--keep-going",
    "--nolock",
]
