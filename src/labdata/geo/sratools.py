r"""Download FASTQ for SRA records with sra-tools (``prefetch`` + ``fasterq-dump``).

This is the package's second external boundary. NCBI *metadata* flows through the
:class:`~labdata.ncbi.entrez.EntrezClient` seam; the *sequence data* is fetched by
shelling out to sra-tools. Every subprocess call is funnelled through the single
:func:`_run` helper, which is the one place tests monkeypatch — so the rest of the
pipeline (directory layout, gzip, cleanup, success flags, parallelism) is exercised
without touching the network or installing any binaries.

The unit of work is one run (``SRR``): :func:`_download_run` runs ``prefetch`` then
``fasterq-dump``, gzips the resulting FASTQ with ``pigz`` (falling back to ``gzip``),
removes the intermediate ``.sra``/temp files, and drops a ``.<SRR>.success`` marker
so a rerun skips finished work. :class:`SraDownloader` applies that over every run
of one :class:`~labdata.geo.records.Experiment`; :meth:`Series.download
<labdata.geo.records.Series.download>` applies it across a whole Series. Both
parallelize at the run level.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from labdata.exceptions import DownloadError

if TYPE_CHECKING:
    from labdata.geo.records import Experiment, Run

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# the external-tool seam (the one place subprocesses are launched)
# --------------------------------------------------------------------------- #


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    """Run an external command, raising :class:`DownloadError` on failure.

    Parameters
    ----------
    cmd : list of str
        The command and its arguments (``cmd[0]`` is the executable).
    cwd : Path or None
        Working directory for the command, if any.

    Raises
    ------
    DownloadError
        If the executable is not installed (``FileNotFoundError``) or it exits
        with a non-zero status. The captured ``stderr`` is included in the message.
    """
    try:
        subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    except FileNotFoundError as err:
        raise DownloadError(
            f"{cmd[0]!r} not found — install sra-tools/pigz (e.g. via pixi/conda)"
        ) from err
    except subprocess.CalledProcessError as err:
        raise DownloadError(
            f"{cmd[0]!r} failed (exit {err.returncode}): {(err.stderr or '').strip()}"
        ) from err


def _have(tool: str) -> bool:
    """Return whether ``tool`` is on ``PATH``."""
    return shutil.which(tool) is not None


# --------------------------------------------------------------------------- #
# per-run worker (the unit of parallelism)
# --------------------------------------------------------------------------- #


def _cleanup_run(srx_dir: Path, accession: str) -> None:
    """Remove this run's intermediate ``prefetch``/``fasterq-dump`` artifacts.

    Leaves only the gzipped FASTQ (``<SRR>*.fastq.gz``); the ``.sra`` download
    directory, stray ``.sra`` files, and ``fasterq.tmp.*`` scratch dirs are dropped.
    """
    prefetch_dir = srx_dir / accession
    if prefetch_dir.is_dir():
        shutil.rmtree(prefetch_dir, ignore_errors=True)
    for sra in srx_dir.glob(f"{accession}*.sra"):
        sra.unlink(missing_ok=True)
    for tmp in srx_dir.glob("fasterq.tmp.*"):
        shutil.rmtree(tmp, ignore_errors=True)


def _download_run(run: Run, srx_dir: Path, *, threads: int = 1) -> bool:
    """Download, extract, and gzip one run (``SRR``) into ``srx_dir``.

    Skips immediately when the ``.<SRR>.success`` flag already exists. Otherwise
    runs ``prefetch`` then ``fasterq-dump``, compresses the FASTQ, cleans up the
    intermediates, and writes the flag last so an interrupted run is never marked
    successful.

    Parameters
    ----------
    run : Run
        The run to download.
    srx_dir : Path
        The (already created) experiment directory the FASTQ is written into.
    threads : int, default 1
        Threads passed to ``fasterq-dump`` and ``pigz`` for this single run.

    Returns
    -------
    bool
        ``True`` once the run's data is present (freshly downloaded or already done).

    Raises
    ------
    DownloadError
        If a tool is missing or any step fails (the flag is then not written, so a
        later rerun retries cleanly).
    """
    accession = run.accession
    flag = srx_dir / f".{accession}.success"
    if flag.exists():
        logger.info("skipping %s — already downloaded", accession)
        return True

    _run(["prefetch", accession, "-O", str(srx_dir)])

    sra_path = srx_dir / accession / f"{accession}.sra"
    target = str(sra_path) if sra_path.exists() else accession
    _run(
        [
            "fasterq-dump",
            target,
            "--split-files",
            "--threads",
            str(threads),
            "-O",
            str(srx_dir),
            "--temp",
            str(srx_dir),
        ]
    )

    fastqs = sorted(srx_dir.glob(f"{accession}*.fastq"))
    if not fastqs:
        raise DownloadError(f"fasterq-dump produced no FASTQ for {accession!r}")
    paths = [str(path) for path in fastqs]
    if _have("pigz"):
        _run(["pigz", "-f", "-p", str(threads), *paths])
    else:
        _run(["gzip", "-f", *paths])

    _cleanup_run(srx_dir, accession)
    flag.touch()
    logger.info("downloaded %s", accession)
    return True


def _safe_download(run: Run, srx_dir: Path) -> bool:
    """Run :func:`_download_run`, returning ``False`` instead of raising on failure.

    Lets a batch continue past one bad run; the failure is logged and the missing
    success flag means a later rerun retries it.
    """
    try:
        return _download_run(run, srx_dir)
    except DownloadError:
        logger.exception("failed to download %s", run.accession)
        return False


def _download_tasks(tasks: list[tuple[Run, Path]], n_parallel: int) -> dict[str, bool]:
    """Download ``(run, srx_dir)`` pairs, parallel at the run level.

    Parameters
    ----------
    tasks : list of (Run, Path)
        Each run paired with the experiment directory it belongs in.
    n_parallel : int
        Maximum runs to download concurrently (``<= 1`` runs them sequentially).

    Returns
    -------
    dict[str, bool]
        Maps each run accession to whether its data is present on completion.
    """
    if n_parallel <= 1:
        return {run.accession: _safe_download(run, srx_dir) for run, srx_dir in tasks}

    results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=n_parallel) as pool:
        futures = {
            pool.submit(_safe_download, run, srx_dir): run.accession for run, srx_dir in tasks
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return results


# --------------------------------------------------------------------------- #
# experiment-level downloader
# --------------------------------------------------------------------------- #


class SraDownloader:
    r"""Download FASTQ for every run of one SRA experiment (``SRX``) with sra-tools.

    Parameters
    ----------
    experiment : Experiment
        The SRA experiment whose runs (``SRR``) will be downloaded.
    output_dir : str or Path, default "."
        Parent directory. A subdirectory named by the experiment accession
        (``<output_dir>/<SRX>``) is created and populated with one gzipped FASTQ
        set per run, plus a ``.<SRR>.success`` marker per finished run.

    Examples
    --------
    >>> from labdata import Experiment  # doctest: +SKIP
    >>> SraDownloader(Experiment("SRX5921017"), "./fastq").download(n_parallel=2)  # doctest: +SKIP
    {'SRR9000001': True, 'SRR9000002': True}
    """

    def __init__(self, experiment: Experiment, output_dir: str | Path = ".") -> None:
        self.experiment = experiment
        self.output_dir = Path(output_dir)

    def __repr__(self) -> str:
        """Return an unambiguous representation."""
        return f"{type(self).__name__}({self.experiment.accession!r}, {str(self.output_dir)!r})"

    @property
    def srx_dir(self) -> Path:
        """The destination directory for this experiment (``<output_dir>/<SRX>``)."""
        return self.output_dir / self.experiment.accession

    def download(self, n_parallel: int = 1) -> dict[str, bool]:
        """Download every run of the experiment, parallel at the run level.

        Creates :attr:`srx_dir`, then downloads each run's gzipped FASTQ into it.
        Runs with an existing ``.<SRR>.success`` flag are skipped; a run that fails
        is recorded as ``False`` without aborting the others.

        Parameters
        ----------
        n_parallel : int, default 1
            Maximum runs to download concurrently.

        Returns
        -------
        dict[str, bool]
            Maps each run accession to whether its data is present on completion.
        """
        srx_dir = self.srx_dir
        srx_dir.mkdir(parents=True, exist_ok=True)
        tasks = [(run, srx_dir) for run in self.experiment.runs]
        return _download_tasks(tasks, n_parallel)
