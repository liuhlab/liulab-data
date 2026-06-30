r"""Download FASTQ for SRA records with sra-tools (``prefetch`` + ``fasterq-dump``).

This is the package's second external boundary. NCBI *metadata* flows through the
:class:`~labdata.ncbi.entrez.EntrezClient` seam; the *sequence data* is fetched by
shelling out to sra-tools. Every subprocess call is funnelled through the single
:func:`_run` helper, which is the one place tests monkeypatch — so the rest of the
pipeline (directory layout, gzip, cleanup, success flags, parallelism) is exercised
without touching the network or installing any binaries.

The unit of work is one run (``SRR``): :func:`_download_run` runs ``prefetch`` then
``fasterq-dump``, gzips the resulting FASTQ with ``pigz`` (falling back to ``gzip``),
removes the intermediate ``.sra``/temp files, and records each finished step in a
``.<SRR>.success`` JSON marker (see :class:`_DownloadFlag`) so a rerun resumes from
where it stopped — skipping ``prefetch``/``fasterq-dump`` once done and re-gzipping
only the FASTQ files not yet compressed. :class:`SraDownloader` applies that over every run
of one :class:`~labdata.geo.records.Experiment`; :meth:`Series.download
<labdata.geo.records.Series.download>` applies it across a whole Series. Both
parallelize at the run level.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from labdata.exceptions import DownloadError

if TYPE_CHECKING:
    from labdata.geo.records import Experiment, Run

logger = logging.getLogger(__name__)

#: Default ``prefetch --max-size``; the tool otherwise refuses runs over 20G.
DEFAULT_MAX_SIZE = "200G"
#: Default attempts for the network step (``prefetch``); see :func:`_run_retry`.
DEFAULT_RETRIES = 3
#: Default base seconds for the linear backoff between ``prefetch`` retries.
DEFAULT_BACKOFF = 5.0


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


def _run_retry(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
) -> None:
    """Run a network command, retrying with linear backoff on failure.

    For the resumable network step (``prefetch``): a retry continues the partial
    download rather than starting over, so retries are cheap. The extraction step
    (``fasterq-dump``) is local-only and is *not* routed through here — retrying it
    gains nothing since its input ``.sra`` is already on disk.

    Parameters
    ----------
    cmd : list of str
        The command and its arguments (``cmd[0]`` is the executable).
    cwd : Path or None
        Working directory for the command, if any.
    retries : int, default :data:`DEFAULT_RETRIES`
        Total attempts before giving up. ``<= 1`` disables retrying.
    backoff : float, default :data:`DEFAULT_BACKOFF`
        Base seconds for the linear backoff; attempt *n* sleeps ``backoff * n``.

    Raises
    ------
    DownloadError
        If every attempt fails (the last failure is re-raised).
    """
    attempts = max(1, retries)
    for attempt in range(1, attempts + 1):
        try:
            _run(cmd, cwd=cwd)
            return
        except DownloadError:
            if attempt == attempts:
                raise
            logger.warning("%s failed (attempt %d/%d); retrying", cmd[0], attempt, attempts)
            time.sleep(backoff * attempt)


def _have(tool: str) -> bool:
    """Return whether ``tool`` is on ``PATH``."""
    return shutil.which(tool) is not None


# --------------------------------------------------------------------------- #
# user-facing progress (a download plan up front, one line per finished run)
# --------------------------------------------------------------------------- #


class _Progress:
    """Thread-safe sink for per-run progress lines (one per finished run).

    A disabled reporter is a no-op, so the worker functions can call it
    unconditionally regardless of whether the caller asked for output.

    Parameters
    ----------
    total : int
        Number of runs in the batch; shown as the ``(n/total)`` counter.
    enabled : bool, default True
        When ``False`` every method is a no-op (used for quiet downloads).
    stream : TextIO or None
        Where lines are written; defaults to ``sys.stderr`` so stdout stays clean.
    """

    def __init__(self, total: int, *, enabled: bool = True, stream: TextIO | None = None) -> None:
        self.total = total
        self.enabled = enabled
        self.stream = stream if stream is not None else sys.stderr
        self._lock = threading.Lock()
        self._done = 0

    def _emit(self, mark: str, accession: str, note: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._done += 1
            position = f"({self._done}/{self.total})"
        print(f"{mark} {accession}  {note}  {position}", file=self.stream, flush=True)

    def downloaded(self, accession: str) -> None:
        """Report that ``accession`` was freshly downloaded."""
        self._emit("✓", accession, "done")

    def skipped(self, accession: str) -> None:
        """Report that ``accession`` was already present and skipped."""
        self._emit("•", accession, "already done")

    def failed(self, accession: str) -> None:
        """Report that ``accession`` failed to download."""
        self._emit("✗", accession, "failed")


def _print_plan(
    label: str,
    tasks: list[tuple[Run, Path]],
    *,
    n_parallel: int,
    output_root: Path,
    stream: TextIO | None = None,
) -> None:
    """Print the download plan: destination, run/experiment counts, and a per-SRX list.

    Groups ``tasks`` by their experiment directory and notes how many runs are
    already complete (a finished :class:`_DownloadFlag`, and so will be skipped).
    """
    out = stream if stream is not None else sys.stderr
    if not tasks:
        print(f"{label}: no SRA runs to download.", file=out, flush=True)
        return

    groups: dict[Path, list[str]] = {}
    already = 0
    for run, srx_dir in tasks:
        groups.setdefault(srx_dir, []).append(run.accession)
        if _DownloadFlag(srx_dir, run.accession).complete:
            already += 1

    n_runs = len(tasks)
    runs_word = "run" if n_runs == 1 else "runs"
    exp_word = "experiment" if len(groups) == 1 else "experiments"
    print(
        f"{label} → {output_root}  "
        f"[{n_runs} {runs_word}, {len(groups)} {exp_word}, n_parallel={n_parallel}]",
        file=out,
        flush=True,
    )
    for srx_dir, runs in groups.items():
        print(f"  {srx_dir.name}: {', '.join(runs)}", file=out, flush=True)
    if already:
        print(f"  ({already} of {n_runs} already done, will be skipped)", file=out, flush=True)


def _print_summary(results: dict[str, bool], *, stream: TextIO | None = None) -> None:
    """Print a one-line tally of succeeded vs failed runs."""
    out = stream if stream is not None else sys.stderr
    ok = sum(1 for success in results.values() if success)
    failed = len(results) - ok
    print(f"Done: {ok} ok, {failed} failed.", file=out, flush=True)


def _run_plan(
    label: str,
    tasks: list[tuple[Run, Path]],
    n_parallel: int,
    *,
    output_root: Path,
    verbose: bool,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    keep_sra: bool = False,
    prefetch_only: bool = False,
) -> dict[str, bool]:
    """Announce the plan, run the tasks (reporting each run), then tally the results.

    The shared entry point behind :meth:`Series.download` and
    :meth:`SraDownloader.download`; ``verbose=False`` silences all three steps.
    """
    if verbose:
        _print_plan(label, tasks, n_parallel=n_parallel, output_root=output_root)
    progress = _Progress(len(tasks), enabled=verbose)
    results = _download_tasks(
        tasks,
        n_parallel,
        max_size=max_size,
        retries=retries,
        backoff=backoff,
        keep_sra=keep_sra,
        prefetch_only=prefetch_only,
        progress=progress,
    )
    if verbose and tasks:
        _print_summary(results)
    return results


# --------------------------------------------------------------------------- #
# per-run worker (the unit of parallelism)
# --------------------------------------------------------------------------- #


class _DownloadFlag:
    """Per-run progress marker recording which download steps have finished.

    Backed by a JSON file (``.<SRR>.success``) in the experiment directory. Each
    step is written as soon as it succeeds, so an interrupted run resumes instead
    of starting over: a finished ``prefetch`` or ``fasterq-dump`` is skipped, and
    ``gzip`` records each FASTQ as it is compressed so already-gzipped files are
    not redone. The run is :attr:`complete` once every step in :attr:`STEPS` is
    marked.

    A legacy empty/non-JSON marker (the format written before this class) is read
    as a fully :attr:`complete` run, so existing downloads are still skipped.

    Parameters
    ----------
    srx_dir : Path
        The experiment directory the marker lives in.
    accession : str
        The run accession (``SRR``) this marker tracks.
    """

    #: The ordered steps that must all be recorded for a run to be :attr:`complete`.
    STEPS = ("prefetch", "fasterq-dump", "cleanup")

    def __init__(self, srx_dir: Path, accession: str) -> None:
        self.path = srx_dir / f".{accession}.success"
        self.accession = accession
        self._state = self._load()

    def _load(self) -> dict[str, object]:
        """Read the marker, treating a missing file as empty and a legacy one as done."""
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            data = None
        if isinstance(data, dict):
            return data
        # A pre-existing empty/plain marker means "fully downloaded" under the old format.
        return dict.fromkeys(self.STEPS, True)

    def _save(self) -> None:
        self._state["accession"] = self.accession
        self.path.write_text(json.dumps(self._state, indent=2, sort_keys=True))

    def done(self, step: str) -> bool:
        """Return whether ``step`` has been recorded as finished."""
        return bool(self._state.get(step))

    def mark(self, step: str) -> None:
        """Record ``step`` as finished and persist the marker."""
        self._state[step] = True
        self._save()

    def is_gzipped(self, name: str) -> bool:
        """Return whether the FASTQ file ``name`` has already been gzipped."""
        gzipped = self._state.get("gzip")
        return isinstance(gzipped, list) and name in gzipped

    def mark_gzipped(self, name: str) -> None:
        """Record FASTQ file ``name`` as gzipped and persist the marker."""
        gzipped = self._state.setdefault("gzip", [])
        if isinstance(gzipped, list) and name not in gzipped:
            gzipped.append(name)
        self._save()

    @property
    def complete(self) -> bool:
        """Whether every step in :attr:`STEPS` has been recorded as finished."""
        return all(self.done(step) for step in self.STEPS)


def _cleanup_run(srx_dir: Path, accession: str, *, keep_sra: bool = False) -> None:
    """Remove this run's intermediate ``prefetch``/``fasterq-dump`` artifacts.

    Leaves only the gzipped FASTQ (``<SRR>*.fastq.gz``); the ``.sra`` download
    directory and ``fasterq.tmp.*`` scratch dirs are dropped. When ``keep_sra``
    is set, the downloaded ``.sra`` is first moved up to ``<srx_dir>/<SRR>.sra``
    so it survives the directory cleanup.
    """
    prefetch_dir = srx_dir / accession
    if keep_sra:
        sra = prefetch_dir / f"{accession}.sra"
        if sra.exists():
            shutil.move(str(sra), str(srx_dir / f"{accession}.sra"))
    if prefetch_dir.is_dir():
        shutil.rmtree(prefetch_dir, ignore_errors=True)
    for tmp in srx_dir.glob("fasterq.tmp.*"):
        shutil.rmtree(tmp, ignore_errors=True)


def _download_run(
    run: Run,
    srx_dir: Path,
    *,
    threads: int = 1,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    keep_sra: bool = False,
    prefetch_only: bool = False,
    progress: _Progress | None = None,
) -> bool:
    """Download, extract, and gzip one run (``SRR``) into ``srx_dir``.

    Skips immediately when the run's :class:`_DownloadFlag` is already complete.
    Otherwise it runs each step that the flag does not yet record — ``prefetch``,
    then ``fasterq-dump``, then ``gzip`` one FASTQ at a time, then cleanup —
    marking the flag after each so an interrupted run resumes from where it
    stopped rather than starting over. Only ``prefetch`` (the network step) is
    retried; it resumes its partial download, so a failed run also recovers
    cleanly on a later rerun.

    With ``prefetch_only`` the run stops after ``prefetch``: the ``.sra`` is left
    in ``<srx_dir>/<SRR>/`` and no extraction, compression, or cleanup runs. The
    flag records only the ``prefetch`` step, so a later full download resumes from
    ``fasterq-dump`` without re-fetching.

    Parameters
    ----------
    run : Run
        The run to download.
    srx_dir : Path
        The (already created) experiment directory the FASTQ is written into.
    threads : int, default 1
        Threads passed to ``fasterq-dump`` and ``pigz`` for this single run.
    max_size : str, default :data:`DEFAULT_MAX_SIZE`
        Passed to ``prefetch --max-size``; raise it for large runs (``prefetch``
        otherwise refuses anything over its 20G default).
    retries : int, default :data:`DEFAULT_RETRIES`
        Attempts for the ``prefetch`` network step before giving up.
    backoff : float, default :data:`DEFAULT_BACKOFF`
        Base seconds for the linear backoff between ``prefetch`` retries.
    keep_sra : bool, default False
        Keep the downloaded ``.sra`` alongside the FASTQ (as
        ``<srx_dir>/<SRR>.sra``) instead of deleting it during cleanup.
    prefetch_only : bool, default False
        Stop after ``prefetch``, leaving the downloaded ``.sra`` in place and
        skipping ``fasterq-dump``, gzip, and cleanup.

    Returns
    -------
    bool
        ``True`` once the run's data is present (freshly downloaded or already
        done); with ``prefetch_only`` once its ``.sra`` has been fetched.

    Raises
    ------
    DownloadError
        If a tool is missing or any step fails (the flag is then not written, so a
        later rerun retries cleanly).
    """
    accession = run.accession
    flag = _DownloadFlag(srx_dir, accession)
    if flag.complete:
        logger.info("skipping %s — already downloaded", accession)
        if progress is not None:
            progress.skipped(accession)
        return True

    prefetch_done = flag.done("prefetch")
    if not prefetch_done:
        _run_retry(
            ["prefetch", accession, "-O", str(srx_dir), "--max-size", max_size],
            retries=retries,
            backoff=backoff,
        )
        flag.mark("prefetch")

    if prefetch_only:
        logger.info("prefetched %s (prefetch-only, skipping extraction)", accession)
        if progress is not None:
            (progress.skipped if prefetch_done else progress.downloaded)(accession)
        return True

    if not flag.done("fasterq-dump"):
        sra_path = srx_dir / accession / f"{accession}.sra"
        target = str(sra_path) if sra_path.exists() else accession
        _run(
            [
                "fasterq-dump",
                target,
                "--split-files",
                "--include-technical",
                "--threads",
                str(threads),
                "-O",
                str(srx_dir),
                "--temp",
                str(srx_dir),
            ]
        )
        flag.mark("fasterq-dump")

    # Gzip one FASTQ at a time, recording each so a rerun compresses only what is left.
    fastqs = sorted(srx_dir.glob(f"{accession}*.fastq"))
    if not fastqs and not list(srx_dir.glob(f"{accession}*.fastq.gz")):
        raise DownloadError(f"fasterq-dump produced no FASTQ for {accession!r}")
    gzip_cmd = ["pigz", "-f", "-p", str(threads)] if _have("pigz") else ["gzip", "-f"]
    for fastq in fastqs:
        if flag.is_gzipped(fastq.name):
            continue
        _run([*gzip_cmd, str(fastq)])
        flag.mark_gzipped(fastq.name)

    if not flag.done("cleanup"):
        _cleanup_run(srx_dir, accession, keep_sra=keep_sra)
        flag.mark("cleanup")
    logger.info("downloaded %s", accession)
    if progress is not None:
        progress.downloaded(accession)
    return True


def _safe_download(
    run: Run,
    srx_dir: Path,
    *,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    keep_sra: bool = False,
    prefetch_only: bool = False,
    progress: _Progress | None = None,
) -> bool:
    """Run :func:`_download_run`, returning ``False`` instead of raising on failure.

    Lets a batch continue past one bad run; the failure is logged and the missing
    success flag means a later rerun retries it.
    """
    try:
        return _download_run(
            run,
            srx_dir,
            max_size=max_size,
            retries=retries,
            backoff=backoff,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
            progress=progress,
        )
    except DownloadError:
        logger.exception("failed to download %s", run.accession)
        if progress is not None:
            progress.failed(run.accession)
        return False


def _download_tasks(
    tasks: list[tuple[Run, Path]],
    n_parallel: int,
    *,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    keep_sra: bool = False,
    prefetch_only: bool = False,
    progress: _Progress | None = None,
) -> dict[str, bool]:
    """Download ``(run, srx_dir)`` pairs, parallel at the run level.

    Parameters
    ----------
    tasks : list of (Run, Path)
        Each run paired with the experiment directory it belongs in.
    n_parallel : int
        Maximum runs to download concurrently (``<= 1`` runs them sequentially).
    max_size : str, default :data:`DEFAULT_MAX_SIZE`
        Passed to ``prefetch --max-size`` for every run.
    retries : int, default :data:`DEFAULT_RETRIES`
        Attempts for the ``prefetch`` network step per run before giving up.
    backoff : float, default :data:`DEFAULT_BACKOFF`
        Base seconds for the linear backoff between ``prefetch`` retries.
    keep_sra : bool, default False
        Keep each run's downloaded ``.sra`` next to its FASTQ.
    prefetch_only : bool, default False
        Stop each run after ``prefetch``, skipping extraction and compression.

    Returns
    -------
    dict[str, bool]
        Maps each run accession to whether its data is present on completion.
    """

    def work(run: Run, srx_dir: Path) -> bool:
        return _safe_download(
            run,
            srx_dir,
            max_size=max_size,
            retries=retries,
            backoff=backoff,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
            progress=progress,
        )

    if n_parallel <= 1:
        return {run.accession: work(run, srx_dir) for run, srx_dir in tasks}

    results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=n_parallel) as pool:
        futures = {pool.submit(work, run, srx_dir): run.accession for run, srx_dir in tasks}
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

    def download(
        self,
        n_parallel: int = 1,
        *,
        max_size: str = DEFAULT_MAX_SIZE,
        retries: int = DEFAULT_RETRIES,
        backoff: float = DEFAULT_BACKOFF,
        keep_sra: bool = False,
        prefetch_only: bool = False,
        verbose: bool = True,
    ) -> dict[str, bool]:
        """Download every run of the experiment, parallel at the run level.

        Creates :attr:`srx_dir`, then downloads each run's gzipped FASTQ into it.
        Runs with an existing ``.<SRR>.success`` flag are skipped; a run that fails
        is recorded as ``False`` without aborting the others.

        Parameters
        ----------
        n_parallel : int, default 1
            Maximum runs to download concurrently. On a flaky link, fewer
            concurrent connections (``1`` or ``2``) is often more reliable.
        max_size : str, default :data:`DEFAULT_MAX_SIZE`
            Passed to ``prefetch --max-size``; raise it for large runs (``prefetch``
            otherwise refuses anything over its 20G default).
        retries : int, default :data:`DEFAULT_RETRIES`
            Attempts for the resumable ``prefetch`` network step per run; a retry
            continues the partial download rather than restarting.
        backoff : float, default :data:`DEFAULT_BACKOFF`
            Base seconds for the linear backoff between ``prefetch`` retries.
        keep_sra : bool, default False
            Keep each run's downloaded ``.sra`` next to its FASTQ (as
            ``<SRX>/<SRR>.sra``) instead of deleting it after extraction.
        prefetch_only : bool, default False
            Only ``prefetch`` each run's ``.sra`` (into ``<SRX>/<SRR>/``), skipping
            ``fasterq-dump``, gzip, and cleanup. A later full download resumes from
            extraction without re-fetching.
        verbose : bool, default True
            Print a download plan up front and one line per finished run to
            ``stderr``. Set ``False`` to download silently.

        Returns
        -------
        dict[str, bool]
            Maps each run accession to whether its data is present on completion.
        """
        srx_dir = self.srx_dir
        srx_dir.mkdir(parents=True, exist_ok=True)
        tasks = [(run, srx_dir) for run in self.experiment.runs]
        return _run_plan(
            self.experiment.accession,
            tasks,
            n_parallel,
            output_root=srx_dir,
            verbose=verbose,
            max_size=max_size,
            retries=retries,
            backoff=backoff,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
        )
