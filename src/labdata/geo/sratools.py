r"""Per-run sra-tools stages behind the Snakemake download DAG.

This is the package's second external boundary. NCBI *metadata* flows through the
:class:`~labdata.ncbi.entrez.EntrezClient` seam; the *sequence data* is fetched by
shelling out to sra-tools. Every subprocess call is funnelled through the single
:func:`_run` helper, the one place tests monkeypatch — so the stages (directory
layout, gzip, cleanup, success flags) are exercised without touching the network or
installing any binaries.

The download of one run (``SRR``) is split into three stages, each idempotent and
each recording its progress in a ``.<SRR>.success`` JSON marker (see
:class:`_DownloadFlag`) so a rerun resumes where it stopped: :func:`prefetch_run`
(network), :func:`extract_run` (``fasterq-dump``), and :func:`compress_run`
(``pigz``/``gzip`` + cleanup). Orchestration — scheduling these stages across many
runs with a resource-aware, network-vs-CPU decoupled DAG — lives in
:mod:`labdata.geo.pipeline` (Snakemake). :class:`SraDownloader` drives that DAG over
every run of one :class:`~labdata.geo.sra_records.Experiment`; :class:`_SraDownloadMixin`
drives it across a whole :class:`~labdata.geo.geo_records.Series` or
:class:`~labdata.geo.bio_project_records.BioProject`.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterable
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from labdata.exceptions import AccessionError, DownloadError
from labdata.geo import pipeline
from labdata.geo.sra_records import Experiment

if TYPE_CHECKING:
    from labdata.geo.sra_records import Run

logger = logging.getLogger(__name__)

#: Default ``prefetch --max-size``; the tool otherwise refuses runs over 20G.
DEFAULT_MAX_SIZE = "200G"
#: Default attempts for the network step (``prefetch``); see :func:`_run_retry`.
DEFAULT_RETRIES = 3
#: Default base seconds for the linear backoff between ``prefetch`` retries.
DEFAULT_BACKOFF = 5.0
#: Default cap on concurrent ``prefetch`` (NCBI network) jobs in the download DAG.
DEFAULT_NCBI_PARALLEL = 4
#: Default threads handed to ``fasterq-dump``/``pigz`` for each run.
DEFAULT_THREADS_PER_RUN = 12


# --------------------------------------------------------------------------- #
# the external-tool seam (the one place subprocesses are launched)
# --------------------------------------------------------------------------- #


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    """Run an external command, raising :class:`DownloadError` on failure.

    The single external-tool seam for the download: sra-tools (``prefetch``/
    ``fasterq-dump``), the gzip tools (``pigz``/``gzip``), and ``curl`` (used to
    fetch original-format files) all go through here, and tests monkeypatch this
    one function.

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
            f"{cmd[0]!r} not found — install sra-tools/pigz/curl (e.g. via pixi/conda)"
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


def _verify_gzip(path: Path, *, threads: int = 1) -> None:
    """Decompress-test ``path``, raising :class:`DownloadError` if it is not intact.

    Runs ``pigz -t`` (or ``gzip -t`` when pigz is absent) over the whole file so a
    truncated or corrupt ``.gz`` is caught here — at download time — rather than by a
    downstream reader. A gzip stream cut short by an interrupted write fails this test.

    Parameters
    ----------
    path : Path
        The gzip file to verify.
    threads : int, default 1
        Threads passed to ``pigz -t`` (ignored by the ``gzip`` fallback).

    Raises
    ------
    DownloadError
        If the gzip test tool is missing, or it reports ``path`` as corrupt.
    """
    test_cmd = ["pigz", "-t", "-p", str(threads)] if _have("pigz") else ["gzip", "-t"]
    _run([*test_cmd, str(path)])


# --------------------------------------------------------------------------- #
# user-facing progress (a download plan up front, one line per finished run)
# --------------------------------------------------------------------------- #


def _print_plan(
    label: str,
    tasks: list[tuple[Run, Path]],
    *,
    ncbi_parallel: int,
    cores: int,
    output_root: Path,
    original: AbstractSet[str] = frozenset(),
    stream: TextIO | None = None,
) -> None:
    """Print the download plan: destination, run/experiment counts, and a per-SRX list.

    Groups ``tasks`` by their experiment directory and notes how many runs are
    already complete (a ``.<SRR>.done`` marker present, and so will be skipped).
    Experiments fetched in original format (their run accessions in ``original``)
    are flagged. The per-run progress that follows is emitted by Snakemake.
    """
    out = stream if stream is not None else sys.stderr
    if not tasks:
        print(f"{label}: no SRA runs to download.", file=out, flush=True)
        return

    groups: dict[Path, list[str]] = {}
    already = 0
    for run, srx_dir in tasks:
        groups.setdefault(srx_dir, []).append(run.accession)
        marker = (
            f".{run.accession}.original.done"
            if run.accession in original
            else f".{run.accession}.done"
        )
        if (srx_dir / marker).exists():
            already += 1

    n_runs = len(tasks)
    runs_word = "run" if n_runs == 1 else "runs"
    exp_word = "experiment" if len(groups) == 1 else "experiments"
    print(
        f"{label} → {output_root}  "
        f"[{n_runs} {runs_word}, {len(groups)} {exp_word}, "
        f"ncbi_parallel={ncbi_parallel}, cores={cores}]",
        file=out,
        flush=True,
    )
    for srx_dir, runs in groups.items():
        tag = "  [original format]" if any(run in original for run in runs) else ""
        print(f"  {srx_dir.name}: {', '.join(runs)}{tag}", file=out, flush=True)
    if already:
        print(f"  ({already} of {n_runs} already done, will be skipped)", file=out, flush=True)


def _print_summary(results: dict[str, bool], *, stream: TextIO | None = None) -> None:
    """Print a one-line tally of succeeded vs failed runs."""
    out = stream if stream is not None else sys.stderr
    ok = sum(1 for success in results.values() if success)
    failed = len(results) - ok
    print(f"Done: {ok} ok, {failed} failed.", file=out, flush=True)


#: Rough on-disk bytes per base for a transient run: uncompressed FASTQ (~2 B/base)
#: plus its ``.sra`` coexist during extraction. Used only for the preflight estimate.
_DISK_BYTES_PER_BASE = 3.0


def _human(num_bytes: float) -> str:
    """Format a byte count as a short human-readable string (e.g. ``12.3 GiB``)."""
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"


def _warn_if_low_disk(
    tasks: list[tuple[Run, Path]], output_root: Path, *, stream: TextIO | None = None
) -> None:
    """Warn (never block) if free space on ``output_root`` looks insufficient.

    Estimates need from the already-seeded :attr:`Run.total_bases` (no extra
    network). The estimate is deliberately rough — a wrong guess must never stop a
    legitimate download — so any error (missing metadata, unreadable path) silently
    skips the check.
    """
    out = stream if stream is not None else sys.stderr
    try:
        total_bases = sum(int(run.total_bases or 0) for run, _ in tasks)
        if total_bases <= 0:
            return
        free = shutil.disk_usage(output_root).free
    except (OSError, ValueError, TypeError):
        return
    estimate = int(total_bases * _DISK_BYTES_PER_BASE)
    if free < estimate:
        print(
            f"warning: ~{_human(estimate)} may be needed but only {_human(free)} is free "
            f"on {output_root} — the download could run out of space.",
            file=out,
            flush=True,
        )


def _run_plan(
    label: str,
    tasks: list[tuple[Run, Path]],
    *,
    output_root: Path,
    verbose: bool,
    ncbi_parallel: int = DEFAULT_NCBI_PARALLEL,
    cores: int | None = None,
    threads_per_run: int = DEFAULT_THREADS_PER_RUN,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    keep_sra: bool = False,
    prefetch_only: bool = False,
    original: AbstractSet[str] = frozenset(),
) -> dict[str, bool]:
    """Announce the plan, run the Snakemake DAG, then tally the results.

    The shared entry point behind :meth:`Series.download` and
    :meth:`SraDownloader.download`. Scheduling is delegated to
    :func:`labdata.geo.pipeline.run`, which caps concurrent ``prefetch`` at
    ``ncbi_parallel`` while ``cores`` bounds total CPU across extraction and
    compression. Runs whose accession is in ``original`` are fetched in
    original format (download only). ``verbose=False`` silences the plan/summary
    *and* Snakemake's own progress.
    """
    resolved_cores = cores if cores is not None else (os.cpu_count() or 1)
    if verbose:
        _print_plan(
            label,
            tasks,
            ncbi_parallel=ncbi_parallel,
            cores=resolved_cores,
            output_root=output_root,
            original=original,
        )
        # Estimate disk only for runs that still need downloading (skip the ones a
        # prior run already finished), so a rerun of a done pipeline stays quiet.
        # Original-format runs are excluded — their on-disk size is not the SRA
        # base count the estimate is built from.
        pending = [
            (run, srx_dir)
            for run, srx_dir in tasks
            if run.accession not in original and not (srx_dir / f".{run.accession}.done").exists()
        ]
        if pending:
            _warn_if_low_disk(pending, output_root)
    results = pipeline.run(
        tasks,
        output_root=output_root,
        ncbi_parallel=ncbi_parallel,
        cores=resolved_cores,
        threads_per_run=threads_per_run,
        max_size=max_size,
        retries=retries,
        backoff=backoff,
        keep_sra=keep_sra,
        prefetch_only=prefetch_only,
        original=original,
        verbose=verbose,
    )
    if verbose and tasks:
        _print_summary(results)
    return results


# --------------------------------------------------------------------------- #
# per-run worker (the unit of parallelism)
# --------------------------------------------------------------------------- #


def _staging_dir(srx_dir: Path, accession: str) -> Path:
    """Return the staging directory for a run's uncompressed FASTQ during processing."""
    return srx_dir / f".{accession}.fq"


def _stash_sra(srx_dir: Path, accession: str) -> None:
    """Move a run's downloaded ``.sra`` up beside its FASTQ (for ``keep_sra``).

    Called during extraction, before Snakemake reclaims the temporary prefetch
    directory, so the ``.sra`` survives at ``<srx_dir>/<SRR>.sra``.
    """
    sra = srx_dir / accession / f"{accession}.sra"
    if sra.exists():
        shutil.move(str(sra), str(srx_dir / f"{accession}.sra"))


def prefetch_run(
    run: Run,
    srx_dir: Path,
    *,
    max_size: str = DEFAULT_MAX_SIZE,
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
) -> None:
    """Fetch one run's ``.sra`` into ``<srx_dir>/<SRR>/`` with ``prefetch``.

    The network stage of the download DAG. ``prefetch`` is retried with linear
    backoff; because it resumes a partial download, a retry is cheap and an
    interrupted fetch continues on a rerun rather than starting over. Resume across
    stages is handled by Snakemake, so this always runs the tool.

    Parameters
    ----------
    run : Run
        The run to fetch.
    srx_dir : Path
        The (already created) experiment directory the ``.sra`` is fetched into
        (under ``<SRR>/``).
    max_size : str, default :data:`DEFAULT_MAX_SIZE`
        Passed to ``prefetch --max-size``; raise it for large runs (``prefetch``
        otherwise refuses anything over its 20G default).
    retries : int, default :data:`DEFAULT_RETRIES`
        Attempts for the network step before giving up.
    backoff : float, default :data:`DEFAULT_BACKOFF`
        Base seconds for the linear backoff between retries.

    Raises
    ------
    DownloadError
        If ``prefetch`` is missing or every attempt fails.
    """
    accession = run.accession
    _run_retry(
        ["prefetch", accession, "-O", str(srx_dir), "--max-size", max_size],
        retries=retries,
        backoff=backoff,
    )
    logger.info("prefetched %s", accession)


def extract_run(
    run: Run,
    srx_dir: Path,
    *,
    threads: int = 1,
    staging: Path | None = None,
    keep_sra: bool = False,
) -> None:
    """Extract one run's FASTQ from its ``.sra`` with ``fasterq-dump``.

    The first local (CPU-bound) stage of the download DAG; expects
    :func:`prefetch_run` to have run first. The uncompressed FASTQ (and
    ``fasterq-dump`` scratch) go into ``staging`` — a directory Snakemake tracks as
    a temporary output and removes once :func:`compress_run` has consumed it. With
    ``keep_sra`` the ``.sra`` is stashed beside the (eventual) FASTQ before its
    prefetch directory is reclaimed.

    Parameters
    ----------
    run : Run
        The run to extract.
    srx_dir : Path
        The experiment directory holding the fetched ``.sra``.
    threads : int, default 1
        Threads passed to ``fasterq-dump``.
    staging : Path, optional
        Directory to write the uncompressed FASTQ into (defaults to
        ``<srx_dir>/.<SRR>.fq``).
    keep_sra : bool, default False
        Move the downloaded ``.sra`` up to ``<srx_dir>/<SRR>.sra`` instead of
        letting it be reclaimed.

    Raises
    ------
    DownloadError
        If ``fasterq-dump`` is missing or fails.
    """
    accession = run.accession
    staging = staging if staging is not None else _staging_dir(srx_dir, accession)
    staging.mkdir(parents=True, exist_ok=True)
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
            str(staging),
            "--temp",
            str(staging),
        ]
    )
    if keep_sra:
        _stash_sra(srx_dir, accession)
    logger.info("extracted %s", accession)


def compress_run(run: Run, srx_dir: Path, *, threads: int = 1, staging: Path | None = None) -> None:
    """Gzip one run's extracted FASTQ into ``srx_dir``.

    The final local stage of the download DAG; expects :func:`extract_run` to have
    run first. Gzips each FASTQ from ``staging`` (``pigz``, falling back to
    ``gzip``) into ``<srx_dir>/<SRR>*.fastq.gz``, skipping any whose ``.gz`` already
    exists so an interrupted compress resumes without redoing finished files. The
    ``staging`` directory is a temporary Snakemake output, removed once this stage
    completes.

    Parameters
    ----------
    run : Run
        The run whose FASTQ to compress.
    srx_dir : Path
        The experiment directory the gzipped FASTQ are written into.
    threads : int, default 1
        Threads passed to ``pigz``.
    staging : Path, optional
        Directory holding the uncompressed FASTQ (defaults to
        ``<srx_dir>/.<SRR>.fq``).

    Every gzipped mate is decompress-tested (:func:`_verify_gzip`) before it is
    trusted: a freshly compressed file is checked and, on failure, removed rather than
    left in place, and an already-present file from an interrupted run is re-checked on
    resume and recompressed if it is truncated. This refuses to leave a corrupt ``.gz``
    behind, catching the failure here instead of at a downstream reader.

    Raises
    ------
    DownloadError
        If no FASTQ were produced, a gzip tool is missing/fails, or a compressed mate
        fails its integrity check.
    """
    accession = run.accession
    staging = staging if staging is not None else _staging_dir(srx_dir, accession)
    fastqs = sorted(staging.glob(f"{accession}*.fastq"))
    if not fastqs and not list(srx_dir.glob(f"{accession}*.fastq.gz")):
        raise DownloadError(f"fasterq-dump produced no FASTQ for {accession!r}")
    gzip_cmd = ["pigz", "-f", "-p", str(threads)] if _have("pigz") else ["gzip", "-f"]
    for fastq in fastqs:
        final = srx_dir / f"{fastq.name}.gz"
        if final.exists():
            try:
                _verify_gzip(final, threads=threads)
                continue  # per-file resume: this mate is already compressed and intact
            except DownloadError:
                logger.warning("existing %s failed gzip check; recompressing", final.name)
                final.unlink()
        _run([*gzip_cmd, str(fastq)])
        shutil.move(f"{fastq}.gz", str(final))
        try:
            _verify_gzip(final, threads=threads)
        except DownloadError:
            final.unlink(missing_ok=True)  # never leave a truncated .gz in place
            raise
    logger.info("compressed %s", accession)


# --------------------------------------------------------------------------- #
# original-format download (the alternative to prefetch/extract/compress)
# --------------------------------------------------------------------------- #


def _md5(path: Path) -> str:
    """Return the hex MD5 digest of ``path``, read in fixed-size chunks."""
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_original_run(
    run: Run, srx_dir: Path, *, retries: int = DEFAULT_RETRIES, backoff: float = DEFAULT_BACKOFF
) -> None:
    r"""Download one run's *original-format* files with ``curl`` — download only.

    The alternative to the ``prefetch → extract → compress`` chain, used when the
    SRA-normalized ``.sra`` is missing or useless (e.g. a 10X run whose ``.sra``
    stored only one read) and the submitter's own uploads must be fetched instead.
    Each original file listed by the SRA Data Locator (:attr:`Run.original_files`)
    is fetched with ``curl`` into ``<srx_dir>/<SRR>_<name>`` and, when the locator
    reports a checksum, verified against it. Files land flat in the experiment dir,
    each name prefixed with the run accession — matching the sra-tools output layout
    (``<SRX>/<SRR>…``) and keeping runs from colliding when their submitted files
    share a name. Nothing is extracted or compressed — the original format is
    heterogeneous and left for a later processing step.

    ``curl`` resumes a partial file (``-C -``) and the whole call is retried with
    linear backoff, so an interrupted download continues on a rerun rather than
    starting over. Files already present (and md5-verified, when a checksum is
    known) are skipped, giving per-file resume across runs.

    Parameters
    ----------
    run : Run
        The run whose original-format files to fetch.
    srx_dir : Path
        The (already created) experiment directory; files land directly in it,
        each name prefixed with the run accession (``<SRR>_<name>``).
    retries : int, default :data:`DEFAULT_RETRIES`
        Attempts for each ``curl`` fetch before giving up.
    backoff : float, default :data:`DEFAULT_BACKOFF`
        Base seconds for the linear backoff between retries.

    Raises
    ------
    DownloadError
        If the run lists no original-format files, a file has no download URL,
        ``curl`` is missing or fails, or a downloaded file's md5 does not match the
        locator's checksum.
    """
    accession = run.accession
    files = run.original_files
    if not files:
        raise DownloadError(
            f"no original-format files listed for {accession!r} — nothing to download"
        )
    srx_dir.mkdir(parents=True, exist_ok=True)
    for remote in files:
        if not remote.url:
            raise DownloadError(f"no download URL for original file {remote.name!r} ({accession})")
        dest = srx_dir / f"{accession}_{remote.name}"
        if dest.exists() and (remote.md5 is None or _md5(dest) == remote.md5):
            continue  # per-file resume: already present (and verified when a checksum is known)
        # ``remote.url`` is SDL's anonymous, worldwide-egress HTTPS S3 link
        # (``https://sra-pub-src-*.s3.amazonaws.com/…``), so a plain ``curl`` GET
        # needs no AWS credentials/CLI and works off-cluster — matching the package's
        # one-subprocess-seam, minimal-deps design.
        # TODO(perf): each file is one sequential single-connection stream. For the
        # tens-of-GB originals a multipart-parallel S3 client (``aws s3 cp
        # --no-sign-request`` or ``s5cmd``) would be markedly faster. If throughput
        # matters, dispatch to one when available and fall back to ``curl`` here; the
        # locator/verify/DAG layers stay unchanged.
        _run_retry(
            ["curl", "-fSL", "-C", "-", "-o", str(dest), remote.url],
            retries=retries,
            backoff=backoff,
        )
        if remote.md5 is not None and (actual := _md5(dest)) != remote.md5:
            raise DownloadError(
                f"md5 mismatch for original file {remote.name!r} ({accession}): "
                f"expected {remote.md5}, got {actual}"
            )
    logger.info("downloaded original files for %s", accession)


# --------------------------------------------------------------------------- #
# record-level download behavior (shared by Series and BioProject)
# --------------------------------------------------------------------------- #


def _validate_srx_whitelist(values: Iterable[str] | None, *, field: str) -> set[str]:
    """Normalize + validate ``values`` into a set of ``SRX`` accessions (``None`` → empty).

    Each entry is normalized (stripped, upper-cased) and validated as a well-formed
    SRA *experiment* accession; anything that is not an ``SRX`` accession raises
    :class:`~labdata.exceptions.AccessionError`. Shared by the ``select_srx`` and
    ``original_srx`` selectors.
    """
    if values is None:
        return set()
    wanted: set[str] = set()
    for entry in values:
        accession = Experiment(entry).accession  # normalizes + validates the digits
        if not accession.startswith("SRX"):
            raise AccessionError(f"{field} expects an SRX accession, got {entry!r}")
        wanted.add(accession)
    return wanted


class _SraDownloadMixin:
    """FASTQ-download behavior shared by records that resolve SRA experiments.

    Mixed into :class:`~labdata.geo.geo_records.Series` and
    :class:`~labdata.geo.bio_project_records.BioProject`. Both expose an
    :attr:`accession` and a list of linked :attr:`experiments` — all
    :meth:`download` needs — and lay their FASTQ out identically under a
    directory named for the record's accession.
    """

    accession: str
    experiments: list[Experiment]

    def download(
        self,
        output_dir: str | Path = ".",
        *,
        ncbi_parallel: int = DEFAULT_NCBI_PARALLEL,
        cores: int | None = None,
        threads_per_run: int = DEFAULT_THREADS_PER_RUN,
        select_srx: Iterable[str] | None = None,
        original_srx: Iterable[str] | None = None,
        max_size: str | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        keep_sra: bool = False,
        prefetch_only: bool = False,
        verbose: bool = True,
    ) -> dict[str, bool]:
        """Download FASTQ for every run of every SRA experiment in this record.

        Lays the data out as ``<output_dir>/<accession>/<SRX>/<SRR>*.fastq.gz`` —
        one directory per experiment under a directory named for this record — via
        a Snakemake DAG (``prefetch → extract → compress``). Runs already marked
        done (a ``.<SRR>.success`` flag) are skipped, so this is safe to rerun on
        an unstable connection.

        Concurrency is resource-aware: at most ``ncbi_parallel`` runs ``prefetch``
        from NCBI at once (keeping the link busy without tripping throttling), while
        ``cores`` bounds total CPU across the local extraction/compression steps —
        so those two stages overlap instead of blocking each other.

        Pass ``select_srx`` to download only some of the record's experiments (see
        below).

        Parameters
        ----------
        output_dir : str or Path, default "."
            Parent directory; a ``<output_dir>/<accession>`` subtree is created for
            this record.
        ncbi_parallel : int, default :data:`DEFAULT_NCBI_PARALLEL`
            Maximum runs to ``prefetch`` (the NCBI network step) concurrently. Keep
            this modest to avoid NCBI throttling; it is independent of ``cores``.
        cores : int, optional
            Total CPU cores the DAG may use across all steps (defaults to the
            machine's CPU count). On an HPC allocation, pass the allotted cores
            (e.g. ``$SLURM_CPUS_ON_NODE``).
        threads_per_run : int, default :data:`DEFAULT_THREADS_PER_RUN`
            Threads handed to ``fasterq-dump``/``pigz`` for each run.
        select_srx : iterable of str, optional
            A whitelist of SRA experiment accessions (``SRX…``). When given, only
            the record's experiments whose accession is listed are downloaded and
            every other experiment is skipped; ``None`` (the default) downloads
            them all. Entries are matched case-insensitively and every one must be
            a well-formed ``SRX`` accession — anything else raises
            :class:`~labdata.exceptions.AccessionError`.
        original_srx : iterable of str, optional
            A set of SRA experiment accessions (``SRX…``) to fetch in *original
            format* instead of the default sra-tools path: every run of a listed
            experiment has its submitter-uploaded files downloaded as-is (no
            ``fasterq-dump``/gzip), because its ``.sra`` is missing or useless
            (e.g. a 10X run whose ``.sra`` stored only one read). This selects the
            *mode*; it does not by itself widen the selection — an experiment must
            still survive ``select_srx`` to be downloaded at all. Each entry must
            be a well-formed ``SRX`` accession or :class:`~labdata.exceptions.AccessionError`
            is raised.
        max_size : str, optional
            Passed to ``prefetch --max-size`` (defaults to :data:`DEFAULT_MAX_SIZE`);
            raise it for runs that exceed ``prefetch``'s 20G default.
        retries : int, optional
            Attempts for the resumable ``prefetch`` network step per run (defaults
            to :data:`DEFAULT_RETRIES`).
        backoff : float, optional
            Base seconds for the linear backoff between ``prefetch`` retries
            (defaults to :data:`DEFAULT_BACKOFF`).
        keep_sra : bool, default False
            Keep each run's downloaded ``.sra`` next to its FASTQ (as
            ``<accession>/<SRX>/<SRR>.sra``) instead of deleting it after extraction.
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

        Examples
        --------
        >>> Series("GSE229022").download("./fastq", ncbi_parallel=3, cores=16)  # doctest: +SKIP
        {'SRR24084454': True, 'SRR24084455': True, ...}
        >>> BioProject("PRJNA1027859").download("./fastq", ncbi_parallel=3)  # doctest: +SKIP
        {'SRR26452081': True, 'SRR26452082': True, ...}
        >>> Series("GSE229022").download("./fastq", select_srx=["SRX19885398"])  # doctest: +SKIP
        {'SRR24084454': True}
        """
        # ``None`` overrides fall through to the sra-tools defaults.
        overrides = {
            key: value
            for key, value in {"max_size": max_size, "retries": retries, "backoff": backoff}.items()
            if value is not None
        }
        # Original-format experiments select the download *mode*, not the selection.
        original_set = _validate_srx_whitelist(original_srx, field="original_srx")
        base = Path(output_dir) / self.accession
        tasks: list[tuple[Run, Path]] = []
        original: set[str] = set()
        for experiment in self._selected_experiments(select_srx):
            srx_dir = base / experiment.accession
            srx_dir.mkdir(parents=True, exist_ok=True)
            is_original = experiment.accession in original_set
            for run in experiment.runs:
                tasks.append((run, srx_dir))
                if is_original:
                    original.add(run.accession)
        return _run_plan(
            self.accession,
            tasks,
            output_root=base,
            verbose=verbose,
            ncbi_parallel=ncbi_parallel,
            cores=cores,
            threads_per_run=threads_per_run,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
            original=original,
            **overrides,
        )

    def _selected_experiments(self, select_srx: Iterable[str] | None) -> list[Experiment]:
        """Return this record's experiments, restricted to the ``select_srx`` whitelist.

        ``None`` selects every experiment. Otherwise each whitelist entry must be a
        well-formed ``SRX`` experiment accession — it is normalized (stripped and
        upper-cased) and validated, and anything that is not an ``SRX`` accession
        raises :class:`~labdata.exceptions.AccessionError`. Experiments whose
        accession is not listed are then dropped.
        """
        if select_srx is None:
            return self.experiments
        wanted = _validate_srx_whitelist(select_srx, field="select_srx")
        return [experiment for experiment in self.experiments if experiment.accession in wanted]


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
    >>> SraDownloader(Experiment("SRX5921017"), "./fastq").download(ncbi_parallel=2)  # doctest: +SKIP
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
        *,
        ncbi_parallel: int = DEFAULT_NCBI_PARALLEL,
        cores: int | None = None,
        threads_per_run: int = DEFAULT_THREADS_PER_RUN,
        max_size: str = DEFAULT_MAX_SIZE,
        retries: int = DEFAULT_RETRIES,
        backoff: float = DEFAULT_BACKOFF,
        keep_sra: bool = False,
        prefetch_only: bool = False,
        original: bool = False,
        verbose: bool = True,
    ) -> dict[str, bool]:
        """Download every run of the experiment as a resource-aware Snakemake DAG.

        Creates :attr:`srx_dir`, then downloads each run's gzipped FASTQ into it.
        Runs with an existing ``.<SRR>.done`` marker are skipped; a run that fails
        is recorded as ``False`` without aborting the others.

        Pass ``original=True`` to fetch this experiment's *original-format* files
        instead — each run's submitter-uploaded files are downloaded as-is into
        ``<SRX>/<SRR>/`` (no ``fasterq-dump``/gzip), for runs whose ``.sra`` is
        missing or useless.

        Parameters
        ----------
        ncbi_parallel : int, default :data:`DEFAULT_NCBI_PARALLEL`
            Maximum runs to ``prefetch`` (the NCBI network step) concurrently;
            independent of ``cores``. Keep it modest to avoid NCBI throttling.
        cores : int, optional
            Total CPU cores the DAG may use across all steps (defaults to the
            machine's CPU count).
        threads_per_run : int, default :data:`DEFAULT_THREADS_PER_RUN`
            Threads handed to ``fasterq-dump``/``pigz`` for each run.
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
        original : bool, default False
            Fetch the experiment's original-format files (download only) instead of
            the ``prefetch → extract → compress`` chain; see above.
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
        original_runs = {run.accession for run in self.experiment.runs} if original else set()
        # output_root is the parent of the SRX dir so the run->srx map (and the
        # Snakefile's ``{srx}`` wildcard) sees the experiment accession as a single
        # path component, matching the whole-record layout.
        return _run_plan(
            self.experiment.accession,
            tasks,
            output_root=self.output_dir,
            verbose=verbose,
            ncbi_parallel=ncbi_parallel,
            cores=cores,
            threads_per_run=threads_per_run,
            max_size=max_size,
            retries=retries,
            backoff=backoff,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
            original=original_runs,
        )
