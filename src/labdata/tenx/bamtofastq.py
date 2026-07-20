r"""Per-run cellranger-BAM → FASTQ stage behind the Snakemake tenx DAG.

This is a 10x-specific counterpart to :mod:`labdata.geo.sratools`. When a 10x run's
SRA-normalized ``.sra`` is broken (e.g. it dropped the technical reads, leaving only
R1), the usable source is the submitter's original **cellranger BAM**, which
:mod:`labdata.geo` can fetch with the original-format download path. Turning that BAM
back into correctly-split FASTQ requires 10x Genomics' own ``bamtofastq`` — a generic
BAM→FASTQ tool cannot decode the 10x tags / ``@RG`` headers that separate the barcode
(R1), cDNA (R2), and index (I1) reads, and would corrupt the read structure for a
downstream STARsolo remap.

Every subprocess call is funnelled through the single :func:`_run` helper — the one
place tests monkeypatch — so the stage (BAM discovery, the tool call, flattening its
nested output into the download-style layout) is exercised without the ``bamtofastq``
binary. Orchestration — scheduling this stage across many runs — lives in
:mod:`labdata.tenx.pipeline` (Snakemake); :class:`TenxConverter` drives that DAG over a
whole :class:`~labdata.geo.geo_records.Series` or
:class:`~labdata.geo.bio_project_records.BioProject`, auto-detecting which runs carry a
10x BAM.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import TextIO

from labdata.exceptions import AccessionError, DownloadError
from labdata.geo import BioProject, Experiment, Run, Series
from labdata.tenx import pipeline

logger = logging.getLogger(__name__)

#: Default threads handed to ``bamtofastq`` (``--nthreads``) per run.
DEFAULT_THREADS_PER_RUN = 4
#: Default reads per output FASTQ chunk (``bamtofastq --reads-per-fastq``); the tool's
#: own default. Larger values mean fewer, bigger chunk files.
DEFAULT_READS_PER_FASTQ = 50_000_000


# --------------------------------------------------------------------------- #
# the external-tool seam (the one place subprocesses are launched)
# --------------------------------------------------------------------------- #


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    """Run an external command, raising :class:`DownloadError` on failure.

    The single external-tool seam for the conversion: 10x Genomics' ``bamtofastq``
    goes through here, and tests monkeypatch this one function.

    Parameters
    ----------
    cmd : list of str
        The command and its arguments (``cmd[0]`` is the executable).
    cwd : Path or None
        Working directory for the command, if any.

    Raises
    ------
    DownloadError
        If the executable is not installed (``FileNotFoundError``) or it exits with a
        non-zero status. The captured ``stderr`` is included in the message.
    """
    try:
        subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    except FileNotFoundError as err:
        raise DownloadError(
            f"{cmd[0]!r} not found — install 10x Genomics bamtofastq (e.g. via pixi/conda)"
        ) from err
    except subprocess.CalledProcessError as err:
        raise DownloadError(
            f"{cmd[0]!r} failed (exit {err.returncode}): {(err.stderr or '').strip()}"
        ) from err


# --------------------------------------------------------------------------- #
# auto-detection of 10x-BAM runs
# --------------------------------------------------------------------------- #


def is_tenx_bam_run(run: Run) -> bool:
    """Return whether ``run`` has an original-format 10x cellranger BAM.

    Inspects the run's original/submitted files (via the SRA Data Locator) for a BAM:
    an entry whose SDL type is ``TenX``/``bam`` or whose name ends in ``.bam``. This is
    the signal that the run should be recovered from its cellranger BAM rather than the
    (possibly broken) SRA archive — the runs :class:`TenxConverter` selects by default.

    Parameters
    ----------
    run : Run
        The SRA run to inspect.

    Returns
    -------
    bool
        ``True`` if the run lists an original-format BAM, else ``False``.
    """
    return any(
        file.type.lower() in {"tenx", "bam"} or file.name.lower().endswith(".bam")
        for file in run.original_files
    )


# --------------------------------------------------------------------------- #
# per-run worker (the unit of parallelism)
# --------------------------------------------------------------------------- #


def _scratch_dir(srx_dir: Path, accession: str) -> Path:
    """Return the scratch directory ``bamtofastq`` writes its nested output into."""
    return srx_dir / f".{accession}.b2f"


def _find_bam(srx_dir: Path, accession: str) -> Path:
    """Return the single on-disk original BAM for ``accession`` under ``srx_dir``.

    The original-format download lays a run's submitted files flat under
    ``srx_dir`` with SRR-prefixed names; the cellranger BAM is the ``<SRR>_*.bam``
    there.

    Raises
    ------
    DownloadError
        If no BAM (or more than one) is found — the former usually means the
        original-format download has not been run yet.
    """
    bams = sorted(srx_dir.glob(f"{accession}_*.bam"))
    if not bams:
        raise DownloadError(
            f"no cellranger BAM for {accession!r} under {srx_dir} — run "
            f"`labdata geo download … --original-srx {srx_dir.name}` first"
        )
    if len(bams) > 1:
        names = ", ".join(bam.name for bam in bams)
        raise DownloadError(f"multiple BAMs for {accession!r} under {srx_dir}: {names}")
    return bams[0]


def _flatten_output(scratch: Path, srx_dir: Path, accession: str) -> list[Path]:
    """Move ``bamtofastq``'s nested FASTQ up into ``srx_dir`` with SRR-prefixed names.

    ``bamtofastq`` writes ``<scratch>/<library>/bamtofastq_S1_L00N_R{1,2}_00N.fastq.gz``.
    Each file is moved to ``<srx_dir>/<SRR>_S1_L00N_R{1,2}_00N.fastq.gz`` — the leading
    ``bamtofastq`` token replaced by the run accession — so the result sits flat under
    the experiment dir (globbing as ``<SRX>/<SRR>*.fastq.gz`` like the download output)
    while keeping the ``R1``/``R2``/``I1`` and lane markers STARsolo needs. When the BAM
    yields more than one library subdir, the subdir name is folded into the prefix to
    keep names unique.

    Returns
    -------
    list of Path
        The moved FASTQ files, in name order.

    Raises
    ------
    DownloadError
        If ``bamtofastq`` produced no FASTQ — the BAM lacked the 10x/``@RG`` headers
        the tool needs (e.g. an SRA-normalized BAM rather than the original format).
    """
    fastqs = sorted(scratch.rglob("*.fastq.gz"))
    if not fastqs:
        raise DownloadError(
            f"bamtofastq produced no FASTQ for {accession!r} — is the BAM the original "
            f"cellranger format (with 10x tags and @RG headers)?"
        )
    multi_library = len({fastq.parent for fastq in fastqs}) > 1
    moved: list[Path] = []
    for fastq in fastqs:
        # Strip the tool's constant leading token, keeping the "_S1_L001_R1_001..." tail.
        tail = (
            fastq.name[len("bamtofastq") :] if fastq.name.startswith("bamtofastq") else fastq.name
        )
        prefix = f"{accession}_{fastq.parent.name}" if multi_library else accession
        dest = srx_dir / f"{prefix}{tail}"
        shutil.move(str(fastq), str(dest))
        moved.append(dest)
    return moved


def bamtofastq_run(
    run: Run,
    srx_dir: Path,
    *,
    threads: int = DEFAULT_THREADS_PER_RUN,
    reads_per_fastq: int = DEFAULT_READS_PER_FASTQ,
    remove_bam: bool = False,
) -> list[Path]:
    """Convert one run's on-disk cellranger BAM to FASTQ with ``bamtofastq``.

    The single stage of the tenx DAG. Locates the run's original-format BAM
    (``<srx_dir>/<SRR>_*.bam``), runs 10x's ``bamtofastq`` into a scratch dir, then
    flattens that nested output into ``<srx_dir>/<SRR>_S1_L00N_R{1,2}_00N.fastq.gz`` (see
    :func:`_flatten_output`) and removes the scratch. Idempotent: a stale scratch from
    an interrupted run is cleared first (``bamtofastq`` refuses a pre-existing output
    dir), so a rerun reconverts cleanly.

    Parameters
    ----------
    run : Run
        The run whose BAM to convert.
    srx_dir : Path
        The experiment directory holding ``<SRR>_<name>.bam``; the FASTQ land here.
    threads : int, default :data:`DEFAULT_THREADS_PER_RUN`
        Threads passed to ``bamtofastq --nthreads``.
    reads_per_fastq : int, default :data:`DEFAULT_READS_PER_FASTQ`
        Reads per output chunk (``bamtofastq --reads-per-fastq``).
    remove_bam : bool, default False
        Delete the source BAM once the FASTQ are produced. The deletion happens only
        *after* a successful conversion, so an interrupted run leaves the BAM in place
        for a resumed rerun to reconvert from.

    Returns
    -------
    list of Path
        The converted FASTQ files written under ``srx_dir``.

    Raises
    ------
    DownloadError
        If the BAM is missing/ambiguous, ``bamtofastq`` is missing or fails, or it
        produced no FASTQ (a non-original BAM).
    """
    accession = run.accession
    bam = _find_bam(srx_dir, accession)
    scratch = _scratch_dir(srx_dir, accession)
    if scratch.exists():
        shutil.rmtree(scratch)  # bamtofastq refuses an existing output dir
    _run(
        [
            "bamtofastq",
            f"--nthreads={threads}",
            f"--reads-per-fastq={reads_per_fastq}",
            str(bam),
            str(scratch),
        ]
    )
    moved = _flatten_output(scratch, srx_dir, accession)
    shutil.rmtree(scratch, ignore_errors=True)
    if remove_bam:
        bam.unlink(missing_ok=True)  # only reached on success — the FASTQ now stand in
    logger.info("converted %s (%d FASTQ)", accession, len(moved))
    return moved


# --------------------------------------------------------------------------- #
# user-facing progress (a conversion plan up front, a tally at the end)
# --------------------------------------------------------------------------- #


def _print_plan(
    label: str,
    tasks: list[tuple[Run, Path]],
    *,
    cores: int,
    output_root: Path,
    stream: TextIO | None = None,
) -> None:
    """Print the conversion plan: destination, run/experiment counts, per-SRX runs.

    Groups ``tasks`` by their experiment directory and notes how many runs are already
    converted (a ``.<SRR>.tenx.done`` marker present, so will be skipped).
    """
    out = stream if stream is not None else sys.stderr
    if not tasks:
        print(f"{label}: no 10x-BAM runs to convert.", file=out, flush=True)
        return

    groups: dict[Path, list[str]] = {}
    already = 0
    for run, srx_dir in tasks:
        groups.setdefault(srx_dir, []).append(run.accession)
        if (srx_dir / f".{run.accession}.tenx.done").exists():
            already += 1

    n_runs = len(tasks)
    runs_word = "run" if n_runs == 1 else "runs"
    exp_word = "experiment" if len(groups) == 1 else "experiments"
    print(
        f"{label} → {output_root}  [bamtofastq {n_runs} {runs_word}, "
        f"{len(groups)} {exp_word}, cores={cores}]",
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


# --------------------------------------------------------------------------- #
# record-level converter (over a whole Series / BioProject)
# --------------------------------------------------------------------------- #


def _as_record(accession_or_record: str | Series | BioProject) -> Series | BioProject:
    """Return a downloadable record for ``accession_or_record``, auto-detecting kind.

    A record is passed through unchanged. A ``PRJNA…``/``PRJEB…``/``PRJDB…`` accession
    builds a :class:`~labdata.geo.bio_project_records.BioProject`; anything else is
    treated as a GEO Series and validated by :class:`~labdata.geo.geo_records.Series`.
    """
    if isinstance(accession_or_record, Series | BioProject):
        return accession_or_record
    if accession_or_record.strip().upper().startswith("PRJ"):
        return BioProject(accession_or_record)
    return Series(accession_or_record)


def _tasks_from_disk(base: Path, wanted: set[str] | None) -> list[tuple[Run, Path]]:
    """Build ``(run, srx_dir)`` tasks by scanning ``base`` for on-disk cellranger BAMs.

    Walks ``base/<SRX>/`` and includes every run whose SRR-prefixed ``<SRR>_*.bam``
    sits flat in the experiment dir (the original-format download layout), needing no
    network. The run accession is the leading ``<SRR>`` token of the BAM name. Hidden
    dirs (e.g. the ``.labdata`` control dir) are skipped, as are names that are not
    well-formed ``SRX``/``SRR`` accessions. When ``wanted`` is given, only experiments
    whose accession is listed are considered.

    Parameters
    ----------
    base : Path
        The ``<output_dir>/<accession>`` root the original-format download wrote into.
    wanted : set of str or None
        SRX accessions to keep (``None`` keeps all).

    Returns
    -------
    list of (Run, Path)
        One task per run with a BAM, in accession order.
    """
    if not base.is_dir():
        return []
    tasks: list[tuple[Run, Path]] = []
    for srx_dir in sorted(p for p in base.iterdir() if p.is_dir() and not p.name.startswith(".")):
        if wanted is not None and srx_dir.name not in wanted:
            continue
        seen: set[str] = set()
        for bam in sorted(srx_dir.glob("*.bam")):
            srr = bam.name.split("_", 1)[0]  # names are "<SRR>_<submitter-name>.bam"
            if srr in seen:
                continue
            try:
                run = Run(srr)  # validates the SRR accession shape
            except AccessionError:
                continue
            seen.add(srr)
            tasks.append((run, srx_dir))
    return tasks


def _validate_srx_whitelist(values: Iterable[str] | None) -> set[str] | None:
    """Normalize + validate ``values`` into a set of ``SRX`` accessions (``None`` → ``None``).

    Each entry is normalized and validated as a well-formed SRA *experiment* accession;
    anything that is not an ``SRX`` accession raises :class:`AccessionError`.
    """
    if values is None:
        return None
    wanted: set[str] = set()
    for entry in values:
        accession = Experiment(entry).accession  # normalizes + validates the digits
        if not accession.startswith("SRX"):
            raise AccessionError(f"select_srx expects an SRX accession, got {entry!r}")
        wanted.add(accession)
    return wanted


class TenxConverter:
    r"""Convert a project's 10x cellranger BAMs to FASTQ with ``bamtofastq``.

    Drives the tenx Snakemake DAG over every 10x-BAM run of a GEO Series or BioProject:
    for each run whose original files include a cellranger BAM (see
    :func:`is_tenx_bam_run`), the already-downloaded BAM is converted to FASTQ laid out
    like the download pipeline (``<accession>/<SRX>/<SRR>_S1_L00N_R{1,2}_00N.fastq.gz``).
    The BAMs must already be on disk from a prior original-format download
    (``labdata geo download … --original-srx``); this step does not fetch them.

    Parameters
    ----------
    accession_or_record : str, Series, or BioProject
        The project to convert. A ``GSE…`` / ``PRJ…`` accession is resolved to the
        matching record; a record is used as-is.
    output_dir : str or Path, default "."
        Parent directory; the ``<output_dir>/<accession>`` subtree (the same one the
        original-format download wrote the BAMs into) is read from and written to.

    Examples
    --------
    >>> TenxConverter("GSE208154", "./data").convert(cores=16)  # doctest: +SKIP
    {'SRR20172067': True, ...}
    """

    def __init__(
        self, accession_or_record: str | Series | BioProject, output_dir: str | Path = "."
    ) -> None:
        self.record = _as_record(accession_or_record)
        self.output_dir = Path(output_dir)

    def __repr__(self) -> str:
        """Return an unambiguous representation."""
        return f"{type(self).__name__}({self.record.accession!r}, {str(self.output_dir)!r})"

    def convert(
        self,
        *,
        cores: int | None = None,
        threads_per_run: int = DEFAULT_THREADS_PER_RUN,
        reads_per_fastq: int = DEFAULT_READS_PER_FASTQ,
        select_srx: Iterable[str] | None = None,
        all_runs: bool = False,
        from_disk: bool = False,
        remove_bam: bool = False,
        verbose: bool = True,
    ) -> dict[str, bool]:
        """Convert every 10x-BAM run of this record's experiments to FASTQ.

        Auto-detects which runs carry a cellranger BAM (:func:`is_tenx_bam_run`) and
        converts each via a Snakemake DAG, laying the FASTQ out as
        ``<output_dir>/<accession>/<SRX>/<SRR>_S1_L00N_R{1,2}_00N.fastq.gz``. Runs
        already marked done (a ``.<SRR>.tenx.done`` marker) are skipped, so this is safe
        to rerun.

        Parameters
        ----------
        cores : int, optional
            Total CPU cores the DAG may use (defaults to the machine's CPU count). On an
            HPC allocation, pass the allotted cores (e.g. ``$SLURM_CPUS_ON_NODE``).
        threads_per_run : int, default :data:`DEFAULT_THREADS_PER_RUN`
            Threads handed to ``bamtofastq`` per run (``--nthreads``).
        reads_per_fastq : int, default :data:`DEFAULT_READS_PER_FASTQ`
            Reads per output FASTQ chunk (``bamtofastq --reads-per-fastq``).
        select_srx : iterable of str, optional
            A whitelist of SRA experiment accessions (``SRX…``). When given, only those
            experiments are considered; ``None`` (the default) considers them all. Each
            entry must be a well-formed ``SRX`` accession or :class:`AccessionError` is
            raised.
        all_runs : bool, default False
            Convert every run of the selected experiments, skipping the 10x-BAM
            auto-detection. Use when you know all runs carry a cellranger BAM (e.g. to
            avoid the extra SDL lookups) or to force conversion of a run SDL does not
            flag.
        from_disk : bool, default False
            Build the task list by scanning the on-disk tree
            (``<output_dir>/<accession>/<SRX>/<SRR>_*.bam``) instead of resolving the
            record through NCBI. This needs no network or NCBI credentials — the point
            when converting already-downloaded BAMs on an offline HPC compute node.
            ``select_srx`` still filters by experiment; ``all_runs`` and the SDL
            auto-detection do not apply (a BAM's presence on disk is the signal).
        remove_bam : bool, default False
            Delete each run's source BAM once its FASTQ are produced, reclaiming the
            large original. Deletion happens only after a run converts successfully.
        verbose : bool, default True
            Print a conversion plan up front and a summary to ``stderr``. Set ``False``
            to run silently.

        Returns
        -------
        dict[str, bool]
            Maps each converted run accession to whether its FASTQ are present on
            completion (empty if there are no 10x-BAM runs).
        """
        base = self.output_dir / self.record.accession
        if from_disk:
            tasks = _tasks_from_disk(base, _validate_srx_whitelist(select_srx))
        else:
            tasks = [
                (run, base / experiment.accession)
                for experiment in self._selected_experiments(select_srx)
                for run in experiment.runs
                if all_runs or is_tenx_bam_run(run)
            ]
        for srx_dir in {srx_dir for _, srx_dir in tasks}:
            srx_dir.mkdir(parents=True, exist_ok=True)
        return _run_conversion(
            self.record.accession,
            tasks,
            output_root=base,
            cores=cores,
            threads_per_run=threads_per_run,
            reads_per_fastq=reads_per_fastq,
            remove_bam=remove_bam,
            verbose=verbose,
        )

    def _selected_experiments(self, select_srx: Iterable[str] | None) -> list[Experiment]:
        """Return this record's experiments, restricted to the ``select_srx`` whitelist."""
        wanted = _validate_srx_whitelist(select_srx)
        if wanted is None:
            return self.record.experiments
        return [exp for exp in self.record.experiments if exp.accession in wanted]


def _run_conversion(
    label: str,
    tasks: list[tuple[Run, Path]],
    *,
    output_root: Path,
    cores: int | None,
    threads_per_run: int,
    reads_per_fastq: int,
    remove_bam: bool,
    verbose: bool,
) -> dict[str, bool]:
    """Announce the plan, run the tenx Snakemake DAG, then tally the results."""
    resolved_cores = cores if cores is not None else (os.cpu_count() or 1)
    if verbose:
        _print_plan(label, tasks, cores=resolved_cores, output_root=output_root)
    results = pipeline.run(
        tasks,
        output_root=output_root,
        cores=resolved_cores,
        threads_per_run=threads_per_run,
        reads_per_fastq=reads_per_fastq,
        remove_bam=remove_bam,
        verbose=verbose,
    )
    if verbose and tasks:
        _print_summary(results)
    return results
