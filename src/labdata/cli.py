"""Command-line interface — a thin Typer wrapper over the labdata API.

Logic lives in :mod:`labdata.ncbi`, :mod:`labdata.geo`, etc.; this module only
translates arguments, dispatches, and chooses an output format.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import typer

from labdata import BioProject, Run, Series, TenxConverter, experiments_for
from labdata import __version__ as _package_version
from labdata.exceptions import LabdataError
from labdata.geo import sratools
from labdata.ncbi.config import (
    NcbiCredentials,
    config_path,
    prompt_and_save,
    save_credentials,
)
from labdata.tenx import bamtofastq

app = typer.Typer(
    help="Liu Lab data curation, download, and organization utilities.",
    no_args_is_help=True,
)
ncbi_app = typer.Typer(help="Manage NCBI E-utilities credentials.", no_args_is_help=True)
app.add_typer(ncbi_app, name="ncbi")
geo_app = typer.Typer(help="Download and organize GEO/SRA data.", no_args_is_help=True)
app.add_typer(geo_app, name="geo")
tenx_app = typer.Typer(
    help="10x Genomics special handling (cellranger BAM → FASTQ).", no_args_is_help=True
)
app.add_typer(tenx_app, name="tenx")


@app.command()
def version() -> None:
    """Print the installed package version."""
    typer.echo(_package_version)


@ncbi_app.command("configure")
def ncbi_configure(
    email: str = typer.Option("", "--email", help="NCBI contact email (prompts if omitted)."),
    api_key: str = typer.Option("", "--api-key", help="Optional NCBI API key."),
) -> None:
    """Store NCBI E-utilities credentials in the cache file.

    With ``--email`` the value is saved non-interactively; otherwise you are
    prompted for the email (and an optional API key).
    """
    if email:
        creds = NcbiCredentials(email=email, api_key=api_key or None)
        path = save_credentials(creds)
    else:
        creds = prompt_and_save()
        path = config_path()
    typer.echo(f"Saved NCBI credentials for {creds.email} to {path}")


def _record_for_download(accession: str) -> Series | BioProject:
    """Return the downloadable record for ``accession``, auto-detecting its kind.

    A BioProject accession (``PRJNA…``/``PRJEB…``/``PRJDB…``) builds a
    :class:`~labdata.BioProject`; anything else is treated as a GEO Series and
    validated by :class:`~labdata.Series` (which raises on a malformed accession).
    Both expose the same ``download`` signature, so the caller need not care which.
    """
    if accession.strip().upper().startswith("PRJ"):
        return BioProject(accession)
    return Series(accession)


def _resolve_select_srx(select_srx: list[str]) -> list[str] | None:
    """Resolve the ``--select-srx`` values into an SRX whitelist, or ``None``.

    The values are taken literally, except that a single value naming an existing
    file is read as a whitelist file — one accession per line, with blank lines and
    ``#`` comments ignored. Returns ``None`` when nothing was supplied, so the whole
    record is downloaded rather than nothing.
    """
    if not select_srx:
        return None
    if len(select_srx) == 1:
        candidate = Path(select_srx[0])
        if candidate.is_file():
            accessions: list[str] = []
            for line in candidate.read_text().splitlines():
                token = line.strip()
                if token and not token.startswith("#"):
                    accessions.append(token)
            return accessions
    return list(select_srx)


@geo_app.command("download")
def geo_download(
    accession: str = typer.Argument(
        ..., help="GEO Series (e.g. GSE131907) or BioProject (e.g. PRJNA1027859) accession."
    ),
    output_dir: Path = typer.Option(
        Path(), "--output", "-o", help="Parent directory; a <GSE>/ subtree is created in it."
    ),
    ncbi_parallel: int = typer.Option(
        sratools.DEFAULT_NCBI_PARALLEL,
        "--ncbi-parallel",
        "-j",
        min=1,
        help="Max concurrent prefetch (NCBI network) jobs; kept low to avoid throttling.",
    ),
    cores: int = typer.Option(
        0, "--cores", min=0, help="Total CPU cores for the DAG (0 = the machine's CPU count)."
    ),
    threads_per_run: int = typer.Option(
        sratools.DEFAULT_THREADS_PER_RUN,
        "--threads-per-run",
        min=1,
        help="Threads for fasterq-dump/pigz per run.",
    ),
    select_srx: list[str] = typer.Option(
        [],
        "--select-srx",
        help="Whitelist of SRX experiment accessions to download; repeatable. A "
        "single value that is an existing file is read as a whitelist file (one "
        "accession per line). Omit to download every experiment.",
    ),
    original_srx: list[str] = typer.Option(
        [],
        "--original-srx",
        help="SRX experiment accessions to fetch in ORIGINAL FORMAT (submitter's "
        "own files, download only) instead of the sra-tools path; repeatable, or a "
        "single value naming a whitelist file. Use when the .sra is missing/useless "
        "(e.g. a 10X run). Sets the mode only — still subject to --select-srx.",
    ),
    max_size: str = typer.Option(
        "", "--max-size", help="prefetch --max-size, e.g. 500G (defaults to 200G)."
    ),
    retries: int = typer.Option(
        0, "--retries", min=0, help="prefetch attempts on failure (0 keeps the default)."
    ),
    backoff: float = typer.Option(
        0.0, "--backoff", min=0.0, help="Base seconds for retry backoff (0 keeps the default)."
    ),
    keep_sra: bool = typer.Option(
        False, "--keep-sra", help="Keep each run's .sra file next to its FASTQ."
    ),
    prefetch_only: bool = typer.Option(
        False, "--prefetch-only", help="Only prefetch each run's .sra; skip extraction/gzip."
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress the download plan/progress."),
) -> None:
    """Download FASTQ for a whole GEO Series or BioProject (every SRX/SRR) as a Snakemake DAG.

    The accession kind is auto-detected: a GEO Series (``GSE…``) or a BioProject
    (``PRJ…``) both work. Lays the data out as
    ``<output>/<accession>/<SRX>/<SRR>*.fastq.gz`` and skips runs that are already
    complete, so it is safe to rerun. Exits non-zero if any run fails.

    Concurrency is resource-aware: ``--ncbi-parallel`` caps concurrent prefetch (the
    NCBI network step) while ``--cores`` bounds total CPU across extraction/gzip, so
    the two overlap. On an HPC allocation, pass ``--cores $SLURM_CPUS_ON_NODE``.

    Use ``--select-srx`` (repeatable, or a whitelist file) to download only some of
    the record's experiments; without it every experiment is downloaded. Use
    ``--original-srx`` to fetch specific experiments in original format (download
    only) — for runs whose ``.sra`` is missing or useless.
    """
    try:
        results = _record_for_download(accession).download(
            output_dir,
            ncbi_parallel=ncbi_parallel,
            cores=cores or None,
            threads_per_run=threads_per_run,
            select_srx=_resolve_select_srx(select_srx),
            original_srx=_resolve_select_srx(original_srx),
            max_size=max_size or None,
            retries=retries or None,
            backoff=backoff or None,
            keep_sra=keep_sra,
            prefetch_only=prefetch_only,
            verbose=not quiet,
        )
    except LabdataError as err:
        typer.secho(f"error: {err}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from err

    failed = sorted(acc for acc, ok in results.items() if not ok)
    if failed:
        typer.secho(
            f"{len(failed)} run(s) failed: {', '.join(failed)}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=1)


@geo_app.command("experiments")
def geo_experiments(
    accession: str = typer.Argument(
        ..., help="Any GEO/SRA/BioProject accession (GSE, GSM, PRJNA, SRP, SRS, SAMN, SRX, SRR)."
    ),
) -> None:
    """Print the SRA experiment accessions under ACCESSION, one per line.

    Resolves a GEO Series/Sample, BioProject, study, sample or BioSample down to its SRA
    experiments — traversing a GEO SuperSeries, which owns no runs of its own. Prints nothing (and
    still exits 0) for a record that carries no raw SRA data.
    """
    try:
        experiments = experiments_for(accession)
    except LabdataError as err:
        typer.secho(f"error: {err}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from err
    for experiment in experiments:
        typer.echo(experiment.accession)


# --------------------------------------------------------------------------- #
# hidden per-stage subcommands — the bridge the Snakemake DAG (pipeline.smk)
# shells back into; not part of the public CLI surface.
# --------------------------------------------------------------------------- #


def _run_stage(stage: Callable[[], object]) -> None:
    """Run one pipeline stage, exiting non-zero on failure so Snakemake sees it.

    The stage's return value is ignored — stages act by their filesystem side effects;
    only a raised :class:`LabdataError` matters here.
    """
    try:
        stage()
    except LabdataError as err:
        typer.secho(f"error: {err}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from err


@geo_app.command("_prefetch", hidden=True)
def geo_prefetch_stage(
    srr: str = typer.Argument(..., help="Run accession (SRR/ERR/DRR)."),
    srx_dir: Path = typer.Argument(..., help="Experiment directory to fetch into."),
    max_size: str = typer.Option(sratools.DEFAULT_MAX_SIZE, "--max-size"),
    retries: int = typer.Option(sratools.DEFAULT_RETRIES, "--retries", min=1),
    backoff: float = typer.Option(sratools.DEFAULT_BACKOFF, "--backoff", min=0.0),
) -> None:
    """Prefetch one run's .sra — the network stage of the download DAG (internal)."""
    _run_stage(
        lambda: sratools.prefetch_run(
            Run(srr), srx_dir, max_size=max_size, retries=retries, backoff=backoff
        )
    )


@geo_app.command("_extract", hidden=True)
def geo_extract_stage(
    srr: str = typer.Argument(..., help="Run accession (SRR/ERR/DRR)."),
    srx_dir: Path = typer.Argument(..., help="Experiment directory holding the .sra."),
    threads: int = typer.Option(1, "--threads", min=1),
    staging: Path = typer.Option(None, "--staging", help="Directory for the uncompressed FASTQ."),
    keep_sra: bool = typer.Option(False, "--keep-sra"),
) -> None:
    """Extract one run's FASTQ with fasterq-dump — a stage of the download DAG (internal)."""
    _run_stage(
        lambda: sratools.extract_run(
            Run(srr), srx_dir, threads=threads, staging=staging, keep_sra=keep_sra
        )
    )


@geo_app.command("_compress", hidden=True)
def geo_compress_stage(
    srr: str = typer.Argument(..., help="Run accession (SRR/ERR/DRR)."),
    srx_dir: Path = typer.Argument(..., help="Experiment directory the FASTQ.gz go into."),
    threads: int = typer.Option(1, "--threads", min=1),
    staging: Path = typer.Option(
        None, "--staging", help="Directory holding the uncompressed FASTQ."
    ),
) -> None:
    """Gzip one run's FASTQ from staging — the final stage of the download DAG (internal)."""
    _run_stage(lambda: sratools.compress_run(Run(srr), srx_dir, threads=threads, staging=staging))


@geo_app.command("_original", hidden=True)
def geo_original_stage(
    srr: str = typer.Argument(..., help="Run accession (SRR/ERR/DRR)."),
    srx_dir: Path = typer.Argument(..., help="Experiment directory to download into."),
    retries: int = typer.Option(sratools.DEFAULT_RETRIES, "--retries", min=1),
    backoff: float = typer.Option(sratools.DEFAULT_BACKOFF, "--backoff", min=0.0),
) -> None:
    """Download one run's original-format files with curl — a download-only DAG stage (internal)."""
    _run_stage(
        lambda: sratools.download_original_run(Run(srr), srx_dir, retries=retries, backoff=backoff)
    )


# --------------------------------------------------------------------------- #
# tenx: cellranger BAM → FASTQ
# --------------------------------------------------------------------------- #


@tenx_app.command("bamtofastq")
def tenx_bamtofastq(
    accession: str = typer.Argument(
        ..., help="GEO Series (e.g. GSE208154) or BioProject (e.g. PRJNA…) accession."
    ),
    output_dir: Path = typer.Option(
        Path(),
        "--output",
        "-o",
        help="Parent dir holding the already-downloaded BAMs (a <GSE>/ subtree).",
    ),
    cores: int = typer.Option(
        0, "--cores", min=0, help="Total CPU cores for the DAG (0 = the machine's CPU count)."
    ),
    threads_per_run: int = typer.Option(
        bamtofastq.DEFAULT_THREADS_PER_RUN,
        "--threads-per-run",
        min=1,
        help="Threads for bamtofastq per run (--nthreads).",
    ),
    reads_per_fastq: int = typer.Option(
        bamtofastq.DEFAULT_READS_PER_FASTQ,
        "--reads-per-fastq",
        min=1,
        help="Reads per output FASTQ chunk (bamtofastq --reads-per-fastq).",
    ),
    select_srx: list[str] = typer.Option(
        [],
        "--select-srx",
        help="Whitelist of SRX experiment accessions to consider; repeatable, or a "
        "single value naming a whitelist file. Omit to consider every experiment.",
    ),
    all_runs: bool = typer.Option(
        False,
        "--all-runs",
        help="Convert every run of the selected experiments, skipping 10x-BAM auto-detection.",
    ),
    from_disk: bool = typer.Option(
        False,
        "--from-disk",
        help="Find runs by scanning the on-disk <SRX>/<SRR>_*.bam tree instead of NCBI "
        "(no network/credentials — for offline HPC conversion of downloaded BAMs).",
    ),
    remove_bam: bool = typer.Option(
        False,
        "--remove-bam",
        help="Delete each run's source BAM once its FASTQ are produced (only after success).",
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress the conversion plan/summary."
    ),
) -> None:
    """Convert a project's already-downloaded cellranger BAMs to FASTQ with bamtofastq.

    Auto-detects which runs carry a 10x cellranger BAM and converts each (the BAM must
    already be on disk from ``labdata geo download … --original-srx``). Lays the FASTQ
    out like the download pipeline —
    ``<output>/<accession>/<SRX>/<SRR>_S1_L00N_R{1,2}_00N.fastq.gz`` — and skips runs
    already converted, so it is safe to rerun. Exits non-zero if any run fails.

    On an offline HPC compute node, pass ``--from-disk`` so the run list is built by
    scanning the downloaded BAM tree rather than querying NCBI.
    """
    try:
        results = TenxConverter(accession, output_dir).convert(
            cores=cores or None,
            threads_per_run=threads_per_run,
            reads_per_fastq=reads_per_fastq,
            select_srx=_resolve_select_srx(select_srx),
            all_runs=all_runs,
            from_disk=from_disk,
            remove_bam=remove_bam,
            verbose=not quiet,
        )
    except LabdataError as err:
        typer.secho(f"error: {err}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from err

    failed = sorted(acc for acc, ok in results.items() if not ok)
    if failed:
        typer.secho(
            f"{len(failed)} run(s) failed: {', '.join(failed)}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=1)


@tenx_app.command("_bamtofastq", hidden=True)
def tenx_bamtofastq_stage(
    srr: str = typer.Argument(..., help="Run accession (SRR/ERR/DRR)."),
    srx_dir: Path = typer.Argument(..., help="Experiment directory holding <SRR>_<name>.bam."),
    threads: int = typer.Option(bamtofastq.DEFAULT_THREADS_PER_RUN, "--threads", min=1),
    reads_per_fastq: int = typer.Option(
        bamtofastq.DEFAULT_READS_PER_FASTQ, "--reads-per-fastq", min=1
    ),
    remove_bam: bool = typer.Option(False, "--remove-bam"),
) -> None:
    """Convert one run's cellranger BAM to FASTQ — the tenx DAG's only stage (internal)."""
    _run_stage(
        lambda: bamtofastq.bamtofastq_run(
            Run(srr),
            srx_dir,
            threads=threads,
            reads_per_fastq=reads_per_fastq,
            remove_bam=remove_bam,
        )
    )
