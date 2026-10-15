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
from labdata.ncbi.entrez import EntrezClient
from labdata.store import init_store, open_store, save_pointer
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
store_app = typer.Typer(
    help="Configure, fetch into and build the shared record store.", no_args_is_help=True
)
app.add_typer(store_app, name="store")


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


@store_app.command("init")
def store_init(
    root: Path = typer.Argument(..., help="The store root; created if missing."),
) -> None:
    """Make a folder a store: write labdata-store.toml. Refuses if one is already there."""
    try:
        marker = init_store(root)
    except LabdataError as err:
        typer.echo(str(err), err=True)
        raise typer.Exit(1) from err
    typer.echo(f"Wrote {marker}")


@store_app.command("verify")
def store_verify(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
    write_missing: bool = typer.Option(
        False, help="First write a README and manifest where a snapshot or raw folder lacks one."
    ),
) -> None:
    """Check every snapshots/, raw/ and builds/ folder against its manifest, sha256 included."""
    from labdata.store.provenance import folders, verify

    store = open_store(root)
    problems = verify(store.root, write_missing=write_missing)
    for line in problems:
        typer.echo(line)
    if problems:
        raise typer.Exit(1)
    typer.echo(f"All {len(folders(store.root))} folders match their manifests")


@store_app.command("configure")
def store_configure(
    root: Path = typer.Argument(..., help="The store root, the folder holding labdata-store.toml."),
) -> None:
    """Save the store root in the per-user pointer file.

    ``open_store()`` reads it when no root argument is given and ``LABDATA_STORE`` is unset.
    """
    path = save_pointer(root)
    typer.echo(f"Saved store root {root} to {path}")


def _store_client() -> EntrezClient:
    """Return the Entrez client store fetches use; never prompts, so batch jobs never hang."""
    return EntrezClient(allow_prompt=False)


@store_app.command("fetch")
def store_fetch(
    accessions: list[str] = typer.Argument(
        ..., help="GEO Series, BioProject, SRA study or SRA experiment accessions."
    ),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    r"""Fetch family SOFT, Entrez links, SRA package XML and PubMed (\[si]) results into raw/.

    A GEO Series brings its SubSeries and the SRA packages of every experiment its samples
    link to. A BioProject or SRA study is expanded to its experiments through the newest
    SRA_Accessions.tab snapshot. Items already saved are skipped, so a rerun resumes.
    """
    from labdata.store.fetch import fetch as fetch_raw

    store = open_store(root)
    report = fetch_raw(store.root, accessions, client=_store_client())
    if report.series:
        typer.echo(f"Saved {len(report.series)} GEO Series to {store.root / 'raw' / 'geo-soft'}")
    typer.echo(f"Saved {len(report.experiments)} SRA packages to {store.root / 'raw' / 'sra-xml'}")
    for accession, pmids in report.pubmed_si.items():
        typer.echo(f"{accession}[si]: {', '.join(pmids) or 'no PubMed IDs'}")


@store_app.command("pack")
def store_pack(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Merge the fetched parts under work/parts/ into raw/, one bucket file at a time.

    Rerun it after a cut-off run: parts stay until their bucket is rewritten.
    """
    from labdata.store.pack import pack

    store = open_store(root)
    written = pack(store.root)
    typer.echo(f"Packed {len(written)} bucket files into {store.root / 'raw'}")


@store_app.command("build")
def store_build(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
    build_id: str | None = typer.Option(None, help="Build folder name; default: a UTC timestamp."),
    workers: int | None = typer.Option(
        None, min=1, help="Parallel workers; default: SLURM_CPUS_ON_NODE, then the CPU count."
    ),
    chunk_mb: int = typer.Option(64, min=1, help="MB of source text a worker parses at a time."),
    fraction: float | None = typer.Option(
        None, help="Gaps and coverage over this seeded sample of the target list; default: all."
    ),
) -> None:
    """Build the store's tables from raw/ and snapshots/ with no network, then switch to them.

    Memory is bounded by about one chunk per worker plus DuckDB's cap
    (LABDATA_DUCKDB_MEMORY, default a share of the job's memory).
    """
    from labdata.store.build import build as build_store

    store = open_store(root)
    out = build_store(
        store.root,
        build_id,
        workers=workers,
        chunk_bytes=chunk_mb << 20,
        fraction=_fraction(fraction),
    )
    typer.echo(f"Built {out} and switched the store to it")


@store_app.command("prune")
def store_prune(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Delete old builds and unread snapshots once they are out of use for 24 hours.

    Keeps the current build and the snapshots it reads. A build stays readable for 24 hours
    after the store switched away from it.
    """
    from labdata.store.prune import prune

    store = open_store(root)
    report = prune(store.root)
    for path in report.deleted:
        typer.echo(f"Deleted {path}")
    if not report.deleted:
        typer.echo("Deleted nothing")


@store_app.command("copy")
def store_copy(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
    to: str | None = typer.Option(
        None, help="rclone destination <remote>:<bucket>/<path>; default: copy_to in the marker."
    ),
) -> None:
    """Copy the marker, current snapshots, raw/ and current build to a bucket with rclone.

    Refused unless the current build is complete. When rclone's Google login has expired,
    it skips with a message and exits 0; rerun after `rclone config reconnect`.
    """
    from labdata.store.gcs import copy

    try:
        result = copy(root if root is not None else open_store().root, to)
    except LabdataError as err:
        typer.echo(str(err), err=True)
        raise typer.Exit(1) from err
    typer.echo(result.message if result.skipped else f"Copied the store to {result.destination}")


@store_app.command("coverage")
def store_coverage(
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Print target, held and gap counts of the current build; exit 1 if coverage fails.

    Coverage fails when a target Series is neither held nor a gap, or a gap is unexplained.
    """
    from labdata.store.gaps import coverage

    store = open_store(root)
    build = store.settings.get("build")
    if not build:
        raise typer.BadParameter("the store names no build; run `labdata store build`")
    report = coverage(store.root, store.root / "builds" / str(build))
    typer.echo(f"Target Series: {report.target} (geo-series snapshot {report.snapshot_date})")
    typer.echo(f"Held: {report.held}")
    typer.echo(f"Gaps: {sum(report.gaps.values())}")
    for reason, count in report.gaps.items():
        typer.echo(f"  {reason}: {count}")
    if report.passed:
        typer.echo("Coverage passes.")
        return
    typer.echo(f"Coverage fails for {report.failures} Series:")
    for accession, problem in report.failing:
        typer.echo(f"  {accession}: {problem}")
    if report.failures > len(report.failing):
        typer.echo(f"  ... and {report.failures - len(report.failing)} more")
    raise typer.Exit(1)


def _fraction(fraction: float | None) -> float | None:
    from labdata.store import targets

    try:
        return targets.check_fraction(fraction)
    except ValueError as err:
        raise typer.BadParameter(str(err), param_hint="--fraction") from err


@store_app.command("run")
def store_run(
    fraction: float | None = typer.Option(
        None, help="Run on this seeded sample of the target list, e.g. 0.01; default: all."
    ),
    cores: int | None = typer.Option(
        None, min=1, help="Cores the steps may use; default: SLURM_CPUS_ON_NODE, then all."
    ),
    ncbi: int = typer.Option(2, min=1, help="Network steps that may run at once."),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Fetch, pack, build, prune and copy the store in one resumable run.

    Uses the newest snapshot of each source. Rerun the same command to resume: it starts at
    the first unfinished step and fetches only what is not saved yet. Markers and logs are
    under work/run/<snapshot date>-<fraction>/.
    """
    import logging

    from labdata.store import pipeline

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    code = pipeline.run(open_store(root).root, fraction=_fraction(fraction), cores=cores, ncbi=ncbi)
    if code:
        typer.echo(f"The run stopped with steps unfinished (snakemake exit {code}).", err=True)
        raise typer.Exit(code)
    typer.echo("The run finished: every step done.")


@store_app.command("_targets", hidden=True)
def store_targets(
    fraction: float | None = typer.Option(
        None, help="This seeded sample of the target list; default: all."
    ),
    snapshot_date: str | None = typer.Option(
        None, help="geo-series snapshot date; default: the newest."
    ),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Write the target Series list, or a round's sample of it, under work/targets/."""
    import pyarrow.parquet as pq

    from labdata.store import targets

    store = open_store(root)
    path = targets.target_list(store.root, snapshot_date, _fraction(fraction))
    typer.echo(f"{pq.read_metadata(path).num_rows} target Series in {path}")


@store_app.command("_geo", hidden=True)
def store_geo_lane(
    accessions: list[str] = typer.Argument(
        None, help="GEO Series; default: the target Series list of the snapshot."
    ),
    series: Path | None = typer.Option(
        None, help="Only these GEO Series: a text file, one per line, or a Parquet `accession`."
    ),
    snapshot_date: str | None = typer.Option(
        None, help="geo-series snapshot date; default: the newest."
    ),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Fetch family SOFT and filelist.txt of every target Series into parts under work/."""
    import logging

    import pyarrow.parquet as pq

    from labdata.store import geo_lane, targets

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    store = open_store(root)
    series_list = list(accessions or [])
    if series is not None:
        series_list += _accession_file(series)
    elif not series_list:
        path = targets.target_list(store.root, snapshot_date)
        batches = pq.ParquetFile(path).iter_batches(columns=["accession"])
        series_list = [gse for batch in batches for gse in batch.column(0).to_pylist()]
    report = geo_lane.run(store.root, series_list, client=_store_client())
    typer.echo(
        f"GEO lane: {report.held} Series held, {report.filelists} filelists, gaps: "
        f"{report.soft_missing} soft-missing, {report.fetch_failed} fetch-failed"
    )


@store_app.command("extract")
def store_extract(
    source: str = typer.Option(..., help="biosample, bioproject, sra-accessions or geo-series."),
    workers: int | None = typer.Option(
        None, min=1, help="Parallel workers; default: SLURM_CPUS_ON_NODE, then the CPU count."
    ),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Extract one snapshot into Parquet parts for `labdata store build`; resumes a killed run.

    Run once per source after a new snapshot lands. Set RAY_ADDRESS to use a running Ray
    cluster instead of starting one on this node.
    """
    from labdata.store.extract import extract

    store = open_store(root)
    try:
        out = extract(store.root, source, workers=workers)
    except ValueError as err:
        raise typer.BadParameter(str(err), param_hint="--source") from err
    typer.echo(f"Extracted {source}: {out}")


@store_app.command("_eutils", hidden=True)
def store_eutils(
    series: Path | None = typer.Option(
        None, help="Only these GEO Series: a text file, one per line, or a Parquet `accession`."
    ),
    date: str | None = typer.Option(None, help="SRA_Accessions.tab snapshot date."),
    root: Path | None = typer.Option(
        None, help="Store root; default: LABDATA_STORE, then pointer."
    ),
) -> None:
    """Fetch the SRA package of every GEO-linked experiment into parts under work/parts/."""
    import datetime as dt
    import logging

    from labdata.store import eutils

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    store = open_store(root)
    wanted = None if series is None else _accession_file(series)
    listed = eutils.experiment_list(store.root, date=date, series=wanted)
    today = dt.datetime.now(dt.UTC).date().isoformat()
    report = eutils.fetch_packages(store.root, listed, client=_store_client(), date=today)
    typer.echo(
        f"SRA packages: {report.fetched} saved, {report.missing} with none, "
        f"{report.failed} to retry"
    )


def _accession_file(path: Path) -> list[str]:
    """Read accessions from a Parquet ``accession`` column or a text file, one per line."""
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        return [str(a) for a in pq.read_table(path, columns=["accession"]).column(0).to_pylist()]
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


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
    """Download one run's original-format files with curl (an internal download-only DAG stage)."""
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
