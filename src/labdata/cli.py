"""Command-line interface — a thin Typer wrapper over the labdata API.

Logic lives in :mod:`labdata.ncbi`, :mod:`labdata.geo`, etc.; this module only
translates arguments, dispatches, and chooses an output format.
"""

from __future__ import annotations

from pathlib import Path

import typer

from labdata import Series
from labdata import __version__ as _package_version
from labdata.exceptions import LabdataError
from labdata.ncbi.config import (
    NcbiCredentials,
    config_path,
    prompt_and_save,
    save_credentials,
)

app = typer.Typer(
    help="Liu Lab data curation, download, and organization utilities.",
    no_args_is_help=True,
)
ncbi_app = typer.Typer(help="Manage NCBI E-utilities credentials.", no_args_is_help=True)
app.add_typer(ncbi_app, name="ncbi")
geo_app = typer.Typer(help="Download and organize GEO/SRA data.", no_args_is_help=True)
app.add_typer(geo_app, name="geo")


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


@geo_app.command("download")
def geo_download(
    accession: str = typer.Argument(..., help="GEO Series accession, e.g. GSE131907."),
    output_dir: Path = typer.Option(
        Path(), "--output", "-o", help="Parent directory; a <GSE>/ subtree is created in it."
    ),
    n_parallel: int = typer.Option(
        1, "--parallel", "-j", min=1, help="Maximum runs to download concurrently."
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
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress the download plan/progress."),
) -> None:
    """Download FASTQ for a whole GEO Series (every SRX/SRR) with sra-tools.

    Lays the data out as ``<output>/<GSE>/<SRX>/<SRR>*.fastq.gz`` and skips runs that
    are already complete, so it is safe to rerun. Exits non-zero if any run fails.
    """
    try:
        results = Series(accession).download(
            output_dir,
            n_parallel,
            max_size=max_size or None,
            retries=retries or None,
            backoff=backoff or None,
            keep_sra=keep_sra,
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
