"""Command-line interface — a thin Typer wrapper over the labdata API.

Logic lives in :mod:`labdata.ncbi`, :mod:`labdata.geo`, etc.; this module only
translates arguments, dispatches, and chooses an output format.
"""

from __future__ import annotations

import typer

from labdata import __version__ as _package_version
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
