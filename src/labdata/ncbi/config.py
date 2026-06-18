"""NCBI E-utilities credential resolution.

NCBI asks every E-utilities client to identify itself with an email address, and
optionally accepts an API key that raises the rate limit from 3 to 10 requests
per second. This module resolves those two values from, in order:

1. the ``NCBI_EMAIL`` / ``NCBI_API_KEY`` environment variables;
2. a cached config file at ``<cache>/ncbi.toml``;
3. an interactive prompt (only on a TTY), whose answers are then cached.

In a non-interactive context with nothing configured, :func:`load_credentials`
raises :class:`CredentialsError` with an actionable message rather than hanging.
"""

from __future__ import annotations

import os
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir
from labdata.exceptions import CredentialsError

#: Environment variable holding the contact email NCBI requires.
EMAIL_ENV = "NCBI_EMAIL"

#: Environment variable holding the optional NCBI API key.
API_KEY_ENV = "NCBI_API_KEY"

#: Basename of the cached credentials file under the cache directory.
CONFIG_FILENAME = "ncbi.toml"


@dataclass(frozen=True, slots=True)
class NcbiCredentials:
    """NCBI E-utilities credentials.

    Parameters
    ----------
    email : str
        Contact email reported to NCBI on every request (required).
    api_key : str or None
        Optional NCBI API key; raises the request rate limit when present.
    """

    email: str
    api_key: str | None = None


def config_path() -> Path:
    """Return the path to the cached credentials file (which may not exist).

    Returns
    -------
    pathlib.Path
        ``<cache>/ncbi.toml``.
    """
    return liulab_data_cache_dir() / CONFIG_FILENAME


def load_credentials(*, allow_prompt: bool = True) -> NcbiCredentials:
    """Resolve NCBI credentials from env vars, the cache file, or a prompt.

    Parameters
    ----------
    allow_prompt : bool
        When ``True`` (default) and nothing is configured, prompt interactively
        on a TTY and cache the answers. When ``False``, never prompt.

    Returns
    -------
    NcbiCredentials
        The resolved credentials.

    Raises
    ------
    CredentialsError
        If no credentials are configured and prompting is disabled or impossible
        (e.g. a non-interactive process).
    """
    creds = _from_env() or _from_file()
    if creds is not None:
        return creds

    if allow_prompt and _is_interactive():
        return prompt_and_save()

    raise CredentialsError(
        f"no NCBI credentials found. Set the {EMAIL_ENV} environment variable "
        f"(and optionally {API_KEY_ENV}), or run `labdata ncbi configure`."
    )


def save_credentials(creds: NcbiCredentials) -> Path:
    """Persist credentials to the cache file with owner-only permissions.

    Parameters
    ----------
    creds : NcbiCredentials
        The credentials to write.

    Returns
    -------
    pathlib.Path
        The path the credentials were written to.
    """
    ensure_cache_dir()
    path = config_path()
    path.write_text(_to_toml(creds), encoding="utf-8")
    path.chmod(0o600)
    return path


def prompt_and_save() -> NcbiCredentials:
    """Prompt for credentials interactively and cache them.

    Unlike :func:`load_credentials`, this always prompts (overwriting any cached
    file), which is what ``labdata ncbi configure`` wants.

    Returns
    -------
    NcbiCredentials
        The entered credentials, already written to the cache file.
    """
    creds = _prompt()
    save_credentials(creds)
    return creds


def _from_env() -> NcbiCredentials | None:
    email = os.environ.get(EMAIL_ENV, "").strip()
    if not email:
        return None
    api_key = os.environ.get(API_KEY_ENV, "").strip() or None
    return NcbiCredentials(email=email, api_key=api_key)


def _from_file() -> NcbiCredentials | None:
    path = config_path()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    section = data.get("ncbi", data)
    email = str(section.get("email", "")).strip()
    if not email:
        return None
    api_key = str(section.get("api_key", "")).strip() or None
    return NcbiCredentials(email=email, api_key=api_key)


def _is_interactive() -> bool:
    """Return whether stdin is a TTY (indirection so tests can override it)."""
    return sys.stdin.isatty()


def _prompt() -> NcbiCredentials:
    print(
        "liulab-data needs an email address to identify you to NCBI E-utilities.\n"
        "An optional API key (https://www.ncbi.nlm.nih.gov/account/settings/) raises the "
        "rate limit; leave it blank to skip.",
        file=sys.stderr,
    )
    email = input("NCBI email: ").strip()
    if not email:
        raise CredentialsError("an NCBI email is required; none was entered.")
    api_key = input("NCBI API key (optional): ").strip() or None
    return NcbiCredentials(email=email, api_key=api_key)


def _to_toml(creds: NcbiCredentials) -> str:
    lines = ["[ncbi]", f'email = "{_toml_str(creds.email)}"']
    if creds.api_key:
        lines.append(f'api_key = "{_toml_str(creds.api_key)}"')
    return "\n".join(lines) + "\n"


def _toml_str(value: str) -> str:
    if any(ch in value for ch in '"\\\n\r'):
        raise CredentialsError(f"value cannot be stored (contains quotes/backslashes): {value!r}")
    return value
