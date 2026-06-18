"""Tests for labdata.ncbi.config — credential resolution and caching."""

from pathlib import Path

import pytest

import labdata.ncbi.config as config_mod
from labdata.exceptions import CredentialsError
from labdata.ncbi.config import (
    NcbiCredentials,
    config_path,
    load_credentials,
    save_credentials,
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point the cache at a tmp dir and clear NCBI env vars for every test."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    monkeypatch.delenv("NCBI_EMAIL", raising=False)
    monkeypatch.delenv("NCBI_API_KEY", raising=False)


def test_load_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NCBI_EMAIL", "a@b.org")
    monkeypatch.setenv("NCBI_API_KEY", "key123")
    assert load_credentials(allow_prompt=False) == NcbiCredentials("a@b.org", "key123")


def test_env_email_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NCBI_EMAIL", "a@b.org")
    creds = load_credentials(allow_prompt=False)
    assert creds.email == "a@b.org"
    assert creds.api_key is None


def test_env_takes_precedence_over_file(monkeypatch: pytest.MonkeyPatch) -> None:
    save_credentials(NcbiCredentials("file@lab.org", "filekey"))
    monkeypatch.setenv("NCBI_EMAIL", "env@lab.org")
    assert load_credentials(allow_prompt=False).email == "env@lab.org"


def test_save_and_load_round_trip() -> None:
    path = save_credentials(NcbiCredentials("x@y.org", "k"))
    assert path == config_path()
    assert path.stat().st_mode & 0o777 == 0o600
    assert load_credentials(allow_prompt=False) == NcbiCredentials("x@y.org", "k")


def test_save_without_api_key_round_trips() -> None:
    save_credentials(NcbiCredentials("x@y.org"))
    assert load_credentials(allow_prompt=False) == NcbiCredentials("x@y.org", None)


def test_missing_without_prompt_raises() -> None:
    with pytest.raises(CredentialsError, match="NCBI_EMAIL"):
        load_credentials(allow_prompt=False)


def test_non_interactive_does_not_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_is_interactive", lambda: False)
    with pytest.raises(CredentialsError):
        load_credentials(allow_prompt=True)


def test_prompt_path_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_is_interactive", lambda: True)
    answers = iter(["typed@lab.org", "typedkey"])
    monkeypatch.setattr("builtins.input", lambda *_a, **_k: next(answers))

    creds = load_credentials(allow_prompt=True)

    assert creds == NcbiCredentials("typed@lab.org", "typedkey")
    assert config_path().is_file()  # answers were cached
    # A second call is served from the cached file, no prompt needed.
    assert load_credentials(allow_prompt=False) == creds


def test_prompt_blank_email_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_a, **_k: "")
    with pytest.raises(CredentialsError, match="email is required"):
        load_credentials(allow_prompt=True)
