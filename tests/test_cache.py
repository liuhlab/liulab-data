"""Tests for labdata._cache — cache-directory resolution."""

from pathlib import Path

import pytest

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir


def test_cache_dir_uses_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert liulab_data_cache_dir() == tmp_path / "liulab-data"


def test_cache_dir_defaults_to_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    assert liulab_data_cache_dir() == Path.home() / ".cache" / "liulab-data"


def test_ensure_cache_dir_creates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    created = ensure_cache_dir()
    assert created == tmp_path / "liulab-data"
    assert created.is_dir()
