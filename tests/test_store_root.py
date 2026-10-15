"""Tests for finding and checking the store root (labdata.store)."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import Store, open_store
from labdata.cli import app
from labdata.exceptions import LabdataError, NotAStoreError, StoreNotConfiguredError
from labdata.store import save_pointer


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point the per-user folder at a tmp dir and clear the store env var for every test."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def make_store(path: Path) -> Path:
    """Create a store root carrying a minimal marker."""
    path.mkdir(parents=True)
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


def test_open_store_from_argument(tmp_path: Path) -> None:
    root = make_store(tmp_path / "store")
    store = open_store(root)
    assert isinstance(store, Store)
    assert store.root == root
    assert store.settings == {"format_version": 1}


def test_open_store_refuses_folder_without_marker(tmp_path: Path) -> None:
    (tmp_path / "not-a-store").mkdir()
    with pytest.raises(NotAStoreError, match=r"labdata-store\.toml") as info:
        open_store(tmp_path / "not-a-store")
    assert isinstance(info.value, LabdataError)


def test_unconfigured_error_names_all_three_fixes() -> None:
    with pytest.raises(StoreNotConfiguredError) as info:
        open_store()
    message = str(info.value)
    assert "open_store(root=" in message
    assert "LABDATA_STORE" in message
    assert "labdata store configure" in message


def test_open_store_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = make_store(tmp_path / "store")
    monkeypatch.setenv("LABDATA_STORE", str(root))
    assert open_store().root == root


def test_configure_writes_pointer_that_open_store_reads(tmp_path: Path) -> None:
    root = make_store(tmp_path / "store")
    result = CliRunner().invoke(app, ["store", "configure", str(root)])
    assert result.exit_code == 0, result.output
    pointer = tmp_path / "cache" / "liulab-data" / "store.toml"
    assert tomllib.loads(pointer.read_text(encoding="utf-8")) == {"store": {"root": str(root)}}
    assert open_store().root == root


def test_first_match_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    by_arg = make_store(tmp_path / "by-arg")
    by_env = make_store(tmp_path / "by-env")
    by_pointer = make_store(tmp_path / "by-pointer")
    save_pointer(by_pointer)
    assert open_store().root == by_pointer
    monkeypatch.setenv("LABDATA_STORE", str(by_env))
    assert open_store().root == by_env
    assert open_store(by_arg).root == by_arg


def test_opening_writes_nothing_under_root(tmp_path: Path) -> None:
    root = make_store(tmp_path / "store")
    before = {p: p.stat().st_mtime_ns for p in [root, *root.rglob("*")]}
    root.chmod(0o555)  # a reader has no write permission on the shared store
    try:
        open_store(root)
    finally:
        root.chmod(0o755)
    assert {p: p.stat().st_mtime_ns for p in [root, *root.rglob("*")]} == before
