"""The store's root README: written at each switch, rewritten by ``labdata store readme``."""

from __future__ import annotations

import re
import shutil
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from tests._slices import built_root


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def _built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("readme") / "store", "geo_slice")


@pytest.fixture
def root(_built: Path, tmp_path: Path) -> Path:
    """A fresh copy of the geo slice, built once as ``b1`` from snapshots of 2026-10-14."""
    out = tmp_path / "store"
    shutil.copytree(_built, out)
    return out


def test_a_build_writes_the_root_readme_for_the_build_it_switched_to(root: Path) -> None:
    with (root / "labdata-store.toml").open("rb") as fh:
        switched = tomllib.load(fh)["switched"]
    text = (root / "README.md").read_text(encoding="utf-8")
    assert "Current build: `b1`" in text
    assert f"switched to it at {switched}" in text
    assert "| `geo-series` | 2026-10-14 |" in text
    for heading in ("labdata-store.toml", "builds/", "raw/", "snapshots/", "support/", "work/"):
        assert re.search(rf"^## {re.escape(heading)}", text, re.MULTILINE), heading
    assert str(root) not in text


def test_the_readme_command_rewrites_it_with_this_copys_sizes(root: Path) -> None:
    (root / "README.md").unlink()
    logs = root / "support" / "logs" / "jobs"
    logs.mkdir(parents=True)
    (logs / "labdata-run.1.log").write_bytes(b"x" * 2500)
    result = CliRunner().invoke(app, ["store", "readme", "--root", str(root)])
    assert result.exit_code == 0, result.output
    text = (root / "README.md").read_text(encoding="utf-8")
    assert "| `support/` | 2.5 kB |" in text
    assert "Current build: `b1`" in text


def test_the_readme_examples_run_against_the_store(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    text = (root / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"^```python\n(.*?)^```", text, re.MULTILINE | re.DOTALL)
    assert len(blocks) == 2
    monkeypatch.chdir(root)
    for block in blocks:
        exec(block.replace("<store>", str(root)), {})
