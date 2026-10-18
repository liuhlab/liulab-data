"""The store's root README: written at each switch, rewritten by ``labdata store readme``."""

from __future__ import annotations

import json
import os
import re
import shutil
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.store import readme
from labdata.store.build import build
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


def test_the_readme_duckdb_examples_list_each_row_once(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import duckdb

    text = (root / "README.md").read_text(encoding="utf-8")
    block = re.findall(r"^```python\n(.*?)^```", text, re.MULTILINE | re.DOTALL)[1]
    results: list[list[tuple[Any, ...]]] = []
    sql = duckdb.sql

    def run(query: str) -> Any:
        relation = sql(query)
        results.append(relation.fetchall())
        return relation

    monkeypatch.setattr(duckdb, "sql", run)
    monkeypatch.chdir(root)
    exec(block, {})
    assert results
    for rows in results:
        assert rows
        assert len(rows) == len(set(rows)), rows


def test_a_build_still_switches_when_its_readme_cannot_be_written(
    root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (root / "README.md").unlink()
    (root / "README.md").mkdir()
    build(root, "b2")
    with (root / "labdata-store.toml").open("rb") as fh:
        assert tomllib.load(fh)["build"] == "b2"
    assert "labdata store readme" in caplog.text


def test_the_readme_sizes_skip_files_that_vanish_while_it_counts(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = root / "work" / "vanishing"
    work.mkdir(parents=True)
    (work / "gone.parquet").write_bytes(b"x" * 10)
    real_walk = os.walk

    def walk(top: Path, *args: Any, **kwargs: Any) -> Iterator[tuple[str, list[str], list[str]]]:
        for dirpath, dirs, names in real_walk(top, *args, **kwargs):
            for name in names:
                if name == "gone.parquet":
                    (Path(dirpath) / name).unlink()
            yield dirpath, dirs, names

    monkeypatch.setattr(os, "walk", walk)
    result = CliRunner().invoke(app, ["store", "readme", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert "Current build: `b1`" in (root / "README.md").read_text(encoding="utf-8")


def test_the_readme_credits_no_checkout_when_the_build_names_no_producer(root: Path) -> None:
    manifest = root / "builds" / "b1" / "manifest.json"
    data = json.loads(manifest.read_text(encoding="utf-8"))
    commit = data.pop("produced_by")["commit"]
    manifest.write_text(json.dumps(data), encoding="utf-8")
    result = CliRunner().invoke(app, ["store", "readme", "--root", str(root)])
    assert result.exit_code == 0, result.output
    text = (root / "README.md").read_text(encoding="utf-8")
    assert "It was made by\nan unrecorded command, at an unrecorded commit." in text
    assert commit not in text


def test_the_duckdb_examples_are_the_readme_queries_each_over_the_build() -> None:
    examples = readme.duckdb_examples()
    assert len(examples) == 3
    assert all("read_parquet('{build}/" in sql for sql in examples)


def test_a_readme_with_no_duckdb_example_fails_loudly() -> None:
    with pytest.raises(ValueError, match="no DuckDB example"):
        readme.duckdb_examples("# A README\n\nNo queries here.\n")
