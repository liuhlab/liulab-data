"""The paper store's root: ``labdata papers init``, its lock and ``paper-status.json``."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.papers import open_paper_store, paper_lock, read_status, write_status
from labdata.papers.status import PaperStatus


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)


def geo_store(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


def test_init_writes_the_marker_readme_and_pointer(tmp_path: Path) -> None:
    store = geo_store(tmp_path / "store")
    root = tmp_path / "paper-store"
    args = ["papers", "init", str(root), "--store", str(store)]
    result = CliRunner().invoke(
        app, [*args, "--copy-to", "gcs:lab/datalake/paper-store", "--drive-folder", "1AbC"]
    )
    assert result.exit_code == 0, result.output
    assert tomllib.loads((root / "labdata-papers.toml").read_text(encoding="utf-8")) == {
        "format_version": 1,
        "store": str(store),
        "copy_to": "gcs:lab/datalake/paper-store",
        "drive_remote": "",
        "drive_folder": "1AbC",
    }
    assert "paper-status.json" in (root / "README.md").read_text(encoding="utf-8")
    pointer = tmp_path / "cache" / "liulab-data" / "papers.toml"
    assert tomllib.loads(pointer.read_text(encoding="utf-8")) == {"papers": {"root": str(root)}}
    opened = open_paper_store()
    assert opened.root == root
    assert opened.store.root == store

    again = CliRunner().invoke(app, args)
    assert again.exit_code == 1
    assert "already" in again.output


def test_init_refuses_a_folder_that_is_not_a_geo_store(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app, ["papers", "init", str(tmp_path / "p"), "--store", str(tmp_path)]
    )
    assert result.exit_code == 1
    assert "labdata-store.toml" in result.output
    assert not (tmp_path / "p" / "labdata-papers.toml").exists()


_HOLDER = """
import sys
from pathlib import Path
from labdata.papers import paper_lock
with paper_lock(Path(sys.argv[1])):
    print("got it", flush=True)
"""


def test_a_second_process_waits_on_the_lock(tmp_path: Path) -> None:
    with paper_lock(tmp_path):
        other = subprocess.Popen(
            [sys.executable, "-c", _HOLDER, str(tmp_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert other.stderr is not None
        assert "waiting" in other.stderr.readline()
        assert other.poll() is None
    out, _ = other.communicate(timeout=60)
    assert out == "got it\n"
    assert other.returncode == 0


def test_status_round_trips_and_starts_empty(tmp_path: Path) -> None:
    assert read_status(tmp_path) == PaperStatus()
    status = PaperStatus(
        last_pass={"kind": "daily", "start": "2026-11-07T00:31:00Z", "outcome": "running"},
        last_daily_success="2026-11-06",
        fetches={"date": "2026-11-07", "by_tier": {"openalex": 3}},
        quota={"date": "2026-11-07", "by_key": {"lab1": 3}},
        counts={"fetched": {"": 1}, "manual": {"no-route": 2}},
        paperpile_pending=True,
    )
    write_status(tmp_path, status)
    assert read_status(tmp_path) == status
    assert sorted(p.name for p in tmp_path.iterdir()) == ["paper-status.json"]
