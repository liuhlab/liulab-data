"""The paper store's root: ``labdata papers init``, its lock and ``paper-status.json``."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import StoreNotConfiguredError
from labdata.papers import open_paper_store, paper_lock, read_status
from labdata.papers.status import STATUS, PaperStatus, encode
from labdata.store import save_pointer


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)


def geo_store(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return path


def test_init_writes_the_marker_readme_and_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LABDATA_STORE", str(geo_store(tmp_path / "store")))
    root = tmp_path / "paper-store"
    args = ["papers", "init", str(root)]
    result = CliRunner().invoke(
        app, [*args, "--copy-to", "gcs:lab/datalake/paper-store", "--drive-folder", "1AbC"]
    )
    assert result.exit_code == 0, result.output
    assert tomllib.loads((root / "labdata-papers.toml").read_text(encoding="utf-8")) == {
        "format_version": 1,
        "copy_to": "gcs:lab/datalake/paper-store",
        "drive_remote": "",
        "drive_folder": "1AbC",
    }
    assert "paper-status.json" in (root / "README.md").read_text(encoding="utf-8")
    pointer = tmp_path / "cache" / "liulab-data" / "papers.toml"
    assert tomllib.loads(pointer.read_text(encoding="utf-8")) == {"papers": {"root": str(root)}}
    assert open_paper_store().root == root

    again = CliRunner().invoke(app, args)
    assert again.exit_code == 1
    assert "already" in again.output


def test_init_refuses_when_no_geo_store_opens(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, ["papers", "init", str(tmp_path / "p")])
    assert result.exit_code == 1
    assert "store not configured" in result.output
    assert not (tmp_path / "p" / "labdata-papers.toml").exists()


def test_the_geo_store_is_the_variable_then_the_pointer_never_the_settings_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    by_env, by_pointer, old = (geo_store(tmp_path / n) for n in ("env", "pointer", "old"))
    root = tmp_path / "papers"
    root.mkdir()
    (root / "labdata-papers.toml").write_text(f'store = "{old}"\n', encoding="utf-8")
    paper = open_paper_store(root)
    with pytest.raises(StoreNotConfiguredError, match="store not configured"):
        paper.store  # noqa: B018
    save_pointer(by_pointer)
    assert open_paper_store(root).store.root == by_pointer
    monkeypatch.setenv("LABDATA_STORE", str(by_env))
    assert open_paper_store(root).store.root == by_env


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
    (tmp_path / STATUS).write_bytes(encode(status))
    assert read_status(tmp_path) == status
    assert sorted(p.name for p in tmp_path.iterdir()) == ["paper-status.json"]
