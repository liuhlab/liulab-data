"""Prune old builds and snapshots after a grace period; copy the current store with rclone."""

from __future__ import annotations

import datetime as dt
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner, Result

from labdata.cli import app
from labdata.store import extract
from labdata.store.build import build
from labdata.store.prune import GRACE, prune
from tests._slices import built_root

_HOUR = dt.timedelta(hours=1)
_DEST = "gcs:lab-bucket/geo-sra-store"
_EXPIRED = (
    "ERROR : Attempt 1/3 failed with 1 errors and: couldn't fetch token - maybe it has "
    'expired? - refresh with "rclone config reconnect gcs:": oauth2: cannot fetch token: '
    '400 Bad Request\nResponse: {"error": "invalid_grant", '
    '"error_description": "Token has been expired or revoked."}\n'
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def _built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp("built") / "store", "prjna1027859")


@pytest.fixture
def root(_built: Path, tmp_path: Path) -> Path:
    """A fresh copy of a store built once as ``b1``, reading snapshots of 2026-10-14."""
    out = tmp_path / "store"
    shutil.copytree(_built, out)
    return out


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _snapshot(root: Path, source: str, date: str) -> Path:
    """Add a copy of the 2026-10-14 snapshot of ``source`` under another date."""
    folder = root / "snapshots" / source / date
    shutil.copytree(root / "snapshots" / source / "2026-10-14", folder)
    return folder


# ---------------------------------------------------------------- prune


def test_prune_keeps_what_the_grace_covers_and_deletes_the_rest_after_it(root: Path) -> None:
    newer = _snapshot(root, "bioproject", "2026-11-01")
    extract.extract(root, "bioproject", workers=1)
    build(root, "b2")
    pending = _snapshot(root, "biosample", "2026-12-01")
    read_by_b1 = root / "snapshots" / "bioproject" / "2026-10-14"

    report = prune(root, now=_now() + _HOUR)
    assert report.deleted == []
    assert (root / "builds" / "b1").is_dir()

    report = prune(root, now=_now() + 25 * _HOUR)
    assert report.deleted == [root / "builds" / "b1", read_by_b1]
    assert sorted(p.name for p in (root / "builds").iterdir()) == ["b2"]
    assert not extract.extract_dir(root, "bioproject", "2026-10-14").exists()
    for kept in (newer, pending, root / "snapshots" / "biosample" / "2026-10-14"):
        assert kept.is_dir()


def test_prune_deletes_an_old_snapshot_no_build_reads_after_the_grace(root: Path) -> None:
    unread = _snapshot(root, "geo-series", "2026-01-01")
    build(root, "b2")
    assert prune(root, now=_now() + _HOUR).deleted == []
    assert prune(root, now=_now() + 25 * _HOUR).deleted == [root / "builds" / "b1", unread]


def test_prune_keeps_a_build_out_of_use_for_exactly_the_grace(root: Path) -> None:
    unread = _snapshot(root, "geo-series", "2026-01-01")
    build(root, "b2")
    with (root / "labdata-store.toml").open("rb") as fh:
        away = dt.datetime.fromisoformat(tomllib.load(fh)["switched_away"]["b1"])
    assert prune(root, now=away + GRACE).deleted == []
    second = dt.timedelta(seconds=1)
    assert prune(root, now=away + GRACE + second).deleted == [root / "builds" / "b1", unread]


def test_prune_rewrites_the_readme_sizes_after_it_deletes(root: Path) -> None:
    _snapshot(root, "geo-series", "2026-01-01")
    build(root, "b2")
    readme = root / "README.md"
    before = readme.read_text(encoding="utf-8")
    assert prune(root, grace=dt.timedelta(0)).deleted
    after = readme.read_text(encoding="utf-8")
    assert after != before
    result = CliRunner().invoke(app, ["store", "readme", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert readme.read_text(encoding="utf-8") == after


def test_prune_dates_builds_from_before_switch_times_by_the_marker(root: Path) -> None:
    build(root, "b2")
    marker = root / "labdata-store.toml"
    marker.write_text('format_version = 1\nbuild = "b2"\n', encoding="utf-8")
    assert prune(root, now=_now() + _HOUR).deleted == []
    two_days_ago = (_now() - 48 * _HOUR).timestamp()
    os.utime(marker, (two_days_ago, two_days_ago))
    assert prune(root).deleted == [root / "builds" / "b1"]


def test_prune_grace_zero_deletes_at_once_all_but_the_current_build_and_its_reads(
    root: Path,
) -> None:
    unread = _snapshot(root, "geo-series", "2026-01-01")
    build(root, "b2")
    result = CliRunner().invoke(app, ["store", "prune", "--root", str(root), "--grace", "0"])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in (root / "builds").iterdir()) == ["b2"]
    assert not unread.exists()
    for source in ("bioproject", "biosample", "geo-series", "sra-accessions"):
        assert (root / "snapshots" / source / "2026-10-14").is_dir()


def test_prune_never_deletes_the_current_build(root: Path) -> None:
    result = CliRunner().invoke(app, ["store", "prune", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert "Deleted nothing" in result.output
    assert prune(root, now=_now() + 1000 * _HOUR).deleted == []
    assert (root / "builds" / "b1").is_dir()


# ---------------------------------------------------------------- copy


class _Rclone:
    """Stand-in for the rclone seam: records each command, answers with one result."""

    def __init__(self, returncode: int = 0, stderr: str = "") -> None:
        self.calls: list[list[str]] = []
        self.returncode = returncode
        self.stderr = stderr

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)


@pytest.fixture
def rclone(monkeypatch: pytest.MonkeyPatch) -> _Rclone:
    fake = _Rclone()
    monkeypatch.setattr("labdata.store.gcs._run_rclone", fake)
    return fake


def _copy(root: Path, *extra: str) -> Result:
    return CliRunner().invoke(app, ["store", "copy", "--root", str(root), *extra])


def test_copy_syncs_exactly_the_marker_readme_snapshots_raw_and_current_build(
    root: Path, rclone: _Rclone
) -> None:
    marker = root / "labdata-store.toml"
    marker.write_text(marker.read_text() + f'copy_to = "{_DEST}"\n', encoding="utf-8")
    build(root, "b2")
    result = CliRunner().invoke(app, ["store", "copy", "--root", str(root)])
    assert result.exit_code == 0, result.output
    selected = [
        "--filter=- .*",
        "--filter=- .*/**",
        "--filter=+ /snapshots/bioproject/2026-10-14/**",
        "--filter=+ /snapshots/biosample/2026-10-14/**",
        "--filter=+ /snapshots/geo-series/2026-10-14/**",
        "--filter=+ /snapshots/sra-accessions/2026-10-14/**",
        "--filter=+ /raw/**",
        "--filter=+ /builds/b2/**",
        "--filter=+ /README.md",
    ]
    progress = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]
    assert rclone.calls == [
        ["rclone", "copy", str(root), _DEST, *selected, "--filter=- **", *progress],
        [
            "rclone",
            "sync",
            str(root),
            _DEST,
            "--delete-excluded",
            *selected,
            "--filter=+ /labdata-store.toml",
            "--filter=- **",
            *progress,
        ],
    ]


def test_copy_is_refused_on_a_folder_with_no_marker(tmp_path: Path, rclone: _Rclone) -> None:
    result = _copy(tmp_path, "--to", _DEST)
    assert result.exit_code != 0
    assert "no labdata-store.toml" in result.output
    assert rclone.calls == []


@pytest.mark.parametrize(
    ("gone", "message"),
    [
        ("builds/b1/holdings.parquet", "builds/b1/holdings.parquet"),
        ("raw/sra-xml", "raw/sra-xml/"),
        ("snapshots/biosample/2026-10-14", "snapshots/biosample/2026-10-14"),
        ("builds/b1", "builds/b1"),
    ],
)
def test_copy_is_refused_when_the_current_build_is_incomplete(
    root: Path, rclone: _Rclone, gone: str, message: str
) -> None:
    path = root / gone
    shutil.rmtree(path) if path.is_dir() else path.unlink()
    result = _copy(root, "--to", _DEST)
    assert result.exit_code != 0
    assert "refused" in result.output
    assert message in result.output
    assert rclone.calls == []


def test_copy_needs_a_destination(root: Path, rclone: _Rclone) -> None:
    result = _copy(root)
    assert result.exit_code != 0
    assert "copy_to" in result.output
    assert rclone.calls == []


def test_an_expired_login_skips_the_copy_with_the_bucket_behind_message(
    root: Path, rclone: _Rclone
) -> None:
    rclone.returncode, rclone.stderr = 1, _EXPIRED
    result = _copy(root, "--to", _DEST)
    assert result.exit_code == 0, result.output
    assert "bucket is behind" in result.output
    assert "rclone config reconnect gcs:" in result.output
    assert len(rclone.calls) == 1


def test_another_rclone_failure_fails_the_copy(root: Path, rclone: _Rclone) -> None:
    rclone.returncode, rclone.stderr = 3, "ERROR : directory not found\n"
    result = _copy(root, "--to", _DEST)
    assert result.exit_code == 1
    assert "directory not found" in result.output
