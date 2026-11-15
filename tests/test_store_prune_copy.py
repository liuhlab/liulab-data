"""Prune old builds and snapshots after a grace period; copy the current store with rclone."""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner, Result

from labdata.cli import app
from labdata.store import extract, raw, refresh
from labdata.store.build import build
from labdata.store.gcs import COPIED
from labdata.store.pack import pack
from labdata.store.prune import GRACE, prune
from tests._slices import BuiltStore

_HOUR = dt.timedelta(hours=1)
_DEST = "gcs:lab-bucket/geo-sra-store"
_SOURCES = ("bioproject", "biosample", "geo-series", "sra-accessions")
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
def _built(built_store: BuiltStore, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_store(tmp_path_factory.mktemp("built") / "store", "prjna1027859")


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
    assert extract.extract_dir(root, "bioproject", "2026-11-01").is_dir()
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

_PROGRESS = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]
_TOP = ["README.md", "labdata-store.toml"]


class _Rclone:
    """Stand-in for the rclone seam: records each command and the file list it reads.

    It keeps what a ``copyto`` sends to the bucket, and a ``copy`` from the bucket fetches it.
    Every command answers with ``returncode`` and ``stderr``, and the one named ``expiring``
    meets an expired login.
    """

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.lists: list[list[str]] = []
        self.bucket: dict[str, bytes] = {}
        self.returncode, self.stderr = 0, ""
        self.expiring = ""

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        listed: list[str] = []
        for arg in args:
            if arg.startswith("--files-from-raw="):
                listed = Path(arg.split("=", 1)[1]).read_text(encoding="utf-8").splitlines()
                self.lists.append(listed)
        if args[1] == self.expiring:
            return subprocess.CompletedProcess(args, 1, "", _EXPIRED)
        if self.returncode == 0 and args[1] == "copyto" and args[3].startswith(f"{_DEST}/"):
            self.bucket[args[3].removeprefix(f"{_DEST}/")] = Path(args[2]).read_bytes()
        if self.returncode == 0 and args[1] == "copy" and args[2] == _DEST:
            for name in (n for n in listed if n in self.bucket):
                (Path(args[3]) / name).write_bytes(self.bucket[name])
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)

    def commands(self) -> list[str]:
        return [args[1] for args in self.calls]

    def record(self) -> list[str]:
        """The paths the copy record in the bucket names."""
        return sorted(json.loads(self.bucket[COPIED]))


@pytest.fixture
def rclone(monkeypatch: pytest.MonkeyPatch) -> _Rclone:
    fake = _Rclone()
    monkeypatch.setattr("labdata.store.gcs._run_rclone", fake)
    return fake


def _copy(root: Path, *extra: str) -> Result:
    return CliRunner().invoke(app, ["store", "copy", "--root", str(root), *extra])


def _copy_to(root: Path) -> None:
    marker = root / "labdata-store.toml"
    marker.write_text(marker.read_text() + f'copy_to = "{_DEST}"\n', encoding="utf-8")


def _files(root: Path, *folders: str) -> list[str]:
    """Every file under ``folders``, hidden ones aside, as paths from the store root."""
    return sorted(
        p.relative_to(root).as_posix()
        for folder in folders
        for p in (root / folder).rglob("*")
        if p.is_file() and not p.name.startswith(".")
    )


def _full(root: Path) -> None:
    result = _copy(root, "--full")
    assert result.exit_code == 0, result.output


def test_full_uploads_the_current_state_then_the_marker_then_the_record_and_deletes_nothing(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    (root / "snapshots" / "bioproject" / "2026-10-14" / "bioproject.xml.sha256").write_text("x")
    unread = _snapshot(root, "geo-series", "2026-01-01")

    _full(root)

    sent = _files(root, "builds/b1", "raw", *(f"snapshots/{s}/2026-10-14" for s in _SOURCES))
    assert rclone.commands() == ["lsjson", "copy", "copy", "copyto"]
    probe, files, top, record = rclone.calls
    assert probe == ["rclone", "lsjson", _DEST, "--max-depth", "1"]
    # Every file of the current state lists both sides; the marker goes after the build.
    assert files[:4] == ["rclone", "copy", str(root), _DEST]
    assert "--no-traverse" not in files
    assert rclone.lists[:2] == [sent, _TOP]
    assert top[:4] == ["rclone", "copy", str(root), _DEST]
    assert record[1:] == ["copyto", record[2], f"{_DEST}/{COPIED}"]
    assert rclone.record() == sorted([*sent, *_TOP])
    assert (root / COPIED).read_bytes() == rclone.bucket[COPIED]
    assert not any(str(unread.relative_to(root)) in p for p in rclone.record())


def test_a_routine_copy_sends_the_new_build_then_deletes_the_old_one_by_name(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _full(root)
    rclone.calls.clear()
    rclone.lists.clear()
    _snapshot(root, "bioproject", "2026-11-01")
    extract.extract(root, "bioproject", workers=1)
    build(root, "b2")

    result = _copy(root)

    assert result.exit_code == 0, result.output
    every_time = [
        "raw/pubmed-si/README.md",
        "raw/pubmed-si/manifest.json",
        "raw/sra-xml/README.md",
        "raw/sra-xml/manifest.json",
        # The fixture's snapshots have no manifest to give them a sha256.
        "snapshots/bioproject/2026-11-01/bioproject.xml",
        "snapshots/biosample/2026-10-14/biosample_set.xml.gz",
        "snapshots/geo-series/2026-10-14/gds_gse_esummary.jsonl.gz",
        "snapshots/sra-accessions/2026-10-14/SRA_Accessions.tab",
    ]
    old = [*_files(root, "builds/b1"), "snapshots/bioproject/2026-10-14/bioproject.xml"]
    assert rclone.commands() == ["lsjson", "copy", "copy", "delete", "copyto"]
    # The raw files are unchanged, so none goes up; the old build is still on the disk.
    assert rclone.lists == [sorted([*_files(root, "builds/b2"), *every_time]), _TOP, old]
    assert "--no-traverse" in rclone.calls[1]
    assert rclone.calls[3][:4] == ["rclone", "delete", _DEST, "--no-traverse"]
    raw = [p for p in _files(root, "raw") if p.endswith(".parquet")]
    assert rclone.record() == sorted([*_files(root, "builds/b2"), *every_time, *raw, *_TOP])

    rclone.lists.clear()
    assert _copy(root).exit_code == 0
    small = ["builds/b2/README.md", "builds/b2/manifest.json"]
    assert rclone.lists == [[*small, *every_time], _TOP]


def test_a_side_file_leaves_the_bucket_only_with_its_build_raw_source_or_snapshot(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    extra = "snapshots/bioproject/2026-10-14/bioproject.xml.sha256"
    (root / extra).write_text("x")
    _full(root)
    # Side files the manifest does not name, missing on the disk while what holds them stays.
    missing = ["raw/pubmed-si/README.md", "builds/b1/README.md", extra]
    for path in missing:
        (root / path).unlink()
    rclone.calls.clear()

    assert _copy(root).exit_code == 0
    assert "delete" not in rclone.commands()
    assert set(missing) <= set(rclone.record())

    # The build and the snapshot leave the current state, and their side files the bucket.
    _snapshot(root, "bioproject", "2026-11-01")
    extract.extract(root, "bioproject", workers=1)
    build(root, "b2")
    rclone.lists.clear()

    assert _copy(root).exit_code == 0
    deleted = rclone.lists[-1]
    assert {"builds/b1/README.md", extra} <= set(deleted)
    assert "raw/pubmed-si/README.md" not in deleted
    assert "raw/pubmed-si/README.md" in rclone.record()
    assert not {"builds/b1/README.md", extra} & set(rclone.record())


def test_a_copy_record_that_fails_to_go_up_leaves_the_disks_so_the_next_copy_deletes_again(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _full(root)
    build(root, "b2")
    rclone.expiring = "copyto"

    assert "bucket is behind" in _copy(root).output
    assert "builds/b1/manifest.json" in json.loads((root / COPIED).read_text())

    rclone.expiring = ""
    rclone.lists.clear()
    assert _copy(root).exit_code == 0
    assert "builds/b1/manifest.json" in rclone.lists[2]


def _drop_bucket(root: Path, *names: str) -> list[str]:
    """Remove every record of raw ``pubmed-si`` files, so pack deletes them; build ``b2``."""
    gone = [f"raw/pubmed-si/{name}" for name in names or ["SRPnnnnnn.parquet"]]
    for path in gone:
        rows = pq.read_table(root / path, columns=["accession"]).column(0).to_pylist()
        raw.write_removals(root, "pubmed-si", rows)
    pack(root, ["pubmed-si"])
    build(root, "b2")
    assert not any((root / path).exists() for path in gone)
    return gone


def test_a_raw_file_pack_dropped_is_deleted_in_the_bucket_by_name(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _full(root)
    rclone.lists.clear()
    # The fixture holds three raw files; a copy may always delete one.
    gone = _drop_bucket(root)

    assert _copy(root).exit_code == 0

    assert rclone.lists[2] == sorted([*_files(root, "builds/b1"), *gone])
    assert not set(gone) & set(rclone.record())


def test_a_copy_that_would_delete_too_many_raw_files_sends_nothing_unless_accepted(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _full(root)
    rclone.calls.clear()
    gone = _drop_bucket(root, "SRPnnnnnn.parquet", "PRJNA1nnnnnn.parquet")

    refused = _copy(root)
    assert refused.exit_code == 1
    assert "2 of the 3 raw files" in refused.output
    assert "labdata store copy --accept-deletes 2" in refused.output
    assert "--full" not in refused.output
    assert rclone.commands() == ["lsjson"]
    assert _copy(root, "--accept-deletes", "1").exit_code == 1
    full = _copy(root, "--full", "--accept-deletes", "2")
    assert full.exit_code == 1
    assert "--full deletes nothing" in full.output
    assert rclone.commands() == ["lsjson", "lsjson"]

    rclone.lists.clear()
    assert _copy(root, "--accept-deletes", "2").exit_code == 0
    assert set(gone) <= set(rclone.lists[2])
    assert not set(gone) & set(rclone.record())


def test_with_no_record_on_disk_a_copy_fetches_the_buckets_and_sends_only_changes(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _full(root)
    (root / COPIED).unlink()
    rclone.calls.clear()
    rclone.lists.clear()

    assert _copy(root).exit_code == 0

    assert rclone.commands() == ["lsjson", "copy", "copy", "copy", "copyto"]
    assert rclone.calls[1][:3] == ["rclone", "copy", _DEST]
    assert "--no-traverse" in rclone.calls[1]
    assert rclone.lists[0] == [COPIED, "labdata-store.toml"]
    assert not any(p.endswith(".parquet") for p in rclone.lists[1])
    # The local marker is never overwritten by the bucket's.
    assert "copy_to" in (root / "labdata-store.toml").read_text()


def _bucket_marker(rclone: _Rclone, root: Path, build_id: str) -> None:
    """Put in the bucket a marker naming ``build_id``, as the last mirror left it."""
    text = (root / "labdata-store.toml").read_text(encoding="utf-8")
    current = tomllib.loads(text)["build"]
    rclone.bucket["labdata-store.toml"] = text.replace(f'"{current}"', f'"{build_id}"').encode()


def _status(root: Path, **status: object) -> None:
    refresh.write_status(root, {"outcome": "succeeded", **status})


def test_with_no_record_a_copy_seeds_one_from_the_build_the_buckets_marker_names(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _status(root, copy_pending=False)
    _bucket_marker(rclone, root, "b1")

    assert _copy(root).exit_code == 0

    # The seeded record goes up first, then only the small files: nothing is deleted.
    assert rclone.commands() == ["lsjson", "copy", "copyto", "copy", "copy", "copyto"]
    assert all(not p.endswith(".parquet") for p in rclone.lists[1])
    assert rclone.lists[2] == _TOP
    sent = _files(root, "builds/b1", "raw", *(f"snapshots/{s}/2026-10-14" for s in _SOURCES))
    assert rclone.record() == sorted([*sent, *_TOP])


def test_with_no_record_after_a_switch_a_copy_sends_the_new_build_and_deletes_the_old(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _bucket_marker(rclone, root, "b1")
    build(root, "b2")
    # The switch raised copy_pending; the copy before it had worked.
    _status(root, copy_pending=True, copy_pending_at_switch=False)

    assert _copy(root).exit_code == 0

    assert rclone.commands() == ["lsjson", "copy", "copyto", "copy", "copy", "delete", "copyto"]
    assert set(_files(root, "builds/b2")) <= set(rclone.lists[1])
    assert rclone.lists[3] == _files(root, "builds/b1")
    assert not any(p.startswith("builds/b1/") for p in rclone.record())


@pytest.mark.parametrize(
    ("marker_build", "status"),
    [
        ("b0", {"copy_pending": False}),
        ("b1", {"copy_pending": True, "copy_pending_at_switch": True}),
        ("b1", {}),
    ],
    ids=["build-gone", "copy-was-pending", "no-status-record"],
)
def test_with_no_record_a_copy_that_cannot_trust_the_bucket_skips_and_asks_for_full(
    root: Path, rclone: _Rclone, marker_build: str, status: dict[str, object]
) -> None:
    _copy_to(root)
    if status:
        _status(root, **status)
    _bucket_marker(rclone, root, marker_build)

    result = _copy(root)

    assert result.exit_code == 0, result.output
    assert "labdata store copy --full" in result.output
    assert rclone.commands() == ["lsjson", "copy"]
    assert COPIED not in rclone.bucket


def test_with_no_record_on_disk_or_in_the_bucket_a_copy_sends_nothing_and_asks_for_full(
    root: Path, rclone: _Rclone
) -> None:
    _copy_to(root)
    _status(root, copy_pending=False)

    result = _copy(root)

    assert result.exit_code == 0, result.output
    assert "labdata store copy --full" in result.output
    # It only checked the login and looked for the bucket's record and marker.
    assert rclone.commands() == ["lsjson", "copy"]
    assert rclone.lists == [[COPIED, "labdata-store.toml"]]


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


@pytest.mark.parametrize("fault", ["rclone", "record"])
def test_any_failure_past_the_destination_marks_the_copy_pending(
    root: Path, rclone: _Rclone, fault: str
) -> None:
    _copy_to(root)
    _full(root)
    _status(root, copy_pending=False)
    if fault == "rclone":
        rclone.returncode, rclone.stderr = 3, "ERROR : bucket not found\n"
    else:
        (root / COPIED).write_text("not json")

    assert _copy(root).exit_code == 1
    assert refresh.read_status(root)["copy_pending"] is True
