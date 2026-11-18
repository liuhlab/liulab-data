"""Prune old builds and snapshots after a grace period; copy the current store with rclone."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import tomllib
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner, Result

from labdata.cli import app
from labdata.store import extract, lifecycle, raw
from labdata.store.gcs import COPIED, check
from labdata.store.lifecycle import build
from labdata.store.prune import GRACE, prune
from tests._fake_gcs import FakeGcs, KilledError
from tests._slices import BuiltStore

_HOUR = dt.timedelta(hours=1)
_DEST = "gcs:lab-bucket/geo-sra-store"
_SOURCES = ("bioproject", "biosample", "geo-series", "sra-accessions")


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


def test_prune_removes_a_killed_builds_leftovers_at_once_but_not_an_unswitched_builds(
    root: Path,
) -> None:
    (root / "builds" / "b2").mkdir()
    lifecycle.write_status(root, {"update": "run", "outcome": "failed", "build": "b2"})
    killed = [root / "builds" / ".b3.4242.tmp", root / "work" / "build" / "b3"]
    kept = [root / "builds" / "b2", root / "work" / "build" / "b2", root / "builds" / "b1"]
    spill = root / "work" / "tmp" / "4242"
    for folder in [*killed, *kept, spill]:
        folder.mkdir(parents=True, exist_ok=True)
    (spill / "duckdb_temp_block-1.block").write_bytes(b"x")

    assert sorted(prune(root).deleted) == sorted([*killed, spill])
    assert all(folder.is_dir() for folder in kept)


def test_prune_never_deletes_the_current_build(root: Path) -> None:
    result = CliRunner().invoke(app, ["store", "prune", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert "Deleted nothing" in result.output
    assert prune(root, now=_now() + 1000 * _HOUR).deleted == []
    assert (root / "builds" / "b1").is_dir()


# ---------------------------------------------------------------- copy

_TOP = ["README.md", "labdata-store.toml"]
_STATUS = "refresh-status.json"


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    return FakeGcs.install(monkeypatch, tmp_path / "gcs", _DEST)


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


def _record(gcs: FakeGcs) -> list[str]:
    """The paths the copy record in GCS names."""
    return sorted(json.loads(gcs.read(COPIED)))


def _current(root: Path) -> list[str]:
    """The current state of a store built once as ``b1``: what its copy sends."""
    snapshots = [f"snapshots/{s}/2026-10-14" for s in _SOURCES]
    return sorted([*_files(root, "builds/b1", "raw", *snapshots), *_TOP])


def _holds_its_record(gcs: FakeGcs, root: Path) -> None:
    """Assert GCS holds exactly what its copy record names, as the work disk holds it."""
    assert set(gcs.files()) - {_STATUS} == {*_record(gcs), COPIED}
    for path in _record(gcs):
        assert gcs.md5[path] == hashlib.md5((root / path).read_bytes()).hexdigest(), path


def test_full_uploads_the_current_state_then_the_marker_then_the_record_and_deletes_nothing(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    (root / "snapshots" / "bioproject" / "2026-10-14" / "bioproject.xml.sha256").write_text("x")
    unread = _snapshot(root, "geo-series", "2026-01-01")

    _full(root)

    assert gcs.phases(COPIED, _STATUS) == ["files", "top", "record", "status"]
    assert gcs.sent()[-4:] == [*_TOP, COPIED, _STATUS]
    assert gcs.files() == sorted([*_current(root), COPIED, _STATUS])
    # Every file of the current state lists both sides.
    assert gcs.walks == 1
    assert (root / COPIED).read_bytes() == gcs.read(COPIED)
    assert not any(str(unread.relative_to(root)) in p for p in gcs.files())


def test_after_each_copy_gcs_holds_exactly_the_files_its_copy_record_names(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    _holds_its_record(gcs, root)
    gone = _drop_bucket(root)

    assert _copy(root).exit_code == 0

    _holds_its_record(gcs, root)
    assert _record(gcs) == sorted(json.loads((root / COPIED).read_text()))
    assert not set(gone) & set(gcs.files())
    assert not any(p.startswith("builds/b1/") for p in gcs.files())


def test_a_routine_copy_sends_the_new_build_then_deletes_the_old_one_by_name(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    gcs.forget()
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
    # The raw files are unchanged, so none goes up; the old build is still on the disk.
    assert gcs.phases(COPIED, _STATUS) == ["files", "top", "delete", "record", "status"]
    sent = [*_files(root, "builds/b2"), *every_time, *_TOP, COPIED, _STATUS]
    assert sorted(gcs.sent()) == sorted(sent)
    assert sorted(gcs.deleted()) == old
    assert gcs.walks == 0
    raw = [p for p in _files(root, "raw") if p.endswith(".parquet")]
    assert _record(gcs) == sorted([*_files(root, "builds/b2"), *every_time, *raw, *_TOP])

    gcs.forget()
    assert _copy(root).exit_code == 0
    small = ["builds/b2/README.md", "builds/b2/manifest.json"]
    assert sorted(gcs.sent()) == sorted([*small, *every_time, *_TOP, COPIED, _STATUS])


def test_a_side_file_leaves_gcs_only_with_its_build_raw_source_or_snapshot(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    extra = "snapshots/bioproject/2026-10-14/bioproject.xml.sha256"
    (root / extra).write_text("x")
    _full(root)
    # Side files the manifest does not name, missing on the disk while what holds them stays.
    missing = ["raw/pubmed-si/README.md", "builds/b1/README.md", extra]
    for path in missing:
        (root / path).unlink()
    gcs.forget()

    assert _copy(root).exit_code == 0
    assert gcs.deleted() == []
    assert set(missing) <= set(_record(gcs)) & set(gcs.files())

    # The build and the snapshot leave the current state, and their side files GCS.
    _snapshot(root, "bioproject", "2026-11-01")
    extract.extract(root, "bioproject", workers=1)
    build(root, "b2")
    gcs.forget()

    assert _copy(root).exit_code == 0
    assert {"builds/b1/README.md", extra} <= set(gcs.deleted())
    assert "raw/pubmed-si/README.md" not in gcs.deleted()
    assert "raw/pubmed-si/README.md" in set(_record(gcs)) & set(gcs.files())
    assert not {"builds/b1/README.md", extra} & set(_record(gcs))


def test_a_copy_record_that_fails_to_go_up_leaves_the_disks_so_the_next_copy_deletes_again(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    build(root, "b2")
    gcs.expired = "copyto"

    assert "rclone config reconnect gcs:" in _copy(root).output
    assert "builds/b1/manifest.json" in json.loads((root / COPIED).read_text())
    assert "builds/b1/manifest.json" in _record(gcs)

    gcs.expired = False
    gcs.forget()
    assert _copy(root).exit_code == 0
    assert "builds/b1/manifest.json" in gcs.deleted()
    _holds_its_record(gcs, root)


def test_a_copy_cut_off_before_its_copy_record_ends_the_same_when_run_again(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    _status(root, copy_pending=False)
    before = gcs.read(COPIED)
    _drop_bucket(root)
    gcs.kill = COPIED

    assert isinstance(_copy(root).exception, KilledError)
    # GCS holds the new build, but its copy record still names the old one.
    assert gcs.read(COPIED) == before
    assert (root / COPIED).read_bytes() == before
    assert lifecycle.read_status(root)["copy_pending"] is True

    gcs.kill = ""
    assert _copy(root).exit_code == 0
    _holds_its_record(gcs, root)
    assert (root / COPIED).read_bytes() == gcs.read(COPIED)
    assert lifecycle.read_status(root)["copy_pending"] is False


def test_a_copy_sends_the_status_record_last_marked_finalized_and_again_if_cut_off(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    # The build by hand left the store built: the first copy finalizes it.
    _full(root)
    assert gcs.sent()[-2:] == [COPIED, _STATUS]
    assert _finalized(gcs, root)["outcome"] == "finalized"
    before = gcs.read(_STATUS)
    build(root, "b2")
    # A refresh past its switch, copying as its last step.
    switched = {"last_success": "2026-11-01", "copy_pending_at_switch": False, "ended": None}
    _status(root, **switched, copy_pending=True)
    gcs.kill = _STATUS

    assert isinstance(_copy(root).exception, KilledError)
    # GCS holds b2 and its copy record, and the status record of the copy before.
    assert "builds/b2/manifest.json" in _record(gcs)
    assert gcs.read(_STATUS) == before

    gcs.kill = ""
    gcs.forget()
    assert _copy(root).exit_code == 0
    assert gcs.sent()[-2:] == [COPIED, _STATUS]
    status = _finalized(gcs, root)
    assert status["last_success"] == "2026-11-01"
    assert status["ended"]


def test_a_copy_cut_after_its_copy_record_lands_passes_the_next_check_and_ends_finalized(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    build(root, "b2")
    switched = {"last_success": "2026-11-01", "copy_pending_at_switch": False, "ended": None}
    _status(root, **switched, copy_pending=True)
    gcs.cut = COPIED

    assert isinstance(_copy(root).exception, KilledError)
    assert gcs.read(COPIED) != (root / COPIED).read_bytes()

    gcs.cut = ""
    # The check an update starts with: it adopts the record GCS holds.
    assert check(root)
    assert (root / COPIED).read_bytes() == gcs.read(COPIED)
    assert _copy(root).exit_code == 0
    assert _finalized(gcs, root)["last_success"] == "2026-11-01"
    assert not [p for p in gcs.files() if "pending" in p]
    _holds_its_record(gcs, root)


@pytest.mark.parametrize("outcome", ["running", "failed"])
def test_a_copy_leaves_the_status_record_of_a_refresh_that_has_not_switched(
    root: Path, gcs: FakeGcs, outcome: str
) -> None:
    _copy_to(root)
    _full(root)
    before = gcs.read(_STATUS)
    _status(root, outcome=outcome, folder="work/refresh/20261101T000000Z", copy_pending=True)
    gcs.forget()

    assert _copy(root).exit_code == 0

    assert _STATUS not in gcs.sent()
    assert gcs.read(_STATUS) == before
    # A rerun of the refresh resumes it.
    status = lifecycle.read_status(root)
    assert (status["outcome"], status["copy_pending"]) == (outcome, False)


def _finalized(gcs: FakeGcs, root: Path) -> dict[str, object]:
    """Assert GCS and the work disk hold the same status record, finalized; return it."""
    assert gcs.read(_STATUS) == (root / _STATUS).read_bytes()
    status = json.loads(gcs.read(_STATUS))
    assert (status["outcome"], status["copy_pending"]) == ("finalized", False)
    return status


def _drop_bucket(root: Path, *names: str) -> list[str]:
    """Remove every record of raw ``pubmed-si`` files, so pack deletes them; build ``b2``."""
    gone = [f"raw/pubmed-si/{name}" for name in names or ["SRPnnnnnn.parquet"]]
    for path in gone:
        rows = pq.read_table(root / path, columns=["accession"]).column(0).to_pylist()
        raw.remove(root, "pubmed-si", rows)
    raw.pack(root, ["pubmed-si"])
    build(root, "b2")
    assert not any((root / path).exists() for path in gone)
    return gone


def test_a_raw_file_pack_dropped_is_deleted_in_gcs_by_name(root: Path, gcs: FakeGcs) -> None:
    _copy_to(root)
    _full(root)
    gcs.forget()
    # The fixture holds three raw files; a copy may always delete one.
    gone = _drop_bucket(root)

    assert _copy(root).exit_code == 0

    assert sorted(gcs.deleted()) == sorted([*_files(root, "builds/b1"), *gone])
    assert not set(gone) & set(gcs.files())


def test_a_copy_that_would_delete_too_many_raw_files_sends_nothing_unless_accepted(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    gcs.forget()
    gone = _drop_bucket(root, "SRPnnnnnn.parquet", "PRJNA1nnnnnn.parquet")

    refused = _copy(root)
    assert refused.exit_code == 1
    assert "2 of the 3 raw files" in refused.output
    assert "labdata store copy --accept-deletes 2" in refused.output
    assert "--full" not in refused.output
    assert _copy(root, "--accept-deletes", "1").exit_code == 1
    full = _copy(root, "--full", "--accept-deletes", "2")
    assert full.exit_code == 1
    assert "--full deletes nothing" in full.output
    assert gcs.log == []

    assert _copy(root, "--accept-deletes", "2").exit_code == 0
    assert set(gone) <= set(gcs.deleted())
    assert not set(gone) & set(gcs.files())


def test_a_copy_from_a_disk_behind_gcs_sends_nothing_until_a_restore_brings_it_in_step(
    root: Path, gcs: FakeGcs, tmp_path: Path
) -> None:
    _copy_to(root)
    _full(root)
    other = tmp_path / "other"
    shutil.copytree(root, other)
    build(other, "b2")
    assert _copy(other).exit_code == 0
    gcs.forget()
    # This disk still names b1: its copy record is not the one in GCS, nor is a pending one.
    _status(root, copy_pending=False)
    shutil.copy(root / COPIED, root / f"{COPIED}.pending")

    refused = _copy(root)

    assert refused.exit_code == 1
    assert "labdata store restore" in refused.output
    assert gcs.log == []
    assert lifecycle.read_status(root)["copy_pending"] is True
    assert "builds/b2/manifest.json" in _record(gcs)

    assert CliRunner().invoke(app, ["store", "restore", "--root", str(root)]).exit_code == 0
    gcs.forget()

    assert _copy(root).exit_code == 0
    # The check read GCS's MD5 alone.
    assert not any(op == "get" for op, _ in gcs.log)
    _holds_its_record(gcs, root)


def test_a_routine_copy_to_a_gcs_emptied_since_sends_nothing_and_asks_for_full(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    for path in gcs.files():
        gcs.remove(path)
    gcs.forget()

    refused = _copy(root)

    # Changes alone would leave GCS partial.
    assert refused.exit_code == 1
    assert "labdata store copy --full" in refused.output
    assert gcs.log == []
    assert lifecycle.read_status(root)["copy_pending"] is True
    _full(root)
    _holds_its_record(gcs, root)


def test_with_no_record_on_disk_while_gcs_holds_one_a_copy_sends_nothing_and_asks_to_restore(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _full(root)
    # A second disk: it would roll GCS back to its own state.
    (root / COPIED).unlink()
    _status(root, copy_pending=False)
    gcs.forget()

    refused = _copy(root)

    assert refused.exit_code == 1
    assert f"labdata store restore --from {_DEST}" in refused.output
    assert gcs.log == []
    assert lifecycle.read_status(root)["copy_pending"] is True


def _gcs_marker(gcs: FakeGcs, root: Path, build_id: str) -> None:
    """Put in GCS a marker naming ``build_id``, as the last mirror left it."""
    text = (root / "labdata-store.toml").read_text(encoding="utf-8")
    current = tomllib.loads(text)["build"]
    gcs.write("labdata-store.toml", text.replace(f'"{current}"', f'"{build_id}"').encode())


def _status(root: Path, **status: object) -> None:
    lifecycle.write_status(root, {"outcome": "built", **status})


def _lose_record(root: Path, gcs: FakeGcs) -> None:
    """Make GCS a mirror of the current build with no copy record, as before copy records."""
    _full(root)
    gcs.remove(COPIED)
    (root / COPIED).unlink()
    gcs.forget()


def test_with_no_record_a_copy_seeds_one_from_the_build_the_gcs_marker_names(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _lose_record(root, gcs)
    _status(root, copy_pending=False)

    assert _copy(root).exit_code == 0

    # The seeded record goes up first, then only the small files: nothing is deleted.
    assert gcs.phases(COPIED, _STATUS) == ["get", "record", "files", "top", "record", "status"]
    assert all(not p.endswith(".parquet") for p in gcs.sent())
    assert _record(gcs) == _current(root)
    _holds_its_record(gcs, root)


@pytest.mark.parametrize("damage", ["missing", "short"])
def test_with_no_record_a_copy_refuses_to_seed_one_naming_files_gcs_lacks(
    root: Path, gcs: FakeGcs, damage: str
) -> None:
    _copy_to(root)
    _lose_record(root, gcs)
    _status(root, copy_pending=False)
    path = "builds/b1/holdings.parquet"
    if damage == "missing":
        gcs.remove(path)
    else:
        gcs.write(path, gcs.read(path)[:-1])

    result = _copy(root)

    assert result.exit_code == 1, result.output
    assert "labdata store copy --full" in result.output
    assert gcs.sent() == []
    assert COPIED not in gcs.files()
    assert lifecycle.read_status(root)["copy_pending"] is True


def test_with_no_record_after_a_switch_a_copy_sends_the_new_build_and_deletes_the_old(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _lose_record(root, gcs)
    build(root, "b2")
    # The switch raised copy_pending; the copy before it had worked.
    _status(root, copy_pending=True, copy_pending_at_switch=False)

    assert _copy(root).exit_code == 0

    steps = ["get", "record", "files", "top", "delete", "record", "status"]
    assert gcs.phases(COPIED, _STATUS) == steps
    assert set(_files(root, "builds/b2")) <= set(gcs.sent())
    assert sorted(gcs.deleted()) == _files(root, "builds/b1")
    _holds_its_record(gcs, root)


@pytest.mark.parametrize(
    ("marker_build", "status"),
    [
        ("b0", {"copy_pending": False}),
        ("b1", {"copy_pending": True, "copy_pending_at_switch": True}),
        ("b1", {}),
    ],
    ids=["build-gone", "copy-was-pending", "no-status-record"],
)
def test_with_no_record_a_copy_that_cannot_trust_gcs_skips_and_asks_for_full(
    root: Path, gcs: FakeGcs, marker_build: str, status: dict[str, object]
) -> None:
    _copy_to(root)
    (root / _STATUS).unlink()
    if status:
        _status(root, **status)
    _gcs_marker(gcs, root, marker_build)

    result = _copy(root)

    assert result.exit_code == 1, result.output
    assert "labdata store copy --full" in result.output
    assert gcs.sent() == []
    assert gcs.files() == ["labdata-store.toml"]


def test_with_no_record_on_disk_or_in_gcs_a_copy_sends_nothing_and_asks_for_full(
    root: Path, gcs: FakeGcs
) -> None:
    _copy_to(root)
    _status(root, copy_pending=False)

    result = _copy(root)

    assert result.exit_code == 1, result.output
    assert "labdata store copy --full" in result.output
    assert gcs.log == []
    assert gcs.files() == []


def test_copy_is_refused_on_a_folder_with_no_marker(tmp_path: Path, gcs: FakeGcs) -> None:
    result = _copy(tmp_path, "--to", _DEST)
    assert result.exit_code != 0
    assert "no labdata-store.toml" in result.output
    assert gcs.calls == []


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
    root: Path, gcs: FakeGcs, gone: str, message: str
) -> None:
    path = root / gone
    shutil.rmtree(path) if path.is_dir() else path.unlink()
    result = _copy(root, "--to", _DEST)
    assert result.exit_code != 0
    assert "refused" in result.output
    assert message in result.output
    assert gcs.calls == []


def test_copy_needs_a_destination(root: Path, gcs: FakeGcs) -> None:
    result = _copy(root)
    assert result.exit_code != 0
    assert "copy_to" in result.output
    assert gcs.calls == []


def test_an_expired_login_skips_the_copy_and_says_how_to_log_in(root: Path, gcs: FakeGcs) -> None:
    gcs.expired = True
    result = _copy(root, "--to", _DEST)
    assert result.exit_code == 1, result.output
    assert "is behind" in result.output
    assert "rclone config reconnect gcs:" in result.output
    assert gcs.log == []


def test_another_rclone_failure_fails_the_copy(root: Path, gcs: FakeGcs) -> None:
    gcs.error = "ERROR : directory not found\n"
    result = _copy(root, "--to", _DEST)
    assert result.exit_code == 1
    assert "directory not found" in result.output


@pytest.mark.parametrize("fault", ["rclone", "record"])
def test_any_failure_past_the_destination_marks_the_copy_pending(
    root: Path, gcs: FakeGcs, fault: str
) -> None:
    _copy_to(root)
    _full(root)
    _status(root, copy_pending=False)
    if fault == "rclone":
        gcs.error = "ERROR : bucket not found\n"
    else:
        (root / COPIED).write_text("not json")

    assert _copy(root).exit_code == 1
    assert lifecycle.read_status(root)["copy_pending"] is True


def test_a_restore_onto_a_disk_without_a_copy_record_leaves_exactly_the_state_gcs_names(
    root: Path, gcs: FakeGcs, tmp_path: Path
) -> None:
    _copy_to(root)
    _full(root)
    gcs.forget()
    disk = tmp_path / "other"
    shutil.copytree(root, disk)
    (disk / COPIED).unlink()
    lost = next(p for p in _record(gcs) if p.startswith("raw/") and p.endswith(".parquet"))
    # State GCS does not hold, which the next build would read.
    stray = [str(Path(lost).with_name("stray.parquet")), "builds/b0/manifest.json"]
    (disk / "builds" / "b0").mkdir()
    for path in stray:
        shutil.copy(disk / lost, disk / path)
    (disk / lost).unlink()
    (disk / "work" / "eutils").mkdir(parents=True, exist_ok=True)
    (disk / "work" / "eutils" / "half-done.tsv").write_text("mine")
    kept = {p: (disk / p).read_bytes() for p in _files(disk, "work", "extracts")}
    assert any(p.startswith("extracts/") for p in kept)

    result = CliRunner().invoke(app, ["store", "restore", "--root", str(disk)])

    assert result.exit_code == 0, result.output
    assert sorted(p for op, p in gcs.log if op == "get") == sorted([COPIED, _STATUS, lost])
    assert (disk / lost).read_bytes() == gcs.read(lost)
    assert (disk / COPIED).read_bytes() == gcs.read(COPIED)
    assert (disk / _STATUS).read_bytes() == gcs.read(_STATUS)
    assert sorted([*_files(disk, "builds", "raw", "snapshots"), *_TOP]) == _record(gcs)
    assert check(disk)
    assert {p: (disk / p).read_bytes() for p in _files(disk, "work", "extracts")} == kept
