"""``labdata papers copy``: the routine copy sends only what changed since the last success."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.papers import init_paper_store, open_paper_store, tables
from labdata.papers.copy import COPIED, PaperCopyError, copy
from tests._fake_gcs import FakeGcs
from tests._paper_fakes import fixture_build

DEST = "gcs:bucket/datalake/paper-store"


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    return FakeGcs.install(monkeypatch, tmp_path / "gcs", DEST)


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    store = fixture_build(tmp_path / "store", [("GSE1", "PMID:41000001", "SOFT")])
    path = tmp_path / "papers"
    init_paper_store(path, store, copy_to=DEST)
    return path


def hold(root: Path, paper: str, kind: str, tier: str, data: bytes = b"%PDF") -> str:
    """Write one paper file and its ``files`` row, replacing any row it had; return its path."""
    path = f"papers/{paper}/{paper}.{kind}"
    (root / path).parent.mkdir(parents=True, exist_ok=True)
    (root / path).write_bytes(data)
    rows = [r for r in tables.read(root, tables.FILES) if r["path"] != path]
    row = {"paper": paper, "kind": kind, "name": f"{paper}.{kind}", "path": path, "tier": tier}
    rows.append({**row, "size": len(data), "md5": hashlib.md5(data).hexdigest()})
    tables.write(root, tables.FILES, rows)
    return path


def drop(root: Path, path: str) -> None:
    """Remove a paper file and its ``files`` row, as PMC does to the PDF it replaces."""
    (root / path).unlink()
    rows = [r for r in tables.read(root, tables.FILES) if r["path"] != path]
    tables.write(root, tables.FILES, rows)


def full(root: Path, gcs: FakeGcs) -> None:
    """Run a full copy, then forget its calls."""
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 0, result.output
    gcs.forget()


def status(root: Path) -> dict:
    return json.loads((root / "paper-status.json").read_text())


def record(gcs: FakeGcs) -> list[str]:
    """The paths the copy record in GCS names."""
    return pq.read_table(pa.BufferReader(gcs.read(COPIED))).column("path").to_pylist()


def papers(paths: list[str]) -> list[str]:
    """The paper files among ``paths``: those below the top level."""
    return sorted(p for p in paths if "/" in p)


def test_full_uploads_every_file_then_the_record_and_deletes_nothing(
    root: Path, gcs: FakeGcs
) -> None:
    path = hold(root, "1", "pdf", "host")

    copy(open_paper_store(root), full=True)

    # The paper files first; the top level, tables and marker among it, after them.
    assert gcs.phases(COPIED) == ["files", "top", "record"]
    assert papers(gcs.files()) == papers(record(gcs)) == [path]
    top = sorted(p.name for p in root.iterdir() if p.is_file() and not p.name.startswith("."))
    assert sorted(set(gcs.files()) - {path, COPIED}) == sorted(set(top) - {"paper-status.json"})
    # Every file lists both sides.
    assert gcs.walks == 1
    assert status(root)["copy_pending"] is False
    assert status(root)["last_copy_success"]


def test_after_each_copy_gcs_holds_exactly_the_files_its_copy_record_names(
    root: Path, gcs: FakeGcs
) -> None:
    for paper in range(2, 21):
        hold(root, str(paper), "pdf", "host")
    pdf = hold(root, "1", "pdf", "openalex")
    copy(open_paper_store(root), full=True)
    assert gcs.files() == sorted([*record(gcs), COPIED])
    hold(root, "1", "xml", "pmc", b"<article/>")
    drop(root, pdf)

    copy(open_paper_store(root))

    assert gcs.files() == sorted([*record(gcs), COPIED])
    for path in record(gcs):
        assert gcs.md5[path] == hashlib.md5((root / path).read_bytes()).hexdigest(), path


def test_a_full_copy_from_a_disk_without_the_whole_store_uploads_nothing(
    root: Path, gcs: FakeGcs
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, gcs)
    # A disk that was not restored: it lacks files GCS holds, and the copy record.
    drop(root, paths[0])
    drop(root, paths[1])
    (root / COPIED).unlink()

    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])

    assert result.exit_code == 1
    assert "Restore the store from the bucket first" in result.output
    # It only fetched the copy record in GCS, which stays as it was.
    assert gcs.sent() == []
    assert gcs.deleted() == []
    assert papers(record(gcs)) == sorted(paths)
    assert status(root)["copy_pending"] is True


def test_a_routine_copy_sends_only_the_new_rows_and_lists_no_paper_folder(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "1", "pdf", "openalex")
    full(root, gcs)
    new = hold(root, "2", "pdf", "host")

    copy(open_paper_store(root))

    assert papers(gcs.sent()) == [new]
    assert gcs.phases(COPIED) == ["files", "top", "record"]
    assert gcs.walks == 0
    assert papers(record(gcs)) == sorted([new, "papers/1/1.pdf"])


def test_a_pdf_pmc_replaced_is_deleted_in_gcs_by_the_next_copy(root: Path, gcs: FakeGcs) -> None:
    # One file in 20 left the table: the most one copy deletes.
    for paper in range(2, 21):
        hold(root, str(paper), "pdf", "host")
    pdf = hold(root, "1", "pdf", "openalex")
    full(root, gcs)
    xml = hold(root, "1", "xml", "pmc", b"<article/>")
    drop(root, pdf)

    copy(open_paper_store(root))

    # The tables go up before the PDF they dropped is deleted, the record after it.
    assert gcs.phases(COPIED) == ["files", "top", "delete", "record"]
    assert papers(gcs.sent()) == [xml]
    assert gcs.deleted() == [pdf]
    assert xml in record(gcs)
    assert pdf not in record(gcs)
    assert pdf not in gcs.files()

    gcs.forget()
    copy(open_paper_store(root))
    assert papers(gcs.sent()) == []
    assert gcs.deleted() == []


def test_a_copy_that_would_delete_more_than_one_file_in_20_copies_nothing_and_says_why(
    root: Path, gcs: FakeGcs
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, gcs)
    drop(root, paths[0])
    drop(root, paths[1])

    with pytest.raises(PaperCopyError, match="2 of the 20 files"):
        copy(open_paper_store(root))

    # Not even the tables go up: the files table may be damaged.
    assert gcs.log == []
    assert status(root)["copy_pending"] is True


def test_files_moved_on_purpose_are_deleted_in_gcs_up_to_the_accepted_count(
    root: Path, gcs: FakeGcs
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, gcs)
    drop(root, paths[0])
    drop(root, paths[1])
    run = CliRunner().invoke

    refused = run(app, ["papers", "copy", "--root", str(root)])
    assert refused.exit_code == 1
    assert "--accept-deletes 2" in refused.output
    too_few = run(app, ["papers", "copy", "--root", str(root), "--accept-deletes", "1"])
    assert too_few.exit_code == 1
    assert gcs.log == []

    done = run(app, ["papers", "copy", "--root", str(root), "--accept-deletes", "2"])

    assert done.exit_code == 0, done.output
    assert gcs.deleted() == paths[:2]
    assert papers(record(gcs)) == sorted(paths[2:])
    assert status(root)["copy_pending"] is False
    # A full copy deletes nothing, so it takes no accepted deletes.
    with pytest.raises(PaperCopyError, match="--full"):
        copy(open_paper_store(root), full=True, accept_deletes=2)


def test_a_store_of_fewer_than_20_files_still_deletes_one_dropped_file(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "2", "pdf", "host")
    pdf = hold(root, "1", "pdf", "openalex")
    full(root, gcs)
    drop(root, pdf)

    copy(open_paper_store(root))

    assert gcs.deleted() == [pdf]
    assert papers(record(gcs)) == ["papers/2/2.pdf"]


def test_a_copy_skipped_on_an_expired_login_sends_its_files_on_the_next_success(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "0", "pdf", "host")
    full(root, gcs)
    first = hold(root, "1", "pdf", "openalex")
    gcs.expired = True
    before = status(root)["last_copy_success"]

    assert copy(open_paper_store(root)).skipped
    assert status(root)["copy_pending"] is True
    assert status(root)["last_copy_success"] == before

    second = hold(root, "2", "pdf", "host")
    gcs.expired = False
    copy(open_paper_store(root))

    assert papers(gcs.sent()) == [first, second]
    assert status(root)["copy_pending"] is False


def test_a_copy_record_that_fails_to_go_up_leaves_the_disks_so_the_files_go_again(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "0", "pdf", "host")
    full(root, gcs)
    new = hold(root, "1", "pdf", "host")
    gcs.expired = "copyto"

    assert copy(open_paper_store(root)).skipped
    assert status(root)["copy_pending"] is True

    gcs.expired = False
    gcs.forget()
    copy(open_paper_store(root))

    assert papers(gcs.sent()) == [new]
    assert new in record(gcs)


def test_with_no_record_on_disk_a_copy_fetches_the_gcs_one_and_sends_only_changes(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "1", "pdf", "host")
    full(root, gcs)
    (root / COPIED).unlink()
    new = hold(root, "2", "pdf", "host")

    copy(open_paper_store(root))

    assert ("get", COPIED) in gcs.log
    assert papers(gcs.sent()) == [new]
    assert gcs.walks == 0
    assert status(root)["copy_pending"] is False


def test_a_work_disk_restored_from_gcs_sends_no_paper_file_and_deletes_nothing(
    root: Path, gcs: FakeGcs, tmp_path: Path
) -> None:
    for paper in range(1, 4):
        hold(root, str(paper), "pdf", "host")
    full(root, gcs)
    # The restore: what GCS holds, its copy record among it, on a new disk.
    restored = tmp_path / "restored"
    shutil.copytree(root, restored, ignore=shutil.ignore_patterns(".*", "paper-status.json"))
    (restored / COPIED).write_bytes(gcs.read(COPIED))

    copy(open_paper_store(restored))

    assert gcs.phases(COPIED) == ["top", "record"]
    assert status(restored)["copy_pending"] is False


def test_with_no_record_on_disk_or_in_gcs_a_copy_sends_nothing_and_asks_for_full(
    root: Path, gcs: FakeGcs
) -> None:
    hold(root, "1", "pdf", "openalex")

    result = copy(open_paper_store(root))

    assert result.skipped
    assert "--full" in result.message
    assert gcs.log == []
    assert gcs.files() == []
    assert status(root)["copy_pending"] is True


def test_a_file_fetched_again_at_the_same_path_goes_up_again(root: Path, gcs: FakeGcs) -> None:
    path = hold(root, "1", "pdf", "host")
    full(root, gcs)
    hold(root, "1", "pdf", "host", b"%PDF-1.7 newer")

    copy(open_paper_store(root))

    assert papers(gcs.sent()) == [path]
    assert gcs.read(path) == b"%PDF-1.7 newer"


def test_a_missing_files_table_never_deletes_gcs(root: Path, gcs: FakeGcs) -> None:
    hold(root, "1", "pdf", "host")
    full(root, gcs)
    tables.FILES.path(root).unlink()

    with pytest.raises(PaperCopyError, match="is missing or empty"):
        copy(open_paper_store(root))
    assert gcs.calls == []
    assert status(root)["copy_pending"] is True


@pytest.mark.parametrize("full_copy", [False, True], ids=["routine", "full"])
def test_an_empty_files_table_copies_nothing(root: Path, gcs: FakeGcs, full_copy: bool) -> None:
    hold(root, "1", "pdf", "host")
    full(root, gcs)
    tables.write(root, tables.FILES, [])

    # Its tables would replace those in GCS, and its copy record would drop every file.
    with pytest.raises(PaperCopyError, match="is missing or empty"):
        copy(open_paper_store(root), full=full_copy)
    assert gcs.calls == []
    assert status(root)["copy_pending"] is True


def test_any_failure_past_the_destination_sets_copy_pending(root: Path, gcs: FakeGcs) -> None:
    hold(root, "1", "pdf", "host")
    full(root, gcs)
    (root / COPIED).write_bytes(b"not parquet")

    with pytest.raises(pa.ArrowInvalid):
        copy(open_paper_store(root))
    assert status(root)["copy_pending"] is True
