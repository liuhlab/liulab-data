"""``labdata papers copy``: the routine copy sends only what changed since the last success."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.papers import init_paper_store, open_paper_store, tables
from labdata.papers.copy import COPIED, PaperCopyError, copy
from tests._paper_fakes import fixture_build

DEST = "gcs:bucket/datalake/paper-store"
EXPIRED = "oauth2: cannot fetch token: invalid_grant"


class Rclone:
    """Stand-in for the rclone runner: records each command and the file list it reads.

    It keeps what a ``copyto`` sends to the bucket, and a ``copy`` from the bucket fetches it.
    Every command fails with ``returncode``, or only the one named ``expiring``, on an
    expired login.
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
                path = Path(arg.split("=", 1)[1])
                listed = path.read_text(encoding="utf-8").splitlines()
                self.lists.append(listed)
        if args[1] == self.expiring:
            return subprocess.CompletedProcess(args, 1, "", EXPIRED)
        if self.returncode == 0 and args[1] == "copyto" and args[3].startswith(f"{DEST}/"):
            self.bucket[args[3].removeprefix(f"{DEST}/")] = Path(args[2]).read_bytes()
        if self.returncode == 0 and args[1] == "copy" and args[2] == DEST:
            for name in (n for n in listed if n in self.bucket):
                (Path(args[3]) / name).write_bytes(self.bucket[name])
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)

    def commands(self) -> list[str]:
        return [args[1] for args in self.calls]

    def record(self) -> list[str]:
        """The paths the copy record in the bucket names."""
        data = pa.BufferReader(self.bucket[COPIED])
        return pq.read_table(data).column("path").to_pylist()


@pytest.fixture
def rclone(monkeypatch: pytest.MonkeyPatch) -> Rclone:
    fake = Rclone()
    monkeypatch.setattr("labdata.papers.copy._run_rclone", fake)
    return fake


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


def full(root: Path, rclone: Rclone) -> None:
    """Run a full copy, then forget its rclone commands."""
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 0, result.output
    rclone.calls.clear()
    rclone.lists.clear()


def status(root: Path) -> dict:
    return json.loads((root / "paper-status.json").read_text())


def test_full_uploads_every_file_then_the_record_and_deletes_nothing(
    root: Path, rclone: Rclone
) -> None:
    path = hold(root, "1", "pdf", "host")

    copy(open_paper_store(root), full=True)

    up = ["rclone", "copy", str(root), DEST]
    hidden = ["--filter=- .*", "--filter=- .*/**", "--filter=- /paper-status.json"]
    progress = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]
    assert rclone.commands() == ["copy", "lsjson", "copy", "copy", "copyto"]
    fetch, probe, files, top, record = rclone.calls
    # It first looks for the bucket's copy record, to check the disk against it.
    assert fetch[:4] == ["rclone", "copy", DEST, str(root)]
    assert rclone.lists == [[COPIED]]
    assert probe == ["rclone", "lsjson", DEST, "--max-depth", "1"]
    # The paper files first; the top level, tables and marker among it, after them.
    assert files == [*up, *hidden, "--filter=- /*", *progress]
    assert top[:4] == up
    assert "--max-depth=1" in top
    assert record[3] == f"{DEST}/{COPIED}"
    assert rclone.record() == [path]
    assert status(root)["copy_pending"] is False
    assert status(root)["last_copy_success"]


def test_a_full_copy_from_a_disk_without_the_whole_store_uploads_nothing(
    root: Path, rclone: Rclone
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, rclone)
    # A disk that was not restored: it lacks files the bucket holds, and the copy record.
    drop(root, paths[0])
    drop(root, paths[1])
    (root / COPIED).unlink()

    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])

    assert result.exit_code == 1
    assert "Restore the store from the bucket first" in result.output
    # It only fetched the bucket's copy record, which stays as it was.
    assert rclone.commands() == ["copy"]
    assert rclone.lists == [[COPIED]]
    assert rclone.record() == sorted(paths)
    assert status(root)["copy_pending"] is True


def test_a_routine_copy_sends_only_the_new_rows_and_lists_no_paper_folder(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "1", "pdf", "openalex")
    full(root, rclone)
    new = hold(root, "2", "pdf", "host")

    copy(open_paper_store(root))

    assert rclone.lists == [[new]]
    assert rclone.commands() == ["lsjson", "copy", "copy", "copyto"]
    probe, files, top, _ = rclone.calls
    assert probe == ["rclone", "lsjson", DEST, "--max-depth", "1"]
    assert "--no-traverse" in files
    # The top level only: tables, README, manual queue and marker.
    assert top[:4] == ["rclone", "copy", str(root), DEST]
    assert "--max-depth=1" in top
    assert rclone.record() == sorted([new, "papers/1/1.pdf"])


def test_a_pdf_pmc_replaced_is_deleted_in_the_bucket_by_the_next_copy(
    root: Path, rclone: Rclone
) -> None:
    # One file in 20 left the table: the most one copy deletes.
    for paper in range(2, 21):
        hold(root, str(paper), "pdf", "host")
    pdf = hold(root, "1", "pdf", "openalex")
    full(root, rclone)
    xml = hold(root, "1", "xml", "pmc", b"<article/>")
    drop(root, pdf)

    copy(open_paper_store(root))

    # The tables go up before the PDF they dropped is deleted, the record after it.
    assert rclone.commands() == ["lsjson", "copy", "copy", "delete", "copyto"]
    assert rclone.lists == [[xml], [pdf]]
    assert rclone.calls[3][:4] == ["rclone", "delete", DEST, "--no-traverse"]
    assert xml in rclone.record()
    assert pdf not in rclone.record()

    rclone.calls.clear()
    rclone.lists.clear()
    copy(open_paper_store(root))
    assert rclone.lists == []


def test_a_copy_that_would_delete_more_than_one_file_in_20_copies_nothing_and_says_why(
    root: Path, rclone: Rclone
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, rclone)
    drop(root, paths[0])
    drop(root, paths[1])

    with pytest.raises(PaperCopyError, match="2 of the 20 files"):
        copy(open_paper_store(root))

    # Not even the tables go up: the files table may be damaged.
    assert rclone.calls == []
    assert status(root)["copy_pending"] is True


def test_files_moved_on_purpose_are_deleted_in_the_bucket_up_to_the_accepted_count(
    root: Path, rclone: Rclone
) -> None:
    paths = [hold(root, str(paper), "pdf", "host") for paper in range(1, 21)]
    full(root, rclone)
    drop(root, paths[0])
    drop(root, paths[1])
    run = CliRunner().invoke

    refused = run(app, ["papers", "copy", "--root", str(root)])
    assert refused.exit_code == 1
    assert "--accept-deletes 2" in refused.output
    too_few = run(app, ["papers", "copy", "--root", str(root), "--accept-deletes", "1"])
    assert too_few.exit_code == 1
    assert rclone.lists == []

    done = run(app, ["papers", "copy", "--root", str(root), "--accept-deletes", "2"])

    assert done.exit_code == 0, done.output
    assert rclone.lists == [paths[:2]]
    assert rclone.record() == sorted(paths[2:])
    assert status(root)["copy_pending"] is False
    # A full copy deletes nothing, so it takes no accepted deletes.
    with pytest.raises(PaperCopyError, match="--full"):
        copy(open_paper_store(root), full=True, accept_deletes=2)


def test_a_store_of_fewer_than_20_files_still_deletes_one_dropped_file(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "2", "pdf", "host")
    pdf = hold(root, "1", "pdf", "openalex")
    full(root, rclone)
    drop(root, pdf)

    copy(open_paper_store(root))

    assert rclone.lists == [[pdf]]
    assert rclone.record() == ["papers/2/2.pdf"]


def test_a_copy_skipped_on_an_expired_login_sends_its_files_on_the_next_success(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "0", "pdf", "host")
    full(root, rclone)
    first = hold(root, "1", "pdf", "openalex")
    rclone.returncode, rclone.stderr = 1, EXPIRED
    before = status(root)["last_copy_success"]

    assert copy(open_paper_store(root)).skipped
    assert status(root)["copy_pending"] is True
    assert status(root)["last_copy_success"] == before

    second = hold(root, "2", "pdf", "host")
    rclone.returncode, rclone.stderr = 0, ""
    rclone.lists.clear()
    copy(open_paper_store(root))

    assert rclone.lists == [[first, second]]
    assert status(root)["copy_pending"] is False


def test_a_copy_record_that_fails_to_go_up_leaves_the_disks_so_the_files_go_again(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "0", "pdf", "host")
    full(root, rclone)
    new = hold(root, "1", "pdf", "host")
    rclone.expiring = "copyto"

    assert copy(open_paper_store(root)).skipped
    assert status(root)["copy_pending"] is True

    rclone.expiring = ""
    rclone.lists.clear()
    copy(open_paper_store(root))

    assert rclone.lists == [[new]]
    assert new in rclone.record()


def test_with_no_record_on_disk_a_copy_fetches_the_buckets_and_sends_only_changes(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "1", "pdf", "host")
    full(root, rclone)
    (root / COPIED).unlink()
    rclone.calls.clear()
    new = hold(root, "2", "pdf", "host")

    copy(open_paper_store(root))

    assert rclone.lists == [[COPIED], [new]]
    fetch = rclone.calls[0]
    assert fetch[:4] == ["rclone", "copy", DEST, str(root)]
    assert "--no-traverse" in fetch
    assert status(root)["copy_pending"] is False


def test_a_work_disk_restored_from_the_bucket_sends_no_paper_file_and_deletes_nothing(
    root: Path, rclone: Rclone, tmp_path: Path
) -> None:
    for paper in range(1, 4):
        hold(root, str(paper), "pdf", "host")
    full(root, rclone)
    # The restore: what the bucket holds, its copy record among it, on a new disk.
    restored = tmp_path / "restored"
    shutil.copytree(root, restored, ignore=shutil.ignore_patterns(".*", "paper-status.json"))
    (restored / COPIED).write_bytes(rclone.bucket[COPIED])
    rclone.calls.clear()

    copy(open_paper_store(restored))

    assert rclone.lists == []
    assert rclone.commands() == ["lsjson", "copy", "copyto"]
    assert status(restored)["copy_pending"] is False


def test_with_no_record_on_disk_or_in_the_bucket_a_copy_sends_nothing_and_asks_for_full(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "1", "pdf", "openalex")

    result = copy(open_paper_store(root))

    assert result.skipped
    assert "--full" in result.message
    # It only looked for the bucket's record.
    assert rclone.commands() == ["copy"]
    assert rclone.lists == [[COPIED]]
    assert status(root)["copy_pending"] is True


def test_a_file_fetched_again_at_the_same_path_goes_up_again(root: Path, rclone: Rclone) -> None:
    path = hold(root, "1", "pdf", "host")
    full(root, rclone)
    hold(root, "1", "pdf", "host", b"%PDF-1.7 newer")

    copy(open_paper_store(root))

    assert rclone.lists == [[path]]


def test_a_missing_files_table_never_deletes_the_bucket(root: Path, rclone: Rclone) -> None:
    hold(root, "1", "pdf", "host")
    full(root, rclone)
    tables.FILES.path(root).unlink()

    with pytest.raises(PaperCopyError, match="is missing or empty"):
        copy(open_paper_store(root))
    assert rclone.calls == []
    assert status(root)["copy_pending"] is True


@pytest.mark.parametrize("full_copy", [False, True], ids=["routine", "full"])
def test_an_empty_files_table_copies_nothing(root: Path, rclone: Rclone, full_copy: bool) -> None:
    hold(root, "1", "pdf", "host")
    full(root, rclone)
    tables.write(root, tables.FILES, [])

    # Its tables would replace the bucket's, and its copy record would drop every file.
    with pytest.raises(PaperCopyError, match="is missing or empty"):
        copy(open_paper_store(root), full=full_copy)
    assert rclone.calls == []
    assert status(root)["copy_pending"] is True
