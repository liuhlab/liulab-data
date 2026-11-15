"""``labdata papers copy``: the routine copy sends only what changed since the last success."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.papers import init_paper_store, open_paper_store, tables
from labdata.papers.copy import PaperCopyError, copy
from tests._paper_fakes import fixture_build

DEST = "gcs:bucket/datalake/paper-store"
EXPIRED = "oauth2: cannot fetch token: invalid_grant"


class Rclone:
    """Stand-in for the rclone runner: records each command and the file list it reads."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.lists: list[list[str]] = []
        self.returncode, self.stderr = 0, ""

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        for arg in args:
            if arg.startswith("--files-from-raw="):
                path = Path(arg.split("=", 1)[1])
                self.lists.append(path.read_text(encoding="utf-8").splitlines())
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)

    def commands(self) -> list[str]:
        return [args[1] for args in self.calls]


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


def full(root: Path) -> None:
    result = CliRunner().invoke(app, ["papers", "copy", "--root", str(root), "--full"])
    assert result.exit_code == 0, result.output


def status(root: Path) -> dict:
    return json.loads((root / "paper-status.json").read_text())


def test_full_mirrors_the_store_as_before(root: Path, rclone: Rclone) -> None:
    full(root)

    hidden = ["--filter=- .*", "--filter=- .*/**", "--filter=- /paper-status.json"]
    progress = ["--stats=1m", "--stats-one-line", "--stats-log-level=NOTICE"]
    assert rclone.calls == [
        ["rclone", "lsjson", DEST, "--max-depth", "1"],
        ["rclone", "copy", str(root), DEST, *hidden, "--filter=- /labdata-papers.toml", *progress],
        ["rclone", "sync", str(root), DEST, "--delete-excluded", *hidden, *progress],
    ]
    assert status(root)["copy_pending"] is False
    assert status(root)["last_copy_success"]


def test_a_routine_copy_sends_only_the_new_rows_and_lists_no_paper_folder(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "1", "pdf", "openalex")
    full(root)
    rclone.calls.clear()
    new = hold(root, "2", "pdf", "host")

    copy(open_paper_store(root))

    assert rclone.lists == [[new]]
    assert rclone.commands() == ["lsjson", "copy", "copy"]
    probe, files, top = rclone.calls
    assert probe == ["rclone", "lsjson", DEST, "--max-depth", "1"]
    assert "--no-traverse" in files
    # The top level only: tables, README, manual queue and marker.
    assert top[:4] == ["rclone", "copy", str(root), DEST]
    assert "--max-depth=1" in top


def test_a_pdf_pmc_replaced_is_deleted_in_the_bucket_by_the_next_copy(
    root: Path, rclone: Rclone
) -> None:
    pdf = hold(root, "1", "pdf", "openalex")
    full(root)
    rclone.calls.clear()
    xml = hold(root, "1", "xml", "pmc", b"<article/>")
    drop(root, pdf)

    copy(open_paper_store(root))

    # The tables go up before the PDF they dropped is deleted.
    assert rclone.commands() == ["lsjson", "copy", "copy", "delete"]
    assert rclone.lists == [[xml], [pdf]]
    assert rclone.calls[3][:4] == ["rclone", "delete", DEST, "--no-traverse"]

    rclone.calls.clear()
    rclone.lists.clear()
    copy(open_paper_store(root))
    assert rclone.lists == []


def test_a_copy_skipped_on_an_expired_login_sends_its_files_on_the_next_success(
    root: Path, rclone: Rclone
) -> None:
    full(root)
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


def test_with_no_earlier_copy_a_routine_copy_is_skipped_and_asks_for_full(
    root: Path, rclone: Rclone
) -> None:
    hold(root, "1", "pdf", "openalex")

    result = copy(open_paper_store(root))

    assert result.skipped
    assert "--full" in result.message
    assert rclone.calls == []
    assert status(root)["copy_pending"] is True


def test_a_file_fetched_again_at_the_same_path_goes_up_again(root: Path, rclone: Rclone) -> None:
    path = hold(root, "1", "pdf", "host")
    full(root)
    hold(root, "1", "pdf", "host", b"%PDF-1.7 newer")

    copy(open_paper_store(root))

    assert rclone.lists == [[path]]


def test_a_missing_files_table_never_deletes_the_bucket(root: Path, rclone: Rclone) -> None:
    hold(root, "1", "pdf", "host")
    full(root)
    rclone.calls.clear()
    tables.FILES.path(root).unlink()

    with pytest.raises(PaperCopyError, match="missing"):
        copy(open_paper_store(root))
    assert rclone.calls == []
