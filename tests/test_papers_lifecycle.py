"""The paper store's life cycle: a step ends with GCS holding what it changed, finalized."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import urllib.parse
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from labdata._http import RateLimiter
from labdata.cli import app
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import init_paper_store, lifecycle, open_paper_store, paper_dir, tables
from labdata.papers.openalex import Account, OpenAlex
from labdata.papers.status import STATUS, read_status
from tests._fake_gcs import FakeGcs
from tests._paper_fakes import Article, FakeWeb, Reply, fixture_build, work

DEST = "gcs:bucket/datalake/paper-store"
TODAY = dt.date(2026, 11, 7)
HOST = "https://host.example.org/paper.pdf"


class Web(FakeWeb):
    """Adds OpenAlex's paid copies, each a PDF; a request to :data:`HOST` breaks the pass."""

    @contextmanager
    def open(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Iterator[Reply]:
        netloc = urllib.parse.urlsplit(url).netloc
        if netloc == "content.openalex.org":
            yield Reply(b"%PDF-1.7 " + url.encode())
        elif url == HOST:
            raise RuntimeError("the pass broke after buying its PDFs")
        else:
            with super().open(method, url, body=body, headers=headers, timeout=timeout) as reply:
                yield reply


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    return FakeGcs.install(monkeypatch, tmp_path / "gcs", DEST)


def paper(key: str, **row: Any) -> dict[str, Any]:
    """An old paper waiting for OpenAlex's copy."""
    return {
        "paper": key,
        "pmid": int(key),
        "openalex": f"W{key}",
        "published": dt.date(2020, 1, 1),
        "added": dt.date(2026, 10, 3),
        "looked_up": dt.date(2026, 10, 3),
        "work_type": "article",
        "openalex_pdf": True,
        "host_pdf_urls": [],
        "status": "waiting",
        "reason": "openalex-backlog",
        **row,
    }


def hold(root: Path, key: str) -> str:
    """Write one paper file and add its ``files`` row; return its path."""
    path = (paper_dir(key) / f"{key}.pdf").as_posix()
    (root / path).parent.mkdir(parents=True)
    (root / path).write_bytes(b"%PDF " + key.encode())
    row = {"paper": key, "kind": "pdf", "path": path}
    md5 = hashlib.md5((root / path).read_bytes()).hexdigest()
    tables.write(root, tables.FILES, [*tables.read(root, tables.FILES), {**row, "md5": md5}])
    return path


def copied_store(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    """A paper store holding ``rows`` and one paper file, copied to GCS in full."""
    links = [(f"GSE{n}", f"PMID:{r['paper']}", "SOFT") for n, r in enumerate(rows, 1)]
    root = tmp_path / "papers"
    init_paper_store(root, fixture_build(tmp_path / "store", links), copy_to=DEST)
    tables.write(root, tables.PAPERS, rows)
    hold(root, "1")
    lifecycle.copy(open_paper_store(root), full=True)
    return root


def test_a_daily_pass_that_fails_after_buying_pdfs_still_sends_them_to_gcs(
    tmp_path: Path, gcs: FakeGcs
) -> None:
    bought = ["41000001", "41000002"]
    rows = [paper(k) for k in bought] + [
        paper("41000003", openalex_pdf=False, host_pdf_urls=[HOST])
    ]
    root = copied_store(tmp_path, rows)
    web = Web()
    limiter = RateLimiter(1000.0, 8)

    with pytest.raises(RuntimeError, match="after buying its PDFs"):
        lifecycle.daily(
            open_paper_store(root),
            client=EntrezClient(NcbiCredentials("t@t.org", "abc"), transport=web, limiter=limiter),
            openalex=OpenAlex([Account("key", "a@x.org")], transport=web, limiter=limiter),
            today=TODAY,
            transport=web,
            host_limiter=RateLimiter(1000.0, 1),
            log=lambda _line: None,
        )

    for key in bought:
        path = (paper_dir(key) / f"{key}.pdf").as_posix()
        assert gcs.read(path) == (root / path).read_bytes()
    status = read_status(root)
    assert status.last_pass["outcome"] == "failed"
    assert (status.finalized, status.copy_pending) == (True, False)
    assert gcs.read(STATUS) == (root / STATUS).read_bytes()


def test_a_pass_that_adds_no_paper_file_still_ends_with_gcs_holding_its_record(
    tmp_path: Path, gcs: FakeGcs
) -> None:
    root = copied_store(tmp_path, [paper("41000001")])
    gcs.forget()
    web = FakeWeb(articles={"41000001": Article("41000001", "A paper", 2020)}, works=[work("W1")])
    limiter = RateLimiter(1000.0, 8)

    lifecycle.daily(
        open_paper_store(root),
        client=EntrezClient(NcbiCredentials("t@t.org", "abc"), transport=web, limiter=limiter),
        openalex=OpenAlex(transport=web),
        today=TODAY,
        transport=web,
        log=lambda _line: None,
    )

    assert not any("/" in path for path in gcs.sent())
    assert json.loads(gcs.read(STATUS))["last_pass"]["outcome"] == "success"
    assert gcs.read(STATUS) == (root / STATUS).read_bytes()
    assert (read_status(root).finalized, read_status(root).copy_pending) == (True, False)


@pytest.mark.parametrize("step", ["daily", "pmc-sync"])
def test_a_pass_run_by_hand_ends_finalized_and_exits_1_while_its_copy_is_pending(
    tmp_path: Path, gcs: FakeGcs, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    root = copied_store(tmp_path, [paper("41000001")])
    # A pass stopped before its copy: the store holds a paper file GCS lacks.
    path = hold(root, "2")
    web = FakeWeb(articles={"41000001": Article("41000001", "A paper", 2020)}, works=[work("W1")])
    limiter = RateLimiter(1000.0, 8)
    client = EntrezClient(NcbiCredentials("t@t.org", "abc"), transport=web, limiter=limiter)
    monkeypatch.setattr("labdata.cli._store_client", lambda: client)
    monkeypatch.setattr("labdata.cli._openalex_client", lambda: OpenAlex(transport=web))
    monkeypatch.setattr("labdata.cli._paper_transport", lambda: web)
    run = ["papers", step, "--root", str(root), "--today", TODAY.isoformat()]
    gcs.expired = True

    pending = CliRunner().invoke(app, run)

    assert pending.exit_code == 1, pending.output
    assert "rclone config reconnect gcs:" in pending.output
    status = read_status(root)
    assert (status.last_pass["outcome"], status.finalized, status.copy_pending) == (
        "success",
        False,
        True,
    )
    assert path not in gcs.files()

    gcs.expired = False
    done = CliRunner().invoke(app, run)

    assert done.exit_code == 0, done.output
    assert gcs.read(path) == (root / path).read_bytes()
    assert (read_status(root).finalized, read_status(root).copy_pending) == (True, False)
    assert gcs.read(STATUS) == (root / STATUS).read_bytes()


def test_a_copy_after_a_killed_pass_sends_that_pass_as_failed_never_running(
    tmp_path: Path, gcs: FakeGcs
) -> None:
    root = copied_store(tmp_path, [paper("41000001")])
    killed = {"kind": "daily", "start": "2026-11-07T00:30:00+00:00", "outcome": "running"}
    (root / STATUS).write_text(json.dumps({"last_pass": killed, "finalized": True}))
    hold(root, "2")

    lifecycle.copy(open_paper_store(root))

    sent = json.loads(gcs.read(STATUS))
    assert (sent["last_pass"]["outcome"], sent["finalized"], sent["copy_pending"]) == (
        "failed",
        True,
        False,
    )


def test_a_pass_on_a_disk_behind_gcs_refuses_at_its_start_unless_the_login_has_expired(
    tmp_path: Path, gcs: FakeGcs, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copied_store(tmp_path, [paper("41000001")])
    before = read_status(root)
    # Another disk copied since this one did.
    gcs.write(".copied-files.parquet", b"another disk's record")
    gcs.forget()
    web = FakeWeb(articles={"41000001": Article("41000001", "A paper", 2020)}, works=[work("W1")])
    limiter = RateLimiter(1000.0, 8)
    client = EntrezClient(NcbiCredentials("t@t.org", "abc"), transport=web, limiter=limiter)
    monkeypatch.setattr("labdata.cli._store_client", lambda: client)
    monkeypatch.setattr("labdata.cli._openalex_client", lambda: OpenAlex(transport=web))
    monkeypatch.setattr("labdata.cli._paper_transport", lambda: web)
    run = ["papers", "daily", "--root", str(root), "--today", TODAY.isoformat()]

    refused = CliRunner().invoke(app, run)

    assert refused.exit_code == 1
    assert "labdata papers restore" in refused.output
    status = read_status(root)
    assert status.last_pass == before.last_pass
    assert (status.finalized, status.copy_pending) == (False, True)
    assert gcs.log == []

    gcs.expired = True
    warned = CliRunner().invoke(app, run)

    assert "not checked against GCS" in warned.output
    assert read_status(root).last_pass["outcome"] == "success"
