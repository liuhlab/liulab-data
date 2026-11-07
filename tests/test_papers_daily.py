"""``labdata papers daily`` and ``labdata papers configure``: quota, host links, manual queue."""

from __future__ import annotations

import datetime as dt
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
from labdata.exceptions import CredentialsError
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import daily as daily_mod
from labdata.papers import init_paper_store, paper_dir, tables
from labdata.papers.openalex import Account, OpenAlex, load_accounts
from tests._paper_fakes import FakeWeb, Reply, fixture_build

TODAY = "2026-11-07"
PDF = b"%PDF-1.7\nfake paper\n"
KEYS = [Account("secret-key-a", "a@x.org"), Account("secret-key-b", "b@x.org")]


class Headed(Reply):
    """A canned reply with headers."""

    def __init__(self, body: bytes | str, status: int = 200, **headers: str) -> None:
        super().__init__(body, status)
        self.headers = {k.replace("_", "-").lower(): v for k, v in headers.items()}

    def getheader(self, name: str) -> str | None:
        return self.headers.get(name.lower())


class DailyWeb(FakeWeb):
    """Adds OpenAlex's content API, host PDF links and PubMed's MEDLINE text.

    ``content`` maps a work ID to the reply status (200 serves a PDF). ``hosts`` maps a URL to
    a reply. ``stop_key`` answers 429 to one key after it has spent ``stop_after`` files.
    """

    def __init__(self, **kw: Any) -> None:
        self.content: dict[str, int] = kw.pop("content", {})
        self.hosts: dict[str, Headed] = kw.pop("hosts", {})
        self.stop_key: str | None = kw.pop("stop_key", None)
        self.stop_after = kw.pop("stop_after", 0)
        self.kill_after: int | None = kw.pop("kill_after", None)
        super().__init__(**kw)
        self.spent: dict[str, int] = {}
        self.downloads: list[str] = []
        self.headers_seen: list[Mapping[str, str]] = []
        self.urls_seen: list[str] = []

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
        parts = urllib.parse.urlsplit(url)
        self.urls_seen.append(url)
        self.headers_seen.append(headers)
        form = urllib.parse.parse_qs(body.decode()) if body else {}
        if parts.netloc == "content.openalex.org":
            yield self._content(parts.path, headers)
        elif url in self.hosts:
            self.calls.append("host")
            yield self.hosts[url]
        elif parts.netloc == "eutils.ncbi.nlm.nih.gov" and form.get("rettype") == ["medline"]:
            self.calls.append("medline")
            ids = form["id"][0].split(",")
            # PubMed answers in its own order, not the order asked.
            yield Reply("\n\n".join(_medline(i) for i in sorted(ids)) + "\n")
        else:
            with super().open(method, url, body=body, headers=headers, timeout=timeout) as r:
                yield r

    def _content(self, path: str, headers: Mapping[str, str]) -> Reply:
        key = headers.get("Authorization", "").removeprefix("Bearer ")
        if self.kill_after is not None and len(self.downloads) >= self.kill_after:
            raise KeyboardInterrupt
        if key == self.stop_key and self.spent.get(key, 0) >= self.stop_after:
            self.calls.append("content-429")
            return Headed("over budget", 429)
        self.calls.append("content")
        self.spent[key] = self.spent.get(key, 0) + 1
        wid = path.removeprefix("/works/").removesuffix(".pdf")
        status = self.content.get(wid, 200)
        if status == 200:
            self.downloads.append(wid)
            return Headed(PDF + wid.encode(), 200, content_type="application/pdf")
        return Headed("error", status)


def _medline(pmid: str) -> str:
    return f"PMID- {pmid}\nOWN - NLM\nTI  - Paper {pmid}.\nAU  - Liu H"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    monkeypatch.delenv("OPENALEX_API_KEYS", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(daily_mod, "_sleep", lambda _s: None)


def use(monkeypatch: pytest.MonkeyPatch, fake: FakeWeb, accounts: list[Account]) -> None:
    def client() -> EntrezClient:
        return EntrezClient(
            NcbiCredentials("t@t.org", "abc"), transport=fake, limiter=RateLimiter(1000.0, 8)
        )

    monkeypatch.setattr("labdata.cli._store_client", client)
    monkeypatch.setattr(
        "labdata.cli._openalex_client",
        lambda: OpenAlex(accounts, transport=fake, limiter=RateLimiter(1000.0, 8)),
    )
    monkeypatch.setattr("labdata.cli._paper_transport", lambda: fake)
    monkeypatch.setattr(daily_mod, "HOST_PACE", (1000.0, 1))


def daily(root: Path, today: str = TODAY) -> str:
    result = CliRunner().invoke(app, ["papers", "daily", "--root", str(root), "--today", today])
    assert result.exit_code == 0, result.output
    return result.output


def paper(
    key: str,
    *,
    published: dt.date = dt.date(2020, 1, 1),
    added: dt.date = dt.date(2026, 10, 3),
    pdf: bool = True,
    hosts: tuple[str, ...] = (),
    status: str = "waiting",
    reason: str | None = "openalex-backlog",
) -> dict[str, Any]:
    pmid = int(key) if key.isdigit() else None
    return {
        "paper": key,
        "pmid": pmid,
        "doi": None if pmid else key,
        "openalex": f"W{key}",
        "published": published,
        "added": added,
        "looked_up": added,
        "work_type": "article",
        "openalex_pdf": pdf,
        "host_pdf_urls": list(hosts),
        "status": status,
        "reason": reason,
    }


def paper_store(tmp_path: Path, rows: list[dict[str, Any]], unlinked: tuple[str, ...] = ()) -> Path:
    """A paper store holding ``rows``; the build links every row's paper but ``unlinked``."""
    links = [
        (f"GSE{n}", f"PMID:{r['paper']}" if r["paper"].isdigit() else f"DOI:{r['paper']}", "SOFT")
        for n, r in enumerate(rows, 1)
        if r["paper"] not in unlinked
    ]
    store = fixture_build(tmp_path / "store", links or [("GSE1", "PMID:41000001", "SOFT")])
    root = tmp_path / "papers"
    init_paper_store(root, store)
    tables.write(root, tables.PAPERS, rows)
    return root


def by_key(root: Path, table: tables.PaperTable) -> dict[str, dict[str, Any]]:
    return {r["paper"]: r for r in tables.read(root, table)}


def status_file(root: Path) -> dict[str, Any]:
    return json.loads((root / "paper-status.json").read_text())


def backlog(n: int) -> list[dict[str, Any]]:
    """``n`` old papers with an OpenAlex copy, published one day apart, newest last."""
    start = dt.date(2015, 1, 1)
    return [paper(str(42000000 + i), published=start + dt.timedelta(days=i)) for i in range(n)]


def test_each_key_spends_100_new_papers_first_then_newest_and_a_rerun_spends_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    accounts = [*KEYS, Account("secret-key-c", "c@x.org")]
    new = paper("43000000", published=dt.date(2010, 1, 1), added=dt.date(2026, 11, 3))
    root = paper_store(tmp_path, [*backlog(350), new])
    fake = DailyWeb()
    use(monkeypatch, fake, accounts)
    out = daily(root)

    assert fake.spent == {a.key: 100 for a in accounts}
    papers = by_key(root, tables.PAPERS)
    fetched = {k for k, r in papers.items() if r["status"] == "fetched"}
    assert len(fetched) == 300
    assert "43000000" in fetched
    # The 51 oldest of the backlog wait for tomorrow.
    assert {k for k, r in papers.items() if r["status"] == "waiting"} == {
        str(42000000 + i) for i in range(51)
    }
    one = papers["42000349"]
    assert one["pdf_tier"] == "openalex"
    pdf = root / paper_dir("42000349") / "42000349.pdf"
    assert pdf.read_bytes() == PDF + b"W42000349"
    files = by_key(root, tables.FILES)
    assert files["42000349"]["tier"] == "openalex"
    assert files["42000349"]["path"] == pdf.relative_to(root).as_posix()
    assert files["42000349"]["size"] == pdf.stat().st_size
    assert files["42000349"]["url"] == "https://content.openalex.org/works/W42000349.pdf"

    status = status_file(root)
    assert status["quota"] == {"date": TODAY, "by_key": {a.email: 100 for a in accounts}}
    assert status["fetches"] == {"date": TODAY, "by_tier": {"openalex": 300}}
    assert status["last_daily_success"] == TODAY
    assert status["last_pass"]["kind"] == "daily"
    assert status["counts"] == {"fetched": {"": 300}, "waiting": {"openalex-backlog": 51}}

    # No key is sent in a URL, shown in the output or written to the store.
    assert not any("secret" in u for u in fake.urls_seen)
    stored = "".join(
        p.read_text(errors="replace") for p in root.rglob("*") if p.is_file() and p.suffix != ".pdf"
    )
    assert "secret" not in out + stored
    assert {a["account"] for a in tables.read(root, tables.ATTEMPTS)} == {a.email for a in accounts}

    calls = len(fake.calls)
    daily(root)
    assert len(fake.calls) - calls == fake.calls[calls:].count("medline")


def test_a_429_stops_that_key_for_the_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = paper_store(tmp_path, backlog(250))
    fake = DailyWeb(stop_key="secret-key-b", stop_after=10)
    use(monkeypatch, fake, KEYS)
    daily(root)
    assert fake.spent == {"secret-key-a": 100, "secret-key-b": 10}
    assert fake.count("content-429") == 1
    assert status_file(root)["quota"]["by_key"] == {"a@x.org": 100, "b@x.org": 10}

    calls = len(fake.calls)
    daily(root)
    assert "content" not in fake.calls[calls:]
    assert "content-429" not in fake.calls[calls:]

    # Tomorrow both keys spend again.
    daily(root, "2026-11-08")
    assert fake.spent["secret-key-a"] == 200


def test_three_openalex_errors_move_a_paper_to_its_host_link_then_to_manual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    host = "https://journal.org/a.pdf"
    root = paper_store(
        tmp_path,
        [
            paper("41000001", hosts=(host,)),
            paper("41000002"),
            # Recent: waits for PMC, so neither tier is tried.
            paper("41000003", published=dt.date(2026, 6, 1), hosts=("https://x.org/b.pdf",)),
        ],
    )
    fake = DailyWeb(
        content={"W41000001": 503, "W41000002": 502},
        hosts={host: Headed("<html>Are you a robot?</html>", 200, content_type="text/html")},
    )
    use(monkeypatch, fake, KEYS[:1])
    for day in ("2026-11-07", "2026-11-08"):
        daily(root, day)
        daily(root, day)  # a rerun the same day tries nothing again
        assert by_key(root, tables.PAPERS)["41000001"]["status"] == "waiting"
    assert fake.count("content") == 4
    assert fake.count("host") == 0
    daily(root, "2026-11-09")
    assert fake.count("content") == 6
    assert fake.count("host") == 1

    papers = by_key(root, tables.PAPERS)
    assert (papers["41000001"]["status"], papers["41000001"]["reason"]) == (
        "manual",
        "host-blocked",
    )
    assert (papers["41000002"]["status"], papers["41000002"]["reason"]) == (
        "manual",
        "openalex-failed",
    )
    assert (papers["41000003"]["status"], papers["41000003"]["reason"]) == (
        "waiting",
        "pmc-embargo",
    )
    assert not (root / paper_dir("41000001")).exists()
    daily(root, "2026-11-10")
    assert fake.count("content") == 6
    assert fake.count("host") == 1


def test_a_retracted_paper_spends_no_quota_and_tries_no_host_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    host = "https://journal.org/r.pdf"
    root = paper_store(
        tmp_path,
        [
            {**paper("41000010", hosts=(host,)), "retracted": True},
            {**paper("41000011", pdf=False, hosts=(host,)), "retracted": True},
        ],
    )
    fake = DailyWeb(hosts={host: Headed(PDF, 200)})
    use(monkeypatch, fake, KEYS)
    daily(root)

    assert fake.spent == {}
    assert fake.count("host") == 0
    papers = by_key(root, tables.PAPERS)
    assert {k: (r["status"], r["reason"]) for k, r in papers.items()} == {
        "41000010": ("skipped", "retracted"),
        "41000011": ("skipped", "retracted"),
    }


def test_a_paper_no_build_links_gets_no_quota_and_no_host_try(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    host = "https://journal.org/u.pdf"
    root = paper_store(
        tmp_path,
        [paper("41000020"), paper("41000021", pdf=False, hosts=(host,))],
        unlinked=("41000020", "41000021"),
    )
    fake = DailyWeb(hosts={host: Headed(PDF, 200)})
    use(monkeypatch, fake, KEYS)
    daily(root)

    assert fake.spent == {}
    assert fake.count("host") == 0
    assert set(by_key(root, tables.PAPERS)) == {"41000020", "41000021"}


def test_a_host_link_serves_a_paper_openalex_has_no_copy_of(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = "https://www.biorxiv.org/c.pdf"
    root = paper_store(tmp_path, [paper("10.1101/c", pdf=False, hosts=(first,))])
    fake = DailyWeb(
        hosts={
            first: Headed("", 302, location="/c.full.pdf"),
            "https://www.biorxiv.org/c.full.pdf": Headed(PDF, 200),
        }
    )
    use(monkeypatch, fake, [])
    out = daily(root)
    assert "OpenAlex tier is skipped" in out
    row = by_key(root, tables.PAPERS)["10.1101/c"]
    assert (row["status"], row["pdf_tier"]) == ("fetched", "host")
    assert (root / paper_dir("10.1101/c") / "10.1101%2Fc.pdf").read_bytes() == PDF
    assert status_file(root)["fetches"]["by_tier"] == {"host": 1}


def test_a_paper_with_a_pmcid_spends_quota_only_after_the_pmc_bucket_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [paper("41000001"), paper("41000002")]
    for row in rows:
        row["pmcid"] = "PMC" + row["paper"]
    root = paper_store(tmp_path, rows)
    tables.write(
        root,
        tables.ATTEMPTS,
        [{"paper": "41000002", "tier": "pmc", "date": dt.date(2026, 11, 3), "outcome": "failed"}],
    )
    fake = DailyWeb()
    use(monkeypatch, fake, KEYS[:1])
    daily(root)
    assert fake.downloads == ["W41000002"]


def test_a_killed_pass_rerun_downloads_nothing_twice_and_spends_no_quota_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, backlog(20))
    fake = DailyWeb(kill_after=5)
    use(monkeypatch, fake, KEYS[:1])
    killed = CliRunner().invoke(app, ["papers", "daily", "--root", str(root), "--today", TODAY])
    assert killed.exit_code != 0
    assert status_file(root)["last_pass"]["outcome"] == "failed"
    assert len(fake.downloads) == 5

    fake.kill_after = None
    daily(root)
    assert len(fake.downloads) == len(set(fake.downloads)) == 20
    assert status_file(root)["quota"]["by_key"] == {"a@x.org": 20}
    assert len(tables.read(root, tables.FILES)) == 20
    assert len(tables.read(root, tables.ATTEMPTS)) == 20
    assert not (root / ".daily-journal.jsonl").exists()


def test_the_manual_queue_holds_exactly_the_manual_papers_newest_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manual = {"status": "manual", "reason": "no-route", "pdf": False}
    rows = [
        paper("41000001", published=dt.date(2019, 5, 1), **manual),  # type: ignore[arg-type]
        paper("41000002", published=dt.date(2023, 5, 1), **manual),  # type: ignore[arg-type]
        paper("10.1101/d", published=dt.date(2021, 5, 1), **manual),  # type: ignore[arg-type]
        paper("41000004", published=dt.date(2024, 1, 1), status="skipped", reason="placeholder"),
        paper("41000005", published=dt.date(2025, 1, 1)),
    ]
    rows[0]["doi"] = "10.1/one"
    root = paper_store(tmp_path, rows)
    fake = DailyWeb(content={"W41000005": 404})
    use(monkeypatch, fake, KEYS[:1])
    daily(root)

    nbib = (root / "manual-queue.nbib").read_text()
    assert [
        line.removeprefix("PMID- ") for line in nbib.splitlines() if line.startswith("PMID")
    ] == [
        "41000005",
        "41000002",
        "41000001",
    ]
    assert nbib.count("\n\n") == 3
    assert (root / "manual-queue-dois.txt").read_text() == "10.1101/d\n10.1/one\n"
    papers = by_key(root, tables.PAPERS)
    assert (papers["41000005"]["status"], papers["41000005"]["reason"]) == (
        "manual",
        "openalex-failed",
    )
    assert status_file(root)["counts"]["skipped"] == {"placeholder": 1}


def test_configure_saves_each_account_and_never_shows_a_key(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        ["papers", "configure"],
        input="a@x.org\nsecret-key-a\nb@x.org\nsecret-key-b\n\n",
    )
    assert result.exit_code == 0, result.output
    assert "secret" not in result.output
    assert [(a.key, a.email) for a in load_accounts()] == [
        ("secret-key-a", "a@x.org"),
        ("secret-key-b", "b@x.org"),
    ]
    path = tmp_path / "cache" / "liulab-data" / "openalex.toml"
    assert path.stat().st_mode & 0o777 == 0o600


def test_a_malformed_openalex_account_stops_the_step_with_a_message_and_no_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, [paper("41000001")])
    monkeypatch.setenv("OPENALEX_API_KEYS", "secret-key-only")
    monkeypatch.setenv("NCBI_EMAIL", "t@t.org")
    with pytest.raises(CredentialsError, match="labdata papers configure"):
        load_accounts()
    for step in ("daily", "pmc-sync"):
        result = CliRunner().invoke(app, ["papers", step, "--root", str(root)])
        assert result.exit_code == 1
        assert "OPENALEX_API_KEYS" in result.output
        assert "secret-key-only" not in result.output
        assert not isinstance(result.exception, CredentialsError)


def test_missing_ncbi_credentials_stop_the_step_with_a_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, [paper("41000001")])
    monkeypatch.delenv("NCBI_EMAIL", raising=False)
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    result = CliRunner().invoke(app, ["papers", "daily", "--root", str(root)])
    assert result.exit_code == 1
    assert "labdata ncbi configure" in result.output
    assert not isinstance(result.exception, CredentialsError)


def test_no_openalex_keys_skips_the_openalex_tier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, [paper("41000001")])
    fake = DailyWeb()
    use(monkeypatch, fake, KEYS)
    monkeypatch.setattr("labdata.cli._openalex_client", _real_openalex_client(fake))
    out = daily(root)
    assert "OpenAlex tier is skipped" in out
    assert fake.spent == {}


def _real_openalex_client(fake: FakeWeb) -> Any:
    """The CLI's own account loading, over the fake web: no env, no file."""

    def make() -> OpenAlex:
        return OpenAlex(load_accounts(), transport=fake, limiter=RateLimiter(1000.0, 8))

    return make
