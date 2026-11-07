"""``labdata papers pmc-sync``, intake half: linked papers enter with IDs, details and status."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import init_paper_store, paper_dir, tables
from labdata.papers import openalex as openalex_mod
from labdata.papers.openalex import OpenAlex, OpenAlexError, load_accounts
from tests._paper_fakes import Article, FakeWeb, Reply, fixture_build, work
from tests._slices import built_root

TODAY = "2026-11-07"

#: Old, in PMC, OpenAlex holds a copy.
OLD_PMC = "41000001"
#: Published under 12 months ago; the OpenAlex batch filter misses it.
RECENT = "41000002"
#: Old, nothing serves it.
NO_ROUTE = "41000003"
#: Old, only a free host link.
HOST = "41000004"
#: Below 100,000 and linked only from BioProject XML.
PLACEHOLDER = "12345"
#: Linked by no build, from an earlier sync.
UNLINKED = "39999999"
DATASET = "10.5281/zenodo.1"
PREPRINT = "10.1101/2024.01.01.000001"
#: A DOI on a Series that also links a PMID: out of scope.
EXTRA_DOI = "10.1000/extra"
#: A DOI-only paper whose OpenAlex work has a PMID.
REKEY_DOI = "10.1000/rekey"
REKEY_PMID = "41000005"

LINKS = [
    ("GSE1", f"PMID:{OLD_PMC}", "SOFT"),
    ("GSE1", f"DOI:{EXTRA_DOI}", "SOFT Series_web_link"),
    ("GSE2", f"PMID:{RECENT}", "SOFT"),
    ("PRJNA2", f"PMID:{NO_ROUTE}", "BioProject XML"),
    ("GSE4", f"PMID:{HOST}", "PubMed"),
    ("PRJNA1", f"PMID:{PLACEHOLDER}", "BioProject XML"),
    ("GSE5", f"DOI:{DATASET}", "SOFT Series_web_link"),
    ("GSE6", f"DOI:{PREPRINT}", "SOFT Series_citation"),
    ("GSE8", f"DOI:{REKEY_DOI}", "BioProject XML"),
]


def web() -> FakeWeb:
    return FakeWeb(
        articles={
            OLD_PMC: Article(OLD_PMC, "Old paper", 2025, 1, 10, doi="10.1/old", pmcid="PMC1"),
            RECENT: Article(RECENT, "New paper", 2026, 6, 1, doi="10.1/new"),
            NO_ROUTE: Article(NO_ROUTE, "Closed paper", 2020, 3, 2),
            HOST: Article(
                HOST,
                "Host paper",
                2021,
                authors=(("Wu", "Ann", ("Lab B", "Lab C")), ("Li", "Bo", ())),
                abstract=(("BACKGROUND", "Why."), ("RESULTS", "What.")),
                types=("Journal Article", "Review"),
            ),
            REKEY_PMID: Article(REKEY_PMID, "Rekeyed paper", 2022, doi=REKEY_DOI),
        },
        pmcids={OLD_PMC: "PMC1"},
        works=[
            work("W1", pmid=OLD_PMC, doi="10.1/old", pdf=True),
            work("W2", pmid=RECENT, doi="10.1/new", pdf=True, published="2026-06-01"),
            work("W3", pmid=NO_ROUTE),
            work("W4", pmid=HOST, pdf_urls=("https://host.org/4.pdf",)),
            work("W5", doi=DATASET, kind="dataset"),
            work(
                "W6",
                doi=PREPRINT,
                kind="preprint",
                pdf_urls=("https://www.biorxiv.org/6.pdf",),
                title="A preprint",
            ),
            work("W7", doi=REKEY_DOI, pmid=REKEY_PMID),
        ],
        batch_misses={"W2"},
    )


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    monkeypatch.delenv("OPENALEX_API_KEYS", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)


def use(monkeypatch: pytest.MonkeyPatch, fake: FakeWeb) -> None:
    def client() -> EntrezClient:
        creds = NcbiCredentials("t@t.org", "abc")
        return EntrezClient(creds, transport=fake, limiter=RateLimiter(1000.0, 8))

    monkeypatch.setattr("labdata.cli._store_client", client)
    monkeypatch.setattr(
        "labdata.cli._openalex_client",
        lambda: OpenAlex(transport=fake, limiter=RateLimiter(1000.0, 8)),
    )


def sync(root: Path, *extra: str) -> str:
    result = CliRunner().invoke(
        app, ["papers", "pmc-sync", "--root", str(root), "--today", TODAY, *extra]
    )
    assert result.exit_code == 0, result.output
    return result.output


def by_key(root: Path, table: tables.PaperTable) -> dict[str, dict]:
    return {r["paper"]: r for r in tables.read(root, table)}


def paper_store(tmp_path: Path, links: list[tuple[str, str, str]]) -> Path:
    store = fixture_build(tmp_path / "store", links)
    root = tmp_path / "papers"
    init_paper_store(root, store)
    return root


def test_linked_papers_enter_with_ids_details_and_every_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, LINKS)
    tables.write(
        root,
        tables.PAPERS,
        [{"paper": UNLINKED, "pmid": 39999999, "status": "manual", "reason": "no-route"}],
    )
    fake = web()
    use(monkeypatch, fake)
    sync(root)

    papers = by_key(root, tables.PAPERS)
    assert {k: (r["status"], r["reason"]) for k, r in papers.items()} == {
        OLD_PMC: ("waiting", "openalex-backlog"),
        RECENT: ("waiting", "pmc-embargo"),
        NO_ROUTE: ("manual", "no-route"),
        HOST: ("waiting", "host-link"),
        PLACEHOLDER: ("skipped", "placeholder"),
        DATASET: ("skipped", "not-article"),
        PREPRINT: ("waiting", "host-link"),
        REKEY_PMID: ("manual", "no-route"),
        UNLINKED: ("manual", "no-route"),
    }
    old = papers[OLD_PMC]
    assert (old["pmid"], old["pmcid"], old["doi"], old["openalex"]) == (
        41000001,
        "PMC1",
        "10.1/old",
        "W1",
    )
    assert old["published"] == dt.date(2025, 1, 10)
    assert old["openalex_pdf"] is True
    assert old["added"] == old["looked_up"] == dt.date(2026, 11, 7)
    assert papers[RECENT]["openalex"] == "W2"  # found by the single lookup
    assert papers[HOST]["host_pdf_urls"] == ["https://host.org/4.pdf"]
    assert papers[PREPRINT]["work_type"] == "preprint"
    assert papers[REKEY_PMID]["doi"] == REKEY_DOI
    assert papers[UNLINKED]["looked_up"] is None

    info = by_key(root, tables.PAPER_INFO)
    assert set(info) == {OLD_PMC, RECENT, NO_ROUTE, HOST, PREPRINT, REKEY_PMID, DATASET}
    host = info[HOST]
    assert host["title"] == "Host paper"
    assert host["journal"] == "J Test"
    assert host["authors"] == [
        {"name": "Wu Ann", "affiliations": ["Lab B", "Lab C"]},
        {"name": "Li Bo", "affiliations": []},
    ]
    assert host["abstract"] == "BACKGROUND: Why.\nRESULTS: What."
    assert host["publication_type"] == ["Journal Article", "Review"]
    preprint = info[PREPRINT]
    assert (preprint["title"], preprint["journal"], preprint["abstract"]) == (
        "A preprint",
        "bioRxiv",
        "Cells divide.",
    )
    assert preprint["authors"] == [{"name": "Ann Author", "affiliations": ["Uni X, City"]}]
    assert preprint["publication_type"] == ["preprint"]

    status = json.loads((root / "paper-status.json").read_text())
    assert status["last_pmc_sync_success"] == TODAY
    assert status["last_pass"]["kind"] == "pmc-sync"
    assert status["counts"] == {
        "waiting": {"openalex-backlog": 1, "pmc-embargo": 1, "host-link": 2},
        "manual": {"no-route": 3},
        "skipped": {"placeholder": 1, "not-article": 1},
    }


def test_a_rerun_the_same_month_asks_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, LINKS)
    fake = web()
    use(monkeypatch, fake)
    sync(root)
    first = by_key(root, tables.PAPERS)
    asked = len(fake.calls)
    assert fake.count("efetch") == 1
    assert fake.count("idconv") == 1
    sync(root)
    assert len(fake.calls) == asked
    assert by_key(root, tables.PAPERS) == first


def test_a_new_month_renews_details_and_releases_a_pmc_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, LINKS)
    fake = web()
    use(monkeypatch, fake)
    sync(root)
    fake.pmcids = {**fake.pmcids, RECENT: "PMC2"}
    fake.calls.clear()
    sync(root, "--today", "2026-12-03")
    papers = by_key(root, tables.PAPERS)
    assert papers[RECENT]["pmcid"] == "PMC2"
    assert papers[RECENT]["looked_up"] == dt.date(2026, 12, 3)
    assert papers[PLACEHOLDER]["looked_up"] is None
    assert fake.count("efetch") == 1
    assert fake.count("idconv") == 1


def test_a_doi_only_paper_that_gains_a_pmid_moves_to_the_pmid_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = paper_store(tmp_path, [("GSE8", f"DOI:{REKEY_DOI}", "SOFT Series_web_link")])
    old_dir = root / paper_dir(REKEY_DOI)
    old_dir.mkdir(parents=True)
    (old_dir / "10.1000%2Frekey.pdf").write_bytes(b"%PDF")
    tables.write(
        root,
        tables.PAPERS,
        [{"paper": REKEY_DOI, "doi": REKEY_DOI, "status": "fetched", "pdf_tier": "host"}],
    )
    tables.write(
        root,
        tables.FILES,
        [
            {
                "paper": REKEY_DOI,
                "kind": "pdf",
                "name": "10.1000%2Frekey.pdf",
                "path": (paper_dir(REKEY_DOI) / "10.1000%2Frekey.pdf").as_posix(),
                "tier": "host",
            }
        ],
    )
    tables.write(
        root, tables.ATTEMPTS, [{"paper": REKEY_DOI, "tier": "host", "outcome": "fetched"}]
    )
    use(monkeypatch, web())
    sync(root)

    papers = by_key(root, tables.PAPERS)
    assert set(papers) == {REKEY_PMID}
    assert (papers[REKEY_PMID]["status"], papers[REKEY_PMID]["pdf_tier"]) == ("fetched", "host")
    new = paper_dir(REKEY_PMID) / f"{REKEY_PMID}.pdf"
    assert (root / new).read_bytes() == b"%PDF"
    assert not old_dir.exists()
    assert [(f["paper"], f["name"], f["path"]) for f in tables.read(root, tables.FILES)] == [
        (REKEY_PMID, f"{REKEY_PMID}.pdf", new.as_posix())
    ]
    assert [a["paper"] for a in tables.read(root, tables.ATTEMPTS)] == [REKEY_PMID]
    assert by_key(root, tables.PAPER_INFO)[REKEY_PMID]["title"] == "Rekeyed paper"


def test_the_thin_slice_takes_in_its_papers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = built_root(tmp_path / "store", "geo_slice")
    root = tmp_path / "papers"
    init_paper_store(root, store)
    use(monkeypatch, FakeWeb())
    out = sync(root)
    assert set(by_key(root, tables.PAPERS)) == {
        "19713941",
        "20194744",
        "20929578",
        "33281976",
        "38114495",
        "40167329",
    }
    assert "NCBI:" in out
    assert "OpenAlex:" in out


def test_openalex_accounts_come_from_the_env_before_the_file_and_never_show_a_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "cache" / "liulab-data"
    cache.mkdir(parents=True)
    (cache / "openalex.toml").write_text(
        '[[accounts]]\nkey = "filekey"\nemail = "a@x.org"\n', encoding="utf-8"
    )
    assert [a.email for a in load_accounts()] == ["a@x.org"]
    monkeypatch.setenv("OPENALEX_API_KEYS", "k1:b@x.org, k2:c@x.org")
    accounts = load_accounts()
    assert [(a.key, a.email) for a in accounts] == [("k1", "b@x.org"), ("k2", "c@x.org")]
    assert "k1" not in repr(accounts)

    class Down(FakeWeb):
        def _openalex(self, path, query):  # type: ignore[override]
            self.calls.append("openalex-down")
            return Reply("busy", status=503)

    monkeypatch.setattr(openalex_mod, "_sleep", lambda _s: None)
    down = Down()
    with pytest.raises(OpenAlexError) as err:
        OpenAlex(accounts, transport=down, limiter=RateLimiter(1000.0, 8)).works_by_pmid(["1"])
    assert "k1" not in str(err.value)
    assert down.count("openalex-down") == 5
