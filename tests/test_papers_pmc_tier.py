"""``labdata papers pmc-sync``, PMC tier: XML, PDF and supplements from the PMC bucket."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi._http import RateLimiter
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import init_paper_store, paper_dir, pmc, tables
from labdata.papers.openalex import OpenAlex
from tests._paper_fakes import Article, FakeBucket, FakeWeb, fixture_build, md5

TODAY = "2026-11-07"
#: An open-access article in two versions: XML, PDF, a figure and supplements.
OA = "41000101"
#: An author manuscript: XML only.
AM = "41000102"
#: In PMC, but not in the bucket.
CLOSED = "41000103"
BIG = 10 * 2**20 + 1

OA_XML = b"""<article xmlns:xlink="http://www.w3.org/1999/xlink"><body>
<fig><graphic xlink:href="gr1"/></fig>
<supplementary-material><label>Table S1</label><caption><p>Sample
  sheet.</p></caption><media xlink:href="mmc1.xlsx"/></supplementary-material>
<supplementary-material><label>Data S1</label><caption><title>Counts</title></caption>
<media xlink:href="mmc2.zip"/></supplementary-material>
</body></article>"""
AM_XML = b"<article><body><p>Manuscript.</p></body></article>"


def bucket() -> FakeBucket:
    b = FakeBucket(page=4)
    b.put("PMC100", 1, {"PMC100.1.xml": b"<article>old</article>"})
    b.put(
        "PMC100",
        2,
        {
            "PMC100.2.xml": OA_XML,
            "PMC100.2.pdf": b"%PDF-1.7 article",
            "PMC100.2.txt": b"text",
            "gr1.jpg": b"jpeg",
            "mmc1.xlsx": b"xlsx table",
            "mmc2.zip": b"z" * BIG,
            "table.csv": b"a,b\n1,2\n",
        },
    )
    b.put("PMC200", 1, {"PMC200.1.xml": AM_XML, "PMC200.1.txt": b"t"}, manuscript=True)
    return b


def web(b: FakeBucket) -> FakeWeb:
    return FakeWeb(
        articles={
            OA: Article(OA, "Open paper", 2024, pmcid="PMC100"),
            AM: Article(AM, "Manuscript", 2024, pmcid="PMC200"),
            CLOSED: Article(CLOSED, "Closed", 2024, pmcid="PMC300"),
        },
        pmcids={OA: "PMC100", AM: "PMC200", CLOSED: "PMC300"},
        bucket=b,
    )


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_PAPERS", raising=False)
    monkeypatch.delenv("OPENALEX_API_KEYS", raising=False)
    monkeypatch.setattr(entrez_mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(pmc, "_sleep", lambda _s: None)
    monkeypatch.setattr(pmc, "RATE", 1000.0)


def store(tmp_path: Path, fake: FakeWeb, monkeypatch: pytest.MonkeyPatch) -> Path:
    links = [(f"GSE{n}", f"PMID:{p}", "SOFT") for n, p in enumerate((OA, AM, CLOSED))]
    root = tmp_path / "papers"
    init_paper_store(root, fixture_build(tmp_path / "store", links))

    def client() -> EntrezClient:
        creds = NcbiCredentials("t@t.org", "abc")
        return EntrezClient(creds, transport=fake, limiter=RateLimiter(1000.0, 8))

    monkeypatch.setattr("labdata.cli._store_client", client)
    monkeypatch.setattr(
        "labdata.cli._openalex_client",
        lambda: OpenAlex(transport=fake, limiter=RateLimiter(1000.0, 8)),
    )
    return root


def sync(root: Path, today: str = TODAY) -> str:
    result = CliRunner().invoke(app, ["papers", "pmc-sync", "--root", str(root), "--today", today])
    assert result.exit_code == 0, result.output
    return result.output


def files(root: Path) -> dict[tuple[str, str, str], dict]:
    return {(f["paper"], f["kind"], f["name"]): f for f in tables.read(root, tables.FILES)}


def test_an_oa_article_and_an_author_manuscript_give_their_files_and_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)

    oa, am = paper_dir(OA), paper_dir(AM)
    url = "https://pmc-oa-opendata.s3.amazonaws.com/"
    day = dt.date(2026, 11, 7)
    rows = files(root)
    shown = {
        k: (f["path"], f["size"], f["md5"], f["url"], f["label"], f["caption"], f["fetched"])
        for k, f in rows.items()
    }
    assert shown == {
        (OA, "xml", f"{OA}.xml"): (
            f"{oa.as_posix()}/{OA}.xml",
            len(OA_XML),
            md5(OA_XML),
            f"{url}PMC100.2/PMC100.2.xml",
            None,
            None,
            day,
        ),
        (OA, "pdf", f"{OA}.pdf"): (
            f"{oa.as_posix()}/{OA}.pdf",
            16,
            md5(b"%PDF-1.7 article"),
            f"{url}PMC100.2/PMC100.2.pdf",
            None,
            None,
            day,
        ),
        (OA, "supplement", "mmc1.xlsx"): (
            f"{oa.as_posix()}/suppl/mmc1.xlsx",
            10,
            md5(b"xlsx table"),
            f"{url}PMC100.2/mmc1.xlsx",
            "Table S1",
            "Sample sheet.",
            day,
        ),
        (OA, "supplement", "mmc2.zip"): (
            None,
            BIG,
            md5(b"z" * BIG),
            f"{url}PMC100.2/mmc2.zip",
            "Data S1",
            "Counts",
            None,
        ),
        (OA, "supplement", "table.csv"): (
            f"{oa.as_posix()}/suppl/table.csv",
            8,
            md5(b"a,b\n1,2\n"),
            f"{url}PMC100.2/table.csv",
            None,
            None,
            day,
        ),
        (AM, "xml", f"{AM}.xml"): (
            f"{am.as_posix()}/{AM}.xml",
            len(AM_XML),
            md5(AM_XML),
            f"{url}PMC200.1/PMC200.1.xml",
            None,
            None,
            day,
        ),
    }
    assert {f["tier"] for f in rows.values()} == {"pmc"}
    assert all(f["etag"] for f in rows.values())
    assert (root / oa / f"{OA}.xml").read_bytes() == OA_XML
    assert (root / oa / "suppl" / "mmc1.xlsx").read_bytes() == b"xlsx table"
    assert sorted(p.name for p in (root / oa).rglob("*") if p.is_file()) == [
        f"{OA}.pdf",
        f"{OA}.xml",
        "mmc1.xlsx",
        "table.csv",
    ]
    assert b.fetched("PMC100.2/mmc2.zip") == 0
    assert b.fetched("PMC100.1/PMC100.1.xml") == 0

    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert {k: (r["status"], r["xml_tier"], r["pdf_tier"]) for k, r in papers.items()} == {
        OA: ("fetched", "pmc", "pmc"),
        AM: ("fetched", "pmc", None),
        CLOSED: ("manual", None, None),
    }


def test_a_bad_md5_is_retried_then_recorded_as_a_failed_attempt_with_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    b.bad.add("PMC100.2/PMC100.2.pdf")
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)

    assert b.fetched("PMC100.2/PMC100.2.pdf") == pmc.TRIES
    assert not [p.name for p in (root / paper_dir(OA)).iterdir() if "pdf" in p.name]
    attempts = tables.read(root, tables.ATTEMPTS)
    assert sorted((a["paper"], a["outcome"]) for a in attempts) == [
        (OA, "failed"),
        (AM, "fetched"),
    ]
    assert "MD5" in next(a["detail"] for a in attempts if a["paper"] == OA)
    assert (OA, "pdf", f"{OA}.pdf") not in files(root)
    # The next sync tries the PDF again, and only what it lacks.
    b.bad.clear()
    sync(root, "2026-12-03")
    assert files(root)[(OA, "pdf", f"{OA}.pdf")]["fetched"] == dt.date(2026, 12, 3)
    assert b.fetched("PMC100.2/PMC100.2.xml") == 1


def test_a_changed_etag_renews_the_file_and_a_removed_copy_keeps_its_file_with_a_gone_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)
    first = files(root)

    b.put("PMC100", 2, {"mmc1.xlsx": b"xlsx table, corrected"})
    b.drop("PMC200.1")
    b.day = "2026-12-02"
    gets = len(b.gets)
    sync(root, "2026-12-03")

    after = files(root)
    renewed = after[(OA, "supplement", "mmc1.xlsx")]
    assert (root / renewed["path"]).read_bytes() == b"xlsx table, corrected"
    assert renewed["fetched"] == dt.date(2026, 12, 3)
    assert renewed["etag"] == md5(b"xlsx table, corrected")
    # Only the changed file is fetched again.
    assert [g for g in b.gets[gets:] if g.startswith("PMC") and not g.endswith(".json")] == [
        "PMC100.2/mmc1.xlsx"
    ]
    assert after[(OA, "xml", f"{OA}.xml")] == first[(OA, "xml", f"{OA}.xml")]
    manuscript = after[(AM, "xml", f"{AM}.xml")]
    assert manuscript["gone"] == dt.date(2026, 12, 3)
    assert (root / manuscript["path"]).read_bytes() == AM_XML
    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert papers[AM]["status"] == "fetched"


class KilledError(Exception):
    pass


def test_a_kill_mid_sync_and_a_rerun_download_no_file_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)

    def kill(key: str) -> None:
        if key == "PMC100.2/table.csv":
            raise KilledError

    b.fail = kill
    result = CliRunner().invoke(app, ["papers", "pmc-sync", "--root", str(root), "--today", TODAY])
    assert isinstance(result.exception, KilledError)
    assert (root / paper_dir(OA) / "suppl" / "mmc1.xlsx").is_file()
    assert not list((root / paper_dir(OA)).rglob(".*"))

    b.fail = None
    sync(root)
    objects = [g for g in b.gets if g.startswith("PMC") and not g.endswith(".json")]
    assert len(objects) == len(set(objects))
    assert (OA, "supplement", "table.csv") in files(root)
