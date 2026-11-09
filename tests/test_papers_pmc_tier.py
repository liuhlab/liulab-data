"""``labdata papers pmc-sync``, PMC tier: XML, PDF and supplements from the PMC bucket."""

from __future__ import annotations

import datetime as dt
import errno
import json
from pathlib import Path

import pytest
from backports import zstd
from typer.testing import CliRunner

from labdata._http import RateLimiter
from labdata.cli import app
from labdata.ncbi import entrez as entrez_mod
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import init_paper_store, open_paper_store, paper_dir, pmc, tables
from labdata.papers.openalex import OpenAlex
from tests._paper_fakes import Article, FakeBucket, FakeWeb, Reply, fixture_build, md5

TODAY = "2026-11-07"
#: An open-access article in two versions: XML, PDF, a figure and supplements.
OA = "41000101"
#: An author manuscript: XML only.
AM = "41000102"
#: In PMC, but not in the bucket.
CLOSED = "41000103"
BIG = 10 * 2**20 + 1

OA_XML = b"""<article xmlns:xlink="http://www.w3.org/1999/xlink"><body>
<fig><label>Figure 1</label><caption><p>Cells
  divide.</p></caption><graphic xlink:href="gr1"/></fig>
<fig><label>Figure 2</label><graphic xlink:href="gr2"/></fig>
<p>Where <inline-graphic xlink:href="eq1"/> holds.</p>
<supplementary-material><label>Table S1</label><caption><p>Sample
  sheet.</p></caption><media xlink:href="mmc1.xlsx"/></supplementary-material>
<supplementary-material><label>Data S1</label><caption><title>Counts</title></caption>
<media xlink:href="mmc2.zip"/></supplementary-material>
<supplementary-material><label>Movie S1</label><media xlink:href="mmc3.mp4"/>
</supplementary-material>
</body></article>"""
AM_XML = b"<article><body><p>Manuscript.</p></body></article>"
#: A full-size figure, and the smaller preview the bucket also holds.
GR1 = b"jpeg " * 20
TIF = b"II*\x00" + b"\x00" * 200
CSV = b"a,b\n1,2\n"
MOVIE = b"\x00\x00\x00\x18ftypmp42" * 10


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
            "gr1.jpg": GR1,
            "gr1.gif": b"gif",
            "gr2.tif": TIF,
            "eq1.gif": b"formula",
            "mmc1.xlsx": b"xlsx table",
            "mmc2.zip": b"z" * BIG,
            "mmc3.mp4": MOVIE,
            "mmc4.dat": MOVIE,
            "table.csv": CSV,
        },
    )
    # A video under a name that does not say so: its Content-Type does.
    b.types["PMC100.2/mmc4.dat"] = "video/quicktime"
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
    monkeypatch.setattr("labdata.cli._paper_transport", lambda: fake)
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
            f"{oa.as_posix()}/{OA}.xml.zst",
            len(OA_XML),
            md5(OA_XML),
            f"{url}PMC100.2/PMC100.2.xml",
            None,
            None,
            day,
        ),
        # The XML and figures replace the PMC PDF: listed, not downloaded.
        (OA, "pdf", f"{OA}.pdf"): (
            None,
            16,
            md5(b"%PDF-1.7 article"),
            f"{url}PMC100.2/PMC100.2.pdf",
            None,
            None,
            None,
        ),
        (OA, "figure", "gr1.jpg"): (
            f"{oa.as_posix()}/fig/gr1.jpg",
            len(GR1),
            md5(GR1),
            f"{url}PMC100.2/gr1.jpg",
            "Figure 1",
            "Cells divide.",
            day,
        ),
        (OA, "figure", "gr2.tif"): (
            f"{oa.as_posix()}/fig/gr2.tif.zst",
            len(TIF),
            md5(TIF),
            f"{url}PMC100.2/gr2.tif",
            "Figure 2",
            None,
            day,
        ),
        # A video is never downloaded, whatever its size.
        (OA, "supplement", "mmc3.mp4"): (
            None,
            len(MOVIE),
            md5(MOVIE),
            f"{url}PMC100.2/mmc3.mp4",
            "Movie S1",
            None,
            None,
        ),
        (OA, "supplement", "mmc4.dat"): (
            None,
            len(MOVIE),
            md5(MOVIE),
            f"{url}PMC100.2/mmc4.dat",
            None,
            None,
            None,
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
            f"{oa.as_posix()}/suppl/table.csv.zst",
            8,
            md5(CSV),
            f"{url}PMC100.2/table.csv",
            None,
            None,
            day,
        ),
        (AM, "xml", f"{AM}.xml"): (
            f"{am.as_posix()}/{AM}.xml.zst",
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
    assert sorted(p.name for p in (root / oa).rglob("*") if p.is_file()) == [
        f"{OA}.xml.zst",
        "gr1.jpg",
        "gr2.tif.zst",
        "mmc1.xlsx",
        "table.csv.zst",
    ]
    # A stored .zst file decompresses to the bytes whose MD5 the catalog gives.
    for f in rows.values():
        if f["path"] and f["path"].endswith(".zst"):
            assert md5(zstd.decompress((root / f["path"]).read_bytes())) == f["md5"]
    assert (root / oa / "suppl" / "mmc1.xlsx").read_bytes() == b"xlsx table"
    for skipped in ("mmc2.zip", "mmc3.mp4", "PMC100.2.pdf", "gr1.gif", "eq1.gif"):
        assert b.fetched(f"PMC100.2/{skipped}") == 0
    assert b.fetched("PMC100.1/PMC100.1.xml") == 0

    # The reader gives each file's path and opens it decompressed.
    held = {f.name: f for f in open_paper_store(root).reader.paper(OA).files}
    assert held[f"{OA}.xml"].path == root / oa / f"{OA}.xml.zst"
    with held[f"{OA}.xml"].open() as fh:
        assert fh.read() == OA_XML
    with held["gr1.jpg"].open() as fh:
        assert fh.read() == GR1

    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert {k: (r["status"], r["xml_tier"], r["pdf_tier"]) for k, r in papers.items()} == {
        OA: ("fetched", "pmc", None),
        AM: ("fetched", "pmc", None),
        CLOSED: ("manual", None, None),
    }
    # A paper the bucket lacks is recorded as tried, once, so later tiers may take it.
    sync(root)
    attempts = tables.read(root, tables.ATTEMPTS)
    assert sorted((a["paper"], a["tier"], a["outcome"]) for a in attempts) == [
        (OA, "pmc", "fetched"),
        (AM, "pmc", "fetched"),
        (CLOSED, "pmc", "absent"),
    ]


def test_a_bad_md5_is_retried_then_recorded_as_a_failed_attempt_with_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    b.bad.add("PMC100.2/table.csv")
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)

    assert b.fetched("PMC100.2/table.csv") == pmc.TRIES
    assert not [p.name for p in (root / paper_dir(OA)).rglob("*") if "table" in p.name]
    attempts = tables.read(root, tables.ATTEMPTS)
    assert sorted((a["paper"], a["outcome"]) for a in attempts) == [
        (OA, "failed"),
        (AM, "fetched"),
        (CLOSED, "absent"),
    ]
    assert "MD5" in next(a["detail"] for a in attempts if a["paper"] == OA)
    assert (OA, "supplement", "table.csv") not in files(root)
    # The next sync tries the file again, and only what it lacks.
    b.bad.clear()
    sync(root, "2026-12-03")
    assert files(root)[(OA, "supplement", "table.csv")]["fetched"] == dt.date(2026, 12, 3)
    assert b.fetched("PMC100.2/PMC100.2.xml") == 1


def test_bucket_retries_and_throttling_replies_are_counted_by_status_in_the_pass_and_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    queued = {
        "PMC100.2/gr1.jpg": [503, 503],
        "PMC100.2/mmc1.xlsx": [429],
        "PMC200.1/PMC200.1.xml": [500],
    }

    def busy(key: str) -> Reply | None:
        statuses = queued.get(key)
        return Reply("busy", status=statuses.pop()) if statuses else None

    b.fail = busy
    root = store(tmp_path, web(b), monkeypatch)
    output = sync(root)

    assert (root / paper_dir(OA) / "suppl" / "mmc1.xlsx").is_file()
    last = json.loads((root / "paper-status.json").read_text())["last_pass"]
    assert last["bucket_retries"] == {"429": 1, "500": 1, "503": 2}
    assert last["bucket_throttled"] == {"429": 1, "503": 2}
    assert "retries 429: 1, 500: 1, 503: 2; throttling replies 429: 1, 503: 2" in output


def test_a_failed_pass_still_records_and_logs_its_bucket_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    calls: list[str] = []

    def busy_then_killed(key: str) -> Reply | None:
        if key != "PMC100.2/gr1.jpg":
            return None
        calls.append(key)
        if len(calls) == 1:
            return Reply("busy", status=503)
        raise KilledError

    b.fail = busy_then_killed
    root = store(tmp_path, web(b), monkeypatch)
    result = CliRunner().invoke(app, ["papers", "pmc-sync", "--root", str(root), "--today", TODAY])

    assert isinstance(result.exception, KilledError)
    last = json.loads((root / "paper-status.json").read_text())["last_pass"]
    assert last["outcome"] == "failed"
    assert last["bucket_retries"] == {"503": 1}
    assert last["bucket_throttled"] == {"503": 1}
    assert "retries 503: 1; throttling replies 503: 1" in result.output


def test_a_file_rewritten_after_its_record_passes_by_its_own_etag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    # The bucket rewrites some files in place without renewing the record's MD5.
    b.objects["PMC100.2/table.csv"] = b"a,b\n1,3\n"
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)
    row = files(root)[(OA, "supplement", "table.csv")]
    assert row["md5"] == md5(b"a,b\n1,3\n")
    assert zstd.decompress((root / row["path"]).read_bytes()) == b"a,b\n1,3\n"
    assert b.fetched("PMC100.2/table.csv") == 1


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
    assert zstd.decompress((root / manuscript["path"]).read_bytes()) == AM_XML
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

    # A hard kill leaves its temporary file behind; the rerun clears it.
    (root / paper_dir(OA) / "suppl" / ".table.csv.999.part").write_bytes(b"a,")
    b.fail = None
    sync(root)
    assert not list((root / paper_dir(OA)).rglob(".*"))
    # A video known only by its Content-Type is asked for again, but no byte of it is read.
    objects = [
        g for g in b.gets if g.startswith("PMC") and not g.endswith(".json") and g not in b.types
    ]
    assert len(objects) == len(set(objects))
    assert (OA, "supplement", "table.csv") in files(root)


def bucket_gets(b: FakeBucket, since: int) -> list[str]:
    """Return the article files fetched after the first ``since`` requests."""
    return sorted(g for g in b.gets[since:] if g.startswith("PMC") and not g.endswith(".json"))


def held_pdf(root: Path, tier: str) -> Path:
    """Put a PDF another tier supplied into the store, as the daily pass writes it."""
    pdf = paper_dir(OA) / f"{OA}.pdf"
    (root / pdf).parent.mkdir(parents=True)
    (root / pdf).write_bytes(b"%PDF from " + tier.encode())
    row = {
        "paper": OA,
        "kind": "pdf",
        "name": f"{OA}.pdf",
        "path": pdf.as_posix(),
        "size": 18,
        "md5": md5(b"%PDF from " + tier.encode()),
        "url": "https://content.openalex.org/works/W1.pdf",
        "tier": tier,
    }
    tables.write(root, tables.FILES, [row])
    papers = tables.read(root, tables.PAPERS)
    for p in papers:
        if p["paper"] == OA:
            p["pdf_tier"] = tier
    tables.write(root, tables.PAPERS, papers)
    return pdf


def pmc_only(root: Path) -> None:
    """Assert the open-access paper holds what any PMC paper holds, and no other tier's PDF."""
    rows = {k: f for k, f in files(root).items() if k[0] == OA}
    assert {f["tier"] for f in rows.values()} == {"pmc"}
    assert rows[(OA, "xml", f"{OA}.xml")]["path"]
    assert rows[(OA, "figure", "gr1.jpg")]["path"]
    assert rows[(OA, "supplement", "mmc1.xlsx")]["path"]
    listed = rows[(OA, "pdf", f"{OA}.pdf")]
    assert (listed["path"], listed["url"]) == (
        None,
        "https://pmc-oa-opendata.s3.amazonaws.com/PMC100.2/PMC100.2.pdf",
    )
    assert not (root / paper_dir(OA) / f"{OA}.pdf").exists()
    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert (papers[OA]["status"], papers[OA]["xml_tier"], papers[OA]["pdf_tier"]) == (
        "fetched",
        "pmc",
        None,
    )


@pytest.mark.parametrize("tier", ["openalex", "host"])
def test_pmc_replaces_a_pdf_another_tier_supplied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tier: str
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    held_pdf(root, tier)
    sync(root)

    pmc_only(root)
    assert b.fetched("PMC100.2/PMC100.2.pdf") == 0
    # A later sync of a changed article keeps the paper as it is.
    held = {
        k.split("/", 1)[1]: v
        for k, v in b.objects.items()
        if k.startswith("PMC100.2/") and not k.endswith(".json")
    }
    b.put("PMC100", 2, {**held, "mmc1.xlsx": b"xlsx table, corrected"})
    b.day = "2026-12-02"
    sync(root, "2026-12-03")
    pmc_only(root)
    assert not [f for f in files(root).values() if f["gone"]]


def test_a_sync_killed_before_the_swap_keeps_both_copies_and_the_next_sync_swaps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    pdf = held_pdf(root, "openalex")
    unlink = Path.unlink

    def killed(self: Path, missing_ok: bool = False) -> None:
        if self == root / pdf:
            raise KilledError
        unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", killed)
    result = CliRunner().invoke(app, ["papers", "pmc-sync", "--root", str(root), "--today", TODAY])
    assert isinstance(result.exception, KilledError)
    assert (root / pdf).read_bytes() == b"%PDF from openalex"
    rows = files(root)
    assert rows[(OA, "pdf", f"{OA}.pdf")]["tier"] == "openalex"
    assert (root / rows[(OA, "xml", f"{OA}.xml")]["path"]).is_file()
    assert (root / rows[(OA, "figure", "gr1.jpg")]["path"]).is_file()

    monkeypatch.setattr(Path, "unlink", unlink)
    gets = len(b.gets)
    sync(root)
    pmc_only(root)
    assert bucket_gets(b, gets) == []


def test_a_pdf_another_tier_supplied_stays_while_pmc_has_no_xml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = FakeBucket(page=4)
    b.put("PMC100", 1, {"PMC100.1.pdf": b"%PDF-1.7 article"})
    root = store(tmp_path, web(b), monkeypatch)
    pdf = held_pdf(root, "openalex")
    sync(root)

    assert (root / pdf).read_bytes() == b"%PDF from openalex"
    assert files(root)[(OA, "pdf", f"{OA}.pdf")]["tier"] == "openalex"
    listed = files(root)[(OA, "pdf", "PMC100.pdf")]
    assert (listed["tier"], listed["path"]) == ("pmc", None)
    assert b.fetched("PMC100.1/PMC100.1.pdf") == 0


def test_a_listed_pmc_pdf_is_downloaded_once_the_article_has_no_xml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)
    assert files(root)[(OA, "pdf", f"{OA}.pdf")]["path"] is None

    b.put("PMC100", 3, {"PMC100.3.pdf": b"%PDF-1.7 article"})
    b.day = "2026-12-02"
    sync(root, "2026-12-03")

    row = files(root)[(OA, "pdf", f"{OA}.pdf")]
    assert row["path"] == (paper_dir(OA) / f"{OA}.pdf").as_posix()
    assert (root / row["path"]).read_bytes() == b"%PDF-1.7 article"


def test_a_store_fetched_before_figures_and_compression_converts_without_downloading_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)
    new = files(root)
    on_disk = sorted(p.relative_to(root) for p in (root / "papers").rglob("*") if p.is_file())

    # Put the store back as the PMC tier first wrote it: plain files, the PMC PDF and small
    # videos held, no figures.
    old = []
    for f in new.values():
        f = dict(f)
        if f["kind"] == "figure":
            (root / f["path"]).unlink()
            continue
        if f["path"] and f["path"].endswith(".zst"):
            plain = root / f["path"].removesuffix(".zst")
            plain.write_bytes(zstd.decompress((root / f["path"]).read_bytes()))
            (root / f["path"]).unlink()
            f["path"] = plain.relative_to(root).as_posix()
        if f["kind"] == "pdf" or f["name"] == "mmc3.mp4":
            source = f["url"].rsplit("/", 2)
            data = b.objects[f"{source[1]}/{source[2]}"]
            sub = "" if f["kind"] == "pdf" else "suppl/"
            f["path"] = (paper_dir(OA) / f"{sub}{f['name']}").as_posix()
            (root / f["path"]).write_bytes(data)
            f["fetched"] = dt.date(2026, 11, 7)
        old.append(f)
    tables.write(root, tables.FILES, old)
    attempts = tables.read(root, tables.ATTEMPTS)
    for a in attempts:
        a["detail"] = a["detail"] and a["detail"].removesuffix(f" v{pmc.LAYOUT}")
    tables.write(root, tables.ATTEMPTS, attempts)
    papers = tables.read(root, tables.PAPERS)
    for p in papers:
        if p["paper"] == OA:
            p["pdf_tier"] = "pmc"
    tables.write(root, tables.PAPERS, papers)

    gets = len(b.gets)
    sync(root)

    assert bucket_gets(b, gets) == ["PMC100.2/gr1.jpg", "PMC100.2/gr2.tif"]
    assert files(root) == new
    assert sorted(p.relative_to(root) for p in (root / "papers").rglob("*") if p.is_file()) == (
        on_disk
    )
    assert {r["paper"]: r["pdf_tier"] for r in tables.read(root, tables.PAPERS)}[OA] is None


def test_a_retracted_paper_is_skipped_keeps_what_it_holds_and_gets_no_downloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    # The PMC record says the manuscript is retracted.
    b.put("PMC200", 1, {"PMC200.1.xml": AM_XML}, manuscript=True, retracted=True)
    fake = web(b)
    root = store(tmp_path, fake, monkeypatch)
    sync(root)

    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert (papers[AM]["status"], papers[AM]["reason"]) == ("skipped", "retracted")
    assert b.fetched("PMC200.1/PMC200.1.xml") == 0
    assert not [f for f in files(root) if f[0] == AM]
    held = sorted(p for p in (root / paper_dir(OA)).rglob("*") if p.is_file())

    # A month later PubMed marks the open-access paper retracted: it keeps its files.
    fake.articles = {
        **fake.articles,
        OA: Article(OA, "Open paper", 2024, pmcid="PMC100", types=("Retracted Publication",)),
    }
    b.put("PMC100", 2, {"gr1.jpg": b"new figure"})
    b.day = "2026-12-02"
    gets = len(b.gets)
    sync(root, "2026-12-03")

    papers = {r["paper"]: r for r in tables.read(root, tables.PAPERS)}
    assert (papers[OA]["status"], papers[OA]["reason"]) == ("skipped", "retracted")
    assert sorted(p for p in (root / paper_dir(OA)).rglob("*") if p.is_file()) == held
    assert bucket_gets(b, gets) == []


def test_a_supplement_with_a_250_character_name_is_stored_and_opens_through_the_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    long_xlsx = ("NIHMS1-supplement-" + "Table_S2_" * 30)[:245] + ".xlsx"
    long_txt = ("NIHMS1-supplement-" + "Notes_S3_" * 30)[:246] + ".txt"
    assert len(long_xlsx) == len(long_txt) == 250
    b = bucket()
    b.put(
        "PMC200",
        1,
        {"PMC200.1.xml": AM_XML, long_xlsx: b"long table", long_txt: b"long notes"},
        manuscript=True,
    )
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)

    rows = files(root)
    held = {f.name: f for f in open_paper_store(root).reader.paper(AM).files}
    for name, data in ((long_xlsx, b"long table"), (long_txt, b"long notes")):
        assert rows[(AM, "supplement", name)]["fetched"] == dt.date(2026, 11, 7)
        with held[name].open() as fh:
            assert fh.read() == data
    # The stored name is the same on every pass, so a rerun downloads nothing again.
    b.put("PMC200", 1, {"PMC200.1.xml": AM_XML + b" "}, manuscript=True)
    gets = len(b.gets)
    sync(root, "2026-12-03")
    assert bucket_gets(b, gets) == ["PMC200.1/PMC200.1.xml"]
    assert files(root)[(AM, "supplement", long_xlsx)] == rows[(AM, "supplement", long_xlsx)]


def test_a_name_the_file_system_refuses_fails_once_without_a_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b = bucket()
    tries: list[str] = []

    def refuse(key: str) -> None:
        if key == "PMC100.2/table.csv":
            tries.append(key)
            raise OSError(errno.ENAMETOOLONG, "File name too long")

    b.fail = refuse
    root = store(tmp_path, web(b), monkeypatch)
    sync(root)

    assert len(tries) == 1
    attempts = tables.read(root, tables.ATTEMPTS)
    assert "File name too long" in next(a["detail"] for a in attempts if a["paper"] == OA)
