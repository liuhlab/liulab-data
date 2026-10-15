"""The stage 2 pilot script: target draw, pooled and concurrent fetch, shards, the report."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import ClassVar

import pytest

from labdata.exceptions import PageNotFound
from labdata.store import raw
from tests._fakes import ExplodingEntrezClient, FakeEntrezClient
from tests._slices import DATA, geo_client, new_root

_SCRIPT = Path(__file__).parent.parent / "scripts" / "store" / "pilot_stage2.py"
_SOFT = DATA / "geo_slice" / "soft"
SEQ = "Expression profiling by high throughput sequencing"
MIXED = f"{SEQ}; Expression profiling by array"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pilot_stage2", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["pilot_stage2"] = module
    spec.loader.exec_module(module)
    return module


pilot = _load()


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _line(accession: str, gdstype: str) -> str:
    return json.dumps({"accession": accession, "gdstype": gdstype}) + "\n"


class _CombinedFake(FakeEntrezClient):
    """A fake that, like NCBI, answers a comma-joined ``id`` with one merged link set."""

    def elink_many(self, dbfrom: str, db: str, uids: list[str]) -> dict[str, list[str]]:
        self.calls.append(("elink_many", (dbfrom, db, tuple(uids))))
        return {
            str(uid): [
                x for part in str(uid).split(",") for x in self._elink.get((dbfrom, db, part), [])
            ]
            for uid in uids
        }


def _with_cache(client: FakeEntrezClient) -> FakeEntrezClient:
    client._cache = {}  # the fake skips EntrezClient.__init__; the pilot clears this cache
    return client


def test_only_sequencing_series_are_targets_and_mixed_ones_are_marked() -> None:
    lines = [_line("GSE10", SEQ), _line("GSE9", "Expression profiling by array")]
    lines += [_line("GSE2", MIXED), "\n"]
    targets = pilot.sequencing_series(lines)
    assert [(t.accession, t.mixed) for t in targets] == [("GSE2", True), ("GSE10", False)]


def test_draws_are_seeded_sized_by_fraction_and_nested() -> None:
    pool = [pilot.Target(f"GSE{i}", SEQ) for i in range(1, 10_001)]
    small = pilot.draw(pool, 0.001, 50)
    large = pilot.draw(pool, 0.01, 50)
    assert (len(small), len(large)) == (10, 100)
    assert {t.accession for t in small} <= {t.accession for t in large}
    assert pilot.draw(pool, 0.0, 50, size=10) == small
    assert large == pilot.draw(pool, 0.01, 50)
    assert large != pilot.draw(pool, 0.01, 51)
    assert [int(t.accession[3:]) for t in large] == sorted(int(t.accession[3:]) for t in large)


class _Body(io.BytesIO):
    def getheader(self, name: str) -> str | None:
        return str(len(self.getvalue())) if name == "Content-Length" else None


def test_read_soft_strips_data_tables_and_keeps_the_original_size_and_hash() -> None:
    table = b"!sample_table_begin\nID\tVALUE\n1\t2\n!sample_table_end\n"
    head = b"^SAMPLE = GSM1\n!Sample_title = a\n"
    original = head + table + b"^PLATFORM = GPL1\n"
    body = gzip.compress(original)
    soft, wire = pilot.read_soft(_Body(body))
    assert soft.text == (head + b"^PLATFORM = GPL1\n").decode()
    assert (soft.original_bytes, wire, soft.gz_bytes) == (len(original), len(body), len(body))
    assert soft.original_sha256 == hashlib.sha256(original).hexdigest()


def test_each_client_call_is_timed_under_the_raw_source_it_feeds() -> None:
    assert pilot.step_of("fetch_text", "https://x/GSE1_family.soft.gz") == raw.GEO_SOFT
    assert pilot.step_of("fetch_text", "https://x/suppl/filelist.txt") == raw.GEO_FILELIST
    assert pilot.step_of("efetch") == raw.SRA_XML
    assert pilot.step_of("esearch") == raw.PUBMED_SI
    assert pilot.step_of("elink_many") == raw.ENTREZ_LINKS


def test_quantiles_use_nearest_rank() -> None:
    q = pilot.quantiles(list(range(1, 101)))
    assert q == {"n": 100, "p50": 50, "p90": 90, "p99": 99, "max": 100}
    assert pilot.quantiles([]) == {"n": 0}


def test_a_torn_last_log_line_is_ignored(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    log.write_text('{"chunk": 0}\n{"chunk": 1', encoding="utf-8")
    assert pilot.read_log(log) == [{"chunk": 0}]


class _Site(BaseHTTPRequestHandler):
    """GEO's download site in small.

    Family SOFT of the fixture Series is served gzipped; GSE2 is throttled once; anything
    else, ``filelist.txt`` included, is missing.
    """

    throttled: ClassVar[set[str]] = set()

    def do_GET(self) -> None:
        name = self.path.rsplit("/", 1)[-1]
        gse = name.split("_")[0]
        if gse == "GSE2" and gse not in self.throttled:
            self.throttled.add(gse)
            self._reply(503, b"busy")
        elif gse == "GSE2":
            self._reply(200, gzip.compress(b"x" * 1000))
        elif name.endswith("_family.soft.gz") and (_SOFT / f"{gse}.soft").is_file():
            self._reply(200, gzip.compress((_SOFT / f"{gse}.soft").read_bytes()))
        else:
            self._reply(404, b"no")

    def _reply(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Serve :class:`_Site` on localhost and yield its base URL; backoff sleeps are skipped."""
    monkeypatch.setattr(time, "sleep", lambda _s: None)
    _Site.throttled = set()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Site)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_the_pooled_get_halves_on_503_retries_and_counts_missing(site: str) -> None:
    paths = {f"GSE{i}": f"/x/GSE{i}_family.soft.gz" for i in (2, 3, 4)}
    values, stats = pilot.pooled_get(paths, pilot.read_soft, connections=4, base=site)
    assert values["GSE3"] is None
    assert values["GSE2"] is not None
    assert stats["items"] == 1
    assert stats["missing_404"] == ["GSE3", "GSE4"]
    assert stats["throttle_events"] == {"HTTP 503": 1}
    assert stats["connections_at_end"] == 2
    assert not stats["failed"]


@pytest.fixture
def main_store(tmp_path: Path) -> Path:
    """A main store: the GEO fixture snapshots, extracted, with typed esummary lines."""
    root = new_root(tmp_path / "main", "geo_slice")
    gz = next((root / "snapshots" / "geo-series").glob("*/gds_gse_esummary.jsonl.gz"))
    lines = [_line("GSE19456", SEQ), _line("GSE28770", MIXED), _line("GSE900001", SEQ)]
    lines.append(_line("GSE1", "Expression profiling by array"))
    with gzip.open(gz, "wt", encoding="utf-8") as fh:
        fh.writelines(lines)
    return root


def test_setup_links_the_main_store_and_a_step_draws_its_targets(
    main_store: Path, tmp_path: Path
) -> None:
    root = tmp_path / "pilot"
    pilot.setup(main_store, root)
    assert (root / "snapshots").resolve() == (main_store / "snapshots").resolve()
    assert (root / "work" / "extract").is_symlink()
    meta = pilot.draw_step(root, "s1", 1.0, 50, 2)
    assert (meta["sequencing_series"], meta["drawn"], meta["drawn_mixed"]) == (3, 3, 1)
    want = {"GSE19456": False, "GSE28770": True, "GSE900001": False}
    assert pilot.read_targets(root, "s1") == want
    assert pilot.draw_step(root, "s1", 1.0, 50, 2) == meta
    with pytest.raises(SystemExit, match="other settings"):
        pilot.draw_step(root, "s1", 1.0, 51, 2)
    with pytest.raises(SystemExit, match="must differ"):
        pilot.setup(main_store, main_store)


def _missing_soft_client() -> FakeEntrezClient:
    client = _with_cache(geo_client())
    url = (
        "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE900nnn/GSE900001/soft/GSE900001_family.soft.gz"
    )
    client._pages = {**client._pages, url: None}
    return client


def test_shards_time_the_stage1_fetch_in_their_own_roots_and_resume(
    main_store: Path, tmp_path: Path
) -> None:
    root = tmp_path / "pilot"
    pilot.setup(main_store, root)
    pilot.draw_step(root, "s1", 1.0, 50, 1)
    for index in (0, 1):
        pilot.run_shard(root, "s1", index, 2, _missing_soft_client())
    logs = pilot.fetch_logs(root, "s1")
    assert sorted(s for entries in logs.values() for e in entries for s in e["series"]) == [
        "GSE19456",
        "GSE28770",
        "GSE900001",
    ]
    failed = {g: f for entries in logs.values() for e in entries for g, f in e["failed"].items()}
    assert failed["GSE900001"]["step"] == raw.GEO_SOFT
    assert failed["GSE900001"]["error"] == PageNotFound.__name__
    assert not (root / "raw").exists()
    pilot.run_shard(root, "s1", 0, 2, ExplodingEntrezClient())  # logged chunks make no call
    with pytest.raises(SystemExit, match="outside"):
        pilot.run_shard(root, "s1", 2, 2, ExplodingEntrezClient())
    summary = pilot.summarize_fetch([e for entries in logs.values() for e in entries])
    assert summary["series"] == 3
    assert summary["failures_by_kind"] == {"geo-soft: PageNotFound": 1}


def test_the_concurrent_fetch_fills_raw_once_per_source_then_builds_and_reports(
    main_store: Path, tmp_path: Path, site: str
) -> None:
    root = tmp_path / "pilot"
    pilot.setup(main_store, root)
    pilot.draw_step(root, "s1", 1.0)
    client = _with_cache(geo_client())
    client.__class__ = _CombinedFake
    result = pilot.concurrent_fetch(
        root, "s1", client, connections=2, inflight=2, batch=3, base=site
    )
    to_sra = [args for name, args in client.calls if name == "elink_many" and args[1] == "sra"]
    assert len(to_sra) == 2  # one combined call per Series
    assert all(len(args[2]) == 1 and "," in args[2][0] for args in to_sra)
    assert result[raw.GEO_SOFT]["missing_404"] == ["GSE900001"]
    assert result[raw.GEO_SOFT]["items"] >= 2
    soft = raw.saved(root, raw.GEO_SOFT, ["GSE19456", "GSE28770"])
    assert sorted(soft) == ["GSE19456", "GSE28770"]
    assert "table_begin" not in soft["GSE19456"]
    original = (root / "pilot" / "s1" / "soft-original.tsv").read_text().splitlines()
    assert any(line.startswith("GSE19456\t") for line in original)
    assert raw.saved(root, raw.GEO_FILELIST, ["GSE19456"]) == {"GSE19456": ""}
    sra = result[raw.SRA_XML]
    assert sra["items"] == sra["requested"] > 0
    assert sra["missing"] == 0
    assert result["pack"]["sources"][raw.SRA_XML]["rows"] == sra["items"]
    assert raw.PUBMED_SI not in result
    extra = pilot.elink_extra(root, "s1")
    assert extra["series"] == 2
    assert extra["total"]["sample_to_srx_elink"] > 0
    assert extra["total"]["sample_to_srx_extra"] <= extra["total"]["sample_to_srx_elink"]
    assert (
        pilot.concurrent_fetch(
            root, "s1", ExplodingEntrezClient(), connections=2, inflight=2, base=site
        )
        == result
    )

    built = pilot.run_build(root, "p1")
    assert built["rows"]["holdings.parquet"] > 0
    assert not (main_store / "builds").exists()
    report = pilot.report(root, "s1")
    assert report["stability"]["stable"]
    assert report["projection_hours"][raw.SRA_XML] is not None
    assert report["bytes"][raw.SRA_XML]["rows"] == sra["items"]
    assert [b["build_id"] for b in report["builds"]] == ["p1"]
    assert "Stage 2 pilot: s1" in (root / "pilot" / "s1" / "results.md").read_text()
