"""The stage 2 pilot script: target draw, metering, resume and the report, on fixture data."""

from __future__ import annotations

import gzip
import importlib.util
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
from tests._slices import geo_client, new_root

_SCRIPT = Path(__file__).parent.parent / "scripts" / "store" / "pilot_stage2.py"
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


def test_only_sequencing_series_are_targets_and_mixed_ones_are_marked() -> None:
    lines = [
        _line("GSE10", SEQ),
        _line("GSE9", "Expression profiling by array"),
        _line("GSE2", MIXED),
        "\n",
    ]
    targets = pilot.sequencing_series(lines)
    assert [(t.accession, t.mixed) for t in targets] == [("GSE2", True), ("GSE10", False)]


def test_the_draw_is_seeded_sized_by_fraction_and_sorted() -> None:
    pool = [pilot.Target(f"GSE{i}", SEQ) for i in range(1, 1001)]
    first = pilot.draw(pool, 0.01, 50)
    assert len(first) == 10
    assert first == pilot.draw(pool, 0.01, 50)
    assert first != pilot.draw(pool, 0.01, 51)
    assert [int(t.accession[3:]) for t in first] == sorted(int(t.accession[3:]) for t in first)
    assert len(pilot.draw(pool[:3], 0.01, 50)) == 1


def test_table_bytes_counts_only_data_table_blocks() -> None:
    table = "!sample_table_begin\nID\tVALUE\n1\t2\n!sample_table_end\n"
    text = "^SAMPLE = GSM1\n!Sample_title = a\n" + table + "^PLATFORM = GPL1\n"
    assert pilot.table_bytes(text) == len(table)
    assert pilot.table_bytes("^SERIES = GSE1\n") == 0


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


@pytest.fixture
def main_store(tmp_path: Path) -> Path:
    """A main store: the GEO fixture snapshots, extracted, with typed esummary lines."""
    root = new_root(tmp_path / "main", "geo_slice")
    gz = next((root / "snapshots" / "geo-series").glob("*/gds_gse_esummary.jsonl.gz"))
    lines = [
        _line("GSE19456", SEQ),
        _line("GSE28770", MIXED),
        _line("GSE900001", SEQ),
        _line("GSE1", "Expression profiling by array"),
    ]
    with gzip.open(gz, "wt", encoding="utf-8") as fh:
        fh.writelines(lines)
    return root


def _with_cache(client: FakeEntrezClient) -> FakeEntrezClient:
    client._cache = {}  # the fake skips EntrezClient.__init__; the pilot clears this cache
    return client


def _missing_soft_client() -> FakeEntrezClient:
    client = _with_cache(geo_client())
    url = (
        "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE900nnn/GSE900001/soft/GSE900001_family.soft.gz"
    )
    client._pages = {**client._pages, url: None}
    return client


class _Site(BaseHTTPRequestHandler):
    """GEO's download site in small: GSE2 is throttled once, GSE3 is missing."""

    throttled: ClassVar[set[str]] = set()

    def do_GET(self) -> None:
        gse = self.path.rsplit("/", 1)[-1].split("_")[0]
        if gse == "GSE2" and gse not in self.throttled:
            self.throttled.add(gse)
            self._reply(503, b"busy")
        elif gse == "GSE3":
            self._reply(404, b"no")
        else:
            self._reply(200, b"x" * 1000)

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


def test_the_pooled_soft_bench_halves_on_503_and_counts_missing_files(
    tmp_path: Path, site: str
) -> None:
    root = tmp_path / "pilot"
    (root / "pilot" / "s").mkdir(parents=True)
    rows = "".join(f"GSE{i}\t0\t{SEQ}\n" for i in range(1, 7))
    (root / "pilot" / "s" / "targets.tsv").write_text("accession\tmixed\tgdstype\n" + rows)
    result = pilot.soft_bench(root, "s", connections=4, base=site)
    assert result["files"] == 5
    assert result["gz_bytes"] == 5000
    assert result["missing_404"] == ["GSE3"]
    assert result["throttle_events"] == {"HTTP 503": 1}
    assert result["connections_at_end"] == 2
    assert not result["failed"]


def test_nested_draws_put_a_smaller_step_inside_a_larger_one() -> None:
    pool = [pilot.Target(f"GSE{i}", SEQ) for i in range(1, 10_001)]
    small = {t.accession for t in pilot.draw(pool, 0.001, 50)}
    large = {t.accession for t in pilot.draw(pool, 0.01, 50)}
    assert (len(small), len(large)) == (10, 100)
    assert small <= large


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


def test_a_step_pins_failures_to_series_resumes_and_reports(
    main_store: Path, tmp_path: Path, site: str
) -> None:
    root = tmp_path / "pilot"
    pilot.setup(main_store, root)
    results = pilot.run_step(
        root,
        "s1",
        _missing_soft_client(),
        fraction=1.0,
        soft_connections=2,
        sra_inflight=2,
        batch=3,
        chunk=2,
        soft_base=site,
    )
    entries = pilot.read_log(root / "pilot" / "s1" / "fetch-log.jsonl")
    assert [e["series"] for e in entries] == [["GSE19456", "GSE28770"], ["GSE900001"]]
    assert entries[1]["failed"]["GSE900001"]["step"] == raw.GEO_SOFT
    assert entries[1]["failed"]["GSE900001"]["error"] == PageNotFound.__name__

    assert results["soft_bench"]["files"] == 3
    sra = results["sra_bench"]
    assert sra["packages"] == sra["requested"] == results["bytes"][raw.SRA_XML]["rows"]
    assert sra["bytes_per_s"] > 0
    assert results["stability"]["stable"]
    assert results["projection"]["soft_hours"] is not None
    assert results["held_target_series"] == 2
    assert results["gaps"] == 1
    assert results["fetch"]["failures_by_kind"] == {"geo-soft: PageNotFound": 1}
    assert results["bytes"][raw.GEO_SOFT]["by_kind"]["mixed"]["n"] == 1
    sizes = (root / "pilot" / "s1" / "soft-sizes.tsv").read_text().splitlines()
    assert len(sizes) == 1 + results["bytes"][raw.GEO_SOFT]["rows"]

    # A rerun of a finished step makes no call.
    pilot.run_step(
        root,
        "s1",
        ExplodingEntrezClient(),
        fraction=1.0,
        soft_connections=2,
        sra_inflight=2,
        chunk=2,
        soft_base=site,
    )
    assert len(pilot.read_log(root / "pilot" / "s1" / "fetch-log.jsonl")) == 2

    built = pilot.run_build(root, "p1")
    assert built["rows"]["holdings.parquet"] > 0
    assert built["maxrss_bytes"] > 0
    assert not (main_store / "builds").exists()
    report = pilot.report(root, "s1")
    assert [b["build_id"] for b in report["builds"]] == ["p1"]
    assert "Stage 2 pilot: s1" in (root / "pilot" / "s1" / "results.md").read_text()


def test_a_torn_last_log_line_is_ignored(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    log.write_text('{"chunk": 0}\n{"chunk": 1', encoding="utf-8")
    assert pilot.read_log(log) == [{"chunk": 0}]
