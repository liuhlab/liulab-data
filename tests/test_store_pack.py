"""Fetch in pieces, pack once: resume at the Entrez seam, and pack's output and memory."""

from __future__ import annotations

import os
import tracemalloc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.store import pack, raw
from labdata.store.fetch import fetch, soft_url
from tests._fakes import FakeEntrezClient
from tests._slices import GEO_SERIES, TODAY, geo_client, new_root


class KilledError(Exception):
    """Stands in for a job killed mid-fetch."""


class _KilledAfter(FakeEntrezClient):
    """The geo slice's client, killed on its ``n``-th SOFT download."""

    def __init__(self, inner: FakeEntrezClient, n: int) -> None:
        self.__dict__.update(inner.__dict__)
        self._left = n

    def fetch_text(self, url: str) -> str:
        if url.endswith("_family.soft.gz"):
            self._left -= 1
            if self._left == 0:
                raise KilledError(url)
        return super().fetch_text(url)


def _raw(root: Path) -> dict[str, list[dict[str, Any]]]:
    """Every raw bucket file's rows, by path under ``raw/``."""
    return {
        str(path.relative_to(root / "raw")): pq.read_table(path).to_pylist()
        for path in sorted((root / "raw").rglob("*.parquet"))
    }


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def test_a_killed_fetch_reruns_without_asking_for_a_saved_piece(tmp_path: Path) -> None:
    whole = new_root(tmp_path / "whole", "geo_slice")
    fetch(whole, GEO_SERIES, client=geo_client(), today=TODAY)

    root = new_root(tmp_path / "cut", "geo_slice")
    with pytest.raises(KilledError):
        fetch(root, GEO_SERIES, client=_KilledAfter(geo_client(), 4), today=TODAY)
    kept = raw.saved(root, raw.GEO_SOFT, GEO_SERIES)
    assert kept, "no piece finished before the kill"
    assert not (root / "raw").exists()

    client = geo_client()
    fetch(root, GEO_SERIES, client=client, today=TODAY)
    asked = {url for name, (url, *_) in client.calls if name == "fetch_text"}
    assert not asked & {soft_url(gse) for gse in kept}
    assert _raw(root) == _raw(whole)
    assert not list((root / "work" / "parts").rglob("*.parquet"))


def test_a_piece_with_no_row_for_an_accession_is_not_asked_again(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def get(todo: list[str]) -> list[tuple[str, str]]:
        calls.append(todo)
        return [(a, "text") for a in todo if a != "SRX2"]

    for _ in range(2):
        raw.fetch_pieces(tmp_path, raw.SRA_XML, ["SRX1", "SRX2", "SRX3000001"], get, TODAY)
    assert sorted(calls) == [["SRX1", "SRX2"], ["SRX3000001"]]


def _texts(n: int, size: int) -> list[raw.RawRow]:
    return [raw.RawRow(f"SRX{7 * i}", os.urandom(size // 2).hex(), TODAY) for i in range(n)]


def test_pack_memory_is_bounded_by_a_chunk_not_the_source(tmp_path: Path) -> None:
    chunk = 1 << 20
    rows = _texts(4000, 4096)
    total = sum(len(row.raw_text) for row in rows)
    assert total > 12 * chunk
    for start in (0, 1):
        mine = rows[start::2]
        piece = raw.Piece(raw.SRA_XML, "SRXnnnnnn", tuple(r.accession for r in mine))
        raw.write_part(tmp_path, piece, iter(mine), chunk_bytes=chunk)
    del rows, mine

    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    previous = pa.default_memory_pool()
    pa.set_memory_pool(pool)
    tracemalloc.start()
    try:
        [path] = pack.pack(tmp_path, [raw.SRA_XML], chunk_bytes=chunk)
        _, python_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        pa.set_memory_pool(previous)
    assert python_peak + pool.max_memory() < 4 * chunk
    out = pq.ParquetFile(path)
    assert out.metadata.num_rows == 4000
    assert out.num_row_groups > 1
    accessions = pq.read_table(path, columns=["accession"]).column(0).to_pylist()
    assert accessions == sorted(accessions)


def test_pack_repacks_buckets_of_another_width_to_the_same_rows(tmp_path: Path) -> None:
    root = new_root(tmp_path / "store", "geo_slice")
    fetch(root, GEO_SERIES, client=geo_client(), today=TODAY)
    before = {source: raw.read_rows(root, source) for source in (raw.GEO_SOFT, raw.SRA_XML)}
    soft = root / "raw" / raw.GEO_SOFT
    for path in list(soft.glob("*.parquet")):
        table = pq.read_table(path)
        for i, accession in enumerate(table.column("accession").to_pylist()):
            old = soft / f"GSE{accession[3:-3]}nnn.parquet"
            mine = table.slice(i, 1)
            if old.exists():
                mine = pa.concat_tables([pq.read_table(old), mine]).sort_by("accession")
            pq.write_table(mine, old)
        path.unlink()
    assert "GSE246nnn.parquet" in {p.name for p in soft.iterdir()}

    result = CliRunner().invoke(app, ["store", "pack", "--root", str(root)])
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in soft.glob("*.parquet")) == [
        "GSE15nnnn.parquet",
        "GSE1nnnn.parquet",
        "GSE23nnnn.parquet",
        "GSE24nnnn.parquet",
        "GSE2nnnn.parquet",
    ]
    assert {s: raw.read_rows(root, s) for s in before} == before
    assert "10,000" in (soft / "README.md").read_text()
