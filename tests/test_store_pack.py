"""Fetch in pieces, pack once: resume at the Entrez seam, and pack's output and memory."""

from __future__ import annotations

import os
import re
import shutil
import time
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


def test_a_piece_written_again_keeps_one_part(tmp_path: Path) -> None:
    for _ in range(3):
        raw.fetch_pieces(
            tmp_path, raw.SRA_XML, ["SRX1"], lambda _: [], TODAY, remember_misses=False
        )
    assert len(raw.part_files(tmp_path, raw.SRA_XML)) == 1


def test_parts_go_oldest_first_and_an_unstamped_part_by_its_mtime(tmp_path: Path) -> None:
    folder = raw.parts_dir(tmp_path, raw.GEO_SOFT)
    folder.mkdir(parents=True)

    def unstamped(tag: str, mtime: int) -> Path:
        """A part with no stamp in its name, as pack's repack part, aged to ``mtime``."""
        path = folder / raw.part_name("GSE1nnnn", tag)
        path.touch()
        os.utime(path, ns=(mtime, mtime))
        return path

    second = 1_000_000_000
    repack = unstamped("repack-GSE1nnn", 0)
    old_row = unstamped("0123456789ab", time.time_ns() - 2 * second)
    old_removal = unstamped("removed-0123456789ab", time.time_ns() - second)
    row = raw.write_part(tmp_path, raw.Piece(raw.GEO_SOFT, "GSE1nnnn", ("GSE10001",)), [])
    [removal] = raw.write_removals(tmp_path, raw.GEO_SOFT, ["GSE10001"])
    late = unstamped("fedcba987654", time.time_ns() + second)

    expected = [repack, old_row, old_removal, row, removal, late]
    assert raw.part_files(tmp_path, raw.GEO_SOFT) == expected


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
    before = {source: list(raw.rows(root, source)) for source in (raw.GEO_SOFT, raw.SRA_XML)}
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
    assert {s: list(raw.rows(root, s)) for s in before} == before
    assert "10,000" in (soft / "README.md").read_text()


def _layered(root: Path) -> Path:
    """A packed bucket, then an older part, a removal part and a newer part over it."""
    old = [raw.RawRow(f"GSE1000{n}", f"{c}0", "d0") for n, c in ((1, "a"), (2, "b"), (3, "c"))]
    pack.write_rows(root, raw.GEO_SOFT, old)
    older = [raw.RawRow("GSE10001", "a1", "d1"), raw.RawRow("GSE10004", "d1", "d1")]
    raw.save(root, raw.GEO_SOFT, older)
    raw.write_removals(root, raw.GEO_SOFT, ["GSE10002", "GSE10004", "GSE10005"])
    newer = [raw.RawRow("GSE10001", "a2", "d2"), raw.RawRow("GSE10005", "e2", "d2")]
    raw.save(root, raw.GEO_SOFT, newer)
    return root


#: What :func:`_layered` holds: each held accession's newest text and fetch date.
_NEWEST = {"GSE10001": ("a2", "d2"), "GSE10003": ("c0", "d0"), "GSE10005": ("e2", "d2")}
_WANTED = [f"GSE1000{n}" for n in range(1, 7)]


def test_every_read_before_pack_sees_what_pack_then_writes(tmp_path: Path) -> None:
    root, source = _layered(tmp_path), raw.GEO_SOFT
    letter = re.compile(r"^([a-z])")

    assert raw.held(root, source) == raw.held(root, source, _WANTED) == set(_NEWEST)
    assert raw.held(root, source, _WANTED, since_pack=True) == {"GSE10001", "GSE10005"}
    assert raw.saved(root, source, _WANTED) == {a: text for a, (text, _) in _NEWEST.items()}
    dates = raw.saved(root, source, _WANTED, column="fetch_date")
    assert dates == {a: d for a, (_, d) in _NEWEST.items()}
    assert raw.scan(root, source, _WANTED, letter) == {"a", "c", "e"}
    assert raw.scan(root, source, _WANTED, letter, since_pack=True) == {"a", "e"}
    assert raw.removing(root, source) == {"GSE10002", "GSE10004"}
    before = [(row.accession, row.raw_text, row.fetch_date) for row in raw.rows(root, source)]
    assert before == [(a, *row) for a, row in _NEWEST.items()]
    [merged] = raw.merged_files(root, source, tmp_path / "merged")
    assert _triples(merged) == before

    pack.pack(root, [source])
    assert _triples(root / "raw" / source / "GSE1nnnn.parquet") == before


def _triples(path: Path) -> list[tuple[str, str, str]]:
    """A Parquet file's rows as they are on disk: accession, text, date."""
    rows = pq.read_table(path).to_pylist()
    return [(r["accession"], r["raw_text"], r["fetch_date"]) for r in rows]


def test_fetch_planning_skips_exactly_the_held_accessions(tmp_path: Path) -> None:
    root, source = _layered(tmp_path), raw.GEO_SOFT

    todo = {a for piece in raw.pieces(root, source, _WANTED) for a in piece.todo}
    assert todo == set(_WANTED) - raw.held(root, source, _WANTED)
    again = {a for piece in raw.pieces(root, source, (), _WANTED) for a in piece.todo}
    assert again == set(_WANTED) - raw.held(root, source, _WANTED, since_pack=True)


def _counted_store(root: Path) -> Path:
    """A packed bucket, then parts that replace, keep, add and remove rows in two buckets."""
    old = [raw.RawRow(f"GSE{n}", "old", "d0") for n in (10001, 10002, 10003, 20001)]
    pack.write_rows(root, raw.GEO_SOFT, old)
    new = [raw.RawRow("GSE10001", "old", "d1"), raw.RawRow("GSE10002", "new", "d1")]
    raw.write_part(root, raw.Piece(raw.GEO_SOFT, "GSE1nnnn", ("GSE10001", "GSE10002")), new)
    added = [raw.RawRow("GSE20002", "added", "d1")]
    raw.write_part(root, raw.Piece(raw.GEO_SOFT, "GSE2nnnn", ("GSE20002",)), added)
    raw.write_removals(root, raw.GEO_SOFT, ["GSE10003"])
    return root


@pytest.mark.parametrize("kill", ["before", "after"])
def test_pack_counts_after_a_kill_and_rerun_match_an_uninterrupted_pack(
    tmp_path: Path, kill: str
) -> None:
    whole, cut = _counted_store(tmp_path / "whole"), _counted_store(tmp_path / "cut")
    counts = tmp_path / "counts.jsonl"
    if kill == "before":
        # A pack killed before its renames: its counts are written, the store is unchanged.
        shutil.copytree(cut, tmp_path / "killed", copy_function=shutil.copy2)
        pack.pack(tmp_path / "killed", counts=counts)
    else:
        # A pack killed after its renames: the buckets are new, the parts are still there.
        parts = cut / "work" / "parts"
        shutil.copytree(parts, tmp_path / "parts", copy_function=shutil.copy2)
        pack.pack(cut, counts=counts)
        shutil.copytree(tmp_path / "parts", parts, dirs_exist_ok=True, copy_function=shutil.copy2)
    pack.pack(cut, counts=counts)
    pack.pack(whole, counts=whole / "counts.jsonl")

    expected = {raw.GEO_SOFT: {"added": 1, "replaced": 1, "removed": 1}}
    assert pack.read_counts(whole / "counts.jsonl") == expected
    assert pack.read_counts(counts) == expected
    assert _raw(cut) == _raw(whole)
