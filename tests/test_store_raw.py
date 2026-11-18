"""The raw module's verbs: fetch in pieces and resume, every read before and after pack, pack."""

from __future__ import annotations

import hashlib
import inspect
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
from labdata.store import lifecycle, raw
from labdata.store.fetch import fetch, soft_url
from tests._fakes import FakeEntrezClient
from tests._slices import GEO_SERIES, TODAY, geo_client, new_root, raw_rows, write_rows


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
    kept = dict(raw.saved(root, raw.GEO_SOFT, GEO_SERIES))
    assert kept, "no piece finished before the kill"
    assert not (root / "raw").exists()

    client = geo_client()
    fetch(root, GEO_SERIES, client=client, today=TODAY)
    asked = {url for name, (url, *_) in client.calls if name == "fetch_text"}
    assert not asked & {soft_url(gse) for gse in kept}
    assert _raw(root) == _raw(whole)
    assert not list((root / "work" / "parts").rglob("*.parquet"))


def test_a_piece_with_no_row_for_an_accession_is_not_asked_again(tmp_path: Path) -> None:
    asked: list[list[str]] = []
    for _ in range(2):
        for piece in raw.pieces(tmp_path, raw.SRA_XML, ["SRX1", "SRX2", "SRX3000001"]):
            asked.append(list(piece.todo))
            rows = [raw.RawRow(a, "text", TODAY) for a in piece.todo if a != "SRX2"]
            raw.save(tmp_path, raw.SRA_XML, rows, asked=piece.todo)
    assert sorted(asked) == [["SRX1", "SRX2"], ["SRX3000001"]]


def test_a_piece_saved_again_keeps_one_part(tmp_path: Path) -> None:
    for _ in range(3):
        raw.save(tmp_path, raw.SRA_XML, [], asked=["SRX1"])
    assert len(list((tmp_path / "work" / "parts" / raw.SRA_XML).glob("*.parquet"))) == 1


def test_parts_go_oldest_first_and_an_unstamped_part_by_its_mtime(tmp_path: Path) -> None:
    folder = tmp_path / "work" / "parts" / raw.GEO_SOFT
    folder.mkdir(parents=True)

    def unstamped(tag: str, mtime: int, **texts: str) -> None:
        """A part with no stamp in its name, as pack's repack part, aged to ``mtime``."""
        path = folder / f"GSE1nnnn.{tag}.parquet"
        hashes = [hashlib.sha256(t.encode()).hexdigest() for t in texts.values()]
        columns = [list(texts), list(texts.values()), hashes, [TODAY] * len(texts)]
        pq.write_table(pa.table(columns, schema=raw.SCHEMA), path)
        os.utime(path, ns=(mtime, mtime))

    second = 1_000_000_000
    unstamped("repack-GSE1nnn", 0, GSE10001="repack", GSE10002="repack", GSE10003="repack")
    unstamped("0123456789ab", time.time_ns() - second, GSE10002="old", GSE10003="old")
    raw.save(tmp_path, raw.GEO_SOFT, [raw.RawRow("GSE10003", "saved", TODAY)])
    unstamped("fedcba987654", time.time_ns() + second, GSE10004="late")
    raw.remove(tmp_path, raw.GEO_SOFT, ["GSE10004"])

    expected = {"GSE10001": "repack", "GSE10002": "old", "GSE10003": "saved", "GSE10004": "late"}
    assert dict(raw.saved(tmp_path, raw.GEO_SOFT)) == expected


def _texts(n: int, size: int) -> list[raw.RawRow]:
    return [raw.RawRow(f"SRX{7 * i}", os.urandom(size // 2).hex(), TODAY) for i in range(n)]


def test_pack_memory_is_bounded_by_a_chunk_not_the_source(tmp_path: Path) -> None:
    chunk = 1 << 20
    rows = _texts(4000, 4096)
    total = sum(len(row.raw_text) for row in rows)
    assert total > 12 * chunk
    for start in (0, 1):
        mine = rows[start::2]
        asked = [r.accession for r in mine]
        raw.save(tmp_path, raw.SRA_XML, iter(mine), asked=asked, chunk_bytes=chunk)
    del rows, mine

    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    previous = pa.default_memory_pool()
    pa.set_memory_pool(pool)
    tracemalloc.start()
    try:
        [path] = raw.pack(tmp_path, [raw.SRA_XML], chunk_bytes=chunk)
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
    before = {source: list(raw_rows(root, source)) for source in (raw.GEO_SOFT, raw.SRA_XML)}
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
    assert {s: list(raw_rows(root, s)) for s in before} == before
    assert "10,000" in (soft / "README.md").read_text()


def _layered(root: Path) -> Path:
    """A packed bucket, then an older part, a removal part and a newer part over it.

    Each part's fetch also asked for an accession it got no row for: the older one's is
    removed after it, the newer one's, GSE10006, is not.
    """
    old = [raw.RawRow(f"GSE1000{n}", f"{c}0", "d0") for n, c in ((1, "a"), (2, "b"), (3, "c"))]
    write_rows(root, raw.GEO_SOFT, old)
    older = [raw.RawRow("GSE10001", "a1", "d1"), raw.RawRow("GSE10004", "d1", "d1")]
    raw.save(root, raw.GEO_SOFT, older, asked=["GSE10001", "GSE10002", "GSE10004"])
    raw.remove(root, raw.GEO_SOFT, ["GSE10002", "GSE10004", "GSE10005"])
    newer = [raw.RawRow("GSE10001", "a2", "d2"), raw.RawRow("GSE10005", "e2", "d2")]
    raw.save(root, raw.GEO_SOFT, newer, asked=["GSE10001", "GSE10005", "GSE10006"])
    return root


#: What :func:`_layered` holds: each held accession's newest text and fetch date.
_NEWEST = {"GSE10001": ("a2", "d2"), "GSE10003": ("c0", "d0"), "GSE10005": ("e2", "d2")}
_WANTED = [f"GSE1000{n}" for n in range(1, 7)]


def test_every_read_before_pack_sees_what_pack_then_writes(tmp_path: Path) -> None:
    root, source = _layered(tmp_path), raw.GEO_SOFT
    assert raw.held(root, source) == raw.held(root, source, _WANTED) == set(_NEWEST)
    assert raw.held(root, source, _WANTED, since_pack=True) == {"GSE10001", "GSE10005"}
    assert dict(raw.saved(root, source, _WANTED)) == {a: text for a, (text, _) in _NEWEST.items()}
    dates = dict(raw.saved(root, source, _WANTED, column="fetch_date"))
    assert dates == {a: d for a, (_, d) in _NEWEST.items()}
    assert dict(raw.saved(root, source, since_pack=True)) == {"GSE10001": "a2", "GSE10005": "e2"}
    before = [(row.accession, row.raw_text, row.fetch_date) for row in raw_rows(root, source)]
    assert before == [(a, *row) for a, row in _NEWEST.items()]
    [merged] = raw.files(root, source, tmp_path / "merged")
    assert _triples(merged) == before

    raw.pack(root, [source])
    [packed] = raw.files(root, source)
    assert _triples(packed) == before


def _triples(path: Path) -> list[tuple[str, str, str]]:
    """A Parquet file's rows as they are on disk: accession, text, date."""
    rows = pq.read_table(path).to_pylist()
    return [(r["accession"], r["raw_text"], r["fetch_date"]) for r in rows]


def test_fetch_planning_skips_exactly_the_held_accessions(tmp_path: Path) -> None:
    root, source = _layered(tmp_path), raw.GEO_SOFT

    # GSE10006 is not held, but a fetch since its last removal asked for it and got no row.
    asked = {"GSE10006"}
    todo = {a for piece in raw.pieces(root, source, _WANTED) for a in piece.todo}
    assert todo == set(_WANTED) - raw.held(root, source, _WANTED) - asked
    again = {a for piece in raw.pieces(root, source, (), _WANTED) for a in piece.todo}
    assert again == set(_WANTED) - raw.held(root, source, _WANTED, since_pack=True) - asked


def _counted_store(root: Path) -> Path:
    """A packed bucket, then parts that replace, keep, add and remove rows in two buckets."""
    old = [raw.RawRow(f"GSE{n}", "old", "d0") for n in (10001, 10002, 10003, 20001)]
    write_rows(root, raw.GEO_SOFT, old)
    new = [raw.RawRow("GSE10001", "old", "d1"), raw.RawRow("GSE10002", "new", "d1")]
    raw.save(root, raw.GEO_SOFT, [*new, raw.RawRow("GSE20002", "added", "d1")])
    raw.remove(root, raw.GEO_SOFT, ["GSE10003"])
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
        raw.pack(tmp_path / "killed", counts=counts)
    else:
        # A pack killed after its renames: the buckets are new, the parts are still there.
        parts = cut / "work" / "parts"
        shutil.copytree(parts, tmp_path / "parts", copy_function=shutil.copy2)
        raw.pack(cut, counts=counts)
        shutil.copytree(tmp_path / "parts", parts, dirs_exist_ok=True, copy_function=shutil.copy2)
    raw.pack(cut, counts=counts)
    raw.pack(whole, counts=whole / "counts.jsonl")

    expected = {raw.GEO_SOFT: {"added": 1, "replaced": 1, "removed": 1}}
    assert lifecycle.read_counts(whole / "counts.jsonl") == expected
    assert lifecycle.read_counts(counts) == expected
    assert _raw(cut) == _raw(whole)


def test_a_build_by_hand_refuses_while_unpacked_parts_remain(tmp_path: Path) -> None:
    root = tmp_path / "store"
    root.mkdir()
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    raw.save(root, raw.GEO_SOFT, [raw.RawRow("GSE1", "soft", TODAY)])

    result = CliRunner().invoke(app, ["store", "build", "--root", str(root)])

    assert result.exit_code != 0
    assert "labdata store pack" in str(result.exception)
    assert not (root / "builds").exists()


def test_the_raw_module_alone_reads_raw_files_through_a_few_verbs() -> None:
    public = [
        name
        for name, member in vars(raw).items()
        if inspect.isfunction(member) and member.__module__ == raw.__name__
        if not name.startswith("_")
    ]
    assert len(public) <= 8, public
    # Every other module naming raw/ handles it as a folder, never reading its rows.
    folder_only = {
        "paths.py": "declares it",
        "gcs.py": "copies its files whole",
        "provenance.py": "documents each folder, reading only the files raw.files gives",
        "cli.py": "prints where it is",
        "build.py": "refuses a store as a scratch root, and has each folder documented",
        "fetch.py": "has each folder documented",
    }
    src = Path(raw.__file__).parents[1]
    naming = {
        path.name
        for path in src.rglob("*.py")
        if path.name != "raw.py" and re.search(r"paths\.(RAW|PARTS)\b", path.read_text())
    }
    assert naming == set(folder_only)
    assert not any(
        re.search(r"\bPARTS\b", p.read_text())
        for p in src.rglob("*.py")
        if p.name not in ("raw.py", "paths.py")
    )
