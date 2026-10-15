"""The checks every thin-slice record passes, run over all 11 records' fixture stores.

Each fixture in ``tests._slices`` is fetched and built once; every record each build holds
is checked.
"""

from __future__ import annotations

import gzip
import hashlib
import shutil
import socket
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from labdata.exceptions import AccessionError, NotFound
from labdata.store import Store, open_store
from labdata.store.build import build
from labdata.store.model import Record
from tests._slices import SLICES, TODAY, built_root

#: An accession of each record type that no fixture snapshot lists.
_UNKNOWN = [
    "GSE999999999",
    "GSM999999999",
    "GPL999999999",
    "SRP999999999",
    "SRX999999999",
    "SRR999999999",
    "SRS999999999",
    "PRJNA999999999",
    "SAMN999999999",
]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module", params=sorted(SLICES))
def built(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return built_root(tmp_path_factory.mktemp(request.param) / "store", request.param)


@pytest.fixture
def store(built: Path) -> Store:
    return open_store(built)


def _held(store: Store) -> list[str]:
    path = store.root / "builds" / str(store.settings["build"]) / "holdings.parquet"
    return pq.read_table(path).column("accession").to_pylist()


def _sources(root: Path) -> list[str]:
    """Every text a record's raw text is cut from: the rows in ``raw/`` and the snapshots."""
    texts = [
        str(text)
        for path in sorted((root / "raw").rglob("*.parquet"))
        for text in pq.read_table(path).column("raw_text").to_pylist()
    ]
    for path in sorted((root / "snapshots").rglob("*")):
        if path.suffix == ".xml":
            texts.append(path.read_text(encoding="utf-8"))
        elif path.name.endswith(".xml.gz"):
            texts.append(gzip.decompress(path.read_bytes()).decode())
    return texts


def test_every_held_record_opens(store: Store) -> None:
    held = _held(store)
    opened = store.open_many(held)
    assert all(isinstance(r, Record) for r in opened.values()), [
        a for a, r in opened.items() if not isinstance(r, Record)
    ]


def test_every_content_hash_recomputes_from_raw_or_a_snapshot(store: Store) -> None:
    held = _held(store)
    raws = store.raw(held)
    opened = store.open_many(held)
    sources = _sources(store.root)
    for accession in held:
        text = raws[accession].text
        digest = hashlib.sha256(text.encode()).hexdigest()
        record = opened[accession]
        assert isinstance(record, Record)
        assert digest == raws[accession].content_hash == record.content_hash, accession
        assert any(text in source for source in sources), accession


def test_an_offline_rebuild_gives_the_same_tables(
    built: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("network blocked")

    root = tmp_path / "copy"
    shutil.copytree(built, root)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    build(root, "offline")
    first, again = root / "builds" / "b1", root / "builds" / "offline"
    tables = sorted(p.relative_to(first) for p in first.rglob("*.parquet"))
    assert tables == sorted(p.relative_to(again) for p in again.rglob("*.parquet"))
    for name in tables:
        assert pq.read_table(first / name).equals(pq.read_table(again / name)), name


@pytest.mark.parametrize("accession", ["GSE", "SRX12a", "PRJ1", "FOO1", ""])
def test_a_malformed_accession_raises_accession_error(store: Store, accession: str) -> None:
    with pytest.raises(AccessionError):
        store.open(accession)


def test_an_accession_no_snapshot_lists_raises_not_found_with_its_date(store: Store) -> None:
    for accession in _UNKNOWN:
        with pytest.raises(NotFound, match=f"{accession}.*{TODAY}"):
            store.open(accession)
