"""Tell "not held" from "does not exist": ``known.parquet``, ``NotFound`` and ``partial=True``.

The store holds PRJNA1027859 (``tests/data/prjna1027859/``). GSE19970 is only in the
geo-series snapshot, so it is known but not held.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import AccessionError, NotFound, NotMirrored
from labdata.store import Partial, Store, known, open_store
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient

_DATA = Path(__file__).parent / "data"
_SNAPSHOTS = _DATA / "prjna1027859" / "snapshots"
_PROJECT = "PRJNA1027859"
_SRX = [f"SRX2428313{i}" for i in range(6)]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "store"
    shutil.copytree(_SNAPSHOTS, root / "snapshots")
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _store(tmp_path: Path, accessions: list[str]) -> Store:
    root = _root(tmp_path)
    client = FakeEntrezClient(
        esearch={f"{_PROJECT}[si]": ["39637862"]},
        packages={srx: (_DATA / "sra_package" / f"{srx}.xml").read_text() for srx in _SRX},
    )
    fetch(root, accessions, client=client, today="2026-10-14")
    build(root, "b1")
    return open_store(root)


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return _store(tmp_path, [_PROJECT])


# ---------------------------------------------------------------- open


def test_an_accession_in_no_snapshot_raises_not_found_with_the_date(store: Store) -> None:
    with pytest.raises(NotFound, match=r"GSE99999999.*2026-10-14"):
        store.open("GSE99999999")


def test_a_known_accession_not_held_raises_not_mirrored_listing_it(store: Store) -> None:
    with pytest.raises(NotMirrored, match="GSE19970"):
        store.open("GSE19970")


def test_open_many_sorts_absent_accessions_with_one_known_lookup(store: Store) -> None:
    result = store.open_many(["GSE19970", "GSM484820", "SRX99999999", _PROJECT])
    assert isinstance(result["GSE19970"], NotMirrored)
    assert isinstance(result["GSM484820"], NotMirrored)
    assert isinstance(result["SRX99999999"], NotFound)
    assert "sra-accessions snapshot of 2026-10-14" in str(result["SRX99999999"])
    assert store.backend.calls["known_many"] == 1


def test_a_malformed_accession_raises_accession_error(store: Store) -> None:
    with pytest.raises(AccessionError):
        store.open("GSE19970X")
    with pytest.raises(AccessionError):
        store.experiments_for("not-an-accession")


def test_known_lists_every_snapshot_with_its_date(store: Store) -> None:
    rows = pq.read_table(store.root / "builds" / "b1" / "known.parquet").to_pylist()
    accessions = [r["accession"] for r in rows]
    assert accessions == sorted(accessions)
    by_acc = {r["accession"]: (r["source"], r["snapshot_date"]) for r in rows}
    assert by_acc["GSE19970"] == ("geo-series", "2026-10-14")
    assert by_acc["GSM484820"] == ("geo-series", "2026-10-14")
    assert by_acc["GPL9126"] == ("geo-series", "2026-10-14")
    assert by_acc["DRR000001"] == ("sra-accessions", "2026-10-14")
    assert by_acc["PRJNA3"] == ("bioproject", "2026-10-14")
    assert by_acc["SAMN00000002"] == ("biosample", "2026-10-14")
    assert "Accession" not in by_acc


# ---------------------------------------------------------------- walks


def test_a_walk_root_in_no_snapshot_raises_not_found(store: Store) -> None:
    with pytest.raises(NotFound, match="2026-10-14"):
        store.experiments_for("PRJNA999999999", partial=True)


def test_a_walk_raises_not_mirrored_listing_every_missing_accession(store: Store) -> None:
    with pytest.raises(NotMirrored, match="GSE19970, GSM484820"):
        store.experiments_for_many([_PROJECT, "GSM484820", "GSE19970"])


def test_partial_returns_the_held_part_with_the_missing_list(store: Store) -> None:
    walk = store.experiments_for_many([_PROJECT, "GSM484820", "GSE19970"], partial=True)
    assert isinstance(walk, Partial)
    assert [e.accession for e in walk.held[_PROJECT]] == _SRX
    assert walk.held["GSE19970"] == []
    assert walk.missing == ("GSE19970", "GSM484820")
    one = store.experiments_for("GSE19970", partial=True)
    assert one == Partial([], ("GSE19970",))


def test_partial_has_an_empty_missing_list_when_all_is_held(store: Store) -> None:
    walk = store.experiments_for(_PROJECT, partial=True)
    assert [e.accession for e in walk.held] == _SRX
    assert walk.missing == ()


# ---------------------------------------------------------------- resumable indexing


def test_a_rerun_skips_a_source_already_indexed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    out = known.index_source(root, "geo-series")
    assert out == root / "work" / "known" / "geo-series-2026-10-14.parquet"

    def fail(*args: object) -> None:
        raise AssertionError("the snapshot was read again")

    monkeypatch.setattr(known.snapshots, "accessions", fail)
    assert known.index_source(root, "geo-series") == out


def test_a_killed_index_leaves_no_file_and_a_rerun_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    real = known.snapshots.accessions

    def killed(snap: object, source: str) -> Iterator[str]:
        yield "SRX1"
        raise KeyboardInterrupt

    monkeypatch.setattr(known.snapshots, "accessions", killed)
    monkeypatch.setattr(known, "_BATCH", 1)
    with pytest.raises(KeyboardInterrupt):
        known.index_source(root, "sra-accessions")
    assert list((root / "work" / "known").iterdir()) == []
    monkeypatch.setattr(known.snapshots, "accessions", real)
    out = known.index_source(root, "sra-accessions")
    assert "SRX24283130" in pq.read_table(out).column("accession").to_pylist()


def test_the_index_known_command_does_one_source(tmp_path: Path) -> None:
    root = _root(tmp_path)
    runner = CliRunner()
    done = runner.invoke(
        app, ["store", "index-known", "--source", "bioproject", "--root", str(root)]
    )
    assert done.exit_code == 0, done.output
    files = [p.name for p in (root / "work" / "known").iterdir()]
    assert files == ["bioproject-2026-10-14.parquet"]
    bad = runner.invoke(app, ["store", "index-known", "--source", "nope", "--root", str(root)])
    assert bad.exit_code != 0
