"""Tell "not held" from "does not exist": ``known.parquet``, ``NotFound`` and ``partial=True``.

The store holds PRJNA1027859 (``tests/data/prjna1027859/``). GSE19970 is only in the
geo-series snapshot, so it is known but not held.
"""

from __future__ import annotations

import datetime as dt
import gzip
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from labdata.exceptions import AccessionError, NotFound, NotMirrored, StoreBuildError
from labdata.store import Partial, Store, extract, known, open_store
from labdata.store.build import build
from labdata.store.fetch import fetch
from tests._fakes import FakeEntrezClient
from tests._slices import copy_snapshots

_DATA = Path(__file__).parent / "data"
_SNAPSHOTS = _DATA / "prjna1027859" / "snapshots"
_PROJECT = "PRJNA1027859"
_SRX = [f"SRX2428313{i}" for i in range(6)]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


def _root(tmp_path: Path, snapshots: Path = _SNAPSHOTS) -> Path:
    root = tmp_path / "store"
    copy_snapshots(snapshots, root)
    (root / "labdata-store.toml").write_text("format_version = 1\n", encoding="utf-8")
    return root


def _store(tmp_path: Path, accessions: list[str], snapshots: Path = _SNAPSHOTS) -> Store:
    root = _root(tmp_path, snapshots)
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
    assert store.reader.calls["known"] == 1


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
    assert by_acc["GSE19970"] == ("geo-series", dt.date(2026, 10, 14))
    assert by_acc["GSM484820"] == ("geo-series", dt.date(2026, 10, 14))
    assert by_acc["GPL9126"] == ("geo-series", dt.date(2026, 10, 14))
    assert by_acc["DRR000001"] == ("sra-accessions", dt.date(2026, 10, 14))
    assert by_acc["PRJNA3"] == ("bioproject", dt.date(2026, 10, 14))
    assert by_acc["SAMN00000002"] == ("biosample", dt.date(2026, 10, 14))
    assert "Accession" not in by_acc


def _with_gone_records(tmp_path: Path) -> Path:
    """Copy the fixture snapshots, adding a suppressed experiment and a withdrawn BioSample.

    A withdrawn Series is simply absent from the geo-series snapshot.
    """
    out = tmp_path / "snapshots"
    shutil.copytree(_SNAPSHOTS, out)
    tab = out / "sra-accessions" / "2026-10-14" / "SRA_Accessions.tab"
    day = "2024-05-28T07:01:47Z"
    gone = f"SRA1\tsuppressed\t{day}\t{day}\t{day}\tEXPERIMENT\tX\tpublic\tSRX99000001"
    with tab.open("a", encoding="utf-8") as fh:
        fh.write(f"SRX99000001\t{gone}\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\n")
    biosample = out / "biosample" / "2026-10-14" / "biosample_set.xml.gz"
    text = gzip.decompress(biosample.read_bytes()).decode()
    withdrawn = (
        '<BioSample access="public" id="99000001" accession="SAMN99000001"><Ids>'
        '<Id db="BioSample" is_primary="1">SAMN99000001</Id></Ids>'
        '<Status status="suppressed" when="2025-01-01T00:00:00.000"/></BioSample>\n'
    )
    text = text.replace("</BioSampleSet>", withdrawn + "</BioSampleSet>")
    biosample.write_bytes(gzip.compress(text.encode(), mtime=0))
    return out


def test_withdrawn_and_suppressed_records_raise_not_found(tmp_path: Path) -> None:
    store = _store(tmp_path, [_PROJECT], _with_gone_records(tmp_path))
    result = store.open_many(["SRX99000001", "SAMN99000001", "GSE99000001"])
    assert {a: type(e) for a, e in result.items()} == dict.fromkeys(result, NotFound)
    public = store.open_many(["DRX000001", "SAMN00000002", "PRJNA3", "GSE19970"])
    assert {a: type(e) for a, e in public.items()} == dict.fromkeys(public, NotMirrored)


def test_a_biosample_extract_without_status_asks_for_a_new_extract(tmp_path: Path) -> None:
    root = _root(tmp_path)
    folder, _ = extract.done_dir(root, "biosample")
    for part in folder.glob("part-*.parquet"):
        pq.write_table(pq.read_table(part).drop_columns("status"), part)
    with pytest.raises(StoreBuildError, match="labdata store extract --source biosample"):
        known.write(root, tmp_path / "known.parquet")


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
