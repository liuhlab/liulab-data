"""A sample build: a seeded fraction of a full store, built into a scratch root."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import StoreBuildError
from labdata.store import _parse, extract, layout, open_store
from labdata.store.build import build
from labdata.store.targets import in_sample
from tests._slices import built_root

_BENCHMARK = Path(__file__).parents[1] / "scripts" / "store" / "benchmark.py"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture(scope="module")
def source(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The geo_slice store, fetched and built once as ``b1``."""
    return built_root(tmp_path_factory.mktemp("source") / "store", "geo_slice")


def _tree(root: Path) -> dict[str, tuple[int, str]]:
    """Every file under ``root`` with its size and sha256."""
    return {
        p.relative_to(root).as_posix(): (
            p.stat().st_size,
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _accessions(build_dir: Path, stem: str) -> set[str]:
    path = build_dir / "records" / f"{stem}.parquet"
    if not path.is_file():
        return set()
    return set(pq.read_table(path, columns=["accession"]).column(0).to_pylist())


def _keys(build_dir: Path) -> dict[str, set[object]]:
    """Each table's row keys: accessions, or whole rows for links."""
    out: dict[str, set[object]] = {}
    for path in sorted(build_dir.rglob("*.parquet")):
        name = path.relative_to(build_dir).as_posix()
        rows = pq.read_table(path).to_pylist()
        if name.startswith("links_by"):
            out[name] = {tuple(r.values()) for r in rows}
        elif name == "supplementary_files.parquet":
            out[name] = {(r["owner"], r["name"]) for r in rows}
        elif name != "notes.parquet":
            out[name] = {r["accession"] for r in rows}
    return out


def test_a_sample_holds_the_sampled_records_of_each_type_and_leaves_the_source_alone(
    source: Path, tmp_path: Path
) -> None:
    before = _tree(source)
    full = source / "builds" / "b1"

    out = build(tmp_path / "scratch", "s1", fraction=0.5, sample_of=source)

    assert out == tmp_path / "scratch" / "builds" / "s1"
    assert _tree(source) == before
    assert open_store(source).settings["build"] == "b1"
    assert layout.check(out) == []
    # A Series and an SRA package are raw rows: held exactly when sampled.
    for stem in ("geo-series", "sra-experiment", "bioproject"):
        whole = _accessions(full, stem)
        got = _accessions(out, stem)
        assert {a for a in whole if in_sample(a, 0.5)} <= got < whole, stem
        if stem != "bioproject":
            assert got == {a for a in whole if in_sample(a, 0.5)}, stem
    # Everything parsed from a sampled Series is kept: all its Samples.
    store, whole = open_store(tmp_path / "scratch"), open_store(source)
    for gse in _accessions(out, "geo-series"):
        assert store.follow(gse, "sample") == whole.follow(gse, "sample")
    held = set(pq.read_table(out / "holdings.parquet").column("accession").to_pylist())
    links = {tuple(r.values()) for r in pq.read_table(out / "links_by_from.parquet").to_pylist()}
    whole_links = {
        tuple(r.values()) for r in pq.read_table(full / "links_by_from.parquet").to_pylist()
    }
    assert links < whole_links
    assert any(dst not in held for _, dst, *_ in links)
    gaps = set(pq.read_table(out / "gaps.parquet").column("accession").to_pylist())
    assert all(in_sample(a, 0.5) for a in gaps)
    known = set(pq.read_table(out / "known.parquet").column("accession").to_pylist())
    whole_known = set(pq.read_table(full / "known.parquet").column("accession").to_pylist())
    assert known < whole_known
    assert all(in_sample(a, 0.5) or a in held for a in known)
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["sample"] == {"of": str(source.absolute()), "fraction": 0.5}


def test_a_sample_holds_the_sampled_snapshot_records_that_nothing_links_to(
    tmp_path: Path,
) -> None:
    source = built_root(tmp_path / "source", "prjna1027859")
    full = _accessions(source / "builds" / "b1", "biosample")
    folder, _ = extract.done_dir(source, "biosample")
    listed = {
        a
        for part in folder.glob("part-*.parquet")
        for a in pq.read_table(part, columns=["accession"]).column(0).to_pylist()
    }
    own = {a for a in listed if in_sample(a, 0.6)}

    out = build(tmp_path / "scratch", "s1", fraction=0.6, sample_of=source)

    got = _accessions(out, "biosample")
    assert own - full, "the snapshot lists a sampled BioSample nothing links to"
    assert own <= got < full | listed


def test_the_same_fraction_gives_the_same_rows_and_a_smaller_one_lies_inside(
    source: Path, tmp_path: Path
) -> None:
    small = build(tmp_path / "a", "s", fraction=0.3, sample_of=source)
    again = build(tmp_path / "b", "s", fraction=0.3, sample_of=source)
    large = build(tmp_path / "c", "s", fraction=0.6, sample_of=source)

    for name in ("links_by_from.parquet", "holdings.parquet", "known.parquet", "notes.parquet"):
        assert pq.read_table(small / name).equals(pq.read_table(again / name)), name
    small_keys, large_keys = _keys(small), _keys(large)
    assert set(small_keys) <= set(large_keys)
    for name, keys in small_keys.items():
        assert keys <= large_keys[name], name
    assert small_keys["holdings.parquet"] < large_keys["holdings.parquet"]


def test_the_parser_never_sees_a_raw_row_outside_the_sample(
    source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[str, str]] = []
    parse = _parse._parse

    def recording(parts: object, kind: str, accession: str, text: str, date: str) -> None:
        seen.append((kind, accession))
        parse(parts, kind, accession, text, date)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(_parse, "_parse", recording)
    build(tmp_path / "scratch", "s1", fraction=0.5, sample_of=source, workers=1)

    raw_rows = [(kind, accession) for kind, accession in seen if kind in _parse.RAW_KINDS]
    assert raw_rows
    assert all(in_sample(accession, 0.5) for _, accession in raw_rows)
    saved = {
        a
        for kind in _parse.RAW_KINDS
        for path in (source / "raw" / kind).glob("*.parquet")
        for a in pq.read_table(path, columns=["accession"]).column(0).to_pylist()
    }
    assert any(not in_sample(a, 0.5) for a in saved)


def test_a_sample_refuses_its_own_source_as_scratch_root(source: Path) -> None:
    with pytest.raises(StoreBuildError, match="scratch"):
        build(source, "s1", fraction=0.5, sample_of=source)


def test_a_scratch_root_takes_samples_of_one_store_only(source: Path, tmp_path: Path) -> None:
    scratch = tmp_path / "scratch"
    build(scratch, "s1", fraction=0.3, sample_of=source)
    build(scratch, "s2", fraction=0.6, sample_of=source)
    assert open_store(scratch).settings["sample_of"] == str(source.absolute())
    other = built_root(tmp_path / "other", "sra_slice")

    with pytest.raises(StoreBuildError, match=r"holds samples of .*pick another scratch root"):
        build(scratch, "s3", fraction=0.3, sample_of=other)
    built = tmp_path / "built"
    built.mkdir()
    (built / "labdata-store.toml").write_text('format_version = 1\nbuild = "b1"\n')
    with pytest.raises(StoreBuildError, match="not a sample"):
        build(built, "s1", fraction=0.3, sample_of=source)
    assert open_store(scratch).settings["build"] == "s2"


def test_the_cli_builds_a_sample_and_the_benchmark_runs_on_it(source: Path, tmp_path: Path) -> None:
    scratch = tmp_path / "scratch"
    result = CliRunner().invoke(
        app,
        [
            "store",
            "build",
            "--root",
            str(scratch),
            "--sample-of",
            str(source),
            "--fraction",
            "0.5",
            "--build-id",
            "s1",
            "--workers",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert open_store(scratch).settings["build"] == "s1"

    no_limits = tmp_path / "limits.json"
    no_limits.write_text("{}")
    out = tmp_path / "result.json"
    done = subprocess.run(
        [
            sys.executable,
            str(_BENCHMARK),
            str(scratch),
            "--sizes",
            "1,2",
            "--limits",
            str(no_limits),
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )
    assert done.returncode == 0, done.stderr
    sample = json.loads(out.read_text())["sample"]
    assert sample["fraction"] == 0.5
    assert 0 < sample["links_to_held"] < 1
    assert "indicative only" in done.stdout


def test_the_cli_needs_an_explicit_scratch_root_for_a_sample(source: Path) -> None:
    result = CliRunner().invoke(
        app, ["store", "build", "--sample-of", str(source), "--fraction", "0.5"]
    )
    assert result.exit_code != 0
    assert "--root" in result.output
