"""A subset: a store cut down to a list of Series, read back against its source."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from labdata.cli import app
from labdata.exceptions import NotMirrored
from labdata.store import layout, open_store
from labdata.store.provenance import verify
from tests._slices import BuiltStore

#: Two Series of the geo_slice. GSE246308 shares its paper, and its BioProject's parent, with
#: GSE246307, GSE246309 and their SuperSeries GSE246310; GSE233931 shares its paper with
#: GSE233685 and its platform with GSE153830.
LISTED = ["GSE233931", "GSE246308"]
UNLISTED = ["GSE246307", "GSE246309", "GSE246310", "GSE233685", "GSE153830"]
SHARED_PAPER = "PMID:40167329"
UMBRELLA = "PRJNA1032473"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def tree(root: Path) -> dict[str, tuple[int, str]]:
    """Every file under ``root`` with its size and sha256."""
    return {
        p.relative_to(root).as_posix(): (
            p.stat().st_size,
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def cut(work: Path, source: Path, series: list[str], root: Path, *more: str) -> tuple[int, str]:
    """Run ``labdata store subset`` with a list file in ``work``; return its exit code and output."""
    listing = work / "series.txt"
    listing.write_text("".join(f"{a}\n" for a in series), encoding="utf-8")
    result = CliRunner().invoke(
        app,
        [
            "store",
            "subset",
            "--from",
            f"{source}",
            "--series",
            f"{listing}",
            "--root",
            f"{root}",
            *more,
        ],
    )
    return result.exit_code, result.output


@dataclass(frozen=True)
class Cut:
    source: Path
    root: Path
    output: str
    #: The source tree before the cut and after it.
    before: dict[str, tuple[int, str]]
    after: dict[str, tuple[int, str]]

    @property
    def build(self) -> Path:
        return self.root / "builds" / "b1-subset"


@pytest.fixture(scope="module")
def made(built_store: BuiltStore, tmp_path_factory: pytest.TempPathFactory) -> Cut:
    """The geo_slice store, built as ``b1``, cut to ``LISTED``."""
    folder = tmp_path_factory.mktemp("subset")
    source = built_store(folder / "store", "geo_slice")
    before = tree(source)
    code, output = cut(folder, source, LISTED, folder / "small")
    assert code == 0, output
    return Cut(source, folder / "small", output, before, tree(source))


def rows(build: Path) -> dict[str, list[dict[str, object]]]:
    """Every table of a build, by file, as rows."""
    return {
        p.relative_to(build).as_posix(): pq.read_table(p).to_pylist()
        for p in sorted(build.rglob("*.parquet"))
    }


def test_the_subset_answers_as_its_source_for_the_listed_series_and_what_they_reach(
    made: Cut,
) -> None:
    small, full = open_store(made.root), open_store(made.source)

    assert small.reader.build_id == "b1-subset"
    assert layout.check(made.build) == []
    assert verify(made.root) == []
    assert made.after == made.before
    assert small.links(LISTED) == full.links(LISTED)
    assert small.raw(LISTED) == full.raw(LISTED)
    assert small.supplementary_files(LISTED) == full.supplementary_files(LISTED)
    for gse in LISTED:
        assert small.open(gse) == full.open(gse)
        assert small.experiments_for(gse) == full.experiments_for(gse)
        assert small.experiments_for(gse), gse
    held = pq.read_table(layout.HOLDINGS.path(made.build)).column("accession").to_pylist()
    assert small.open_many(held) == full.open_many(held)
    # The walk went the whole way down, and to each end's platform and project.
    kinds = {record.type for record in small.open_many(held).values()}  # type: ignore[union-attr]
    assert kinds >= {"GEO Sample", "GEO Platform", "SRA experiment", "SRA run", "BioProject"}


def test_a_shared_paper_and_a_shared_parent_project_add_no_unlisted_series(made: Cut) -> None:
    small, full = open_store(made.root), open_store(made.source)
    table = pq.read_table(layout.HOLDINGS.path(made.build)).to_pylist()

    assert [r["accession"] for r in table if r["type"] == "GEO Series"] == LISTED
    # The paper keeps every Series that links it, as rows; so does the parent BioProject.
    assert small.links(SHARED_PAPER) == full.links(SHARED_PAPER)
    assert {"GSE246307", "GSE246309", "GSE246310"} <= set(
        small.follow(SHARED_PAPER, "PubMed of")[SHARED_PAPER]
    )
    assert small.open(UMBRELLA) == full.open(UMBRELLA)
    # The parent is held as a record only: its other children are not.
    siblings = set(full.follow(UMBRELLA, "umbrella of")[UMBRELLA]) - {"PRJNA1032483"}
    assert siblings
    assert small.reader.held(siblings) == set()
    # An unlisted Series reads as listed but not held, not as unknown.
    for gse in UNLISTED:
        with pytest.raises(NotMirrored):
            small.open(gse)
    assert "GSE246308 SubSeries of GSE246310" in made.output


def test_the_new_root_says_where_it_came_from_and_names_no_path(made: Cut) -> None:
    marker = open_store(made.root).settings
    manifest = json.loads((made.build / "manifest.json").read_text(encoding="utf-8"))
    source = json.loads(
        (made.source / "builds" / "b1" / "manifest.json").read_text(encoding="utf-8")
    )

    assert marker["subset_of"] == "b1"
    assert "copy_to" not in marker
    assert manifest["subset"]["of"] == "b1"
    assert manifest["subset"]["series"] == LISTED
    assert manifest["subset"]["source_commit"] == source["produced_by"]["commit"]
    assert manifest["subset"]["snapshot_dates"] == {
        name: snap["date"] for name, snap in source["inputs"]["snapshots"].items()
    }
    assert sorted(p.name for p in made.root.iterdir()) == [
        "README.md",
        "builds",
        "labdata-store.toml",
    ]
    # The small store is copied between machines: no file names where it was made.
    for path in (p for p in made.root.rglob("*") if p.is_file()):
        data = path.read_bytes()
        assert str(made.source).encode() not in data, path
        assert str(made.root.parent).encode() not in data, path


def test_the_command_prints_the_records_by_type_and_the_bytes_written(made: Cut) -> None:
    held = pq.read_table(layout.HOLDINGS.path(made.build)).to_pylist()
    for kind in {r["type"] for r in held}:
        assert f"{kind}: {sum(r['type'] == kind for r in held)}\n" in made.output
    written = sum(p.stat().st_size for p in made.root.rglob("*") if p.is_file())
    assert f"Wrote {written} bytes" in made.output


def test_a_second_cut_gives_the_same_rows_and_a_rerun_replaces_the_build(
    made: Cut, tmp_path: Path
) -> None:
    code, output = cut(tmp_path, made.source, list(reversed(LISTED)), tmp_path / "again")
    assert code == 0, output
    assert rows(tmp_path / "again" / "builds" / "b1-subset") == rows(made.build)

    # The same root takes the cut again, and a shorter list leaves nothing of the longer one.
    code, output = cut(tmp_path, made.source, LISTED[:1], tmp_path / "again")
    assert code == 0, output
    small = open_store(tmp_path / "again")
    assert small.reader.held(LISTED) == {LISTED[0]}
    assert layout.check(tmp_path / "again" / "builds" / "b1-subset") == []


def test_a_record_type_with_no_rows_has_no_file_and_every_other_table_is_there(
    made: Cut, tmp_path: Path
) -> None:
    # GSE15556 links no SRA experiment, and the source lacks the BioProject it links.
    code, output = cut(
        tmp_path, made.source, ["GSE15556"], tmp_path / "small", "--build-id", "arrays"
    )
    assert code == 0, output
    build = tmp_path / "small" / "builds" / "arrays"

    assert sorted(p.stem for p in (build / "records").iterdir()) == [
        "geo-platform",
        "geo-sample",
        "geo-series",
    ]
    assert all(t.path(build).exists() for t in layout.TABLES)
    assert layout.check(build) == []
    assert open_store(tmp_path / "small").settings["build"] == "arrays"


def test_a_listed_series_the_source_lacks_keeps_its_gap_row(
    built_store: BuiltStore, tmp_path: Path
) -> None:
    source = built_store(tmp_path / "store", "prjna1027859")
    full = source / "builds" / "b1"

    code, output = cut(tmp_path, source, ["GSE19970"], tmp_path / "small")

    assert code == 0, output
    assert "listed, not held by the source: GSE19970" in output
    build = tmp_path / "small" / "builds" / "b1-subset"
    assert layout.check(build) == []
    assert pq.read_table(build / "gaps.parquet").to_pylist() == [
        {"accession": "GSE19970", "reason": "unexplained", "detail": None}
    ]
    for name in ("gaps.parquet", "known.parquet"):
        kept, was = pq.read_metadata(build / name).metadata, pq.read_metadata(full / name).metadata
        keys = [k for k in was if not k.startswith(b"ARROW:")]
        assert keys
        assert {k: kept[k] for k in keys} == {k: was[k] for k in keys}
    with pytest.raises(NotMirrored):
        open_store(tmp_path / "small").open("GSE19970")


@pytest.mark.parametrize(
    ("root", "series", "message"),
    [
        ("source", LISTED, "apart from its source"),
        ("source/builds/inside", LISTED, "apart from its source"),
        ("taken", LISTED, "not empty"),
        ("other", LISTED, "is a subset of b0, not a subset of b1"),
        ("store", LISTED, "is not a subset"),
        ("new", ["GSM7437494"], "not a GEO Series accession"),
        ("new", [], "the Series list is empty"),
    ],
)
def test_a_root_that_is_not_a_fresh_or_matching_subset_is_refused_and_left_alone(
    made: Cut, tmp_path: Path, root: str, series: list[str], message: str
) -> None:
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "notes.txt").write_text("mine", encoding="utf-8")
    for name, marker in (("other", 'subset_of = "b0"\n'), ("store", "")):
        (tmp_path / name).mkdir()
        (tmp_path / name / "labdata-store.toml").write_text(
            f"format_version = 1\n{marker}", encoding="utf-8"
        )
    target = (
        made.source / root.removeprefix("source/") if root.startswith("source") else tmp_path / root
    )
    (tmp_path / "series.txt").touch()
    before = tree(tmp_path)

    code, output = cut(tmp_path, made.source, series, target)

    assert code == 1
    assert message in output
    assert tree(made.source) == made.before
    assert {k: v for k, v in tree(tmp_path).items() if k != "series.txt"} == {
        k: v for k, v in before.items() if k != "series.txt"
    }
