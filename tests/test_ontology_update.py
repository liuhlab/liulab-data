"""The ontology update: one OBO vocabulary from the web into the store, through a fake web."""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import _slurm
from labdata.cli import app
from labdata.ontology import Reason, Term, lifecycle, open_ontology_store, paths, tables
from labdata.ontology import fetch as fetch_mod
from labdata.ontology import vocabularies as vocab_mod
from labdata.ontology.lifecycle import Result, VocabularyResult
from labdata.ontology.root import make_store, read_current
from labdata.ontology.update import update
from labdata.ontology.vocabularies import Vocabulary
from tests._ontology_fakes import (
    OntologyWeb,
    _isolated,  # noqa: F401 - autouse: the cache, no lock owner, no sleeps
)

FBDV = (Path(__file__).parent / "data" / "ontology" / "fbdv.obo").read_bytes()
PURL = "https://purl.obolibrary.org/obo/fbdv.obo"
GITHUB = (
    "https://github.com/FlyBase/drosophila-developmental-ontology/releases/latest/download/fbdv.obo"
)
SNAPSHOT = "fbdv-2026-11-02"
PATO_URL = "https://purl.obolibrary.org/obo/pato.obo"
PATO = b"""format-version: 1.2
data-version: releases/2026-10-01/pato.obo
ontology: pato

[Term]
id: PATO:0000001
name: quality

[Term]
id: PATO:0000014
name: color
"""
HEAD = f"bytes=0-{fetch_mod.HEAD_BYTES - 1}"
#: Slurm jobs by state; Slurm knows no other job.
_JOBS = {"4242": "RUNNING", "4241": "CANCELLED"}


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_store(tmp_path / "ontologies").parent


@pytest.fixture
def web(monkeypatch: pytest.MonkeyPatch) -> OntologyWeb:
    """FBdv alone; its purl redirects to GitHub, as the real one does."""
    fake = OntologyWeb(files={GITHUB: FBDV}, redirects={PURL: GITHUB})
    monkeypatch.setattr("labdata.cli._ontology_transport", lambda: fake)
    monkeypatch.setattr(vocab_mod, "VOCABULARIES", (Vocabulary("fbdv", ("FBdv:",)),))
    return fake


def _texts(root: Path) -> set[tuple[str, str, str, str | None, str | None]]:
    with closing(tables.connect(tables.tables_path(root, SNAPSHOT))) as db:
        return set(db.execute("select id, text, kind, scope, synonym_type from texts"))


def test_a_first_update_makes_fbdv_current_and_a_lookup_answers_from_it(
    root: Path, web: OntologyWeb
) -> None:
    web.failures[GITHUB] = [503]  # one busy answer, retried

    result = CliRunner().invoke(app, ["ontology", "update", "--root", str(root)])

    assert result.exit_code == 0, result.output
    assert SNAPSHOT in result.output
    with open_ontology_store(root) as store:
        assert store.snapshots == {"fbdv": SNAPSHOT}
        assert store.lookup("FBdv:00007012") == Term(
            "FBdv:00007012", "life stage", False, (), None, SNAPSHOT
        )
    # The snapshot keeps its source file as downloaded; nothing is left in downloads.
    assert paths.SNAPSHOTS.at(root, SNAPSHOT, paths.SOURCE_DIR, "fbdv.obo").read_bytes() == FBDV
    assert not any(paths.DOWNLOADS.at(root).iterdir())


@pytest.mark.parametrize("ranges", [True, False], ids=["range", "range-ignored"])
def test_the_release_is_read_from_the_header_before_any_full_download(
    root: Path, web: OntologyWeb, ranges: bool
) -> None:
    # A file longer than the header read, so a full download shows in the bytes given.
    pad = "".join(f"\n[Term]\nid: FBdv:9{i:07d}\nname: padding stage {i}\n" for i in range(3000))
    body = FBDV + pad.encode()
    assert len(body) > 2 * fetch_mod.HEAD_BYTES
    web.files[GITHUB] = body
    if not ranges:
        web.no_ranges.add(GITHUB)

    update(root, transport=web)

    head, full = web.gets(GITHUB)
    assert head.headers["Range"] == f"bytes=0-{fetch_mod.HEAD_BYTES - 1}"
    assert head.given <= fetch_mod.HEAD_BYTES
    assert "Range" not in full.headers
    assert full.given == len(body)


def test_terms_with_another_prefix_are_not_stored(root: Path, web: OntologyWeb) -> None:
    update(root, transport=web)

    with open_ontology_store(root) as store:
        assert store.lookup("GO:0009790") is None
    with closing(tables.connect(tables.tables_path(root, SNAPSHOT))) as db:
        ids = {row[0] for row in db.execute("select id from terms")}
    assert ids == {"FBdv:00005289", "FBdv:00007012", "FBdv:00005333", "FBdv:00005334"}


def test_all_four_synonym_scopes_and_a_synonym_type_are_kept(root: Path, web: OntologyWeb) -> None:
    update(root, transport=web)

    stage = {row for row in _texts(root) if row[0] == "FBdv:00005289"}
    assert stage == {
        ("FBdv:00005289", "embryonic stage", "name", None, None),
        ("FBdv:00005289", "embryo stage", "synonym", "EXACT", None),
        ("FBdv:00005289", "embryonic stages", "synonym", "EXACT", "OMO:0003004"),
        ("FBdv:00005289", "embryogenesis", "synonym", "BROAD", None),
        ("FBdv:00005289", "early life", "synonym", "RELATED", None),
        ("FBdv:00005289", "embryonic stage 1-17", "synonym", "NARROW", None),
    }


def test_an_obsolete_term_and_an_alt_id_resolve_through_lookup(
    root: Path, web: OntologyWeb
) -> None:
    update(root, transport=web)

    with open_ontology_store(root) as store:
        assert store.lookup("FBdv:00005333") == Term(
            "FBdv:00005333",
            "late embryonic stage",
            True,
            ("FBdv:00005289",),
            Reason.OBSOLETE,
            SNAPSHOT,
        )
        gone = store.lookup("FBdv:00005334")
        assert gone is not None
        assert (gone.obsolete, gone.replaced_by) == (True, ())
        assert store.lookup("FBdv:00000001") == Term(
            "FBdv:00000001",
            None,
            False,
            ("FBdv:00005289",),
            Reason.ALTERNATIVE,
            SNAPSHOT,
        )


def test_the_status_record_names_the_outcome_and_the_snapshot(root: Path, web: OntologyWeb) -> None:
    update(root, transport=web)
    first = lifecycle.read_status(root)
    assert first.outcome == lifecycle.Outcome.BUILT
    assert first.start is not None
    assert first.end is not None
    row = first.vocabularies["fbdv"]
    assert (row.release, row.snapshot, row.result) == ("2026-11-02", SNAPSHOT, "updated")

    # The same release again: only the header is read, and the status says so.
    web.calls.clear()
    update(root, transport=web)

    assert [c.headers.get("Range") for c in web.gets(GITHUB)] == [
        f"bytes=0-{fetch_mod.HEAD_BYTES - 1}"
    ]
    again = lifecycle.read_status(root).vocabularies["fbdv"]
    assert (again.snapshot, again.result) == (SNAPSHOT, lifecycle.Result.UNCHANGED)


def _release(body: bytes, old: str, new: str) -> bytes:
    return body.replace(old.encode(), new.encode())


def _downloads(web: OntologyWeb) -> list[str]:
    """Return the URLs whose whole file was served."""
    return [c.url for c in web.calls if "Range" not in c.headers and c.status == 200]


@pytest.fixture
def both(monkeypatch: pytest.MonkeyPatch, web: OntologyWeb) -> OntologyWeb:
    """Declare FBdv and PATO, and serve both."""
    declared = (Vocabulary("fbdv", ("FBdv:",)), Vocabulary("pato", ("PATO:",)))
    monkeypatch.setattr(vocab_mod, "VOCABULARIES", declared)
    web.files[PATO_URL] = PATO
    web.modified[PATO_URL] = "Thu, 01 Oct 2026 08:00:00 GMT"
    web.modified[GITHUB] = "Mon, 02 Nov 2026 08:00:00 GMT"
    return web


@pytest.fixture
def slurm(monkeypatch: pytest.MonkeyPatch) -> None:
    def show_job(job_id: str) -> str | None:
        state = _JOBS.get(job_id)
        return f"JobId={job_id} JobName=update\n   JobState={state} Reason=None\n" if state else ""

    monkeypatch.setattr(_slurm, "show_job", show_job)


def test_a_second_update_downloads_only_the_vocabulary_with_a_new_release(
    root: Path, both: OntologyWeb
) -> None:
    update(root, transport=both)
    both.calls.clear()
    both.files[GITHUB] = _release(FBDV, "2026-11-02", "2026-12-01")
    both.modified[GITHUB] = "Tue, 01 Dec 2026 08:00:00 GMT"

    status = update(root, transport=both)

    assert _downloads(both) == [GITHUB]
    assert status.vocabularies == {
        "fbdv": VocabularyResult(
            "2026-12-01", "fbdv-2026-12-01", Result.UPDATED, None, 4, "2026-12-01"
        ),
        "pato": VocabularyResult(
            "2026-10-01", "pato-2026-10-01", Result.UNCHANGED, None, 2, "2026-10-01"
        ),
    }
    assert lifecycle.read_status(root) == status


@pytest.mark.parametrize("fault", ["download", "broken"])
def test_a_failed_vocabulary_keeps_its_snapshot_while_the_others_switch(
    root: Path, both: OntologyWeb, fault: str
) -> None:
    update(root, transport=both)
    both.files[PATO_URL] = _release(PATO, "2026-10-01", "2026-11-15")
    new = _release(FBDV, "2026-11-02", "2026-12-01")
    if fault == "download":
        both.failures[GITHUB] = [503] * 10
    else:
        both.files[GITHUB] = new[: new.index(b"embryo stage")]  # cut part of the way into a line

    result = CliRunner().invoke(app, ["ontology", "update", "--root", str(root)])

    assert result.exit_code == 1
    row = lifecycle.read_status(root).vocabularies["fbdv"]
    assert (row.result, row.snapshot, row.terms) == (Result.FAILED, SNAPSHOT, 4)
    reason = "HTTP 503" if fault == "download" else "does not parse"
    assert row.reason is not None
    assert reason in row.reason
    assert f"fbdv: failed ({row.reason})" in result.output
    assert read_current(root) == {"fbdv": SNAPSHOT, "pato": "pato-2026-11-15"}
    assert not paths.SNAPSHOTS.at(root, "fbdv-2026-12-01").exists()


def test_a_release_with_far_fewer_own_terms_is_refused(root: Path, web: OntologyWeb) -> None:
    update(root, transport=web)
    new = _release(FBDV, "2026-11-02", "2026-12-01")
    web.files[GITHUB] = new[: new.index(b"[Term]\nid: FBdv:00007012")]  # cut at a stanza

    status = update(root, transport=web)

    row = status.vocabularies["fbdv"]
    assert (status.outcome, row.result, row.snapshot) == ("failed", Result.FAILED, SNAPSHOT)
    assert row.reason is not None
    assert "1 own terms" in row.reason
    assert read_current(root) == {"fbdv": SNAPSHOT}
    assert [p.name for p in paths.SNAPSHOTS.at(root).iterdir()] == [SNAPSHOT]


@pytest.mark.parametrize(
    "releases",
    [("2026-11-02", "2026-12-01", "2027-01-05"), ("9", "10", "11")],
    ids=["dates", "numbers"],
)
def test_after_three_releases_only_the_newest_two_are_kept(
    root: Path, web: OntologyWeb, releases: tuple[str, ...]
) -> None:
    for release in releases:
        web.files[GITHUB] = _release(FBDV, "fbdv/releases/2026-11-02", release)
        update(root, transport=web)

    kept = sorted(p.name for p in paths.SNAPSHOTS.at(root).iterdir())
    assert kept == sorted(f"fbdv-{r}" for r in releases[1:])
    assert read_current(root) == {"fbdv": f"fbdv-{releases[2]}"}


def test_a_snapshot_of_an_older_table_layout_is_remade_with_no_download(
    root: Path, web: OntologyWeb, monkeypatch: pytest.MonkeyPatch
) -> None:
    with monkeypatch.context() as old:
        old.setattr(tables, "TABLE_LAYOUT", 1)
        update(root, transport=web)
    assert tables.layout_of(root, SNAPSHOT) == 1
    web.calls.clear()

    status = update(root, transport=web)

    assert tables.layout_of(root, SNAPSHOT) == tables.TABLE_LAYOUT
    assert _downloads(web) == []
    assert status.vocabularies["fbdv"].result == Result.UNCHANGED
    assert sorted(f.name for f in paths.SNAPSHOTS.at(root, SNAPSHOT).iterdir()) == [
        paths.SOURCE_DIR,
        paths.TABLES_FILE,
    ]
    with open_ontology_store(root) as store:
        assert [c.id for c in store.search("embryo stage", "fbdv")][:1] == ["FBdv:00005289"]


@pytest.mark.parametrize(
    ("job", "refusal"),
    [("4241", None), ("4242", "wait for it to end")],
    ids=["slurm-gone", "slurm-live"],
)
@pytest.mark.usefixtures("slurm")
def test_a_held_lock_is_taken_from_a_gone_owner_and_refused_while_one_lives(
    root: Path, web: OntologyWeb, job: str, refusal: str | None
) -> None:
    owner = {
        "update": "update",
        "job": job,
        "host": "elsewhere",
        "pid": 1,
        "started": "2026-11-21T00:00:00Z",
    }
    lock = paths.LOCK.at(root)
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(json.dumps(owner), encoding="utf-8")

    result = CliRunner().invoke(app, ["ontology", "update", "--root", str(root)])

    if refusal is None:
        assert result.exit_code == 0, result.output
        assert read_current(root) == {"fbdv": SNAPSHOT}
        assert not lock.exists()
    else:
        assert result.exit_code == 1
        assert "holds this store's run lock" in result.output
        assert refusal in result.output
        assert web.calls == []
        assert not paths.STATUS.at(root).exists()
