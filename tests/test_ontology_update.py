"""The ontology update: one OBO vocabulary from the web into the store, through a fake web."""

from __future__ import annotations

import json
import os
import socket
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from labdata import _lock
from labdata.cli import app
from labdata.ontology import Reason, Term, lifecycle, open_ontology_store, paths, tables
from labdata.ontology import fetch as fetch_mod
from labdata.ontology.root import make_store
from labdata.ontology.update import update
from labdata.ontology.vocabularies import Vocabulary
from tests._ontology_fakes import OntologyWeb

FBDV = (Path(__file__).parent / "data" / "ontology" / "fbdv.obo").read_bytes()
PURL = "https://purl.obolibrary.org/obo/fbdv.obo"
GITHUB = (
    "https://github.com/FlyBase/drosophila-developmental-ontology/releases/latest/download/fbdv.obo"
)
SNAPSHOT = "fbdv-2026-11-02"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_ONTOLOGIES", raising=False)
    monkeypatch.delenv(_lock.RUN_OWNER, raising=False)
    monkeypatch.setattr(fetch_mod, "_sleep", lambda _s: None)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_store(tmp_path / "ontologies").parent


@pytest.fixture
def web(monkeypatch: pytest.MonkeyPatch) -> OntologyWeb:
    """FBdv alone; its purl redirects to GitHub, as the real one does."""
    fake = OntologyWeb(files={GITHUB: FBDV}, redirects={PURL: GITHUB})
    monkeypatch.setattr("labdata.cli._ontology_transport", lambda: fake)
    monkeypatch.setattr("labdata.ontology.update.VOCABULARIES", (Vocabulary("fbdv", ("FBdv:",)),))
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
    assert first.vocabularies["fbdv"] == lifecycle.VocabularyResult(
        release="2026-11-02", snapshot=SNAPSHOT, result=lifecycle.Result.UPDATED
    )

    # The same release again: only the header is read, and the status says so.
    web.calls.clear()
    update(root, transport=web)

    assert [c.headers.get("Range") for c in web.gets(GITHUB)] == [
        f"bytes=0-{fetch_mod.HEAD_BYTES - 1}"
    ]
    again = lifecycle.read_status(root).vocabularies["fbdv"]
    assert (again.snapshot, again.result) == (SNAPSHOT, lifecycle.Result.UNCHANGED)


def test_a_second_update_while_a_live_owner_holds_the_lock_is_refused(
    root: Path, web: OntologyWeb
) -> None:
    lock = paths.LOCK.at(root)
    lock.parent.mkdir(parents=True, exist_ok=True)
    owner = {"update": "update", "job": None, "host": socket.gethostname(), "pid": os.getpid()}
    lock.write_text(json.dumps(owner | {"started": "2026-11-21T00:00:00Z"}), encoding="utf-8")

    result = CliRunner().invoke(app, ["ontology", "update", "--root", str(root)])

    assert result.exit_code == 1
    assert "holds this store's run lock" in result.output
    assert web.calls == []
    assert not paths.STATUS.at(root).exists()
