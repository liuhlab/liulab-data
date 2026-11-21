"""The ontology store's copy to GCS and its restore, through ``FakeGcs`` at the rclone seam."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import NoReturn

import pytest
from typer.testing import CliRunner

from labdata import _lock
from labdata.cli import app
from labdata.ontology import Term, lifecycle, open_ontology_store, paths
from labdata.ontology import fetch as fetch_mod
from labdata.ontology.root import make_store, switch
from labdata.ontology.tables import SnapshotData, SourceTerm, write_snapshot
from labdata.ontology.update import update
from tests._fake_gcs import FakeGcs
from tests._ontology_fakes import OntologyWeb

DEST = "gcs:bucket/datalake/ontology-store"
FBDV = (Path(__file__).parent / "data" / "ontology" / "fbdv.obo").read_bytes()
URL = "https://purl.obolibrary.org/obo/fbdv.obo"
SNAPSHOT = "fbdv-2026-11-02"
STATUS = paths.STATUS.rel
RECORD = paths.COPY_RECORD.rel


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_ONTOLOGIES", raising=False)
    monkeypatch.delenv(_lock.RUN_OWNER, raising=False)
    monkeypatch.setattr(fetch_mod, "_sleep", lambda _s: None)


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    return FakeGcs.install(monkeypatch, tmp_path / "gcs", DEST)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_store(tmp_path / "ontologies", copy_to=DEST).parent


@pytest.fixture
def web() -> OntologyWeb:
    return OntologyWeb(files={URL: FBDV})


def gcs_status(gcs: FakeGcs) -> dict:
    return json.loads(gcs.read(STATUS))


def test_an_update_ends_with_a_copy_that_leaves_gcs_holding_the_store_finalized(
    root: Path, gcs: FakeGcs, web: OntologyWeb
) -> None:
    update(root, transport=web)

    snapshot = f"snapshots/{SNAPSHOT}"
    assert gcs.files() == sorted(
        [
            paths.MARKER.rel,
            paths.README.rel,
            paths.CURRENT.rel,
            f"{snapshot}/{paths.TABLES_FILE}",
            f"{snapshot}/{paths.SOURCE_DIR}/fbdv.obo",
            RECORD,
            STATUS,
        ]
    )
    assert gcs.phases(RECORD, STATUS)[-2:] == ["record", "status"]
    assert gcs_status(gcs)["outcome"] == "finalized"
    assert gcs_status(gcs)["vocabularies"]["fbdv"]["snapshot"] == SNAPSHOT
    assert gcs.read(STATUS) == (root / STATUS).read_bytes()


def test_a_copy_refuses_when_the_work_disk_is_behind_gcs(
    root: Path, gcs: FakeGcs, web: OntologyWeb
) -> None:
    update(root, transport=web)
    # Another disk sent to GCS since: its copy record is not this disk's.
    gcs.write(RECORD, gcs.read(RECORD) + b"\n")
    gcs.forget()

    result = CliRunner().invoke(app, ["ontology", "copy", "--root", str(root)])

    assert result.exit_code == 1
    assert "labdata ontology restore" in result.output
    assert gcs.sent() == []


def test_a_restore_into_an_empty_disk_can_look_up_and_search_at_once(
    root: Path, gcs: FakeGcs, web: OntologyWeb, tmp_path: Path
) -> None:
    update(root, transport=web)
    empty = tmp_path / "restored"

    result = CliRunner().invoke(app, ["ontology", "restore", "--root", str(empty), "--from", DEST])

    assert result.exit_code == 0, result.output
    with open_ontology_store(empty) as store:
        assert store.lookup("FBdv:00007012") == Term(
            "FBdv:00007012", "life stage", False, (), None, SNAPSHOT
        )
        assert store.search("life stage", "fbdv")[0].id == "FBdv:00007012"
    # The tables came from GCS whole, and the status record says finalized.
    tables = f"snapshots/{SNAPSHOT}/{paths.TABLES_FILE}"
    assert (empty / tables).read_bytes() == (root / tables).read_bytes()
    assert lifecycle.read_status(empty).outcome == "finalized"


def test_a_pruned_snapshot_leaves_gcs_at_the_next_copy(
    root: Path, gcs: FakeGcs, web: OntologyWeb
) -> None:
    for release in ("2026-09-01", "2026-10-01", "2026-11-02"):
        new = f"fbdv/releases/{release}".encode()
        web.files[URL] = FBDV.replace(b"fbdv/releases/2026-11-02", new)
        update(root, transport=web)

    held = {p.split("/")[1] for p in gcs.files() if p.startswith("snapshots/")}
    assert held == {"fbdv-2026-10-01", SNAPSHOT}
    assert f"snapshots/fbdv-2026-09-01/{paths.TABLES_FILE}" in gcs.deleted()
    assert gcs_status(gcs)["outcome"] == "finalized"


class _Offline:
    """A transport whose every request fails, to stop an update at its first download."""

    def open(self, *args: object, **kwargs: object) -> NoReturn:
        raise RuntimeError("offline")


def test_a_failed_copy_leaves_the_update_built_and_the_next_update_copies_first(
    root: Path, gcs: FakeGcs, web: OntologyWeb
) -> None:
    gcs.expired = "copy"  # the check before the update lists; the copy after it fails

    status = update(root, transport=web)

    assert (status.outcome, status.copy_pending) == ("built", True)
    assert STATUS not in gcs.files()

    gcs.expired = False
    # The next update stops at its first request, so only a copy before it reaches GCS.
    with pytest.raises(RuntimeError, match="offline"):
        update(root, transport=_Offline())

    assert f"snapshots/{SNAPSHOT}/{paths.TABLES_FILE}" in gcs.files()
    assert gcs_status(gcs)["outcome"] == "finalized"
    assert gcs_status(gcs)["copy_pending"] is False


def test_a_copy_that_would_delete_more_snapshots_than_the_guard_allows_stops(
    root: Path, gcs: FakeGcs
) -> None:
    for release in ("2026-09-01", "2026-10-01", "2026-11-01"):
        term = SourceTerm("FBdv:00007012", "life stage")
        switch(root, {"fbdv": write_snapshot(root, SnapshotData("fbdv", release, [term]))})
    copy = ["ontology", "copy", "--root", str(root)]
    assert CliRunner().invoke(app, [*copy, "--full"]).exit_code == 0
    # This disk lost two older snapshots: it adds none, so it may delete only one.
    for lost in ("fbdv-2026-09-01", "fbdv-2026-10-01"):
        shutil.rmtree(paths.SNAPSHOTS.at(root, lost))
    gcs.forget()

    refused = CliRunner().invoke(app, copy)

    assert refused.exit_code == 1
    assert "--accept-deletes 2" in refused.output
    assert gcs.deleted() == gcs.sent() == []

    accepted = CliRunner().invoke(app, [*copy, "--accept-deletes", "2"])

    assert accepted.exit_code == 0, accepted.output
    assert {p.split("/")[1] for p in gcs.files() if "/" in p} == {"fbdv-2026-11-01"}
