"""The GEO/SRA store's life cycle: build, check, switch, then finalize once GCS holds it."""

from __future__ import annotations

import datetime as dt
import json
import os
import socket
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner, Result

from labdata.cli import app
from labdata.ncbi.config import NcbiCredentials
from labdata.ncbi.entrez import EntrezClient
from labdata.store import lifecycle, open_store, raw
from labdata.store.gcs import COPIED, StoreCopyError
from labdata.store.lifecycle import Action
from tests._fake_gcs import FakeGcs
from tests._slices import BuiltStore

_DEST = "gcs:lab-bucket/geo-sra-store"
_STARTED = "2026-11-01T02:00:00Z"
_NOW = dt.datetime(2026, 11, 18, tzinfo=dt.UTC)
_ROUND = "work/run/2026-10-14-all"


def _refresh(days: int, outcome: str, **more: Any) -> dict[str, Any]:
    """A refresh's status record: started ``days`` before :data:`_NOW` by DAG version 4."""
    started = f"{_NOW - dt.timedelta(days=days):%Y-%m-%dT%H:%M:%SZ}"
    record = {"update": "refresh", "started": started, "outcome": outcome, "dag": 4}
    return record | {"folder": "work/refresh/x", "since": "2026-10-14", **more}


@pytest.mark.parametrize(
    ("status", "action", "abandoned"),
    [
        (_refresh(1, "running"), Action.RESUME, None),
        (_refresh(6, "failed"), Action.RESUME, None),
        (_refresh(8, "failed"), Action.NEW, "it started more than 7 days ago"),
        (_refresh(1, "failed", dag=3), Action.NEW, "an older refresh DAG"),
        (_refresh(8, "built", copy_pending=True, dag=3), Action.COPY, None),
        (_refresh(1, "built", copy_pending=False), Action.NEW, None),
        (_refresh(1, "finalized"), Action.NEW, None),
        ({"update": "run", "folder": _ROUND, "outcome": "failed"}, Action.NEW, None),
        ({}, Action.NEW, None),
    ],
)
def test_a_refresh_resumes_before_its_switch_within_a_week_of_one_dag_and_after_it_only_copies(
    status: dict[str, Any], action: str, abandoned: str | None
) -> None:
    plan = lifecycle.plan(status, lifecycle.REFRESH, dag=4, now=_NOW)

    assert plan.action == action
    assert plan.folder == (status["folder"] if action in (Action.RESUME, Action.COPY) else None)
    assert (plan.abandoned or "").startswith(abandoned or "")
    assert (plan.abandoned is None) is (abandoned is None)


def _run(outcome: str, **more: Any) -> dict[str, Any]:
    return {"update": "run", "folder": _ROUND, "outcome": outcome, "started": _STARTED, **more}


@pytest.mark.parametrize(
    ("status", "done", "rerun", "action", "running"),
    [
        # A full run has no age or DAG rule: its round folder is what makes it the same run.
        (_run("failed"), False, False, Action.RESUME, True),
        (_run("running"), False, True, Action.RESUME, True),
        (_run("built", copy_pending=True), False, True, Action.COPY, False),
        (_run("built", copy_pending=False), False, False, Action.RESUME, False),
        # Steps run again in a finalized round: it is running until it switches again.
        (_run("finalized"), True, True, Action.RESUME, True),
        (_run("finalized"), True, False, Action.NOTHING, False),
        # GCS holds the round's build, so a missing last marker reruns nothing.
        (_run("finalized"), False, False, Action.NOTHING, False),
        (
            {"update": "build", "outcome": "finalized", "build": "b1"},
            True,
            False,
            Action.NOTHING,
            False,
        ),
        ({"update": "build", "outcome": "finalized", "build": "b1"}, True, True, Action.NEW, True),
        (_run("failed") | {"folder": "work/run/2026-09-01-all"}, False, False, Action.NEW, True),
        (_refresh(1, "built", copy_pending=True), False, False, Action.NEW, True),
    ],
)
def test_a_full_run_carries_on_in_its_round_folder_unless_only_the_copy_or_nothing_is_left(
    status: dict[str, Any], done: bool, rerun: bool, action: str, running: bool
) -> None:
    plan = lifecycle.plan(status, lifecycle.RUN, _ROUND, done=done, rerun=rerun, now=_NOW)

    assert (plan.action, plan.folder, plan.abandoned) == (action, _ROUND, None)
    assert plan.running is running


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv("LABDATA_STORE", raising=False)


@pytest.fixture
def root(built_store: BuiltStore, tmp_path: Path) -> Path:
    """A store built as ``b1`` with a destination; GCS holds none of it yet."""
    root = built_store(tmp_path / "store", "geo_slice")
    marker = root / "labdata-store.toml"
    marker.write_text(marker.read_text() + f'copy_to = "{_DEST}"\n', encoding="utf-8")
    return root


@pytest.fixture
def gcs(root: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeGcs:
    """GCS holding ``b1``, finalized, as the copy after the last update left it."""
    gcs = FakeGcs.install(monkeypatch, tmp_path / "gcs", _DEST)
    lifecycle.finalize(root, full=True)
    gcs.forget()
    return gcs


def _build(root: Path) -> str:
    return str(open_store(root).settings["build"])


def _gcs_names(gcs: FakeGcs, build_id: str) -> bool:
    return f"builds/{build_id}/manifest.json" in json.loads(gcs.read(COPIED))


def test_a_refresh_switched_on_an_expired_login_ends_built_and_the_next_copy_finalizes_it(
    root: Path, gcs: FakeGcs
) -> None:
    before = lifecycle.read_status(root)
    lifecycle.start(root, "refresh", folder="work/refresh/x", started=_STARTED, since="2026-10-14")
    lifecycle.build(root, "b2", switch=False)
    # Written, not switched to: readers and the last-success date stay.
    assert _build(root) == "b1"
    assert lifecycle.read_status(root).get("last_success") == before.get("last_success")

    lifecycle.switch(root)
    gcs.expired = True
    assert lifecycle.finalize(root).skipped

    status = lifecycle.read_status(root)
    assert _build(root) == "b2"
    assert (status["outcome"], status["copy_pending"]) == ("built", True)
    assert status["last_success"] == "2026-11-01"
    # A rerun of the refresh only copies.
    assert lifecycle.plan(status, "refresh").action == Action.COPY
    assert not _gcs_names(gcs, "b2")

    gcs.expired = False
    assert not lifecycle.finalize(root).skipped

    status = lifecycle.read_status(root)
    assert (status["outcome"], status["copy_pending"]) == ("finalized", False)
    assert lifecycle.plan(status, "refresh").action == Action.NEW
    assert _gcs_names(gcs, "b2")
    assert gcs.read(lifecycle.STATUS) == (root / lifecycle.STATUS).read_bytes()


def test_an_update_on_a_disk_behind_gcs_refuses_at_its_start_unless_the_login_has_expired(
    root: Path, gcs: FakeGcs, caplog: pytest.LogCaptureFixture
) -> None:
    # Another disk copied since this one did.
    gcs.write(COPIED, b"{}\n")

    with pytest.raises(StoreCopyError, match="labdata store restore"):
        lifecycle.start(root, "refresh", folder="work/refresh/x", started=_STARTED)
    # An update by hand checks too, before it builds or asks NCBI anything.
    with pytest.raises(StoreCopyError, match="labdata store restore"):
        lifecycle.build(root, "b2")
    with pytest.raises(StoreCopyError, match="labdata store restore"):
        lifecycle.remove_sra(root, client=EntrezClient(NcbiCredentials("t@t.org", "abc")))

    status = lifecycle.read_status(root)
    assert (status["outcome"], status["copy_pending"]) == ("finalized", True)
    assert not (root / "builds" / "b2").exists()
    assert gcs.log == []

    gcs.expired = True
    lifecycle.start(root, "run", folder="work/run/2026-10-14-all")

    assert lifecycle.read_status(root)["outcome"] == "running"
    assert "not checked against GCS" in caplog.text


def test_a_full_run_whose_build_fails_coverage_leaves_the_store_on_its_build(
    built_store: BuiltStore, tmp_path: Path
) -> None:
    # This slice's target Series has neither a record nor a gap.
    root = built_store(tmp_path / "store", "prjna1027859")
    lifecycle.start(root, "run", folder="work/run/2026-10-14-all")
    lifecycle.build(root, "b2", switch=False)

    with pytest.raises(lifecycle.CheckError) as failed:
        lifecycle.switch(root)

    assert failed.value.coverage.failing == (("GSE19970", "unexplained"),)
    assert _build(root) == "b1"
    assert lifecycle.end(root)["outcome"] == "failed"
    assert lifecycle.finalized(root) is None


def test_a_build_by_hand_is_built_not_finalized_until_a_copy_works(
    root: Path, gcs: FakeGcs
) -> None:
    finalized = gcs.read(lifecycle.STATUS)

    lifecycle.build(root, "b2")

    status = lifecycle.read_status(root)
    assert _build(root) == "b2"
    assert (status["update"], status["outcome"], status["copy_pending"]) == ("build", "built", True)
    assert gcs.read(lifecycle.STATUS) == finalized
    gcs.expired = True
    assert lifecycle.finalize(root).skipped
    assert lifecycle.read_status(root)["outcome"] == "built"

    gcs.expired = False
    lifecycle.finalize(root)

    assert lifecycle.read_status(root)["outcome"] == "finalized"
    assert _gcs_names(gcs, "b2")


#: Commands that write the store, with arguments that ask NCBI nothing on the slice's store.
_STEPS = {
    "copy": [],
    "pack": [],
    "fetch": ["SRX017370"],
    "extract": ["--source", "biosample"],
    "readme": [],
}
#: A process ID no process has.
_GONE = 2**31 - 1


def _step(root: Path, command: str) -> Result:
    return CliRunner().invoke(app, ["store", command, *_STEPS[command], "--root", str(root)])


def _hold_run_lock(root: Path, pid: int | None = None) -> str:
    """Hold the run lock as a refresh driven by process ``pid`` on this host, by default this
    process; return the token its steps inherit."""
    host, pid = socket.gethostname(), pid or os.getpid()
    owner = {"update": "refresh", "job": None, "host": host, "pid": pid}
    (root / "work" / "run.lock").write_text(json.dumps(owner | {"started": _STARTED}))
    return f"{host}:{pid}"


def _files(root: Path) -> dict[str, tuple[int, int]]:
    return {
        str(p.relative_to(root)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("command", _STEPS)
def test_a_step_run_by_hand_refuses_while_an_update_holds_the_run_lock(
    root: Path, gcs: FakeGcs, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    monkeypatch.setattr("labdata.cli._store_client", lambda: None)
    # A lane's part in flight, which a pack would take in and delete.
    raw.save(root, raw.SRA_XML, [raw.RawRow("SRX017370", "<new/>", "2026-11-18")])
    _hold_run_lock(root)
    before = _files(root)

    result = _step(root, command)

    assert result.exit_code == 1
    assert "holds this store's run lock" in result.output
    assert _files(root) == before
    assert gcs.calls == []


@pytest.mark.parametrize("command", _STEPS)
def test_a_step_runs_inside_an_update_and_by_hand_once_no_update_holds_the_run_lock(
    root: Path, gcs: FakeGcs, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    monkeypatch.setattr("labdata.cli._store_client", lambda: None)
    lock = root / "work" / "run.lock"
    monkeypatch.setenv(lifecycle.RUN_OWNER, _hold_run_lock(root))
    held = lock.read_bytes()

    inside = _step(root, command)

    assert inside.exit_code == 0, inside.output
    assert lock.read_bytes() == held
    # The update has ended; its lock is left, as a kill leaves it.
    monkeypatch.delenv(lifecycle.RUN_OWNER)
    _hold_run_lock(root, pid=_GONE)

    by_hand = _step(root, command)

    assert by_hand.exit_code == 0, by_hand.output
    assert not lock.exists()
