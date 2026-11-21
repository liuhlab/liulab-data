"""The ontology store's run lock and its status record, ``ontology-status.json``.

Only this module writes the status record.
"""

from __future__ import annotations

import enum
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

from labdata import _lock
from labdata._files import write_atomic
from labdata.exceptions import LabdataError
from labdata.ontology import paths


class OntologyBusyError(LabdataError):
    """Another update holds the ontology store's run lock."""


class Outcome(enum.StrEnum):
    """How the last update stands."""

    RUNNING = "running"
    #: Ended with a vocabulary failed, or stopped part of the way.
    FAILED = "failed"
    #: Every vocabulary is current on the work disk.
    BUILT = "built"


class Result(enum.StrEnum):
    """What the last update did with one vocabulary."""

    UPDATED = "updated"
    UNCHANGED = "unchanged"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class VocabularyResult:
    """One vocabulary's row in the status record.

    ``release`` is ``None`` when the update could not read it; ``reason`` says why a
    vocabulary failed.
    """

    release: str | None = None
    snapshot: str | None = None
    result: str = ""
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class UpdateStatus:
    """The status record: the last update's start, end and outcome, and one row per vocabulary.

    ``start`` and ``end`` are UTC, ISO 8601; ``end`` is ``None`` while it runs.
    """

    start: str | None = None
    end: str | None = None
    outcome: str = ""
    vocabularies: dict[str, VocabularyResult] = field(default_factory=dict)


@contextmanager
def run_lock(root: Path, holder: str, *, owner_gone: bool = False) -> Iterator[None]:
    """Hold the ontology store's run lock, so two updates never write the store at once.

    Raises
    ------
    OntologyBusyError
        If another update holds it and still runs, or this host cannot tell and
        ``owner_gone`` is False.
    """
    with _lock.run_lock(
        paths.LOCK.at(root), holder, error=OntologyBusyError, owner_gone=owner_gone
    ):
        yield


def read_status(root: Path) -> UpdateStatus:
    """Return the status record; an empty one before the first update.

    Keys the file holds that the record lacks are ignored.
    """
    path = paths.STATUS.at(root)
    if not path.is_file():
        return UpdateStatus()
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    rows = data.pop("vocabularies", {})
    status = UpdateStatus(**_known(UpdateStatus, data))
    for name, row in rows.items():
        status.vocabularies[name] = VocabularyResult(**_known(VocabularyResult, row))
    return status


def write_status(root: Path, status: UpdateStatus) -> None:
    """Write the status record whole."""
    text = json.dumps(asdict(status), indent=2, sort_keys=True) + "\n"
    write_atomic(paths.STATUS.at(root), text)


def _known(cls: type[UpdateStatus | VocabularyResult], data: dict[str, Any]) -> dict[str, Any]:
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in names}
