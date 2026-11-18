"""The tier ladder: a paper's status, its reason and the tier to try next.

The tiers run in :data:`TIERS` order, and a paper moves to the next tier only when the one
before cannot serve it. Tiers record attempts and files; only this module sets a paper's
``status``, ``reason``, ``xml_tier`` and ``pdf_tier``, and every save of the tables settles
them here first.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from labdata.papers.tables import PaperTables

#: The PMC bucket.
PMC = "pmc"
#: A copy OpenAlex sells.
OPENALEX = "openalex"
#: A free link on the publisher's site.
HOST = "host"
#: The manual queue in Paperpile.
PAPERPILE = "paperpile"
#: The tiers in order. A manual paper's next tier is :data:`PAPERPILE`.
TIERS = (PMC, OPENALEX, HOST, PAPERPILE)
#: The tiers tried while a paper waits for PMC to release it: those that spend no quota.
FREE = (PMC, HOST)

#: A paper published this many days ago or less waits for PMC.
EMBARGO_DAYS = 365

#: Days a server or network error from OpenAlex or PMC is tried before the paper moves on.
ERROR_TRIES = 3

#: The reasons on a non-paper's row; the ladder leaves such a row as it is.
NOT_PAPERS = ("placeholder", "not-article")

type _Serves = Callable[[Mapping[str, Any], list[str]], bool]

#: Whether a tier can still serve a paper, from its row and the outcomes of that tier's tries.
_SERVES: dict[str, _Serves] = {
    PMC: lambda row, tried: (
        bool(row.get("pmcid")) and set(tried) <= {"error"} and _error_tries_left(tried)
    ),
    OPENALEX: lambda row, tried: (
        bool(row.get("openalex_pdf")) and "failed" not in tried and _error_tries_left(tried)
    ),
    HOST: lambda row, tried: bool(row.get("host_pdf_urls")) and not tried,
    PAPERPILE: lambda row, tried: True,
}
#: The reason a paper waits on each tier before the manual queue.
_WAITING = {PMC: "pmc-sync", OPENALEX: "openalex-backlog", HOST: "host-link"}
#: The reason a manual paper takes from a tier's last failed try, by its outcome.
_FAILED: dict[str, Callable[[str], str]] = {
    OPENALEX: lambda outcome: "openalex-failed",
    HOST: lambda outcome: "host-blocked" if outcome == "blocked" else "host-failed",
}


@dataclass(frozen=True, slots=True)
class Standing:
    """Where a paper stands on the ladder.

    ``tier`` is the tier to try next; ``None`` when none is due: the paper is fetched or
    skipped, or waits for PMC. ``xml_tier`` and ``pdf_tier`` name the tier of the XML and the
    PDF the store holds.
    """

    status: str
    reason: str | None
    tier: str | None
    xml_tier: str | None
    pdf_tier: str | None


def holds(file: Mapping[str, Any]) -> bool:
    """Return whether a ``files`` row is an XML or PDF the store holds."""
    return file["kind"] in ("xml", "pdf") and bool(file["path"])


def standing(
    row: Mapping[str, Any],
    attempts: Collection[Mapping[str, Any]],
    files: Collection[Mapping[str, Any]],
    today: dt.date,
) -> Standing:
    """Return where one paper stands, from its ``papers`` row, attempts and files.

    Parameters
    ----------
    row : mapping
        The paper's ``papers`` row; its own status counts only on a non-paper's row.
    attempts : collection of mapping
        The paper's ``attempts`` rows.
    files : collection of mapping
        The paper's ``files`` rows.
    today : datetime.date
        The UTC date of the pass.

    Returns
    -------
    Standing
        The paper's status, reason, next tier and the tiers of its XML and PDF.
    """
    xml_tier, pdf_tier = _held_tier(files, "xml"), _held_tier(files, "pdf")

    def at(status: str, reason: str | None, tier: str | None) -> Standing:
        return Standing(status, reason, tier, xml_tier, pdf_tier)

    if row.get("status") == "skipped" and row.get("reason") in NOT_PAPERS:
        return at("skipped", row["reason"], None)
    if row.get("retracted"):
        return at("skipped", "retracted", None)
    if any(holds(f) for f in files):
        return at("fetched", None, None)
    tried = {t: [a["outcome"] for a in attempts if a["tier"] == t] for t in TIERS}
    nearest = next(t for t in TIERS if _SERVES[t](row, tried[t]))
    published = row.get("published")
    if published is not None and published > today - dt.timedelta(days=EMBARGO_DAYS):
        # Quota and manual work wait for PMC; the free tiers are tried meanwhile.
        return at("waiting", "pmc-embargo", nearest if nearest in FREE else None)
    if nearest in _WAITING:
        return at("waiting", _WAITING[nearest], nearest)
    failed = [t for t in TIERS if t in _FAILED and tried[t]]
    reason = _FAILED[failed[-1]](tried[failed[-1]][-1]) if failed else "no-route"
    return at("manual", reason, nearest)


def replaced(files: Collection[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Return the PDFs from later tiers that a paper's PMC copy replaces.

    Once the store holds PMC's XML of a paper, a PDF any other tier gave goes.

    Parameters
    ----------
    files : collection of mapping
        One paper's ``files`` rows.

    Returns
    -------
    list of mapping
        The rows of the PDFs to remove; none while PMC gives no XML.
    """
    if not any(f["tier"] == PMC and f["kind"] == "xml" and f["path"] for f in files):
        return []
    return [f for f in files if f["kind"] == "pdf" and f["tier"] != PMC]


def standings(state: PaperTables, today: dt.date) -> dict[str, Standing]:
    """Return where every paper in ``state`` stands, by paper.

    Parameters
    ----------
    state : PaperTables
        The paper store's tables, loaded.
    today : datetime.date
        The UTC date of the pass.

    Returns
    -------
    dict of str to Standing
        One standing per ``papers`` row.
    """
    attempts: dict[str, list[dict[str, Any]]] = {}
    for a in state.attempts:
        attempts.setdefault(a["paper"], []).append(a)
    files: dict[str, list[dict[str, Any]]] = {}
    for f in state.files:
        if holds(f):
            files.setdefault(f["paper"], []).append(f)
    return {
        key: standing(row, attempts.get(key, []), files.get(key, []), today)
        for key, row in state.papers.items()
    }


def settle(state: PaperTables, today: dt.date) -> None:
    """Set every paper's ``status``, ``reason``, ``xml_tier`` and ``pdf_tier`` in ``state``.

    Parameters
    ----------
    state : PaperTables
        The paper store's tables, loaded.
    today : datetime.date
        The UTC date of the pass.
    """
    for key, got in standings(state, today).items():
        row = state.papers[key]
        row["status"], row["reason"] = got.status, got.reason
        row["xml_tier"], row["pdf_tier"] = got.xml_tier, got.pdf_tier


def _error_tries_left(tried: list[str]) -> bool:
    """Return whether a tier's server or network errors are fewer than :data:`ERROR_TRIES`."""
    return tried.count("error") < ERROR_TRIES


def _held_tier(files: Collection[Mapping[str, Any]], kind: str) -> str | None:
    return next((f["tier"] for f in files if f["kind"] == kind and holds(f)), None)
