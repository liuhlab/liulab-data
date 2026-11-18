"""The tier ladder: a paper's status, its reason and the tier to try next.

The tiers run in :data:`TIERS` order, and a paper moves to the next tier only when the one
before cannot serve it. Tiers record attempts and files; only this module sets a paper's
``status``, ``reason``, ``xml_tier`` and ``pdf_tier``. The daily pass and the PMC sync both
settle papers here.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

from labdata.papers.tables import PaperTables

#: The tiers in order. A manual paper's next tier is the Paperpile step, still to be written.
TIERS = ("pmc", "openalex", "host", "paperpile")

#: A paper published this many days ago or less waits for PMC.
EMBARGO_DAYS = 365

#: Days an OpenAlex server or network error is tried before the paper moves on.
OPENALEX_TRIES = 3

#: The reasons on a row an older PMC sync left for a non-paper; the ladder leaves it as it is.
NOT_PAPERS = ("placeholder", "not-article")


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
    if any(f["kind"] in ("xml", "pdf") and f["path"] for f in files):
        return at("fetched", None, None)
    tried = {t: [a["outcome"] for a in attempts if a["tier"] == t] for t in TIERS}
    pmc = bool(row.get("pmcid")) and not tried["pmc"]
    openalex = (
        bool(row.get("openalex_pdf"))
        and "failed" not in tried["openalex"]
        and tried["openalex"].count("error") < OPENALEX_TRIES
    )
    host = bool(row.get("host_pdf_urls")) and not tried["host"]
    nearest = "pmc" if pmc else "openalex" if openalex else "host" if host else "paperpile"
    published = row.get("published")
    if published is not None and published > today - dt.timedelta(days=EMBARGO_DAYS):
        # Quota and manual work wait for PMC; the free tiers are tried meanwhile.
        return at("waiting", "pmc-embargo", nearest if nearest in ("pmc", "host") else None)
    if openalex:
        return at("waiting", "openalex-backlog", nearest)
    if host:
        return at("waiting", "host-link", nearest)
    if tried["host"]:
        reason = "host-blocked" if tried["host"][-1] == "blocked" else "host-failed"
    elif tried["openalex"]:
        reason = "openalex-failed"
    else:
        reason = "no-route"
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
    if not any(f["tier"] == "pmc" and f["kind"] == "xml" and f["path"] for f in files):
        return []
    return [f for f in files if f["kind"] == "pdf" and f["tier"] != "pmc"]


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
        if f["kind"] in ("xml", "pdf"):
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
        The paper store's tables, loaded; the caller saves them.
    today : datetime.date
        The UTC date of the pass.
    """
    for key, got in standings(state, today).items():
        row = state.papers[key]
        row["status"], row["reason"] = got.status, got.reason
        row["xml_tier"], row["pdf_tier"] = got.xml_tier, got.pdf_tier


def _held_tier(files: Collection[Mapping[str, Any]], kind: str) -> str | None:
    return next((f["tier"] for f in files if f["kind"] == kind and f["path"]), None)
