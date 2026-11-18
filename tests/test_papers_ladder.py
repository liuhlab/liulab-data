"""The tier ladder: a paper's row, attempts and files give its status, reason and next tier."""

from __future__ import annotations

import datetime as dt
from typing import Any

from labdata.papers import ladder

TODAY = dt.date(2026, 11, 7)
HOST = ["https://journal.org/a.pdf"]


def place(
    row: dict[str, Any], tries: tuple[tuple[str, str], ...] = (), *files: dict[str, Any]
) -> tuple[str, str | None, str | None]:
    """Return the status, reason and next tier of an old paper with ``tries`` and ``files``."""
    attempts = [{"tier": tier, "outcome": outcome} for tier, outcome in tries]
    got = ladder.standing({"published": dt.date(2020, 1, 1), **row}, attempts, list(files), TODAY)
    return got.status, got.reason, got.tier


def held(kind: str, tier: str) -> dict[str, Any]:
    return {"kind": kind, "name": f"1.{kind}", "path": f"papers/1.{kind}", "tier": tier}


def test_a_paper_moves_to_the_next_tier_only_when_the_one_before_cannot_serve_it() -> None:
    every = {"pmcid": "PMC1", "openalex_pdf": True, "host_pdf_urls": HOST}
    pmc_failed = ("pmc", "failed")
    error = ("openalex", "error")
    assert {
        "PMC not tried": place(every),
        "PMC not tried, no other route": place({"pmcid": "PMC1"}),
        "not in PMC": place(every, (("pmc", "absent"),)),
        "two OpenAlex errors": place(every, (pmc_failed, error, error)),
        "three OpenAlex errors": place(every, (pmc_failed, error, error, error)),
        "no OpenAlex copy": place({"host_pdf_urls": HOST}),
        "under a year old": place({**every, "pmcid": None, "published": dt.date(2026, 6, 1)}),
        "under a year old, free link": place({"host_pdf_urls": HOST, "published": TODAY}),
        "held": place(every, (), held("pdf", "openalex")),
    } == {
        "PMC not tried": ("waiting", "pmc-sync", "pmc"),
        "PMC not tried, no other route": ("waiting", "pmc-sync", "pmc"),
        "not in PMC": ("waiting", "openalex-backlog", "openalex"),
        "two OpenAlex errors": ("waiting", "openalex-backlog", "openalex"),
        "three OpenAlex errors": ("waiting", "host-link", "host"),
        "no OpenAlex copy": ("waiting", "host-link", "host"),
        "under a year old": ("waiting", "pmc-embargo", None),
        "under a year old, free link": ("waiting", "pmc-embargo", "host"),
        "held": ("fetched", None, None),
    }


def test_a_retracted_paper_is_skipped_and_one_no_tier_serves_is_manual_with_its_last_failure() -> (
    None
):
    every = {"openalex_pdf": True, "host_pdf_urls": HOST}
    assert {
        "retracted": place({**every, "retracted": True}),
        "placeholder": place({**every, "status": "skipped", "reason": "placeholder"}),
        "no source": place({}),
        "OpenAlex failed": place({"openalex_pdf": True}, (("openalex", "failed"),)),
        "host blocked": place(every, (("openalex", "failed"), ("host", "blocked"))),
        "host failed": place({"host_pdf_urls": HOST}, (("host", "failed"),)),
    } == {
        "retracted": ("skipped", "retracted", None),
        "placeholder": ("skipped", "placeholder", None),
        "no source": ("manual", "no-route", "paperpile"),
        "OpenAlex failed": ("manual", "openalex-failed", "paperpile"),
        "host blocked": ("manual", "host-blocked", "paperpile"),
        "host failed": ("manual", "host-failed", "paperpile"),
    }


def test_a_pmc_copy_replaces_the_pdf_a_later_tier_gave_and_the_tier_fields_follow_the_files() -> (
    None
):
    xml = held("xml", "pmc")
    listed = {**held("pdf", "pmc"), "name": "PMC1.pdf", "path": None}
    for tier in ("openalex", "host"):
        later = held("pdf", tier)
        assert ladder.replaced([xml, listed, later]) == [later]
        # With no XML from PMC, the later tier's PDF stays.
        assert ladder.replaced([listed, later]) == []

    # The row's own tier fields count for nothing.
    swapped = ladder.standing({"xml_tier": None, "pdf_tier": "openalex"}, [], [xml, listed], TODAY)
    assert (swapped.status, swapped.xml_tier, swapped.pdf_tier) == ("fetched", "pmc", None)
    host = ladder.standing({"xml_tier": "pmc", "pdf_tier": "pmc"}, [], [held("pdf", "host")], TODAY)
    assert (host.xml_tier, host.pdf_tier) == (None, "host")
