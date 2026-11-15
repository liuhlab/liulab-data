"""The PMC sync: linked papers enter the store with IDs, details and status; PMC files follow.

Papers in scope are the PMIDs and DOIs that target GEO Series and held BioProjects link in
the GEO/SRA store's current build; a DOI counts only for a record that links no PMID. Only
papers enter: a placeholder PMID never does, and a DOI only once OpenAlex finds an article
for it. A paper is looked up at most once a month: its PMCID (if it has none), its PubMed
details and, while it is not fetched, its OpenAlex work. A paper no build links any more
keeps its rows, and so does one the store holds any file for.
"""

from __future__ import annotations

import datetime as dt
import time
from collections import Counter
from collections.abc import Callable, Collection
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import duckdb

from labdata._http import HttpTransport, Transport
from labdata.exceptions import LabdataError
from labdata.ncbi.entrez import EntrezClient
from labdata.papers import pmc
from labdata.papers.openalex import OpenAlex, Work
from labdata.papers.pubmed import Details, details, pmc_ids
from labdata.papers.root import PaperStore, is_placeholder, paper_dir, paper_key, paper_lock
from labdata.papers.status import read_status, write_status
from labdata.papers.tables import PaperTables
from labdata.store.gaps import build_targets
from labdata.store.layout import HOLDINGS, LINKS_BY_TO, sql_string
from labdata.store.reader import current_build, current_path
from labdata.store.targets import check_fraction, in_sample

#: The PubMed publication type of a retracted paper.
RETRACTED_TYPE = "Retracted Publication"

#: A paper published this many days ago or less waits for PMC.
EMBARGO_DAYS = 365

#: Papers looked up between two saves of the tables.
CHUNK = 1000

#: The reasons a row for a non-paper may carry; the sync removes such rows.
_NOT_PAPERS = ("placeholder", "not-article")

_RECORD_TYPES = ("GEO Series", "BioProject")


@dataclass
class SyncReport:
    """What one PMC sync did: papers added and looked up, lookups that failed, bucket replies.

    ``turned_away`` counts the DOIs OpenAlex found no article for.
    """

    added: int = 0
    looked_up: int = 0
    failed: int = 0
    rekeyed: int = 0
    turned_away: int = 0
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    bucket: pmc.Replies = field(default_factory=lambda: pmc.Replies({}, {}))


def linked_papers(build: Path, target_series: Collection[str]) -> dict[str, set[str]]:
    """Return every paper in scope of one build, mapped to the sources of its links.

    Papers in scope are those a target Series or a held BioProject links.

    Parameters
    ----------
    build : pathlib.Path
        The build folder.
    target_series : collection of str
        The target Series list the build was made from.

    Returns
    -------
    dict of str to set of str
        Paper keys (PMID, else lower-case DOI) and the ``source`` of each link to them.
    """
    con = duckdb.connect()
    found = con.execute(
        f'SELECT l."from", l."to", l.source, h.type '
        f"FROM read_parquet({sql_string(str(LINKS_BY_TO.path(build)))}) l "
        f"JOIN read_parquet({sql_string(str(HOLDINGS.path(build)))}) h "
        'ON h.accession = l."from" '
        f"WHERE h.type IN ({', '.join(sql_string(t) for t in _RECORD_TYPES)}) "
        "AND (l.\"to\" LIKE 'PMID:%' OR l.\"to\" LIKE 'DOI:%')"
    ).fetchall()
    rows = [(s, d, src) for s, d, src, t in found if t != "GEO Series" or s in target_series]
    with_pmid = {src for src, dst, _ in rows if dst.startswith("PMID:")}
    out: dict[str, set[str]] = {}
    for src, dst, source in rows:
        if dst.startswith("DOI:") and src in with_pmid:
            continue
        try:
            key = paper_key(dst)
        except ValueError:
            continue
        out.setdefault(key, set()).add(source)
    return out


def store_links(store: PaperStore, state: PaperTables) -> dict[str, set[str]]:
    """Return :func:`linked_papers` of the paper store's current GEO/SRA build.

    A DOI link to a paper the store keys by its PMID, or lists among a paper's
    ``other_dois``, counts for that paper.
    """
    geo = store.store.root
    build = current_path(geo)
    linked = linked_papers(build, build_targets(geo, build))
    by_doi = {r["doi"]: k for k, r in state.papers.items() if r.get("doi") and k.isdigit()}
    by_doi |= {d: k for k, r in state.papers.items() for d in r.get("other_dois") or ()}
    out: dict[str, set[str]] = {}
    for key, sources in linked.items():
        out.setdefault(by_doi.get(key, key), set()).update(sources)
    return out


def status_of(row: dict[str, Any], *, held: bool, today: dt.date) -> tuple[str, str | None]:
    """Return a paper's status and reason from its ``papers`` row and whether files are held.

    A ``manual`` paper whose reason is a failure a tier recorded keeps it until fetched.
    """
    if row.get("retracted"):
        return "skipped", "retracted"
    if held:
        return "fetched", None
    if row.get("status") == "manual" and row.get("reason") not in (None, "no-route"):
        return "manual", row["reason"]
    published = row.get("published")
    if published is not None and published > today - dt.timedelta(days=EMBARGO_DAYS):
        return "waiting", "pmc-embargo"
    if row.get("openalex_pdf"):
        return "waiting", "openalex-backlog"
    if row.get("host_pdf_urls"):
        return "waiting", "host-link"
    return "manual", "no-route"


def pmc_sync(
    store: PaperStore,
    *,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport | None = None,
    sample: float | None = None,
    log: Callable[[str], None] = print,
) -> SyncReport:
    """Run the PMC sync over the paper store's current GEO/SRA build.

    New linked papers are added; papers not looked up this month get their PMCID, PubMed
    details and, unless fetched, OpenAlex work; then the PMC bucket tier runs and every
    linked paper's status is recomputed. The tables are saved after each chunk of papers, so
    a killed sync resumes where it stopped. ``paper-status.json`` records the pass.

    Parameters
    ----------
    store : PaperStore
        The paper store.
    client : EntrezClient
        The NCBI client: PubMed efetch and the ID converter.
    openalex : OpenAlex
        The OpenAlex client.
    today : datetime.date
        The UTC date of the pass.
    transport : Transport or None
        Sends the PMC bucket's requests; kept-open connections by default.
    sample : float or None
        Take only this seeded share of the linked papers, for a trial; ``None`` takes all.
        Only a sync over all papers records its date and the build it read as the last
        successful PMC sync.
    log : callable
        Takes one progress line.

    Returns
    -------
    SyncReport
        Counts of what the sync did, and papers by status and reason.
    """
    sample = check_fraction(sample)
    root = store.root
    with paper_lock(root):
        start = _now()
        # Read before the sync: a build that becomes current during it makes the next one due.
        build = current_build(store.store.root)

        def last_pass(end: str | None, outcome: str) -> dict[str, Any]:
            return {
                "kind": "pmc-sync",
                "start": start,
                "end": end,
                "outcome": outcome,
                "sample": sample,
            }

        def with_bucket(record: dict[str, Any]) -> dict[str, Any]:
            return {
                **record,
                "bucket_retries": report.bucket.retries,
                "bucket_throttled": report.bucket.throttled,
            }

        _record(root, last_pass(None, "running"))
        report = SyncReport()
        try:
            _sync(store, client, openalex, today, transport, sample, log, report)
        except BaseException:
            _record(root, with_bucket(last_pass(_now(), "failed")))
            raise
        status = read_status(root)
        write_status(
            root,
            replace(
                status,
                last_pass=with_bucket(last_pass(_now(), "success")),
                # A sampled sync is a backfill round, never the month's sync.
                last_pmc_sync_success=(
                    today.isoformat() if sample is None else status.last_pmc_sync_success
                ),
                last_pmc_sync_build=build if sample is None else status.last_pmc_sync_build,
                counts=report.counts,
            ),
        )
    return report


def _sync(
    store: PaperStore,
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    transport: Transport | None,
    sample: float | None,
    log: Callable[[str], None],
    report: SyncReport,
) -> None:
    root = store.root
    state = PaperTables.load(root)
    _drop(
        state,
        {
            k
            for k, r in state.papers.items()
            if r.get("status") == "skipped" and r.get("reason") in _NOT_PAPERS
        },
    )
    linked = store_links(store, state)
    if sample is not None:
        linked = {k: v for k, v in linked.items() if in_sample(k, sample)}
    placeholders = {k for k, sources in linked.items() if is_placeholder(k, sources)}
    linked = {k: v for k, v in linked.items() if k not in placeholders}
    # A DOI enters only once OpenAlex finds an article for it.
    for key in linked:
        if key.isdigit() and key not in state.papers:
            state.papers[key] = _new_row(key, today)
            report.added += 1
    month = (today.year, today.month)
    todo = [
        k
        for k in linked
        if k not in state.papers
        or state.papers[k].get("looked_up") is None
        or (state.papers[k]["looked_up"].year, state.papers[k]["looked_up"].month) != month
    ]
    log(
        f"{len(linked)} linked papers, {report.added} new, {len(todo)} to look up, "
        f"{len(placeholders)} placeholders left out"
    )
    clock = time.monotonic()
    for i in range(0, len(todo), CHUNK):
        chunk = todo[i : i + CHUNK]
        rekeyed = _look_up(state, chunk, client, openalex, today, report, log, root)
        for old, new in rekeyed.items():
            linked[new] = linked.pop(old, set()) | linked.get(new, set())
        state.save(root)
        log(
            f"looked up {min(i + CHUNK, len(todo))}/{len(todo)} papers "
            f"in {time.monotonic() - clock:.0f} s; {report.failed} failed, "
            f"{report.turned_away} DOIs turned away"
        )
    linked = {k: v for k, v in linked.items() if k in state.papers}
    report.bucket = pmc.fetch(
        state,
        list(linked),
        root=root,
        transport=transport or HttpTransport(),
        today=today,
        log=log,
        on_failure=lambda replies: setattr(report, "bucket", replies),
    )
    held = state.held()
    for key in linked:
        row = state.papers[key]
        row["status"], row["reason"] = status_of(row, held=key in held, today=today)
    state.save(root)
    counts: dict[str, Counter[str]] = {}
    for row in state.papers.values():
        counts.setdefault(row["status"] or "", Counter())[row["reason"] or ""] += 1
    report.counts = {s: dict(c) for s, c in counts.items()}


def _new_row(key: str, today: dt.date) -> dict[str, Any]:
    pmid = int(key) if key.isdigit() else None
    return {"paper": key, "pmid": pmid, "doi": None if pmid else key, "added": today}


def _look_up(
    state: PaperTables,
    chunk: list[str],
    client: EntrezClient,
    openalex: OpenAlex,
    today: dt.date,
    report: SyncReport,
    log: Callable[[str], None],
    root: Path,
) -> dict[str, str]:
    """Look one chunk of papers up, changing ``state``; return the DOI papers re-keyed.

    A DOI OpenAlex finds no article for leaves no row, unless the store holds a file of it.
    """
    failed: set[str] = set()
    away: set[str] = set()
    rekeyed: dict[str, str] = {}
    dois = [k for k in chunk if not k.isdigit()]
    if dois:
        try:
            works = openalex.works_by_doi(dois)
        except LabdataError as err:
            log(f"OpenAlex DOI lookup failed: {err}")
            failed.update(dois)
            works = {}
        filed = {f["paper"] for f in state.files}
        for doi in dois:
            if doi in failed:
                continue
            work = works.get(doi)
            if work is None or not work.is_article:
                if doi in filed and doi in state.papers:
                    _set_work(state.papers[doi], work)
                else:
                    away.add(doi)
                continue
            # OpenAlex may answer with another work, such as the journal version it merged a
            # preprint into: the paper is that work, under its PMID, else its DOI.
            key = work.pmid or work.doi or doi
            if doi not in state.papers:
                if key not in state.papers:
                    report.added += 1
                state.papers[doi] = _new_row(doi, today)
            row = state.papers[doi]
            _set_work(row, work)
            row["published"] = work.published
            if key != doi:
                _rekey(state, root, doi, key, work.doi or doi)
                rekeyed[doi] = key
                report.rekeyed += 1
            if not key.isdigit():
                _set_work(state.papers[key], work)
                state.papers[key]["published"] = work.published
                state.info[key] = _work_info(key, work)
    pmids = list(dict.fromkeys(key for k in chunk if (key := rekeyed.get(k, k)).isdigit()))
    if pmids:
        ids: dict[str, tuple[str | None, str | None]] = {}
        try:
            ids = pmc_ids(client, [k for k in pmids if not state.papers[k].get("pmcid")])
        except LabdataError as err:
            log(f"ID converter failed: {err}")
            failed.update(pmids)
        found: dict[str, Details] = {}
        try:
            found = details(client, pmids)
        except LabdataError as err:
            log(f"PubMed efetch failed: {err}")
            failed.update(pmids)
        works = {}
        held = state.held()
        wanted = [k for k in pmids if k not in held]
        try:
            works = openalex.works_by_pmid(wanted)
        except LabdataError as err:
            log(f"OpenAlex PMID lookup failed: {err}")
            failed.update(wanted)
        for key in pmids:
            if key in failed:
                continue
            row = state.papers[key]
            pmcid, idconv_doi = ids.get(key, (None, None))
            info = found.get(key)
            work = works.get(key)
            if key in wanted:
                _set_work(row, work)
            same = work is not None and work.pmid == key
            row["pmcid"] = (info and info.pmcid) or pmcid or row.get("pmcid")
            row["doi"] = (
                (info and info.doi) or idconv_doi or (work.doi if same and work else None)
            ) or row.get("doi")
            row["published"] = (
                (info and info.published) or (work and work.published) or row.get("published")
            )
            if info is not None:
                state.info[key] = _pubmed_info(key, row, info)
                if RETRACTED_TYPE in info.publication_type:
                    row["retracted"] = True
    _drop(state, away)
    for key in (rekeyed.get(k, k) for k in chunk):
        if key in failed:
            report.failed += 1
        elif key in away:
            report.turned_away += 1
        else:
            state.papers[key]["looked_up"] = today
            report.looked_up += 1
    return rekeyed


def _set_work(row: dict[str, Any], work: Work | None) -> None:
    row["openalex"] = work.openalex if work else None
    row["work_type"] = work.type if work else None
    row["openalex_pdf"] = work.has_pdf if work else False
    row["host_pdf_urls"] = list(work.pdf_urls) if work else []
    if work is not None and work.retracted:
        row["retracted"] = True


def _pubmed_info(key: str, row: dict[str, Any], info: Details) -> dict[str, Any]:
    return {
        "paper": key,
        "pmid": int(key),
        "pmcid": row.get("pmcid"),
        "doi": row.get("doi"),
        "title": info.title,
        "journal": info.journal,
        "published": info.published,
        "authors": list(info.authors),
        "abstract": info.abstract,
        "publication_type": list(info.publication_type),
    }


def _work_info(key: str, work: Work) -> dict[str, Any]:
    return {
        "paper": key,
        "doi": key,
        "title": work.title,
        "journal": work.journal,
        "published": work.published,
        "authors": list(work.authors),
        "abstract": work.abstract,
        "publication_type": [work.type] if work.type else [],
    }


def _rekey(state: PaperTables, root: Path, doi: str, key: str, own_doi: str) -> None:
    """Move a DOI paper to ``key``, the PMID or DOI of the paper it is, whose DOI is ``own_doi``.

    Rows are re-keyed and files renamed; nothing is fetched again. ``doi`` joins the paper's
    ``other_dois`` unless it is the paper's DOI. When the store already holds ``key``'s
    paper, the DOI paper's files join it unless it holds a file of the same kind and name.
    """
    old_row = state.papers.pop(doi)
    state.info.pop(doi, None)
    if key not in state.papers:
        pmid = int(key) if key.isdigit() else None
        state.papers[key] = {**old_row, "paper": key, "pmid": pmid, "doi": own_doi}
    target = state.papers[key]
    target["doi"] = target.get("doi") or own_doi
    for tier in ("xml_tier", "pdf_tier"):
        target[tier] = target.get(tier) or old_row.get(tier)
    others = {*(target.get("other_dois") or ()), *(old_row.get("other_dois") or ()), doi}
    target["other_dois"] = sorted(others - {target["doi"]}) or None
    old_dir, new_dir = paper_dir(doi), paper_dir(key)
    taken = {(f["kind"], f["name"]) for f in state.files if f["paper"] == key}
    kept = []
    for f in state.files:
        if f["paper"] != doi:
            kept.append(f)
            continue
        name = f["name"]
        if f["kind"] in ("xml", "pdf") and name.startswith(old_dir.name):
            name = new_dir.name + name[len(old_dir.name) :]
        if (f["kind"], name) in taken:
            continue
        path = f["path"]
        if path:
            rel = Path(path).relative_to(old_dir)
            new_path = new_dir / rel.parent / name if f["kind"] != "supplement" else new_dir / rel
            (root / new_path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).replace(root / new_path)
            path = new_path.as_posix()
        kept.append({**f, "paper": key, "name": name, "path": path})
    state.files[:] = kept
    for attempt in state.attempts:
        if attempt["paper"] == doi:
            attempt["paper"] = key
    _remove_empty(root / old_dir)


def _drop(state: PaperTables, keys: set[str]) -> None:
    """Remove ``keys`` from every table, except a paper the ``files`` table lists.

    Its files stay on disk, so its rows stay too: the GCS copy deletes a file its row left.
    """
    keys = keys - {f["paper"] for f in state.files}
    if not keys:
        return
    for key in keys:
        state.papers.pop(key, None)
        state.info.pop(key, None)
    state.files[:] = [f for f in state.files if f["paper"] not in keys]
    state.attempts[:] = [a for a in state.attempts if a["paper"] not in keys]


def _remove_empty(folder: Path) -> None:
    """Remove a paper folder that its files have left, empty sub-folders first."""
    if not folder.is_dir():
        return
    for sub in sorted(folder.rglob("*"), reverse=True):
        if sub.is_dir():
            try:
                sub.rmdir()
            except OSError:
                return
    folder.rmdir()


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _record(root: Path, last_pass: dict[str, Any]) -> None:
    write_status(root, replace(read_status(root), last_pass=last_pass))
