"""Fetch live results into ``raw/``: SOFT, ``filelist.txt``, ``elink``, SRA XML, PubMed ``[si]``.

Every call goes through :class:`~labdata.ncbi.entrez.EntrezClient`. Results are saved before
any build reads them, an empty result included. A fetch resumes: an item whose raw row is
already saved is read back, not fetched again, and rows are written chunk by chunk, so a
killed run keeps what it saved.
"""

from __future__ import annotations

import datetime as dt
import re
import urllib.error
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from labdata.exceptions import EntrezError
from labdata.ncbi.entrez import EntrezClient, _docsum_uid
from labdata.store import raw, snapshots, sources
from labdata.store.model import kind_of
from labdata.store.sources import split_packages

#: Experiments per ``efetch db=sra`` request.
SRA_BATCH = 200
#: How many times a batch's missing packages are requested again before failing.
SRA_RETRIES = 2
#: Upper bound on PMIDs kept per ``[si]`` search.
PUBMED_RETMAX = 10_000
#: Items fetched between two writes of their raw rows.
CHUNK = 100
#: GEO's ``gds`` UID of a Series or Sample is this offset plus its number.
GDS_OFFSET = {"GSE": 200_000_000, "GSM": 300_000_000}

_SOFT_SRX = re.compile(r"^!Sample_relation = SRA: \S*?[?&]term=([SED]RX\d+)", re.M)


@dataclass(frozen=True, slots=True)
class FetchReport:
    """What one fetch holds in ``raw/``: Series, experiments and ``[si]`` searches."""

    experiments: tuple[str, ...]
    pubmed_si: dict[str, tuple[str, ...]]
    series: tuple[str, ...] = field(default=())


def fetch(
    root: Path,
    accessions: Iterable[str],
    *,
    client: EntrezClient,
    today: str | None = None,
) -> FetchReport:
    """Fetch the raw text under some accessions into ``raw/``, skipping what is saved.

    A GEO Series brings its family SOFT and ``filelist.txt``, its SubSeries', the ``elink`` gds to sra (per
    sample) and gds to bioproject (per Series) results, and the SRA packages of every
    experiment those name. A BioProject or SRA study is expanded to its experiments through
    the newest ``SRA_Accessions.tab`` snapshot; an experiment is taken as given. ``[si]`` is
    searched for each Series, each given BioProject and every SRA study the packages name.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    accessions : iterable of str
        GEO Series, BioProject, SRA study or SRA experiment accessions.
    client : EntrezClient
        The Entrez seam.
    today : str or None
        The fetch date to record; defaults to today's UTC date.

    Returns
    -------
    FetchReport
        Every Series, experiment and search now saved for these accessions.

    Raises
    ------
    EntrezError
        If NCBI keeps leaving out a requested package.
    """
    date = today or dt.datetime.now(dt.UTC).date().isoformat()
    given = sorted({a.strip().upper() for a in accessions})
    kinds = {a: kind_of(a) for a in given}
    soft = fetch_series(root, client, [a for a, k in kinds.items() if k == "GEO Series"], date)
    fetch_filelists(root, client, sorted(soft), date)
    linked = fetch_entrez_links(root, client, soft, date)

    experiments = {a for a, k in kinds.items() if k == "SRA experiment"}
    experiments |= experiments_under(
        root, {a for a, k in kinds.items() if k in ("BioProject", "SRA study")}
    )
    for text in soft.values():
        experiments |= set(_SOFT_SRX.findall(text))
    for text in linked.values():
        experiments |= {row.to for row in sources.entrez_links(text) if row.relation == "SRA"}
    packages = _resume(
        root,
        raw.SRA_XML,
        sorted(experiments),
        lambda todo: fetch_packages(client, todo),
        date,
        SRA_BATCH,
    )

    studies = {a for a, k in kinds.items() if k == "SRA study"}
    for text in packages.values():
        studies |= set(_study_refs(text))
    queried = sorted({a for a, k in kinds.items() if k == "BioProject"} | studies | set(soft))
    searched = _resume(
        root,
        raw.PUBMED_SI,
        queried,
        lambda todo: {
            acc: "\n".join(client.esearch("pubmed", f"{acc}[si]", retmax=PUBMED_RETMAX))
            for acc in todo
        },
        date,
    )
    return FetchReport(
        tuple(sorted(packages)),
        {acc: tuple(text.split()) for acc, text in sorted(searched.items())},
        tuple(sorted(soft)),
    )


def _resume(
    root: Path,
    source: str,
    accessions: list[str],
    get: Callable[[list[str]], dict[str, str]],
    date: str,
    chunk: int = CHUNK,
) -> dict[str, str]:
    """Return each accession's raw text: read back if saved, else fetched and saved per chunk."""
    out = raw.saved(root, source, accessions)
    todo = [a for a in accessions if a not in out]
    for start in range(0, len(todo), chunk):
        got = get(todo[start : start + chunk])
        raw.write_rows(root, source, [raw.RawRow(a, text, date) for a, text in got.items()])
        out.update(got)
    return out


def soft_url(series: str) -> str:
    """Return the URL of a Series' gzipped family SOFT on GEO's download site."""
    return (
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{raw.bucket_of(series)}/{series}"
        f"/soft/{series}_family.soft.gz"
    )


def fetch_series(root: Path, client: EntrezClient, series: list[str], date: str) -> dict[str, str]:
    """Return the family SOFT of each Series and, recursively, of its SubSeries."""
    out: dict[str, str] = {}
    todo = sorted(set(series))
    while todo:
        out |= _resume(
            root,
            raw.GEO_SOFT,
            todo,
            lambda chunk: {gse: client.fetch_text(soft_url(gse)) for gse in chunk},
            date,
        )
        todo = sorted({sub for gse in todo for sub in sources.subseries(out[gse])} - set(out))
    return out


def filelist_url(series: str) -> str:
    """Return the URL of a Series' ``suppl/filelist.txt`` on GEO's download site."""
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{raw.bucket_of(series)}/{series}/suppl/filelist.txt"


def fetch_filelists(
    root: Path, client: EntrezClient, series: list[str], date: str
) -> dict[str, str]:
    """Return each Series' ``filelist.txt``; a Series without one gets an empty text."""
    return _resume(
        root,
        raw.GEO_FILELIST,
        series,
        lambda chunk: {gse: _text_or_empty(client, filelist_url(gse)) for gse in chunk},
        date,
    )


def _text_or_empty(client: EntrezClient, url: str) -> str:
    """Return a page's text, or ``""`` when the server answers 404."""
    try:
        return client.fetch_text(url)
    except EntrezError as err:
        cause = err.__cause__
        if isinstance(cause, urllib.error.HTTPError) and cause.code == 404:
            return ""
        raise


def fetch_entrez_links(
    root: Path, client: EntrezClient, soft: dict[str, str], date: str
) -> dict[str, str]:
    """Return each Series' ``elink`` results as text: sample to sra and Series to bioproject.

    One ``elink`` per target database and one ``esummary`` per database resolve a whole
    chunk of Series.
    """

    def get(chunk: list[str]) -> dict[str, str]:
        samples = {
            gse: sorted(a for e, a, _ in sources.soft_blocks(soft[gse]) if e == "SAMPLE")
            for gse in chunk
        }
        sample_uids = {_gds_uid(gsm): gsm for gsms in samples.values() for gsm in gsms}
        series_uids = {_gds_uid(gse): gse for gse in chunk}
        to_sra = client.elink_many("gds", "sra", sorted(sample_uids)) if sample_uids else {}
        to_bp = client.elink_many("gds", "bioproject", sorted(series_uids))
        names = _accessions(client, "sra", {u for us in to_sra.values() for u in us})
        names |= _accessions(client, "bioproject", {u for us in to_bp.values() for u in us})
        lines: dict[str, list[str]] = {gse: [] for gse in chunk}
        for gse in chunk:
            for uid in to_bp.get(_gds_uid(gse), []):
                lines[gse].append(f"{gse}\tbioproject\t{uid}\t{names.get(uid, '')}")
            for gsm in samples[gse]:
                for uid in to_sra.get(_gds_uid(gsm), []):
                    lines[gse].append(f"{gsm}\tsra\t{uid}\t{names.get(uid, '')}")
        return {gse: "\n".join(sorted(rows)) for gse, rows in lines.items()}

    return _resume(root, raw.ENTREZ_LINKS, sorted(soft), get, date)


def _gds_uid(accession: str) -> str:
    return str(GDS_OFFSET[accession[:3]] + int(accession[3:]))


def _accessions(client: EntrezClient, db: str, uids: set[str]) -> dict[str, str]:
    """Map each UID to its accession through one batched ``esummary``."""
    if not uids:
        return {}
    out = {}
    for doc in client.esummary_many(db, sorted(uids)):
        uid = _docsum_uid(doc)
        if db == "sra":
            match = re.search(r'<Experiment\b[^>]*?\bacc="([^"]+)"', str(doc.get("ExpXml", "")))
            accession = match[1] if match else ""
        else:
            accession = str(doc.get("Project_Acc", ""))
        if uid and accession:
            out[uid] = accession
    return out


def experiments_under(root: Path, parents: set[str]) -> set[str]:
    """Return the experiments ``SRA_Accessions.tab`` lists under BioProjects or SRA studies."""
    if not parents:
        return set()
    tab = snapshots.snapshot(root, snapshots.SRA_ACCESSIONS).path
    found: set[str] = set()
    for row in snapshots.tab_rows(
        tab,
        lambda r: (
            r.get("Type") in ("EXPERIMENT", "RUN")
            and (r.get("BioProject") in parents or r.get("Study") in parents)
        ),
        line_filter=lambda line: any(parent in line for parent in parents),
    ):
        found.add(row["Accession"] if row["Type"] == "EXPERIMENT" else row["Experiment"])
    found.discard("")
    return found


def fetch_packages(client: EntrezClient, experiments: list[str]) -> dict[str, str]:
    """Fetch each experiment's ``EXPERIMENT_PACKAGE``, checking that every one came back.

    Raises
    ------
    EntrezError
        If packages are still missing after :data:`SRA_RETRIES` further requests.
    """
    out: dict[str, str] = {}
    for start in range(0, len(experiments), SRA_BATCH):
        todo = experiments[start : start + SRA_BATCH]
        for _attempt in range(SRA_RETRIES + 1):
            reply = client.efetch(db="sra", ids=todo, rettype="full", retmode="xml")
            got = split_packages(reply)
            out.update((srx, got[srx]) for srx in todo if srx in got)
            todo = [srx for srx in todo if srx not in got]
            if not todo:
                break
        if todo:
            raise EntrezError(
                f"efetch db=sra left out {len(todo)} requested packages: {', '.join(todo)}"
            )
    return out


def _study_refs(package: str) -> list[str]:
    return re.findall(r'<STUDY_REF\b[^>]*?\baccession="([^"]+)"', package)
