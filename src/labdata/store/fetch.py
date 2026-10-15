"""Fetch live results into ``raw/``: SOFT, ``filelist.txt``, ``elink``, SRA XML, PubMed ``[si]``.

Every call goes through :class:`~labdata.ncbi.entrez.EntrezClient`. Results are saved before
any build reads them, an empty result included. A fetch resumes: an item whose raw row is
already saved is read back, not fetched again, and rows are written chunk by chunk, so a
killed run keeps what it saved.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from labdata.exceptions import PageNotFound
from labdata.geo.sra_records import fetch_package_texts
from labdata.ncbi.entrez import EntrezClient, docsum_uid
from labdata.store import provenance, raw, snapshots, sources
from labdata.store.model import kind_of

#: Upper bound on PMIDs kept per ``[si]`` search.
PUBMED_RETMAX = 10_000
#: Items fetched between two writes of their raw rows.
CHUNK = 100
#: GEO's ``gds`` UID of a Series or Sample is this offset plus its number.
GDS_OFFSET = {"GSE": 200_000_000, "GSM": 300_000_000}

_SERIES_TYPE = re.compile(r"^!Series_type = (.*?)\s*$", re.M)
_SEQUENCING = "high throughput sequencing"
logger = logging.getLogger(__name__)

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

    A GEO Series brings its family SOFT and ``filelist.txt``, its SubSeries', the
    ``elink`` gds to sra (per sample) and gds to bioproject (per Series) results, and the SRA
    packages of every experiment those name; only sequencing Series are fetched (see
    :func:`fetch_series`). A BioProject or SRA study is expanded to its experiments through
    the newest ``SRA_Accessions.tab`` snapshot; an experiment is taken as given. ``[si]`` is
    searched for each Series, every BioProject given or linked, and every SRA study the
    packages name.

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
        lambda todo: fetch_package_texts(client, todo),
        date,
    )

    studies = {a for a, k in kinds.items() if k == "SRA study"}
    for text in packages.values():
        studies |= set(_study_refs(text))
    projects = {a for a, k in kinds.items() if k == "BioProject"}
    projects |= _linked_bioprojects(soft.values(), linked.values(), packages.values())
    queried = sorted(projects | studies | set(soft))
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
    for folder in sorted(p for p in (root / "raw").glob("*") if p.is_dir()):
        provenance.document_raw(root, folder.name)
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
    """Return the family SOFT of each sequencing Series and, recursively, of its SubSeries.

    A Series is kept when a ``!Series_type`` names high-throughput sequencing, or when it is
    a SuperSeries with a kept SubSeries. A skipped Series is logged with its types and not
    saved.
    """
    out: dict[str, str] = {}
    pending: dict[str, str] = {}
    seen: set[str] = set()
    todo = sorted(set(series))
    while todo:
        seen |= set(todo)
        texts = raw.saved(root, raw.GEO_SOFT, todo)
        out |= texts
        rest = [gse for gse in todo if gse not in texts]
        for start in range(0, len(rest), CHUNK):
            got = {gse: client.fetch_text(soft_url(gse)) for gse in rest[start : start + CHUNK]}
            kept = {gse: text for gse, text in got.items() if _sequencing(text)}
            raw.write_rows(root, raw.GEO_SOFT, [raw.RawRow(g, t, date) for g, t in kept.items()])
            out |= kept
            pending |= {gse: text for gse, text in got.items() if gse not in kept}
            texts |= got
        todo = sorted({sub for text in texts.values() for sub in sources.subseries(text)} - seen)
    while kept := {
        gse: text
        for gse, text in pending.items()
        if any(sub in out for sub in sources.subseries(text))
    }:
        raw.write_rows(root, raw.GEO_SOFT, [raw.RawRow(g, t, date) for g, t in kept.items()])
        out |= kept
        pending = {gse: text for gse, text in pending.items() if gse not in kept}
    for gse, text in sorted(pending.items()):
        types = "; ".join(_SERIES_TYPE.findall(text)) or "no type"
        logger.info("skipped %s: not high-throughput sequencing (%s)", gse, types)
    return out


def _sequencing(soft: str) -> bool:
    """Return whether a family SOFT's Series names a high-throughput sequencing type."""
    return any(_SEQUENCING in kind.lower() for kind in _SERIES_TYPE.findall(soft))


def filelist_url(series: str) -> str:
    """Return the URL of a Series' ``suppl/filelist.txt`` on GEO's download site."""
    return (
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{raw.bucket_of(series)}/{series}"
        "/suppl/filelist.txt"
    )


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
    """Return a page's text, or ``""`` when the page does not exist."""
    try:
        return client.fetch_text(url)
    except PageNotFound:
        return ""


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
        uid = docsum_uid(doc)
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


def _linked_bioprojects(
    soft: Iterable[str], linked: Iterable[str], packages: Iterable[str]
) -> set[str]:
    """Return the BioProjects the saved texts link to: the ones a build will hold."""
    links = [row for text in soft for row in sources.soft(text)[1]]
    links += [row for text in linked for row in sources.entrez_links(text)]
    links += [row for text in packages for row in sources.sra_package(text)[1]]
    return {row.to for row in links if row.relation == "BioProject"}


def _study_refs(package: str) -> list[str]:
    return re.findall(r'<STUDY_REF\b[^>]*?\baccession="([^"]+)"', package)
