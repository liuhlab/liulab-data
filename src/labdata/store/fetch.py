"""Fetch live results into ``raw/``: SRA package XML and PubMed ``[si]`` searches.

Every call goes through :class:`~labdata.ncbi.entrez.EntrezClient`. Results are saved before
any build reads them, an empty result included.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from labdata.exceptions import EntrezError
from labdata.ncbi.entrez import EntrezClient
from labdata.store import raw, snapshots
from labdata.store.model import kind_of
from labdata.store.sources import split_packages

#: Experiments per ``efetch db=sra`` request.
SRA_BATCH = 200
#: How many times a batch's missing packages are requested again before failing.
SRA_RETRIES = 2
#: Upper bound on PMIDs kept per ``[si]`` search.
PUBMED_RETMAX = 10_000


@dataclass(frozen=True, slots=True)
class FetchReport:
    """What one fetch saved: the experiments and the ``[si]`` searches, by accession."""

    experiments: tuple[str, ...]
    pubmed_si: dict[str, tuple[str, ...]]


def fetch(
    root: Path,
    accessions: Iterable[str],
    *,
    client: EntrezClient,
    today: str | None = None,
) -> FetchReport:
    """Fetch the SRA packages and PubMed ``[si]`` results under some accessions into ``raw/``.

    A BioProject or SRA study is expanded to its experiments through the newest
    ``SRA_Accessions.tab`` snapshot; an experiment is taken as given. ``[si]`` is searched
    for each given BioProject and for every SRA study the packages name.

    Parameters
    ----------
    root : pathlib.Path
        The store root.
    accessions : iterable of str
        BioProject, SRA study or SRA experiment accessions.
    client : EntrezClient
        The Entrez seam.
    today : str or None
        The fetch date to record; defaults to today's UTC date.

    Returns
    -------
    FetchReport
        The experiments and searches saved.

    Raises
    ------
    EntrezError
        If NCBI keeps leaving out a requested package.
    """
    date = today or dt.datetime.now(dt.UTC).date().isoformat()
    given = sorted({a.strip().upper() for a in accessions})
    kinds = {a: kind_of(a) for a in given}
    experiments = {a for a, k in kinds.items() if k == "SRA experiment"}
    parents = {a for a, k in kinds.items() if k in ("BioProject", "SRA study")}
    experiments |= experiments_under(root, parents)
    packages = fetch_packages(client, sorted(experiments))
    raw.write_rows(
        root, raw.SRA_XML, [raw.RawRow(srx, text, date) for srx, text in packages.items()]
    )
    studies = {a for a, k in kinds.items() if k == "SRA study"}
    for text in packages.values():
        studies |= set(_study_refs(text))
    queried = sorted({a for a, k in kinds.items() if k == "BioProject"} | studies)
    results = {
        acc: tuple(client.esearch("pubmed", f"{acc}[si]", retmax=PUBMED_RETMAX)) for acc in queried
    }
    raw.write_rows(
        root, raw.PUBMED_SI, [raw.RawRow(a, "\n".join(p), date) for a, p in results.items()]
    )
    return FetchReport(tuple(sorted(packages)), results)


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
