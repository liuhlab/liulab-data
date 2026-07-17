r"""Accession dispatch: any GEO/SRA/BioProject accession -> the experiments under it.

The record classes each resolve *one* kind of accession (a :class:`Series` is a
``GSE``, a :class:`BioProject` is a ``PRJ…``). :func:`experiments_for` is the one
entry point that takes *any* of them and returns the SRA experiments (``SRX``)
beneath, so a caller holding a bare accession string need not know which class
owns it or how the hop is made.

Two resolution routes, chosen by accession kind:

* **link-based** — a GEO Series/Sample or a BioProject exposes an ``experiments``
  property that ``elink``\ s to the ``sra`` database. This traverses a GEO
  *SuperSeries* (which owns no runs of its own) and a BioProject umbrella
  transitively, which the plain "list this record's runs" endpoints do not.
* **search-based** — a study (``SRP``), SRA sample (``SRS``) or BioSample
  (``SAMN``) has no dedicated record class here, so its experiments are found by
  searching the ``sra`` database for the accession and building the hits.

An experiment/run accession resolves to itself / its parent with no search. An
empty list is a legitimate answer (an unreleased or data-less record); deciding
what *emptiness* means is the caller's job, not this resolver's.
"""

from __future__ import annotations

import re

from labdata.exceptions import AccessionError
from labdata.geo.bio_project_records import BioProject
from labdata.geo.geo_records import Sample, Series
from labdata.geo.sra_records import Experiment, Run, _experiments_from_uids
from labdata.ncbi.entrez import EntrezClient

#: Retmax for the ``sra`` ESearch route. NCBI caps a single request at 10 000
#: UIDs, which comfortably covers any real study/sample fan-out.
_SRA_SEARCH_RETMAX = 10000

#: Accession-kind patterns, in match order. ``[SED]R`` covers SRA/ENA/DDBJ.
_GSE = re.compile(r"^GSE\d+$")
_GSM = re.compile(r"^GSM\d+$")
_PRJ = re.compile(r"^PRJ[A-Z]{2}\d+$")
_SRX = re.compile(r"^[SED]RX\d+$")
_SRR = re.compile(r"^[SED]RR\d+$")
#: Study / SRA-sample / BioSample — no dedicated class, resolved by ``sra`` search.
_SEARCHABLE = re.compile(r"^(?:[SED]R[PS]\d+|SAM[NED]A?\d+)$")


def experiments_for(accession: str, *, client: EntrezClient | None = None) -> list[Experiment]:
    r"""Resolve any GEO/SRA/BioProject accession to the SRA experiments under it.

    Parameters
    ----------
    accession : str
        A GEO Series (``GSE``), GEO Sample (``GSM``), BioProject (``PRJNA``/
        ``PRJEB``/``PRJDB``), SRA study (``SRP``), SRA sample (``SRS``), BioSample
        (``SAMN``/``SAMEA``/``SAMD``), SRA experiment (``SRX``) or SRA run
        (``SRR``) accession. Case-insensitive; whitespace is stripped.
    client : EntrezClient or None
        Entrez client to share across the resolved records; a default one is built
        on first network access when ``None``.

    Returns
    -------
    list of Experiment
        The de-duplicated :class:`~labdata.geo.sra_records.Experiment` instances
        under ``accession`` (each sharing ``client``), or an empty list when the
        record links to no SRA data (e.g. an unreleased or metadata-only record).

    Raises
    ------
    AccessionError
        If ``accession`` is not a recognized GEO/SRA/BioProject accession.

    Examples
    --------
    >>> [e.accession for e in experiments_for("GSE229022")]  # doctest: +SKIP
    ['SRX19885398', 'SRX19885399', ...]
    """
    if not isinstance(accession, str):
        raise AccessionError(f"not a resolvable accession: {accession!r}")
    acc = accession.strip().upper()

    if _GSE.match(acc):
        return Series(acc, client=client).experiments
    if _GSM.match(acc):
        return Sample(acc, client=client).experiments
    if _PRJ.match(acc):
        return BioProject(acc, client=client).experiments
    if _SRX.match(acc):
        return [Experiment(acc, client=client)]
    if _SRR.match(acc):
        return [Run(acc, client=client).experiment]
    if _SEARCHABLE.match(acc):
        client = client or EntrezClient()
        sra_uids = client.esearch(db="sra", term=acc, retmax=_SRA_SEARCH_RETMAX)
        return _experiments_from_uids(client, sra_uids)

    raise AccessionError(
        f"not a resolvable accession: {accession!r}. Known kinds: GSE, GSM, "
        "PRJNA/PRJEB/PRJDB, SRP/ERP/DRP, SRS/ERS/DRS, SAMN/SAMEA/SAMD, "
        "SRX/ERX/DRX, SRR/ERR/DRR."
    )
