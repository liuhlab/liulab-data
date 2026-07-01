r"""BioProject — the umbrella record a GEO Series links out to.

A BioProject (``PRJNA``/``PRJEB``/``PRJDB``) groups a study's GEO Series and SRA
data. It lives in the Entrez ``bioproject`` database (:class:`BioProject`), whose
esummary uses the v2.0 ``DocumentSummarySet`` shape. Like the GEO/SRA records it is
a lazy handle: construction only validates the accession; properties fetch and
cache on first access.

Its ``series`` and ``experiments`` return :class:`~labdata.geo.geo_records.Series`
and :class:`~labdata.geo.sra_records.Experiment` instances (sharing this record's
Entrez client), and it inherits :meth:`~labdata.geo.sratools._SraDownloadMixin.download`
so a whole project can be fetched with sra-tools.
"""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any

from labdata.exceptions import EntrezError
from labdata.geo._base import _Record
from labdata.geo.geo_records import Series
from labdata.geo.sra_records import Experiment, _linked_experiments
from labdata.geo.sratools import _SraDownloadMixin
from labdata.ncbi.entrez import _docsum_uid


class BioProject(_SraDownloadMixin, _Record):
    r"""An NCBI BioProject (``PRJNA000000``), resolved lazily through NCBI Entrez.

    BioProject is the umbrella record that groups a study's GEO Series and SRA
    data. It lives in the Entrez ``bioproject`` database, whose esummary uses the
    v2.0 ``DocumentSummarySet`` shape.

    Parameters
    ----------
    accession : str
        A BioProject accession, e.g. ``"PRJNA545296"`` (``^PRJ[A-Z]{2}\d+$``,
        covering NCBI ``PRJNA``, EBI ``PRJEB``, and DDBJ ``PRJDB``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed BioProject accession.
    """

    _ACCESSION_RE = re.compile(r"^PRJ[A-Z]{2}\d+$")
    _KIND = "BioProject"

    @cached_property
    def uid(self) -> str:
        """The Entrez ``bioproject`` UID for this record.

        Raises
        ------
        EntrezError
            If NCBI returns no matching ``bioproject`` record.
        """
        uids = self.client.esearch(db="bioproject", term=self.accession)
        if not uids:
            raise EntrezError(f"no BioProject found for {self.accession!r}")
        return uids[0]

    @cached_property
    def _summary(self) -> dict[str, Any]:
        """The raw ``bioproject`` esummary docsum for this record (fetched once)."""
        return self.client.esummary(db="bioproject", uid=self.uid)

    @property
    def title(self) -> str:
        """The project title."""
        return str(self._summary.get("Project_Title", ""))

    @property
    def name(self) -> str:
        """The project name."""
        return str(self._summary.get("Project_Name", ""))

    @property
    def description(self) -> str:
        """The free-text project description."""
        return str(self._summary.get("Project_Description", ""))

    @property
    def organism(self) -> str:
        """The organism the project targets."""
        return str(self._summary.get("Organism_Name", ""))

    @property
    def data_type(self) -> str:
        """The project data type (e.g. ``Transcriptome or Gene expression``)."""
        return str(self._summary.get("Project_Data_Type", ""))

    @property
    def registration_date(self) -> str:
        """The project's registration date as reported by NCBI."""
        return str(self._summary.get("Registration_Date", ""))

    @property
    def submitter(self) -> str:
        """The submitting organization."""
        return str(self._summary.get("Submitter_Organization", ""))

    @property
    def url(self) -> str:
        """The NCBI BioProject web URL for this record."""
        return f"https://www.ncbi.nlm.nih.gov/bioproject/{self.accession}"

    @cached_property
    def series(self) -> list[Series]:
        """The GEO Series (``GSE``) under this project, as :class:`Series` instances.

        One ``elink`` plus one batched ``esummary``; each :class:`Series` is seeded
        with the UID and summary already fetched.
        """
        gds_uids = self.client.elink(dbfrom="bioproject", db="gds", uid=self.uid)
        found: dict[str, Series] = {}
        for docsum in self.client.esummary_many(db="gds", uids=gds_uids):
            if str(docsum.get("entryType", "")) != "GSE":
                continue
            accession = str(docsum.get("Accession", ""))
            if accession and accession not in found:
                series = Series(accession, client=self.client)
                series._seed(uid=_docsum_uid(docsum), summary=dict(docsum))
                found[accession] = series
        return list(found.values())

    @cached_property
    def experiments(self) -> list[Experiment]:
        """The SRA experiments (``SRX``) under this project, as :class:`Experiment` instances."""
        return _linked_experiments(self.client, self.uid, dbfrom="bioproject")
