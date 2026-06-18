r"""GEO Series (``GSE…``) — metadata, linked publication, samples, and SRA experiments.

A :class:`Series` is a lazy handle on one GEO Series record. Construction only
validates and stores the accession; each property issues its Entrez request on
first access and caches the result, so a freshly built :class:`Series` is cheap
and network traffic happens only for the fields you actually read.
"""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any

from labdata.exceptions import AccessionError, EntrezError
from labdata.ncbi.entrez import EntrezClient

#: GEO Series accessions are ``GSE`` followed by one or more digits.
_GSE_RE = re.compile(r"^GSE\d+$")

#: SRA experiment accessions (``SRX``/``ERX``/``DRX`` + digits) embedded in SRA ExpXml.
_SRX_RE = re.compile(r"\b([SED]RX\d+)\b")


class Series:
    r"""A GEO Series (``GSE000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        A GEO Series accession, e.g. ``"GSE131907"``. Matched case-sensitively
        against ``^GSE\d+$``.
    client : EntrezClient or None
        The Entrez client to use. When ``None`` (default) one is constructed on
        first network access, resolving NCBI credentials at that point.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed GEO Series accession.

    Examples
    --------
    >>> s = Series("GSE131907")          # doctest: +SKIP
    >>> s.pubmed_id                       # doctest: +SKIP
    '32385277'
    >>> s.samples[:2]                     # doctest: +SKIP
    ['GSM3828672', 'GSM3828673']
    """

    def __init__(self, accession: str, *, client: EntrezClient | None = None) -> None:
        if not isinstance(accession, str) or not _GSE_RE.match(accession):
            raise AccessionError(
                f"not a GEO Series accession (expected 'GSE' + digits): {accession!r}"
            )
        self.accession = accession
        self._client = client

    def __repr__(self) -> str:
        """Return an unambiguous representation."""
        return f"Series({self.accession!r})"

    @property
    def client(self) -> EntrezClient:
        """Return the Entrez client, constructing a default one on first use."""
        if self._client is None:
            self._client = EntrezClient()
        return self._client

    @cached_property
    def uid(self) -> str:
        """The GEO DataSets (``gds``) UID for this Series.

        Returns
        -------
        str
            The internal Entrez UID.

        Raises
        ------
        EntrezError
            If NCBI returns no ``gds`` record for the accession.
        """
        term = f"{self.accession}[ACCN] AND gse[ETYP]"
        uids = self.client.esearch(db="gds", term=term)
        if not uids:
            raise EntrezError(f"no GEO Series found for {self.accession!r}")
        return uids[0]

    @cached_property
    def _summary(self) -> dict[str, Any]:
        """The raw ``gds`` esummary docsum for this Series (fetched once)."""
        return self.client.esummary(db="gds", uid=self.uid)

    @property
    def title(self) -> str:
        """The Series title."""
        return str(self._summary.get("title", ""))

    @property
    def summary(self) -> str:
        """The Series summary/abstract text."""
        return str(self._summary.get("summary", ""))

    @property
    def organism(self) -> str:
        """The organism (taxon) the Series targets."""
        return str(self._summary.get("taxon", ""))

    @cached_property
    def platforms(self) -> list[str]:
        """The platform (``GPL``) accessions this Series uses.

        Read directly from the ``gds`` summary (no extra request). The summary
        stores bare platform numbers; they are normalized to ``GPL…`` form.

        Returns
        -------
        list of str
            Platform accessions, e.g. ``["GPL16791"]``.
        """
        raw = str(self._summary.get("GPL", ""))
        tokens = (tok for tok in re.split(r"[;,\s]+", raw) if tok)
        return [tok.upper() if tok.upper().startswith("GPL") else f"GPL{tok}" for tok in tokens]

    @cached_property
    def supplementary_file_types(self) -> list[str]:
        """The supplementary-file *types* GEO lists for this Series.

        Read directly from the ``gds`` summary (no extra request).

        Returns
        -------
        list of str
            File-type tokens, e.g. ``["RDS", "TXT", "XLSX"]``.

        Notes
        -----
        E-utilities exposes only the file *types* (extensions), not individual
        file names or URLs. The full supplementary-file list lives in the GEO
        SOFT/MINiML record or the series FTP directory, neither of which is an
        E-utilities endpoint, so it is intentionally not provided here.
        """
        raw = str(self._summary.get("suppFile", ""))
        return [tok.strip() for tok in raw.split(",") if tok.strip()]

    @cached_property
    def bioproject_ids(self) -> list[str]:
        """The BioProject accessions (``PRJNA…``) linked to this Series.

        Resolved by linking the ``gds`` record to BioProject (``elink``) and
        reading each linked project's ``Project_Acc`` from its summary.

        Returns
        -------
        list of str
            Unique BioProject accessions; empty when the Series links to none.
        """
        uids = self.client.elink(dbfrom="gds", db="bioproject", uid=self.uid)
        found: dict[str, None] = {}
        for bp_uid in uids:
            accession = str(
                self.client.esummary(db="bioproject", uid=bp_uid).get("Project_Acc", "")
            )
            if accession:
                found.setdefault(accession, None)
        return list(found)

    @cached_property
    def pubmed_id(self) -> str | None:
        """The PubMed ID of the associated publication, if any.

        Returns
        -------
        str or None
            The first linked PubMed ID, or ``None`` when the Series lists none.
        """
        pmids = self._summary.get("PubMedIds") or []
        return str(pmids[0]) if pmids else None

    @cached_property
    def samples(self) -> list[str]:
        """The sample (``GSM``) accessions belonging to this Series.

        Returns
        -------
        list of str
            GSM accessions, in the order GEO reports them.
        """
        return [str(sample["Accession"]) for sample in self._summary.get("Samples", [])]

    @cached_property
    def experiments(self) -> list[str]:
        """The SRA experiment (``SRX``) accessions linked to this Series.

        Resolved by linking the ``gds`` record to SRA (``elink``) and scanning
        each linked SRA record's ``ExpXml`` for an experiment accession. SRA's
        summary XML is loosely structured, so accessions are extracted by regex.

        Returns
        -------
        list of str
            Unique SRA experiment accessions; empty when the Series has no SRA data.
        """
        sra_uids = self.client.elink(dbfrom="gds", db="sra", uid=self.uid)
        found: dict[str, None] = {}
        for sra_uid in sra_uids:
            docsum = self.client.esummary(db="sra", uid=sra_uid)
            for accession in _SRX_RE.findall(str(docsum.get("ExpXml", ""))):
                found.setdefault(accession, None)
        return list(found)
