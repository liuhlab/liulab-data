r"""The GEO object model: Series, Sample, Platform, Experiment, and Run.

Each class is a lazy handle on one accessioned record. Construction only
validates and stores the accession; properties issue their NCBI request on first
access and cache it (``functools.cached_property``), so building a record is
cheap and network traffic happens only for the fields you read.

Two families share most of the plumbing:

* **GEO records** (``GSE``/``GSM``/``GPL``) live in the Entrez ``gds`` database
  (:class:`_GdsRecord`). They expose ``geo_url`` and a ``suppl/`` directory.
* **SRA records** (``SRX``/``SRR``) live in the Entrez ``sra`` database
  (:class:`_SraRecord`), whose summary carries the experiment/run XML.

Properties that reference other GEO/SRA objects return **instances** of the
relevant class (sharing this record's Entrez client), not bare accession strings.

All classes are gathered in one module because they reference each other
mutually (a Series yields Samples; a Sample points back at its Series); a single
module sidesteps the import cycle that separate files would create.
"""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any, ClassVar

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import _web
from labdata.ncbi.entrez import EntrezClient

# --------------------------------------------------------------------------- #
# small parsing helpers (SRA ExpXml/Runs are loosely structured XML fragments
# returned as escaped text, so we extract attributes by regex rather than re-parse)
# --------------------------------------------------------------------------- #


def _attr(fragment: str, element: str, attribute: str) -> str:
    """Return the first ``element``'s ``attribute`` value in an XML ``fragment``."""
    match = re.search(rf'<{element}\b[^>]*?\b{attribute}="([^"]*)"', fragment)
    return match.group(1) if match else ""


def _attrs_all(fragment: str, element: str, attribute: str) -> list[str]:
    """Return every ``element``'s ``attribute`` value in an XML ``fragment``."""
    return re.findall(rf'<{element}\b[^>]*?\b{attribute}="([^"]*)"', fragment)


def _tag_text(fragment: str, tag: str) -> str:
    """Return the text content of the first ``<tag>...</tag>`` in ``fragment``."""
    match = re.search(rf"<{tag}\b[^>]*>(.*?)</{tag}>", fragment, re.S)
    return match.group(1) if match else ""


def _int_attr(tag: str, attribute: str) -> int | None:
    """Return an integer ``attribute`` from a single element ``tag``, or ``None``."""
    match = re.search(rf'\b{attribute}="(\d+)"', tag)
    return int(match.group(1)) if match else None


def _ftp_bucket(accession: str) -> str:
    """Return the GEO FTP bucket for ``accession`` (e.g. ``GSE131907`` -> ``GSE131nnn``)."""
    match = re.match(r"^([A-Za-z]+)(\d+)$", accession)
    if match is None:
        return accession
    prefix, digits = match.group(1), match.group(2)
    head = digits[:-3] if len(digits) > 3 else ""
    return f"{prefix}{head}nnn"


def _as_gpl(token: str) -> str:
    """Normalize a platform token to ``GPL…`` form."""
    return token.upper() if token.upper().startswith("GPL") else f"GPL{token}"


def _split_ids(raw: str) -> list[str]:
    """Split a summary field holding several ids on whitespace/comma/semicolon."""
    return [tok for tok in re.split(r"[;,\s]+", raw) if tok]


def _experiments_via_sra(client: EntrezClient, gds_uid: str) -> list[str]:
    """Resolve the SRA experiment (``SRX``) accessions linked from a ``gds`` UID."""
    found: dict[str, None] = {}
    for sra_uid in client.elink(dbfrom="gds", db="sra", uid=gds_uid):
        exp_xml = str(client.esummary(db="sra", uid=sra_uid).get("ExpXml", ""))
        accession = _attr(exp_xml, "Experiment", "acc")
        if accession:
            found.setdefault(accession, None)
    return list(found)


# --------------------------------------------------------------------------- #
# base classes
# --------------------------------------------------------------------------- #


class _Record:
    """Base for an accessioned NCBI record: validation, client, identity."""

    #: Pattern an accession must match (set by each concrete subclass).
    _ACCESSION_RE: ClassVar[re.Pattern[str]]
    #: Human-readable record kind, used in error messages.
    _KIND: ClassVar[str]

    def __init__(self, accession: str, *, client: EntrezClient | None = None) -> None:
        if not isinstance(accession, str) or not self._ACCESSION_RE.match(accession):
            raise AccessionError(f"not a GEO/SRA {self._KIND} accession: {accession!r}")
        self.accession = accession
        self._client = client

    def __repr__(self) -> str:
        """Return an unambiguous representation."""
        return f"{type(self).__name__}({self.accession!r})"

    def __eq__(self, other: object) -> bool:
        """Return ``True`` when ``other`` is the same class with the same accession."""
        return isinstance(other, type(self)) and self.accession == other.accession

    def __hash__(self) -> int:
        """Hash by class and accession (consistent with :meth:`__eq__`)."""
        return hash((type(self).__name__, self.accession))

    @property
    def client(self) -> EntrezClient:
        """Return the Entrez client, constructing a default one on first use."""
        if self._client is None:
            self._client = EntrezClient()
        return self._client


class _GdsRecord(_Record):
    """Base for GEO records (``GSE``/``GSM``/``GPL``) backed by the ``gds`` database."""

    #: Entry-type filter that disambiguates the accession within ``gds``.
    _ETYP: ClassVar[str]
    #: GEO FTP subtree for this record kind (``series``/``samples``/``platforms``).
    _FTP_KIND: ClassVar[str]

    @cached_property
    def uid(self) -> str:
        """The Entrez ``gds`` UID for this record.

        Raises
        ------
        EntrezError
            If NCBI returns no matching ``gds`` record.
        """
        term = f"{self.accession}[ACCN] AND {self._ETYP}[ETYP]"
        uids = self.client.esearch(db="gds", term=term)
        if not uids:
            raise EntrezError(f"no GEO {self._KIND} found for {self.accession!r}")
        return uids[0]

    @cached_property
    def _summary(self) -> dict[str, Any]:
        """The raw ``gds`` esummary docsum for this record (fetched once)."""
        return self.client.esummary(db="gds", uid=self.uid)

    @property
    def title(self) -> str:
        """The record title."""
        return str(self._summary.get("title", ""))

    @property
    def organism(self) -> str:
        """The organism (taxon) the record targets."""
        return str(self._summary.get("taxon", ""))

    @property
    def geo_url(self) -> str:
        """The GEO accession-display URL for this record."""
        return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={self.accession}"

    @property
    def supplementary_http_url(self) -> str:
        """The HTTP URL of this record's ``suppl/`` directory (derived, no request)."""
        bucket = _ftp_bucket(self.accession)
        return f"https://ftp.ncbi.nlm.nih.gov/geo/{self._FTP_KIND}/{bucket}/{self.accession}/suppl/"

    @cached_property
    def supplementary_files(self) -> list[str]:
        """The supplementary file names under :attr:`supplementary_http_url`.

        Fetched lazily by listing the ``suppl/`` directory over HTTP. Join a name
        with :attr:`supplementary_http_url` to form its download URL.

        Returns
        -------
        list of str
            File names, or an empty list when the record has no supplementary files.
        """
        return _web.list_directory(self.supplementary_http_url)


class _SraRecord(_Record):
    """Base for SRA records (``SRX``/``SRR``) backed by the ``sra`` database."""

    @cached_property
    def uid(self) -> str:
        """The Entrez ``sra`` UID for this record's experiment.

        Raises
        ------
        EntrezError
            If NCBI returns no matching ``sra`` record.
        """
        uids = self.client.esearch(db="sra", term=self.accession)
        if not uids:
            raise EntrezError(f"no SRA {self._KIND} found for {self.accession!r}")
        return uids[0]

    @cached_property
    def _summary(self) -> dict[str, Any]:
        """The raw ``sra`` esummary docsum for this record (fetched once)."""
        return self.client.esummary(db="sra", uid=self.uid)

    @property
    def _exp_xml(self) -> str:
        return str(self._summary.get("ExpXml", ""))

    @property
    def _runs_xml(self) -> str:
        return str(self._summary.get("Runs", ""))

    @property
    def organism(self) -> str:
        """The organism (scientific name) recorded in SRA."""
        return _attr(self._exp_xml, "Organism", "ScientificName")

    @property
    def instrument_model(self) -> str:
        """The sequencing instrument model recorded in SRA."""
        return _attr(self._exp_xml, "Platform", "instrument_model")

    @property
    def url(self) -> str:
        """The NCBI SRA web URL for this record."""
        return f"https://www.ncbi.nlm.nih.gov/sra/?term={self.accession}"


# --------------------------------------------------------------------------- #
# concrete GEO records
# --------------------------------------------------------------------------- #


class Series(_GdsRecord):
    r"""A GEO Series (``GSE000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        A GEO Series accession, e.g. ``"GSE131907"`` (``^GSE\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed GEO Series accession.
    """

    _ACCESSION_RE = re.compile(r"^GSE\d+$")
    _KIND = "Series"
    _ETYP = "gse"
    _FTP_KIND = "series"

    @property
    def summary(self) -> str:
        """The Series summary/abstract text."""
        return str(self._summary.get("summary", ""))

    @cached_property
    def pubmed_id(self) -> str | None:
        """The PubMed ID of the associated publication, or ``None``."""
        pmids = self._summary.get("PubMedIds") or []
        return str(pmids[0]) if pmids else None

    @cached_property
    def samples(self) -> list[Sample]:
        """The samples (``GSM``) belonging to this Series, as :class:`Sample` instances."""
        return [
            Sample(str(sample["Accession"]), client=self.client)
            for sample in self._summary.get("Samples", [])
        ]

    @cached_property
    def platforms(self) -> list[Platform]:
        """The platforms (``GPL``) this Series uses, as :class:`Platform` instances."""
        raw = str(self._summary.get("GPL", ""))
        return [Platform(_as_gpl(tok), client=self.client) for tok in _split_ids(raw)]

    @cached_property
    def experiments(self) -> list[Experiment]:
        """The SRA experiments (``SRX``) linked to this Series, as :class:`Experiment` instances."""
        accessions = _experiments_via_sra(self.client, self.uid)
        return [Experiment(acc, client=self.client) for acc in accessions]

    @cached_property
    def bioproject_ids(self) -> list[str]:
        """The BioProject accessions (``PRJNA…``) linked to this Series.

        Returns plain strings: BioProject is not part of the GEO object model.
        """
        found: dict[str, None] = {}
        for bp_uid in self.client.elink(dbfrom="gds", db="bioproject", uid=self.uid):
            accession = str(
                self.client.esummary(db="bioproject", uid=bp_uid).get("Project_Acc", "")
            )
            if accession:
                found.setdefault(accession, None)
        return list(found)


class Sample(_GdsRecord):
    r"""A GEO Sample (``GSM000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        A GEO Sample accession, e.g. ``"GSM3827114"`` (``^GSM\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed GEO Sample accession.
    """

    _ACCESSION_RE = re.compile(r"^GSM\d+$")
    _KIND = "Sample"
    _ETYP = "gsm"
    _FTP_KIND = "samples"

    @property
    def series(self) -> Series | None:
        """The parent :class:`Series`, or ``None`` if the summary lists none."""
        number = str(self._summary.get("GSE", "")).strip()
        first = _split_ids(number)
        return Series(f"GSE{first[0]}", client=self.client) if first else None

    @property
    def platform(self) -> Platform | None:
        """The :class:`Platform` this sample was run on, or ``None``."""
        tokens = _split_ids(str(self._summary.get("GPL", "")))
        return Platform(_as_gpl(tokens[0]), client=self.client) if tokens else None

    @cached_property
    def experiments(self) -> list[Experiment]:
        """The SRA experiments (``SRX``) for this sample, as :class:`Experiment` instances."""
        accessions = _experiments_via_sra(self.client, self.uid)
        return [Experiment(acc, client=self.client) for acc in accessions]


class Platform(_GdsRecord):
    r"""A GEO Platform (``GPL000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        A GEO Platform accession, e.g. ``"GPL16791"`` (``^GPL\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed GEO Platform accession.
    """

    _ACCESSION_RE = re.compile(r"^GPL\d+$")
    _KIND = "Platform"
    _ETYP = "gpl"
    _FTP_KIND = "platforms"

    @property
    def sample_count(self) -> int | None:
        """The number of samples GEO has on this platform, or ``None`` if unknown."""
        value = self._summary.get("n_samples")
        return int(value) if value not in (None, "") else None


# --------------------------------------------------------------------------- #
# concrete SRA records
# --------------------------------------------------------------------------- #


class Experiment(_SraRecord):
    r"""An SRA experiment (``SRX000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA experiment accession, e.g. ``"SRX5921017"`` (``^[SED]RX\d+$``,
        covering SRA/ENA/DDBJ).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed experiment accession.
    """

    _ACCESSION_RE = re.compile(r"^[SED]RX\d+$")
    _KIND = "Experiment"

    @property
    def title(self) -> str:
        """The experiment title."""
        return _attr(self._exp_xml, "Experiment", "name") or _tag_text(self._exp_xml, "Title")

    @property
    def study(self) -> str:
        """The parent study accession (``SRP…``), or empty string if absent."""
        return _attr(self._exp_xml, "Study", "acc")

    @cached_property
    def runs(self) -> list[Run]:
        """The sequencing runs (``SRR``) of this experiment, as :class:`Run` instances."""
        return [Run(acc, client=self.client) for acc in _attrs_all(self._runs_xml, "Run", "acc")]


class Run(_SraRecord):
    r"""An SRA run (``SRR000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA run accession, e.g. ``"SRR9000001"`` (``^[SED]RR\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed run accession.
    """

    _ACCESSION_RE = re.compile(r"^[SED]RR\d+$")
    _KIND = "Run"

    @property
    def _run_tag(self) -> str:
        match = re.search(
            rf'<Run\b[^>]*?\bacc="{re.escape(self.accession)}"[^>]*?/?>', self._runs_xml
        )
        return match.group(0) if match else ""

    @property
    def total_spots(self) -> int | None:
        """The number of spots (reads) in this run, or ``None`` if unreported."""
        return _int_attr(self._run_tag, "total_spots")

    @property
    def total_bases(self) -> int | None:
        """The number of bases in this run, or ``None`` if unreported."""
        return _int_attr(self._run_tag, "total_bases")

    @property
    def is_public(self) -> bool:
        """Whether the run's data is publicly accessible."""
        return _attr(self._run_tag, "Run", "is_public") == "true"

    @cached_property
    def experiment(self) -> Experiment:
        """The parent :class:`Experiment` (``SRX``) of this run."""
        return Experiment(_attr(self._exp_xml, "Experiment", "acc"), client=self.client)
