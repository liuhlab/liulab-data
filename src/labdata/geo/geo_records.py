r"""The GEO object model: Series, Sample, Platform.

GEO records (``GSE``/``GSM``/``GPL``) live in the Entrez ``gds`` database
(:class:`_GdsRecord`); they expose a ``url`` and a ``suppl/`` directory. Each class
is a lazy handle on one accessioned record: construction only validates and stores
the accession; properties issue their NCBI request on first access and cache it
(``functools.cached_property``), so building a record is cheap and network traffic
happens only for the fields you read.

Properties that reference other GEO/SRA objects return **instances** of the
relevant class (sharing this record's Entrez client), not bare accession strings —
a Series yields :class:`~labdata.geo.sra_records.Experiment` and
:class:`~labdata.geo.bio_project_records.BioProject` instances; a Sample points
back at its :class:`Series`.
"""

from __future__ import annotations

import io
import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from labdata.exceptions import EntrezError
from labdata.geo import _web
from labdata.geo._base import (
    _as_gpl,
    _ftp_bucket,
    _read_lengths,
    _read_structure,
    _Record,
    _run_blocks,
    _split_ids,
)
from labdata.geo.sra_records import Experiment, _linked_experiments
from labdata.geo.sratools import _SraDownloadMixin
from labdata.ncbi.entrez import _chunked, _docsum_uid

if TYPE_CHECKING:
    import pandas

    from labdata.geo.bio_project_records import BioProject

#: UIDs per batched EFetch request (keeps the GET URL within limits).
_RUNINFO_CHUNK = 300

#: Rename SRA ``runinfo`` columns to the SRA-Run-Selector-style names callers expect.
_RUNINFO_RENAME = {"avgLength": "AvgSpotLen", "bases": "Bases", "Model": "Instrument"}

#: Preferred leading column order for the SRA run table (remaining columns follow).
_RUN_TABLE_COLUMNS = [
    "Run",
    "BioSample",
    "AvgSpotLen",
    "Bases",
    "ReadStructure",
    "size_MB",
    "Experiment",
    "Instrument",
    "LibraryName",
    "Sample",
    "SampleName",
    "BioProject",
    "SRAStudy",
    "Platform",
    "LibraryStrategy",
    "LibrarySource",
    "LibrarySelection",
    "LibraryLayout",
    "ScientificName",
    "TaxID",
]


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
    def url(self) -> str:
        """The GEO accession-display web URL for this record."""
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

    @property
    def supplementary_file_urls(self) -> list[str]:
        """Full download URLs for this record's supplementary files.

        Each entry is :attr:`supplementary_http_url` joined with a name from
        :attr:`supplementary_files` (resolved lazily on first access).

        Returns
        -------
        list of str
            Direct HTTP URLs, or an empty list when there are no supplementary files.
        """
        base = self.supplementary_http_url
        return [base + name for name in self.supplementary_files]


class Series(_SraDownloadMixin, _GdsRecord):
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
        # Biopython yields PubMedIds as IntegerElement, whose str() is a verbose repr;
        # go through int() so we get the bare numeric id.
        return str(int(pmids[0])) if pmids else None

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
        return _linked_experiments(self.client, self.uid)

    @cached_property
    def bioprojects(self) -> list[BioProject]:
        """The BioProjects (``PRJNA…``) linked to this Series, as :class:`BioProject` instances.

        One ``elink`` plus one batched ``esummary``; each :class:`BioProject` is
        seeded with the UID and summary already fetched.
        """
        from labdata.geo.bio_project_records import BioProject

        bp_uids = self.client.elink(dbfrom="gds", db="bioproject", uid=self.uid)
        found: dict[str, BioProject] = {}
        for docsum in self.client.esummary_many(db="bioproject", uids=bp_uids):
            accession = str(docsum.get("Project_Acc", ""))
            if accession and accession not in found:
                bioproject = BioProject(accession, client=self.client)
                bioproject._seed(uid=_docsum_uid(docsum), summary=dict(docsum))
                found[accession] = bioproject
        return list(found.values())

    def make_sra_run_table(self) -> pandas.DataFrame:
        """Build a tidy, run-level (``SRR``) table for this Series, like SRA Run Selector.

        Resolves the Series' SRA experiments and fetches NCBI's ``runinfo`` report
        in one batched EFetch per chunk, returning one row per sequencing run. The
        columns are SRA's ``runinfo`` fields (``Run``, ``BioSample``, ``Experiment``,
        ``LibraryName``, ``Platform``, …) with a few renamed to the Run-Selector
        names (``AvgSpotLen``, ``Bases``, ``Instrument``). The reported size is
        ``size_MB`` — E-utilities ``runinfo`` does not expose an exact byte count.

        A ``ReadStructure`` column (``{L1}+{L2}+…``, e.g. ``"28+94"``) is added
        from each run's per-read ``<Statistics>``, read from the full SRA XML with
        a second batched EFetch per chunk (see :attr:`Run.read_structure`); it is
        empty for runs SRA reports no read statistics for.

        Returns
        -------
        pandas.DataFrame
            One row per run, key columns ordered first. Empty when the Series has
            no linked SRA runs.

        Examples
        --------
        >>> table = Series("GSE229022").make_sra_run_table()  # doctest: +SKIP
        >>> table[["Run", "BioSample", "Instrument"]].head()  # doctest: +SKIP
        """
        import pandas

        sra_uids = self.client.elink(dbfrom="gds", db="sra", uid=self.uid)
        frames: list[pandas.DataFrame] = []
        structures: dict[str, str] = {}
        for chunk in _chunked(sra_uids, _RUNINFO_CHUNK):
            text = self.client.efetch(db="sra", ids=chunk, rettype="runinfo", retmode="text")
            if text.strip():
                frames.append(pandas.read_csv(io.StringIO(text)))
            xml = self.client.efetch(db="sra", ids=chunk, rettype="full", retmode="xml")
            for accession, block in _run_blocks(xml).items():
                structures[accession] = _read_structure(_read_lengths(block))
        if not frames:
            return pandas.DataFrame()
        table = pandas.concat(frames, ignore_index=True).rename(columns=_RUNINFO_RENAME)
        table["ReadStructure"] = [structures.get(run, "") for run in table["Run"]]
        leading = [column for column in _RUN_TABLE_COLUMNS if column in table.columns]
        remaining = [column for column in table.columns if column not in leading]
        return table.reindex(columns=leading + remaining)


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
        return _linked_experiments(self.client, self.uid)


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
