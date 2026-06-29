r"""The GEO object model: Series, Sample, Platform, Experiment, Run, BioProject.

Each class is a lazy handle on one accessioned record. Construction only
validates and stores the accession; properties issue their NCBI request on first
access and cache it (``functools.cached_property``), so building a record is
cheap and network traffic happens only for the fields you read.

Three families share most of the plumbing:

* **GEO records** (``GSE``/``GSM``/``GPL``) live in the Entrez ``gds`` database
  (:class:`_GdsRecord`). They expose a ``url`` and a ``suppl/`` directory.
* **SRA records** (``SRX``/``SRR``) live in the Entrez ``sra`` database
  (:class:`_SraRecord`), whose summary carries the experiment/run XML.
* **BioProject** (``PRJNA``/``PRJEB``/``PRJDB``) lives in the Entrez
  ``bioproject`` database (:class:`BioProject`); it is the umbrella a Series
  links out to.

Properties that reference other GEO/SRA objects return **instances** of the
relevant class (sharing this record's Entrez client), not bare accession strings.

All classes are gathered in one module because they reference each other
mutually (a Series yields Samples; a Sample points back at its Series); a single
module sidesteps the import cycle that separate files would create.
"""

from __future__ import annotations

import io
import re
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self

from labdata.exceptions import AccessionError, EntrezError
from labdata.geo import _web
from labdata.ncbi.entrez import EntrezClient, _chunked, _docsum_uid
from labdata.ncbi.sdl import RemoteFile, SdlClient

if TYPE_CHECKING:
    import pandas

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


def _read_lengths(run_block: str) -> list[int]:
    """Return the average length of each read in a ``<RUN>`` block, in read order.

    Reads the per-read ``<Read index=… average=…/>`` rows of the run's
    ``<Statistics>`` element (the empirical read lengths SRA reports), rounding
    each average to the nearest base and ordering by ``index``. Returns an empty
    list when the block carries no read statistics.
    """
    lengths: list[tuple[int, int]] = []
    for tag in re.findall(r"<Read\b[^>]*?/?>", run_block):
        index = _int_attr(tag, "index")
        average = re.search(r'\baverage="([\d.]+)"', tag)
        if index is not None and average is not None:
            lengths.append((index, round(float(average.group(1)))))
    return [length for _index, length in sorted(lengths)]


def _read_structure(lengths: list[int]) -> str:
    """Render read lengths as a ``{L1}+{L2}+…`` structure string (empty if none)."""
    return "+".join(str(length) for length in lengths)


def _run_blocks(full_xml: str) -> dict[str, str]:
    """Map each run accession in a full SRA XML to its ``<RUN …>`` element.

    Matches both a self-closing ``<RUN …/>`` and a ``<RUN …>…</RUN>`` body; the
    body form is tempered so a self-closing run never lets a match run on into the
    next run's element.
    """
    return {
        match.group(1): match.group(0)
        for match in re.finditer(
            r'<RUN\b[^>]*?\baccession="([SED]RR\d+)"[^>]*?(?:/>|>(?:(?!</?RUN\b).)*?</RUN>)',
            full_xml,
            re.S,
        )
    }


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


def _linked_experiments(client: EntrezClient, uid: str, *, dbfrom: str = "gds") -> list[Experiment]:
    """Resolve the SRA experiments (``SRX``) linked from a ``dbfrom`` UID.

    One ``elink`` plus one batched ``esummary`` for the whole set; each returned
    :class:`Experiment` is seeded with the UID and summary already fetched, so
    reading its fields costs no further network.
    """
    sra_uids = client.elink(dbfrom=dbfrom, db="sra", uid=uid)
    found: dict[str, Experiment] = {}
    for docsum in client.esummary_many(db="sra", uids=sra_uids):
        accession = _attr(str(docsum.get("ExpXml", "")), "Experiment", "acc")
        if accession and accession not in found:
            experiment = Experiment(accession, client=client)
            experiment._seed(uid=_docsum_uid(docsum), summary=dict(docsum))
            found[accession] = experiment
    return list(found.values())


# --------------------------------------------------------------------------- #
# base classes
# --------------------------------------------------------------------------- #


class _Record:
    """Base for an accessioned NCBI record: validation, client, identity."""

    #: Pattern an accession must match (set by each concrete subclass).
    _ACCESSION_RE: ClassVar[re.Pattern[str]]
    #: Human-readable record kind, used in error messages.
    _KIND: ClassVar[str]

    def __init__(
        self,
        accession: str,
        *,
        client: EntrezClient | None = None,
        sdl_client: SdlClient | None = None,
    ) -> None:
        accession = accession.strip().upper()
        if not isinstance(accession, str) or not self._ACCESSION_RE.match(accession):
            raise AccessionError(f"not a GEO/SRA {self._KIND} accession: {accession!r}")
        self.accession = accession
        self._client = client
        self._sdl_client = sdl_client

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

    @property
    def sdl_client(self) -> SdlClient:
        """Return the SRA Data Locator client, constructing a default one on first use."""
        if self._sdl_client is None:
            self._sdl_client = SdlClient()
        return self._sdl_client

    def _seed(self, *, uid: str | None = None, summary: dict[str, Any] | None = None) -> Self:
        """Pre-populate the ``uid``/``_summary`` caches from an already-fetched response.

        Writes straight into the instance ``__dict__`` under the ``cached_property``
        names, so the seeded values are served without a request. Used when a
        batched lookup has already resolved a linked record's identity and summary.
        """
        if uid is not None:
            self.__dict__["uid"] = uid
        if summary is not None:
            self.__dict__["_summary"] = summary
        return self


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

    def download(
        self,
        output_dir: str | Path = ".",
        n_parallel: int = 1,
        *,
        max_size: str | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        keep_sra: bool = False,
        verbose: bool = True,
    ) -> dict[str, bool]:
        """Download FASTQ for every run of every SRA experiment in this Series.

        Lays the data out as ``<output_dir>/<GSE>/<SRX>/<SRR>*.fastq.gz`` — one
        directory per experiment under a directory named for this Series — via
        sra-tools (``prefetch`` + ``fasterq-dump``). Runs already marked done (a
        ``.<SRR>.success`` flag) are skipped; the whole Series is downloaded in
        parallel at the run (``SRR``) level. The resumable ``prefetch`` step is
        retried on failure, so this is safe to rerun on an unstable connection.

        Parameters
        ----------
        output_dir : str or Path, default "."
            Parent directory; a ``<output_dir>/<GSE>`` subtree is created for this Series.
        n_parallel : int, default 1
            Maximum runs to download concurrently across the whole Series. On a
            flaky link, fewer concurrent connections (``1`` or ``2``) is often
            more reliable.
        max_size : str, optional
            Passed to ``prefetch --max-size`` (defaults to
            :data:`~labdata.geo.sratools.DEFAULT_MAX_SIZE`); raise it for runs that
            exceed ``prefetch``'s 20G default.
        retries : int, optional
            Attempts for the resumable ``prefetch`` network step per run (defaults
            to :data:`~labdata.geo.sratools.DEFAULT_RETRIES`).
        backoff : float, optional
            Base seconds for the linear backoff between ``prefetch`` retries
            (defaults to :data:`~labdata.geo.sratools.DEFAULT_BACKOFF`).
        keep_sra : bool, default False
            Keep each run's downloaded ``.sra`` next to its FASTQ (as
            ``<GSE>/<SRX>/<SRR>.sra``) instead of deleting it after extraction.
        verbose : bool, default True
            Print a download plan up front and one line per finished run to
            ``stderr``. Set ``False`` to download silently.

        Returns
        -------
        dict[str, bool]
            Maps each run accession to whether its data is present on completion.

        Examples
        --------
        >>> Series("GSE229022").download("./fastq", n_parallel=4)  # doctest: +SKIP
        {'SRR24084454': True, 'SRR24084455': True, ...}
        """
        from labdata.geo import sratools

        kwargs = {
            "max_size": max_size,
            "retries": retries,
            "backoff": backoff,
        }
        overrides = {key: value for key, value in kwargs.items() if value is not None}

        base = Path(output_dir) / self.accession
        tasks: list[tuple[Run, Path]] = []
        for experiment in self.experiments:
            srx_dir = base / experiment.accession
            srx_dir.mkdir(parents=True, exist_ok=True)
            tasks.extend((run, srx_dir) for run in experiment.runs)
        return sratools._run_plan(
            self.accession,
            tasks,
            n_parallel,
            output_root=base,
            verbose=verbose,
            keep_sra=keep_sra,
            **overrides,
        )


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
        """The sequencing runs (``SRR``) of this experiment, as :class:`Run` instances.

        A run resolves to the same ``sra`` record as its experiment, so each
        :class:`Run` is seeded with this experiment's summary (no extra fetch).
        """
        return [
            Run(acc, client=self.client)._seed(uid=self.uid, summary=self._summary)
            for acc in _attrs_all(self._runs_xml, "Run", "acc")
        ]


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
    def _full_xml(self) -> str:
        """The full SRA XML for this run's experiment package (fetched once).

        The esummary ``Runs`` fragment carries spot/base totals but not the
        per-read layout, so the read structure is read from the full record.
        """
        return self.client.efetch(db="sra", ids=[self.uid], rettype="full", retmode="xml")

    @property
    def _statistics_block(self) -> str:
        """Return this run's ``<RUN>…</RUN>`` block within :attr:`_full_xml`."""
        return _run_blocks(self._full_xml).get(self.accession, "")

    @property
    def read_lengths(self) -> list[int]:
        """The average length of each read (mate) sequenced per spot, in read order.

        Reads SRA's per-run ``<Statistics>`` from the full record (one EFetch,
        cached). A paired run yields two lengths, e.g. ``[28, 94]``; a single-end
        run yields one. Empty when SRA reports no read statistics.
        """
        return _read_lengths(self._statistics_block)

    @property
    def n_reads(self) -> int | None:
        """The number of reads (mates) sequenced per spot, or ``None`` if unreported."""
        nreads = _int_attr(self._statistics_block, "nreads")
        if nreads is not None:
            return nreads
        return len(self.read_lengths) or None

    @property
    def read_structure(self) -> str:
        """The run's read structure as ``{L1}+{L2}+…`` (e.g. ``"28+94"``), or ``""``.

        A compact rendering of :attr:`read_lengths`: each read's rounded average
        length joined with ``+`` in read order. Empty when the lengths are unknown.
        """
        return _read_structure(self.read_lengths)

    @cached_property
    def experiment(self) -> Experiment:
        """The parent :class:`Experiment` (``SRX``) of this run.

        Shares this run's ``sra`` record, so the experiment is seeded from the
        same summary (no extra fetch).
        """
        accession = _attr(self._exp_xml, "Experiment", "acc")
        return Experiment(accession, client=self.client)._seed(uid=self.uid, summary=self._summary)

    @cached_property
    def files(self) -> list[RemoteFile]:
        """Every file the SRA Data Locator lists for this run (one SDL request, cached).

        Includes both the normalized SRA archive file(s) and any original/submitted
        files (e.g. a 10X Genomics ``possorted_genome_bam``). This is the data behind
        the Run Browser's "Data access" panel.

        Returns
        -------
        list of RemoteFile
            All files SDL exposes for this run, each with its download locations.

        Raises
        ------
        SdlError
            If the SDL request fails or the run is unknown/non-public.

        Examples
        --------
        >>> [(f.name, f.type) for f in Run("SRR20172067").files]  # doctest: +SKIP
        [('possorted_genome_bam_TC2_d15_1.bam', 'TenX'), ('SRR20172067', 'sra')]
        """
        return self.sdl_client.retrieve(self.accession)

    @property
    def original_files(self) -> list[RemoteFile]:
        """The original/submitted files for this run (the "Original format" listing).

        The submitter's own uploads — e.g. the 10X Genomics BAM behind a run whose
        ``.sra`` only stored a single read — as opposed to the normalized SRA archive.
        Filters :attr:`files` to the non-archive types.

        Returns
        -------
        list of RemoteFile
            Files whose type is not a normalized SRA archive type; empty when the
            submission has no original-format files.

        Examples
        --------
        >>> [f.name for f in Run("SRR20172067").original_files]  # doctest: +SKIP
        ['possorted_genome_bam_TC2_d15_1.bam']
        """
        return [file for file in self.files if not file.is_sra_archive]


# --------------------------------------------------------------------------- #
# BioProject (the umbrella a Series links out to)
# --------------------------------------------------------------------------- #


class BioProject(_Record):
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
