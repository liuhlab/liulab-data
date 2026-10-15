r"""The SRA object model: Experiment and Run.

Both are read from the full ``EXPERIMENT_PACKAGE`` XML that ``efetch db=sra
rettype=full`` returns. :func:`experiments_from_package_xml` is the one parser: the
live path fetches packages and hands them to it, and so can a caller holding stored
packages. Each class is a lazy handle on one accessioned record: construction only
validates the accession, and a handle that was not built from a package fetches its
package on first property access.

Properties that reference other SRA objects (``Run.experiment``,
``Experiment.runs``) return **instances** sharing this record's clients, not bare
accession strings.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from functools import cached_property

from labdata.exceptions import EntrezError, LabdataError
from labdata.geo._base import _read_structure, _Record
from labdata.ncbi.entrez import EntrezClient
from labdata.ncbi.sdl import RemoteFile, SdlClient

#: Ids per package EFetch.
_PACKAGE_CHUNK = 100
#: Fresh requests for the ids a package EFetch left out, before giving up.
_PACKAGE_RETRIES = 2

_XML_DECLARATION = re.compile(r"<\?xml[^>]*\?>")
_PACKAGE = re.compile(r"<EXPERIMENT_PACKAGE\b[^>]*>.*?</EXPERIMENT_PACKAGE>", re.S)
_EXPERIMENT_ACC = re.compile(r'<EXPERIMENT\b[^>]*?\baccession="([^"]+)"')
_EMPTY = ET.Element("_")


def experiments_from_package_xml(
    xml: str,
    *,
    client: EntrezClient | None = None,
    sdl_client: SdlClient | None = None,
) -> list[Experiment]:
    """Build experiments, with their runs, from SRA ``EXPERIMENT_PACKAGE`` XML.

    Every property of the returned experiments and their runs is read from ``xml``,
    except ``Run.files`` and ``Run.original_files``, which ask the SRA Data Locator.

    Parameters
    ----------
    xml : str
        One ``EXPERIMENT_PACKAGE``, several concatenated, or an
        ``EXPERIMENT_PACKAGE_SET`` (the ``efetch db=sra rettype=full`` response).
    client : EntrezClient or None
        Entrez client the objects hold; reading their properties does not use it.
    sdl_client : SdlClient or None
        SRA Data Locator client the runs hold for ``files``.

    Returns
    -------
    list of Experiment
        One experiment per distinct accession, in document order. Its ``uid`` is
        ``None``: package XML does not carry the Entrez UID.

    Raises
    ------
    LabdataError
        If ``xml`` is not well-formed XML.
    """
    return [
        _experiment_from_package(package, client, sdl_client) for package in _packages(xml).values()
    ]


def _packages(xml: str) -> dict[str, ET.Element]:
    """Map each experiment accession in ``xml`` to its first ``EXPERIMENT_PACKAGE``."""
    body = _XML_DECLARATION.sub("", xml)
    try:
        root = ET.fromstring(f"<_packages>{body}</_packages>")
    except ET.ParseError as err:
        raise LabdataError(f"not SRA package XML: {err}") from err
    found: dict[str, ET.Element] = {}
    for package in root.iter("EXPERIMENT_PACKAGE"):
        accession = package.find("EXPERIMENT")
        if accession is not None and accession.get("accession"):
            found.setdefault(str(accession.get("accession")), package)
    return found


def _experiment_from_package(
    package: ET.Element, client: EntrezClient | None, sdl_client: SdlClient | None
) -> Experiment:
    accession = package.find("EXPERIMENT")
    assert accession is not None
    experiment = Experiment(str(accession.get("accession")), client=client, sdl_client=sdl_client)
    experiment.__dict__.update(uid=None, _package=package)
    return experiment


def _missing(ids: list[str], packages: dict[str, ET.Element]) -> list[str]:
    """Return the requested ``ids`` the fetched ``packages`` do not account for.

    An accession is accounted for when it names an experiment or one of its runs.
    A UID names nothing in the package, so UIDs are checked by count, all or none.
    """
    if all(i.isdigit() for i in ids):
        return [] if len(packages) >= len(ids) else ids
    named = set(packages)
    for package in packages.values():
        named.update(str(run.get("accession")) for run in package.iter("RUN"))
    return [i for i in ids if i not in named]


def split_packages(xml: str) -> dict[str, str]:
    """Map each experiment accession in an ``efetch db=sra`` reply to its package text.

    Each value is the ``EXPERIMENT_PACKAGE`` element exactly as served; the first
    package of an accession wins.
    """
    out: dict[str, str] = {}
    for package in _PACKAGE.findall(xml):
        match = _EXPERIMENT_ACC.search(package)
        if match:
            out.setdefault(match[1], package)
    return out


def fetch_package_texts(client: EntrezClient, ids: list[str]) -> dict[str, str]:
    """Fetch the package text for SRA UIDs or accessions, checking every id came back.

    A batched EFetch can leave packages out with no error, so the ids left out are
    requested again, fresh, before this gives up.

    Parameters
    ----------
    client : EntrezClient
        The Entrez seam.
    ids : list of str
        SRA UIDs, or experiment or run accessions.

    Returns
    -------
    dict of str to str
        Each experiment accession mapped to its ``EXPERIMENT_PACKAGE`` text as served.

    Raises
    ------
    EntrezError
        If some ids still have no package after the retries.
    """
    found: dict[str, str] = {}
    for start in range(0, len(ids), _PACKAGE_CHUNK):
        chunk = [str(i) for i in ids[start : start + _PACKAGE_CHUNK]]
        texts: dict[str, str] = {}
        parsed: dict[str, ET.Element] = {}
        missing = chunk
        for attempt in range(_PACKAGE_RETRIES + 1):
            reply = client.efetch(
                db="sra", ids=missing, rettype="full", retmode="xml", refresh=attempt > 0
            )
            for accession, text in split_packages(reply).items():
                if accession not in texts:
                    texts[accession] = text
                    parsed[accession] = _parse_package(text)
            missing = _missing(chunk, parsed)
            if not missing:
                break
        else:
            raise EntrezError(
                f"SRA returned {len(texts)} experiment packages for {len(chunk)} "
                f"requested ids; no package for: {', '.join(missing)}"
            )
        for accession, text in texts.items():
            found.setdefault(accession, text)
    return found


def _parse_package(text: str) -> ET.Element:
    try:
        return ET.fromstring(text)
    except ET.ParseError as err:
        raise LabdataError(f"not SRA package XML: {err}") from err


def _fetch_packages(client: EntrezClient, ids: list[str]) -> dict[str, ET.Element]:
    """Fetch and parse the packages for SRA UIDs or accessions; see :func:`fetch_package_texts`."""
    return {a: _parse_package(t) for a, t in fetch_package_texts(client, ids).items()}


def _experiments_from_ids(client: EntrezClient, ids: list[str]) -> list[Experiment]:
    """Build the de-duplicated experiments for SRA UIDs or accessions, batched."""
    return [
        _experiment_from_package(package, client, None)
        for package in _fetch_packages(client, ids).values()
    ]


def _linked_experiments(client: EntrezClient, uid: str, *, dbfrom: str = "gds") -> list[Experiment]:
    """Resolve the SRA experiments (``SRX``) linked from a ``dbfrom`` UID.

    One ``elink`` (to reach the linked ``sra`` UIDs, traversing GEO SuperSeries and
    BioProject umbrellas transitively) plus the batched package fetch.
    """
    return _experiments_from_ids(client, client.elink(dbfrom=dbfrom, db="sra", uid=uid))


def _text(element: ET.Element, path: str) -> str:
    found = element.find(path)
    return (found.text or "") if found is not None else ""


class _SraRecord(_Record):
    """Base for SRA records (``SRX``/``SRR``), read from their experiment package."""

    @cached_property
    def uid(self) -> str | None:
        """The Entrez ``sra`` UID of this record's experiment.

        ``None`` on an object built from package XML, which does not carry it.

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
    def _package(self) -> ET.Element:
        """The ``EXPERIMENT_PACKAGE`` holding this record (fetched once)."""
        return next(iter(_fetch_packages(self.client, [self.accession]).values()))

    @property
    def organism(self) -> str:
        """The organism (scientific name) recorded in SRA."""
        scientific = _text(self._package, "SAMPLE/SAMPLE_NAME/SCIENTIFIC_NAME")
        member = self._package.find("Pool/Member")
        return scientific or (str(member.get("organism", "")) if member is not None else "")

    @property
    def instrument_model(self) -> str:
        """The sequencing instrument model recorded in SRA."""
        return _text(self._package, "EXPERIMENT/PLATFORM/*/INSTRUMENT_MODEL")

    @property
    def url(self) -> str:
        """The NCBI SRA web URL for this record."""
        return f"https://www.ncbi.nlm.nih.gov/sra/?term={self.accession}"


class Experiment(_SraRecord):
    r"""An SRA experiment (``SRX000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA experiment accession, e.g. ``"SRX5921017"`` (``^[SED]RX\d+$``,
        covering SRA/ENA/DDBJ).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.
    sdl_client : SdlClient or None
        SRA Data Locator client its runs use for ``files``.

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
        return _text(self._package, "EXPERIMENT/TITLE")

    @property
    def study(self) -> str:
        """The parent study accession (``SRP…``), or empty string if absent."""
        ref = self._package.find("EXPERIMENT/STUDY_REF")
        return str(ref.get("accession", "")) if ref is not None else ""

    @cached_property
    def runs(self) -> list[Run]:
        """The sequencing runs (``SRR``) of this experiment, as :class:`Run` instances.

        Each run is read from this experiment's package (no extra fetch).
        """
        runs = []
        for element in self._package.iterfind("RUN_SET/RUN"):
            run = Run(
                str(element.get("accession")), client=self._client, sdl_client=self._sdl_client
            )
            run.__dict__.update(uid=None, _package=self._package, experiment=self)
            runs.append(run)
        return runs


class Run(_SraRecord):
    r"""An SRA run (``SRR000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA run accession, e.g. ``"SRR9000001"`` (``^[SED]RR\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.
    sdl_client : SdlClient or None
        SRA Data Locator client to use for ``files``.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed run accession.
    """

    _ACCESSION_RE = re.compile(r"^[SED]RR\d+$")
    _KIND = "Run"

    @property
    def _run(self) -> ET.Element:
        """The ``<RUN>`` element of this run in its package (empty when absent)."""
        for element in self._package.iterfind("RUN_SET/RUN"):
            if element.get("accession") == self.accession:
                return element
        return _EMPTY

    def _int(self, attribute: str) -> int | None:
        value = self._run.get(attribute, "")
        return int(value) if value.isdigit() else None

    @property
    def total_spots(self) -> int | None:
        """The number of spots (reads) in this run, or ``None`` if unreported."""
        return self._int("total_spots")

    @property
    def total_bases(self) -> int | None:
        """The number of bases in this run, or ``None`` if unreported."""
        return self._int("total_bases")

    @property
    def is_public(self) -> bool:
        """Whether the run's data is publicly accessible."""
        return self._run.get("is_public") == "true"

    @property
    def _statistics(self) -> ET.Element:
        found = self._run.find("Statistics")
        return found if found is not None else _EMPTY

    @property
    def read_lengths(self) -> list[int]:
        """The average length of each read (mate) sequenced per spot, in read order.

        Reads SRA's per-run ``<Statistics>``. A paired run yields two lengths, e.g.
        ``[28, 94]``; a single-end run yields one. Empty when SRA reports no read
        statistics.
        """
        reads = [
            (int(read.get("index", "0")), round(float(read.get("average", "0"))))
            for read in self._statistics.iterfind("Read")
            if read.get("index") is not None and read.get("average") is not None
        ]
        return [length for _index, length in sorted(reads)]

    @property
    def n_reads(self) -> int | None:
        """The number of reads (mates) sequenced per spot, or ``None`` if unreported."""
        nreads = self._statistics.get("nreads", "")
        if nreads.isdigit():
            return int(nreads)
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
        """The parent :class:`Experiment` (``SRX``) of this run, from the same package."""
        return _experiment_from_package(self._package, self._client, self._sdl_client)

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
